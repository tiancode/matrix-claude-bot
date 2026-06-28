#!/usr/bin/env python3
"""冒烟自检：不连真实 Matrix，用假 client 跑通「启动 → 收消息 → 派活 → 回复」整条链路，
并校验上下文/回复识别/会话/安全等关键行为。

用法（项目根目录）：
    .venv/bin/python tests/smoke.py

全部通过退出码 0；任一失败退出码 1。改完代码跑一遍能挡住明显回归。
"""
import asyncio
import json
import os
import sys
import time
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bot                                  # noqa: E402
import claude_runner                        # noqa: E402
import projects                             # noqa: E402
from config import settings, redact         # noqa: E402
from nio import RoomMessageText             # noqa: E402


# ---------- 通用假对象 ----------
class FakeRoom:
    def __init__(self, room_id, n_users):
        self.room_id = room_id
        self.users = {f"@u{i}:ex.org": 1 for i in range(n_users)}

    def user_name(self, uid):
        return {"@alice:ex.org": "Alice", "@claudebot:ex.org": "claude-bot"}.get(uid, uid)


def make_event(body, sender="@alice:ex.org", event_id="$in1", in_reply_to=None,
               mentions=None, formatted_body=None):
    content = {}
    if in_reply_to:
        content["m.relates_to"] = {"m.in_reply_to": {"event_id": in_reply_to}}
    if mentions:
        content["m.mentions"] = {"user_ids": mentions}
    if formatted_body:
        content["formatted_body"] = formatted_body
    return types.SimpleNamespace(
        body=body, sender=sender, event_id=event_id,
        server_timestamp=int(time.time() * 1000), source={"content": content})


def make_media_event(body="app.log", filename=None, mxc="mxc://ex.org/abc",
                     sender="@alice:ex.org", event_id="$m1", size=None, mentions=None):
    content = {"url": mxc, "body": body, "msgtype": "m.file"}
    if filename:
        content["filename"] = filename
    if size is not None:
        content["info"] = {"size": size}
    if mentions:
        content["m.mentions"] = {"user_ids": mentions}
    return types.SimpleNamespace(
        body=body, sender=sender, event_id=event_id, url=mxc,
        server_timestamp=int(time.time() * 1000), source={"content": content})


def set_identity():
    bot.MY_ID, bot.MY_NAME, bot.MY_LOCAL = "@claudebot:ex.org", "claude-bot", "claudebot"


# ---------- 1) 启动 + 一条群任务的完整链路 ----------
def test_startup_and_task_flow():
    sent = []
    captured = {}

    class FakeClient:
        def __init__(self, *a, **k):
            self.user_id = "@claudebot:ex.org"
            self.device_id, self.access_token = "DEV", "tok"
            self.rooms, self._cbs = {}, []

        def add_event_callback(self, cb, ev_type):
            self._cbs.append((cb, ev_type))

        async def get_displayname(self, uid):
            return types.SimpleNamespace(displayname="claude-bot")

        async def sync(self, **k):
            return None

        async def room_typing(self, *a, **k):
            return None

        async def room_send(self, rid, mt, content, **k):
            sent.append((rid, content["body"]))
            return types.SimpleNamespace(event_id="$e%d" % len(sent))

        async def join(self, rid):
            return None

        async def sync_forever(self, **k):
            room = FakeRoom("!g:ex.org", 3)
            self.rooms[room.room_id] = room
            prior = make_event("我们昨天聊到登录会掉线", event_id="$prior")  # 先垫一条背景
            ev = make_event("@claude-bot 修一下登录 token 自动刷新",
                            mentions=["@claudebot:ex.org"])
            for cb, ev_type in self._cbs:
                if ev_type is RoomMessageText:
                    await cb(room, prior)
                    await cb(room, ev)
            for _ in range(50):
                pending = [t for t in bot._tasks if not t.done()]
                if not pending:
                    break
                await asyncio.gather(*pending, return_exceptions=True)

    async def fake_ask(key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None):
        captured["prompt"], captured["key"], captured["lock_key"] = prompt, key, lock_key
        return "已改好并提了 PR：https://gitea.example.com/team/app/pulls/7"

    async def fake_login():
        return None

    rec = {"id": "gitea.example.com/team/app", "owner": "team", "repo": "app",
           "path": "/tmp", "base": "main", "host": "https://gitea.example.com"}
    async def fake_ensure(info):   # checkout 视作已在，直接返回（群分派会过 ensure 校验）
        return rec

    bot.AsyncClient = FakeClient
    bot._login = fake_login
    bot.projects.get_room = lambda rid: rec
    bot.projects.ensure_project = fake_ensure
    bot.runner.ask = fake_ask

    orig_allow = settings.allow_users
    orig_pf = settings.pr_followup_enabled               # 关掉 PR 跟进守护循环，否则它会 sleep 住任务回收
    settings.allow_users = ["@alice:ex.org"]            # fail-closed：须显式授权派活人
    settings.pr_followup_enabled = False
    bot._context["!g:ex.org"].clear()
    try:
        asyncio.run(bot.main())
    finally:
        settings.allow_users = orig_allow
        settings.pr_followup_enabled = orig_pf

    assert bot.MY_ID == "@claudebot:ex.org" and bot.MY_NAME == "claude-bot"
    assert captured["key"] == "gitea.example.com/team/app|!g:ex.org"    # 会话按项目+房间（不串台）
    assert captured["lock_key"] == "gitea.example.com/team/app"         # checkout 仍按项目串行
    assert sent and "PR" in sent[0][1]                                  # 答复发回房间
    assert "【当前要你处理的任务】" in captured["prompt"]               # 任务带上下文区块
    assert "Alice" in captured["prompt"] and "[" in captured["prompt"]  # 带时间戳的群上下文
    assert "修一下登录 token" not in captured["prompt"].split("【当前要你处理的任务】")[0]  # 当前任务不在背景里重复
    assert any(s == "claude-bot" for _, s, _ in bot._context["!g:ex.org"])  # bot 回复入上下文


# ---------- 2) 认 reply / 点名 ----------
def test_reply_addressing():
    set_identity()
    bot._sent_events.append("$botmsg1")
    room = FakeRoom("!g:ex.org", 3)

    a, t = bot._is_addressed(room, make_event(
        "> <@claudebot:ex.org> 旧消息\n\n再处理下边界", in_reply_to="$botmsg1"))
    assert a and t == "再处理下边界"                                   # 回复 bot → 点名 + 去引用

    a2, _ = bot._is_addressed(room, make_event(
        "> <@bob:ex.org> 闲聊\n\n哈哈", in_reply_to="$notbot"))
    assert a2 is False                                                 # 回复别人 → 不触发

    fb = ('<mx-reply><blockquote><a href="x">In reply to</a> '
          '<a href="y">@claudebot:ex.org</a><br>旧话</blockquote></mx-reply>看天气')
    a3, _ = bot._is_addressed(room, make_event(
        "> <@claudebot:ex.org> 旧话\n\n看天气", in_reply_to="$notbot", formatted_body=fb))
    assert a3 is False                                                 # 引用块含 bot id → 不误触发

    a4, t4 = bot._is_addressed(room, make_event(
        "@claude-bot 看下 CI", mentions=["@claudebot:ex.org"]))
    assert a4 and t4 == "看下 CI"                                      # 普通 @pill 仍有效


# ---------- 3) 引用回退块剥离（不误伤用户自己的 >） ----------
def test_reply_fallback_strip():
    c = {"m.relates_to": {"m.in_reply_to": {"event_id": "$x"}}}
    assert bot._strip_reply_fallback("> <@bot:ex.org> 旧\n\n新问题", c) == "新问题"
    assert bot._strip_reply_fallback("> 我引用一句\n继续", c) == "> 我引用一句\n继续"


# ---------- 4) track 门控 + 上下文时间戳单调 ----------
def test_track_and_monotonic():
    set_identity()
    sent = []

    class FC:
        async def room_send(self, rid, mt, content, **k):
            sent.append(content["body"])
            return types.SimpleNamespace(event_id="$e%d" % len(sent))

    bot.client = FC()
    rid = "!r:ex.org"
    bot._context[rid].clear()
    bot._context[rid].append((time.time() + 5, "Alice", "更晚的消息"))  # 制造钟差

    asyncio.run(bot.send(rid, "状态：⏳ 绑定中"))             # 默认不 track
    asyncio.run(bot.send(rid, "真正的答复", track=True))      # track

    bodies = [b for _, _, b in bot._context[rid]]
    assert "状态：⏳ 绑定中" not in bodies                     # 状态消息不进上下文
    assert "真正的答复" in bodies                             # 答复进上下文
    ts = [t for t, _, _ in bot._context[rid]]
    assert ts == sorted(ts)                                   # 时间单调，不倒挂


# ---------- 5) 自己账号：手打入上下文但不派活 ----------
def test_own_account_context():
    set_identity()
    bot._synced = True                                        # 初始 sync 之后才处理消息
    room = FakeRoom("!g:ex.org", 3)
    bot._context[room.room_id].clear()
    orig_trigger, orig_spawn = settings.trigger_phrase, bot._spawn
    settings.trigger_phrase = ""
    spawned = []
    bot._spawn = lambda coro: (spawned.append(1), coro.close())
    try:
        asyncio.run(bot.on_message(
            room, make_event("自言自语", sender="@claudebot:ex.org", event_id="$mine")))
    finally:
        bot._spawn, settings.trigger_phrase = orig_spawn, orig_trigger
    assert len(bot._context[room.room_id]) == 1               # 进了上下文
    assert not spawned                                        # 没派活


# ---------- 6) TTL 过期提示（claude_runner） ----------
def test_ttl_notice():
    async def run():
        r = claude_runner.ClaudeRunner()

        async def fake_run(cmd, cwd=None):
            return 0, json.dumps({"result": "ok", "session_id": "s",
                                  "is_error": False}).encode(), b""

        r._run = fake_run
        r._sessions["k"] = ("old", time.time() - 10 ** 9)     # 注入早已过期的会话
        return await r.ask("k", "hi"), await r.ask("k", "hi2")

    o1, o2 = asyncio.run(run())
    assert o1.startswith("（距上次较久")                       # 过期首条提示
    assert not o2.startswith("（距上次")                       # 紧接着不再提示


# ---------- 7) 安全：token 只注入受信主机 + 出口 redact ----------
def test_security_bits():
    orig_host, orig_tok = settings.gitea_host, settings.gitea_token
    settings.gitea_host, settings.gitea_token = "https://gitea.example.com", "secrettok123"
    try:
        assert "secrettok123@gitea.example.com" in projects._auth_url(
            "https://gitea.example.com/o/r.git")               # 受信主机 → 注入 token
        assert "secrettok123" not in projects._auth_url(
            "https://evil.com/o/r.git")                        # 第三方 → 绝不注入
        assert projects.parse_repo_url("看 https://evil.com/a/b") is None      # 非受信主机不当仓库
        assert projects.parse_repo_url("https://gitea.example.com/team/app")["repo"] == "app"
        assert projects.parse_repo_url(                        # 默认端口(:443/https)视为同一主机
            "https://gitea.example.com:443/team/app")["repo"] == "app"
        assert projects.parse_repo_url(                        # userinfo 诡计：host 实为 evil → 拒绝
            "https://gitea.example.com@evil.com/o/r") is None
        assert projects.parse_repo_url(                        # 反向 userinfo：netloc 带 @ 一律可疑 → 拒绝
            "https://evil.com@gitea.example.com/o/r") is None
        assert projects.parse_repo_url(                        # 路径穿越的 owner/repo 一律拒绝
            "https://gitea.example.com/../../etc") is None
        assert projects.parse_repo_url(                        # Gitea 保留路由不当 repo
            "https://gitea.example.com/explore/repos") is None
        assert projects.parse_repo_url(                        # PR 链接仍能路由到 owner/repo
            "看 https://gitea.example.com/team/app/pulls/7")["repo"] == "app"
        assert projects.parse_repo_url(                        # 前面先出现不相关链接也别漏掉受信仓库
            "参考 https://github.com/foo/bar 然后改 https://gitea.example.com/team/app")["repo"] == "app"
        assert redact("tok=secrettok123") == "tok=***"          # 外发出口抹掉凭证

        settings.gitea_host = ""                                # fail-closed：没配受信主机就不认任何仓库
        assert projects.parse_repo_url("https://gitea.example.com/team/app") is None

        import config                                           # 运行期登记的 Matrix token 也要被 redact
        config.register_secret("matrixtok999")
        assert redact("token=matrixtok999 done") == "token=*** done"
    finally:
        settings.gitea_host, settings.gitea_token = orig_host, orig_tok


# ---------- 8) 访问控制：授权判定 + 邀请闸 ----------
def test_authorization_and_invite():
    set_identity()
    orig_allow, orig_rooms = settings.allow_users, settings.room_allowlist
    try:
        settings.room_allowlist = []
        settings.allow_users = ["@alice:ex.org"]               # 配了名单 → 只认名单内
        assert bot._authorized("@alice:ex.org")
        assert not bot._authorized("@eve:ex.org")

        settings.allow_users = []                              # 留空 → 默认所有人可操作
        assert bot._authorized("@eve:ex.org")
        assert bot._authorized("@boss:ex.org")

        # 邀请闸：只接受授权用户的邀请
        joined = []

        class FC:
            async def join(self, rid):
                joined.append(rid)

        bot.client = FC()
        settings.allow_users = ["@alice:ex.org"]
        mk_inv = lambda s: types.SimpleNamespace(
            state_key="@claudebot:ex.org", membership="invite", sender=s)
        room = FakeRoom("!r:ex.org", 2)
        asyncio.run(bot.on_invite(room, mk_inv("@eve:ex.org")))    # 未授权 → 不加入
        asyncio.run(bot.on_invite(room, mk_inv("@alice:ex.org")))  # 授权 → 加入
        assert joined == ["!r:ex.org"]
    finally:
        settings.allow_users, settings.room_allowlist = orig_allow, orig_rooms


# ---------- 9) /reset 连背景对话一起清空 ----------
def test_reset_clears_context():
    set_identity()
    rid = "!g:ex.org"
    room = FakeRoom(rid, 3)
    bot._context[rid].clear()
    bot._context[rid].append((time.time(), "Alice", "上一轮的旧话题"))   # 预置背景

    rec = {"id": "p1", "owner": "o", "repo": "r", "path": "/tmp", "base": "main"}
    orig_get_room, orig_reset, orig_client = bot.projects.get_room, bot.runner.reset, bot.client

    class FC:
        async def room_send(self, r, mt, content, **k):
            return types.SimpleNamespace(event_id="$x")

    bot.projects.get_room = lambda r: rec
    bot.runner.reset = lambda k: None
    bot.client = FC()
    try:
        asyncio.run(bot.handle_task(room, make_event("/reset"), "/reset"))
    finally:
        bot.projects.get_room, bot.runner.reset, bot.client = orig_get_room, orig_reset, orig_client
    assert len(bot._context[rid]) == 0                        # 背景被清空，不会漏进新会话


# ---------- 10) 重试只在"会话失效"时发生（防重复 PR） ----------
def test_retry_only_on_session_error():
    def run(err_text):
        async def go():
            r = claude_runner.ClaudeRunner()
            calls = {"n": 0}

            async def fake_run(cmd, cwd=None):
                calls["n"] += 1
                if calls["n"] == 1:
                    return 1, b"", err_text                   # 第一次失败
                return 0, json.dumps({"result": "ok", "session_id": "s2",
                                      "is_error": False}).encode(), b""

            r._run = fake_run
            r._sessions["k"] = ("oldsid", time.time())        # 有看似有效的会话
            try:
                return calls, await r.ask("k", "hi")
            except RuntimeError:
                return calls, None

        return asyncio.run(go())

    c1, res1 = run(b"Error: No conversation found with session ID: oldsid")
    assert c1["n"] == 2 and res1 == "ok"                      # 会话失效 → 重试并成功
    c2, res2 = run(b"fatal: push rejected after opening PR")
    assert c2["n"] == 1 and res2 is None                      # 普通业务错误 → 不重试，直接报错


# ---------- 11) 分块后代码围栏自洽（每块 ``` 成对） ----------
def test_fence_balance_on_split():
    block = "```python\n" + ("x = 1\n" * 1500) + "```\n"      # 远超 4000 字节的代码块
    chunks = bot._split("先说明一句\n" + block + "结尾一句")
    assert len(chunks) >= 2                                   # 确实被切成多块
    for c in chunks:
        fences = sum(1 for ln in c.splitlines() if ln.lstrip().startswith("```"))
        assert fences % 2 == 0, f"分块围栏不闭合: {fences}"


# ---------- 12) /bind <url> 后跟的任务，绑定成功后接着派下去 ----------
def test_bind_carries_trailing_task():
    set_identity()
    room = FakeRoom("!g:ex.org", 3)
    repo = {"owner": "o", "repo": "r"}
    rec = {"id": "p", "owner": "o", "repo": "r", "base": "main", "path": "/tmp"}
    ran = {}

    async def fake_bind_room(rid, info):
        return rec

    async def fake_handle_task(rm, ev, text):
        ran["text"] = text

    class FC:
        async def room_send(self, *a, **k):
            return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.bind_room, bot.runner.reset, bot.handle_task, bot.client)
    bot.projects.bind_room = fake_bind_room
    bot.runner.reset = lambda k: None
    bot.handle_task = fake_handle_task
    bot.client = FC()
    try:
        ev = make_event("/bind https://gitea.example.com/o/r 修复登录刷新")
        asyncio.run(bot.do_bind(room, repo, ev, "修复登录刷新"))
    finally:
        (bot.projects.bind_room, bot.runner.reset, bot.handle_task, bot.client) = orig
    assert ran.get("text") == "修复登录刷新"                  # 绑定后任务被派下去，没被吞掉


# ---------- 13) 群分派会校验/按需修复本地 checkout ----------
def test_group_dispatch_repairs_checkout():
    set_identity()
    room = FakeRoom("!g:ex.org", 3)
    rec = {"id": "h/o/r", "owner": "o", "repo": "r",
           "path": "/nonexistent", "base": "main", "host": "https://h"}
    called = {}

    async def fake_ensure(info):
        called["info"] = info
        return rec

    orig = (bot.projects.get_room, bot.projects.ensure_project)
    bot.projects.get_room = lambda rid: rec
    bot.projects.ensure_project = fake_ensure
    try:
        out = asyncio.run(bot._dispatch(room, make_event("干活"), "干活"))
    finally:
        (bot.projects.get_room, bot.projects.ensure_project) = orig
    assert out is rec and called.get("info") is rec    # 绑定群派活前过了 ensure（丢了会重 clone）


# ---------- 14) 绑定持久化：原子写 + 损坏时备份 ----------
def test_bindings_atomic_and_corrupt_backup():
    import tempfile
    import projects as pmod
    bpath = os.path.join(tempfile.mkdtemp(), "bindings.json")
    orig = settings.bindings_path
    settings.bindings_path = bpath
    try:
        with open(bpath, "w") as f:
            f.write("{ not valid json")              # 预置一个损坏文件
        P = pmod.Projects()
        assert P.list_projects() == []               # 损坏 → 从空开始
        assert os.path.exists(bpath + ".corrupt")    # 旧文件被备份成 .corrupt

        P._projects = {"h/o/r": {"id": "h/o/r", "owner": "o", "repo": "r"}}
        P._rooms = {"!g:ex.org": "h/o/r"}
        P._save()
        with open(bpath) as f:
            data = json.load(f)                      # 落盘的是合法 JSON
        assert data["rooms"]["!g:ex.org"] == "h/o/r"
        assert not os.path.exists(bpath + ".tmp")    # 临时文件已 rename，无残留
    finally:
        settings.bindings_path = orig


# ---------- 15) 发送失败有日志、且不当成已发进上下文 ----------
def test_send_failure_logged():
    set_identity()
    import logging
    rid = "!r:ex.org"
    bot._context[rid].clear()

    class FC:
        async def room_send(self, *a, **k):
            return types.SimpleNamespace(message="M_LIMIT_EXCEEDED")   # 无 event_id = 失败

    records = []
    h = logging.Handler()
    h.emit = lambda r: records.append(r.getMessage())
    orig_client = bot.client
    bot.client = FC()
    bot.log.addHandler(h)
    try:
        asyncio.run(bot.send(rid, "答复内容", track=True))
    finally:
        bot.log.removeHandler(h)
        bot.client = orig_client
    assert any("失败" in m for m in records)                       # 失败留了日志
    assert "答复内容" not in [b for _, _, b in bot._context[rid]]  # 没发出去就不进上下文


# ---------- 16) _detect_base 探测失败时选实际存在的分支，不盲目回退 main ----------
def test_detect_base_prefers_existing_branch():
    import projects as pmod

    async def fake_git(*args, cwd=None):
        if args[0] == "symbolic-ref":
            return 1, "", "no HEAD"          # 探测不到 origin/HEAD
        if args[0] == "remote":
            return 1, "", ""                 # set-head 也失败
        if args[0] == "rev-parse":           # 只有 origin/master 存在
            return (0, "", "") if args[-1].endswith("/master") else (1, "", "")
        return 1, "", ""

    orig = pmod._git
    pmod._git = fake_git
    try:
        base = asyncio.run(pmod.Projects._detect_base("/x"))
    finally:
        pmod._git = orig
    assert base == "master"                  # main 不存在 → 取实际存在的 master


# ---------- 17) 群里"纯仓库链接"才自动绑定，链接+闲聊不绑 ----------
def test_just_url_autobind_only_for_bare_url():
    set_identity()
    bot._synced = True
    room = FakeRoom("!g:ex.org", 3)          # 群、未绑定
    orig_host = settings.gitea_host
    orig_allow = settings.allow_users        # 真实 .env 可能配了 ALLOW_USERS，这里清空免得把 @alice 挡在门外
    orig = (bot.projects.get_room, bot.do_bind, bot._spawn)
    settings.gitea_host = "https://gitea.example.com"
    settings.allow_users = []
    bot.projects.get_room = lambda rid: None
    bound = []
    bot.do_bind = lambda *a, **k: bound.append(1)
    bot._spawn = lambda coro: coro.close() if hasattr(coro, "close") else None
    try:
        bot._context[room.room_id].clear()
        asyncio.run(bot.on_message(room, make_event("https://gitea.example.com/o/r")))
        n_bare = len(bound)
        asyncio.run(bot.on_message(
            room, make_event("https://gitea.example.com/o/r 顺便问下这个咋样啊", event_id="$in2")))
        n_with_task = len(bound) - n_bare
    finally:
        settings.gitea_host = orig_host
        settings.allow_users = orig_allow
        (bot.projects.get_room, bot.do_bind, bot._spawn) = orig
    assert n_bare == 1                        # 纯链接 → 自动绑定
    assert n_with_task == 0                   # 链接+闲聊 → 不自动绑定


# ---------- 18) 成员未同步的群不被当私聊 + 外发富文本剥外链 img ----------
def test_dm_classification_and_html_hardening():
    # 成员还没同步（users 空、member_count 0/缺）→ 不当 DM，否则 REPLY_IN_DM_ALWAYS 会逐条乱回
    unsynced = types.SimpleNamespace(users={})
    assert bot._is_dm(unsynced) is False
    assert bot._is_dm(types.SimpleNamespace(users={"@a:ex.org": 1, "@b:ex.org": 1})) is True
    assert bot._is_dm(types.SimpleNamespace(
        users={f"@u{i}:ex.org": 1 for i in range(9)}, member_count=0)) is False

    # 外链 <img src> 不得出现在外发 HTML（追踪像素 / 查看者 IP 泄露）；普通链接保留
    html = bot._to_html("![x](http://attacker.example/track.png)") or ""
    assert "<img" not in html and "attacker.example" not in html
    assert '<a href="https://gitea.example.com/team/app/pulls/7">' in (
        bot._to_html("[PR](https://gitea.example.com/team/app/pulls/7)") or "")


async def _drain_tasks():
    for _ in range(50):
        pend = [t for t in bot._tasks if not t.done()]
        if not pend:
            break
        await asyncio.gather(*pend, return_exceptions=True)


# ---------- 19) 媒体：下载落盘 + 入上下文 + 被点名(DM)派活带上文件路径 ----------
def test_media_download_and_dispatch():
    import tempfile
    import glob
    set_identity()
    bot._synced = True
    rid = "!md:ex.org"
    room = FakeRoom(rid, 2)                       # 2 人 → DM → 必回
    bot._context[rid].clear()
    tmp = tempfile.mkdtemp()
    orig = (settings.media_root, settings.media_enabled, bot.client, bot.handle_task)
    settings.media_root, settings.media_enabled = tmp, True
    captured = {}

    async def fake_handle(rm, ev, text, skip_body=None):
        captured["text"] = text

    class FC:
        async def download(self, mxc=None, save_to=None, **k):   # nio 流式落盘到 save_to
            with open(save_to, "wb") as f:
                f.write(b"hello-log-bytes")
            return types.SimpleNamespace(content_type="text/plain", filename="app.log")

    bot.client = FC()
    bot.handle_task = fake_handle
    try:
        async def go():
            await bot._process_media(room, make_media_event(body="app.log", event_id="$md1"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_enabled, bot.client, bot.handle_task) = orig

    files = glob.glob(os.path.join(tmp, "*", "*"))
    assert files, "媒体没落盘"
    with open(files[0], "rb") as f:
        assert f.read() == b"hello-log-bytes"                      # 内容正确写盘
    assert any(files[0] in b for _, _, b in bot._context[rid])     # 上下文带本地路径
    assert files[0] in captured.get("text", "")                    # 派活时把路径喂给 Claude


# ---------- 20) 媒体：声明体积超限则不下载，只在上下文标注 ----------
def test_media_oversize_skipped():
    import tempfile
    set_identity()
    bot._synced = True
    rid = "!mo:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    orig = (settings.media_root, settings.media_max_mb, bot.client, bot.handle_task)
    settings.media_root, settings.media_max_mb = tempfile.mkdtemp(), 1
    called = {"dl": 0}

    class FC:
        async def download(self, mxc=None, **k):
            called["dl"] += 1
            return types.SimpleNamespace(body=b"x", content_type="", filename="big.bin")

    async def fake_handle(rm, ev, text):
        pass

    bot.client, bot.handle_task = FC(), fake_handle
    try:
        async def go():
            await bot._process_media(
                room, make_media_event(body="big.bin", size=5 * 1024 * 1024, event_id="$big"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_max_mb, bot.client, bot.handle_task) = orig
    assert called["dl"] == 0                                       # 超限不下载
    assert any("超过上限" in b for _, _, b in bot._context[rid])   # 上下文有标注


# ---------- 21) 媒体文件名消毒：挡掉 ../ 路径穿越 ----------
def test_media_safe_name():
    s = bot._safe_name("../../etc/passwd", "f")
    assert "/" not in s and not s.startswith(".")                  # 无分隔符、不以点开头
    assert bot._safe_name("", "fallback") == "fallback"            # 空 → 兜底名


# ---------- 22) 媒体滚动删旧：保留最近 N 个；keep=0 不会因 [:-0] 反而全留 ----------
def test_media_prune():
    import tempfile
    d = tempfile.mkdtemp()
    for i in range(5):
        p = os.path.join(d, f"f{i}")
        with open(p, "w") as f:
            f.write("x")
        os.utime(p, (1000 + i, 1000 + i))         # 递增 mtime，f0 最旧
    bot._prune_dir(d, 2)
    left = sorted(os.listdir(d))
    assert left == ["f3", "f4"], left              # 只留最近 2 个

    for i in range(3):
        p = os.path.join(d, f"g{i}")
        with open(p, "w") as f:
            f.write("x")
        os.utime(p, (2000 + i, 2000 + i))
    bot._prune_dir(d, 0)                            # keep=0 被钳到 1，不该一个都不删
    assert os.listdir(d) == ["g2"], os.listdir(d)


# ---------- 23) DM 分诊路由也要过 ensure_project（否则会在丢失的 checkout 上干活） ----------
def test_dm_routing_ensures_checkout():
    set_identity()
    room = FakeRoom("!dmroute:ex.org", 2)                  # 2 人 → DM
    app = {"id": "h/o/app", "owner": "o", "repo": "app",
           "path": "/gone", "base": "main", "host": "https://h"}
    ensured = []

    async def fake_ensure(info):
        ensured.append(info)
        return info

    class R:                                               # 让轻量分诊命中 app
        async def quick(self, prompt):
            return "h/o/app"

    orig = (bot.projects.list_projects, bot.projects.ensure_project,
            bot.projects.get_project, bot.runner)
    bot.projects.list_projects = lambda: [app]
    bot.projects.ensure_project = fake_ensure
    bot.projects.get_project = lambda pid: app if pid == "h/o/app" else None
    bot.runner = R()
    try:
        out = asyncio.run(bot._dispatch(room, make_event("帮我看看那个东西"), "帮我看看那个东西"))
    finally:
        (bot.projects.list_projects, bot.projects.ensure_project,
         bot.projects.get_project, bot.runner) = orig
    assert out is app
    assert ensured == [app]              # 分诊路由也过了 ensure（丢了的 checkout 会被重 clone）


# ---------- 24) 裸仓库名匹配是兜底：排在 LLM 分诊之后，不抢在前面误路由 ----------
def test_dm_loose_name_after_triage():
    set_identity()
    room = FakeRoom("!dmloose:ex.org", 2)
    app = {"id": "h/o/app", "owner": "o", "repo": "app",
           "path": "/x", "base": "main", "host": "https://h"}
    order = []

    async def fake_ensure(info):
        order.append("ensure")
        return info

    async def triage_none(text, known, context=""):
        order.append("triage")
        return None

    orig = (bot.projects.list_projects, bot.projects.ensure_project, bot._triage)
    bot.projects.list_projects = lambda: [app]
    bot.projects.ensure_project = fake_ensure
    bot._triage = triage_none
    try:
        out = asyncio.run(bot._dispatch(room, make_event("那个 app 跑不起来了"), "那个 app 跑不起来了"))
    finally:
        (bot.projects.list_projects, bot.projects.ensure_project, bot._triage) = orig
    assert out is app                     # 裸仓库名兜底命中
    assert order == ["triage", "ensure"]  # 先分诊、放弃后才用裸名兜底


# ---------- 25) 主动插话判定 PASS 时只占用短冷却，不整段沉默 ----------
def test_proactive_pass_keeps_short_cooldown():
    set_identity()
    rid = "!pc:ex.org"
    room = FakeRoom(rid, 3)
    bot._context[rid].clear()
    bot._last_proactive[rid] = 0.0
    orig = (bot.runner, bot.client, settings.proactive_cooldown)
    settings.proactive_cooldown = 600

    class R:
        async def quick(self, prompt):
            return "__PASS__"

    class FC:
        async def room_send(self, *a, **k):
            return types.SimpleNamespace(event_id="$x")

    bot.runner, bot.client = R(), FC()
    try:
        asyncio.run(bot.maybe_proactive(room, make_event("报错了帮我看看"), "报错了帮我看看"))
    finally:
        (bot.runner, bot.client, settings.proactive_cooldown) = orig
    remaining = 600 - (time.time() - bot._last_proactive[rid])
    assert 0 < remaining <= bot._PROACTIVE_PASS_COOLDOWN + 2, remaining   # PASS 后很快能重判


# ---------- 26) 已绑定群里再发裸 URL：给换绑提示而非静默无反应 ----------
def test_group_rebind_hint():
    set_identity()
    bot._synced = True
    rid = "!gb:ex.org"
    room = FakeRoom(rid, 3)
    bot._context[rid].clear()
    bound = {"id": "gitea.example.com/o/old", "owner": "o", "repo": "old"}
    msgs, pending = [], []
    orig = (settings.gitea_host, settings.allow_users, settings.room_allowlist,
            bot.projects.get_room, bot.client, bot._spawn)
    settings.gitea_host = "https://gitea.example.com"
    settings.allow_users, settings.room_allowlist = [], []
    bot.projects.get_room = lambda r: bound

    class FC:
        async def room_send(self, r, mt, content, **k):
            msgs.append(content["body"])
            return types.SimpleNamespace(event_id="$x")

    bot.client = FC()
    bot._spawn = lambda coro: pending.append(coro)
    try:
        async def go():
            await bot.on_message(room, make_event("https://gitea.example.com/o/new"))
            for c in pending:
                await c
        asyncio.run(go())
    finally:
        (settings.gitea_host, settings.allow_users, settings.room_allowlist,
         bot.projects.get_room, bot.client, bot._spawn) = orig
    assert any("换绑" in m for m in msgs)        # 裸 URL 撞上已绑仓库 → 提示换绑


# ---------- 27) 代码块跨分块续块时保留语言标记（语法高亮不丢） ----------
def test_fence_language_preserved():
    block = "```python\n" + ("x = 1\n" * 1500) + "```\n"
    chunks = bot._split("说明\n" + block + "结尾")
    assert len(chunks) >= 2
    cont = [c for c in chunks if c.startswith("```")]      # 续块开头即重开的围栏
    assert cont and all(c.startswith("```python") for c in cont), \
        "分块续块重开围栏时丢了语言标记"


# ---------- 28) DM 多轮：分诊/裸名都没命中时沿用上次项目，不再每条反问 ----------
def test_dm_last_project_fallback():
    set_identity()
    rid = "!dmstick:ex.org"
    room = FakeRoom(rid, 2)                       # DM
    appA = {"id": "h/o/appA", "owner": "o", "repo": "appA",
            "path": "/x", "base": "main", "host": "https://h"}
    ensured = []

    async def fake_ensure(info):
        ensured.append(info)
        return info

    async def triage_none(text, known, context=""):
        return None

    orig = (bot.projects.list_projects, bot.projects.ensure_project,
            bot.projects.get_project, bot._triage)
    bot.projects.list_projects = lambda: [appA]
    bot.projects.ensure_project = fake_ensure
    bot.projects.get_project = lambda pid: appA if pid == "h/o/appA" else None
    bot._triage = triage_none
    bot._last_project_by_room[rid] = "h/o/appA"           # 上一轮已路由到 appA
    try:                                                  # 延续消息没提任何项目名/链接
        out = asyncio.run(bot._dispatch(room, make_event("再补个单元测试吧"), "再补个单元测试吧"))
    finally:
        (bot.projects.list_projects, bot.projects.ensure_project,
         bot.projects.get_project, bot._triage) = orig
        bot._last_project_by_room.pop(rid, None)
    assert out is appA and ensured == [appA]              # 沿用上次项目而不是反问


# ---------- 29) 群里"@bot 仓库URL 任务"一条消息：先绑再派，不再答非所问 ----------
def test_group_url_with_task_binds():
    set_identity()
    bot._synced = True
    rid = "!gbind:ex.org"
    room = FakeRoom(rid, 3)                               # 群、未绑定
    orig_host = settings.gitea_host
    orig = (settings.allow_users, settings.room_allowlist,
            bot.projects.get_room, bot.do_bind, bot._spawn)
    settings.gitea_host = "https://gitea.example.com"
    settings.allow_users, settings.room_allowlist = [], []
    bot.projects.get_room = lambda r: None
    captured = {}

    async def fake_do_bind(room, repo, event=None, task_text=""):
        captured["repo"], captured["task"] = repo, task_text

    bot.do_bind = fake_do_bind
    pend = []
    bot._spawn = lambda coro: pend.append(coro)
    try:
        async def go():
            await bot.on_message(room, make_event(
                "@claude-bot https://gitea.example.com/team/app 帮我修登录刷新",
                mentions=["@claudebot:ex.org"]))
            for c in pend:
                await c
        asyncio.run(go())
    finally:
        settings.gitea_host = orig_host
        (settings.allow_users, settings.room_allowlist,
         bot.projects.get_room, bot.do_bind, bot._spawn) = orig
        bot._context[rid].clear()
    assert captured.get("repo", {}).get("repo") == "app"  # 同条消息里的 URL 被识别并绑定
    assert captured.get("task") == "帮我修登录刷新"        # @bot 与 URL 都剥掉，只剩任务正文


# ---------- 30) _is_dm 只认恰好 2 人：只同步到 bot 自己(1) 不当私聊 ----------
def test_is_dm_requires_exactly_two():
    assert bot._is_dm(types.SimpleNamespace(users={"@bot:ex.org": 1})) is False
    assert bot._is_dm(types.SimpleNamespace(users={"@a:ex.org": 1, "@b:ex.org": 1})) is True
    assert bot._is_dm(types.SimpleNamespace(users={"@bot:ex.org": 1}, member_count=5)) is False


# ---------- 31) _human_gap：25 小时不再塌成"约 1 天" ----------
def test_human_gap_precision():
    assert bot._human_gap(25 * 3600) == "约 25 小时"
    assert "天" in bot._human_gap(50 * 3600)             # 超过 48h 才进"天"


# ---------- 32) CONTEXT_LINES=0 表示不带背景（而非 [-0:] 把全部带上） ----------
def test_context_lines_zero_means_none():
    rid = "!cl0:ex.org"
    bot._context[rid].clear()
    bot._context[rid].append((time.time(), "Alice", "一些背景"))
    orig = settings.context_lines
    settings.context_lines = 0
    try:
        assert bot._format_context(rid) == ""
    finally:
        settings.context_lines = orig
        bot._context[rid].clear()


# ---------- 33) 发送被限流：退避重试而非直接丢块 ----------
def test_send_retries_on_rate_limit():
    set_identity()
    rid = "!rl:ex.org"
    bot._context[rid].clear()
    calls = {"n": 0}

    class FC:
        async def room_send(self, r, mt, content, **k):
            calls["n"] += 1
            if calls["n"] == 1:                          # 第一次限流，给个极短 retry_after
                return types.SimpleNamespace(
                    status_code="M_LIMIT_EXCEEDED", retry_after_ms=10, message="rate")
            return types.SimpleNamespace(event_id="$ok")

    orig = bot.client
    bot.client = FC()
    try:
        asyncio.run(bot.send(rid, "答复", track=True))
    finally:
        bot.client = orig
    assert calls["n"] == 2                                # 重试后发出
    assert "答复" in [b for _, _, b in bot._context[rid]]  # 最终成功 → 入上下文
    bot._context[rid].clear()


# ---------- 34) 会话失效匹配收紧：含 session+not found 的普通报错不再误判可重试 ----------
def test_session_error_matching_tightened():
    f = claude_runner.ClaudeRunner._looks_like_session_error
    assert f(b"", b"Error: No conversation found with session ID: x") is True
    assert f(b"", b"session expired, please restart") is True
    assert f(b"", b"created session; remote: file not found during push") is False  # session+not found 但非会话失效
    assert f(b"", b"fatal: push rejected after opening PR") is False


# ---------- 35) 媒体派活：背景里那行 [文件]… 被剔除，不和当前任务重复 ----------
def test_run_on_project_skips_media_line():
    set_identity()
    rid = "!mskip:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    sender = room.user_name("@alice:ex.org")
    line = "[文件] a.log（text/plain, 10B）已存到本地：/m/a.log"
    bot._context[rid].append((time.time(), "Bob", "之前聊的别的事"))
    bot._context[rid].append((time.time(), sender, line))
    captured = {}

    class R:
        async def ask(self, key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None):
            captured["prompt"] = prompt
            return "ok"

    class FC:
        async def room_send(self, *a, **k):
            return types.SimpleNamespace(event_id="$x")

    rec = {"id": "p", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}
    orig = (bot.runner, bot.client)
    bot.runner, bot.client = R(), FC()
    try:
        ev = make_media_event(body="a.log", event_id="$mm")
        asyncio.run(bot._run_on_project(room, ev, "看看这个文件 /m/a.log", rec, skip_body=line))
    finally:
        (bot.runner, bot.client) = orig
        bot._context[rid].clear()
    assert "[文件] a.log" not in captured["prompt"]        # 媒体行不再重复出现在 prompt
    assert "之前聊的别的事" in captured["prompt"]           # 其它背景仍照常带上


# ---------- 36) 未声明 size 的大文件：流式落盘后按真实大小拦下，不静默吃内存 ----------
def test_media_oversize_undeclared_streamed():
    import tempfile
    set_identity()
    bot._synced = True
    rid = "!mbig:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    orig = (settings.media_root, settings.media_max_mb, settings.media_enabled,
            bot.client, bot.handle_task)
    settings.media_root = tempfile.mkdtemp()
    settings.media_max_mb, settings.media_enabled = 1, True

    class FC:
        async def download(self, mxc=None, save_to=None, **k):
            with open(save_to, "wb") as f:                # 模拟 nio 流式落盘：写 2MB、不声明 size
                f.write(b"x" * (2 * 1024 * 1024))
            return types.SimpleNamespace(body=save_to, content_type="application/octet-stream")

    async def fake_handle(rm, ev, text, skip_body=None):
        pass

    bot.client, bot.handle_task = FC(), fake_handle
    try:
        async def go():
            await bot._process_media(room, make_media_event(body="big.bin", event_id="$ub"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_max_mb, settings.media_enabled,
         bot.client, bot.handle_task) = orig
    assert any("超过上限" in b for _, _, b in bot._context[rid])
    bot._context[rid].clear()


# ---------- 37) 残留目录/半截 .git：clone 前清掉并重 clone，不再永久失败 ----------
def test_ensure_cleans_residual_dir():
    import tempfile
    import projects as pmod
    root = tempfile.mkdtemp()
    P = pmod.Projects()
    P.root = root
    P.bindings_path = os.path.join(root, "bindings.json")
    P._projects, P._rooms = {}, {}
    info = {"host": "https://gitea.example.com", "owner": "o", "repo": "r",
            "clone_url": "https://gitea.example.com/o/r.git",
            "web_url": "https://gitea.example.com/o/r"}
    local = os.path.join(root, "gitea.example.com", "o", "r")
    os.makedirs(local, exist_ok=True)
    with open(os.path.join(local, "leftover.txt"), "w") as f:   # 残留的非 git 脏目录
        f.write("junk")

    async def fake_git(*args, cwd=None):
        if args[0] == "rev-parse" and "--is-inside-work-tree" in args:
            ok = os.path.isdir(os.path.join(cwd, ".git"))
            return (0, "true", "") if ok else (1, "", "not a git repo")
        if args[0] == "clone":
            dest = args[-1]
            assert not os.path.exists(os.path.join(dest, "leftover.txt")), "clone 前没清掉残留"
            os.makedirs(os.path.join(dest, ".git"), exist_ok=True)
            return 0, "", ""
        if args[0] == "symbolic-ref":
            return 0, "refs/remotes/origin/main", ""
        return 0, "", ""

    orig_git = pmod._git
    orig_host, orig_tok = settings.gitea_host, settings.gitea_token
    pmod._git = fake_git
    settings.gitea_host, settings.gitea_token = "https://gitea.example.com", ""
    try:
        rec = asyncio.run(P.ensure_project(info))
    finally:
        pmod._git = orig_git
        settings.gitea_host, settings.gitea_token = orig_host, orig_tok
    assert rec["path"] == local
    assert os.path.isdir(os.path.join(local, ".git"))               # 重新 clone 出了 .git
    assert not os.path.exists(os.path.join(local, "leftover.txt"))  # 残留被清掉


# ---------- 38) 会话 key 按项目+房间隔离：不同房间不串台 ----------
def test_session_key_per_room():
    rec = {"id": "h/o/r"}
    k1 = bot._sess_key(rec, "!a:ex.org")
    k2 = bot._sess_key(rec, "!b:ex.org")
    assert k1 != k2                                   # 两个房间 → 两条会话
    assert "h/o/r" in k1 and "!a:ex.org" in k1        # 仍包含项目维度


# ---------- 39) 默认分支名带斜杠（release/2.0）不被 rsplit 截成 2.0 ----------
def test_detect_base_slash_branch():
    import projects as pmod

    async def fake_git(*args, cwd=None):
        if args[0] == "symbolic-ref":
            return 0, "refs/remotes/origin/release/2.0", ""
        return 1, "", ""

    orig = pmod._git
    pmod._git = fake_git
    try:
        base = asyncio.run(pmod.Projects._detect_base("/x"))
    finally:
        pmod._git = orig
    assert base == "release/2.0"                       # 不再被截成 "2.0"


# ---------- 40) 派活前把工作树拉回干净 base：fetch + checkout -B + reset --hard + clean ----------
def test_prepare_worktree_resets():
    import tempfile
    import projects as pmod
    d = tempfile.mkdtemp()
    os.makedirs(os.path.join(d, ".git"))             # 装成一个 git 工作树
    calls = []

    async def fake_git(*args, cwd=None):
        calls.append(args)
        return 0, "", ""                             # rev-parse 也回 0 → origin/base 存在

    orig = pmod._git
    pmod._git = fake_git
    try:
        asyncio.run(pmod.projects.prepare_worktree({"path": d, "base": "main"}))
    finally:
        pmod._git = orig
    ops = [a[0] for a in calls]
    assert "fetch" in ops                                            # 先 fetch 更新远端
    assert any(a[0] == "checkout" and "-f" in a and "-B" in a for a in calls)  # 强制切回 base
    assert any(a[0] == "reset" and "--hard" in a for a in calls)     # 对齐 origin/base
    assert any(a[0] == "clean" for a in calls)                       # 清掉未跟踪残留


# ---------- 41) 触发词按词边界匹配（claude 不命中 claudette），CJK 词按子串 ----------
def test_trigger_word_boundary():
    orig = settings.trigger_phrase
    try:
        settings.trigger_phrase = "claude"
        assert bot._has_trigger("用 claude 跑一下") is True
        assert bot._has_trigger("claudette 来了") is False           # 子串不误命中
        assert bot._strip_trigger("claude 修一下").strip() == "修一下"  # 去掉时也按边界
        settings.trigger_phrase = "小助手"                            # 非 ASCII → 子串
        assert bot._has_trigger("小助手帮我看看") is True
        settings.trigger_phrase = ""
        assert bot._has_trigger("随便一句话") is False                # 空触发词从不命中
    finally:
        settings.trigger_phrase = orig


# ---------- 43) DM 一般性问题：分诊判 GENERAL 时当通用助手答，不反问"哪个项目" ----------
def test_dm_general_question():
    set_identity()
    room = FakeRoom("!dmgen:ex.org", 2)                   # DM
    app = {"id": "h/o/app", "owner": "o", "repo": "app",
           "path": "/x", "base": "main", "host": "https://h"}
    asked = []

    async def triage_general(text, known, context=""):
        return bot.TRIAGE_GENERAL

    class FC:
        async def room_send(self, r, mt, content, **k):
            asked.append(content["body"])
            return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.list_projects, bot._triage, bot.client)
    bot.projects.list_projects = lambda: [app]
    bot._triage = triage_general
    bot.client = FC()
    try:
        out = asyncio.run(bot._dispatch(room, make_event("用 Python 写个快排"), "用 Python 写个快排"))
    finally:
        (bot.projects.list_projects, bot._triage, bot.client) = orig
    assert out is not None and out.get("general") is True   # 返回通用助手 rec
    assert out["path"] == settings.claude_workdir            # 在隔离 scratch 目录答
    assert not any("哪个项目" in m for m in asked)           # 没有反问


# ---------- 42) 会话 session_id 落盘：重启（新 runner）后仍能恢复，多轮不断 ----------
def test_sessions_persisted_across_restart():
    import tempfile
    import claude_runner as cr
    d = tempfile.mkdtemp()
    orig = settings.store_path
    settings.store_path = d
    try:
        r1 = cr.ClaudeRunner()
        r1._sessions["h/o/r|!room:ex.org"] = ("sid-123", time.time())
        r1._save_sessions()
        r2 = cr.ClaudeRunner()                       # 模拟重启：新实例从盘上加载
        assert r2._sessions.get("h/o/r|!room:ex.org", (None,))[0] == "sid-123"
    finally:
        settings.store_path = orig


def test_project_memory():
    """项目长期记忆：写入→索引→召回→注入系统提示，且按项目隔离、跨"会话"留存、防穿越。"""
    import memory
    pid = "pi.lan:3000/team/app"
    assert memory.recall(pid) == ""                       # 初始无记忆

    p = memory.remember(pid, "基线分支约定",
                        "本项目以 main 为基线分支，发布走 tag，不直接往 main push。",
                        description="基线/发布约定")
    assert p and os.path.exists(p)                         # 事实落了文件

    d = memory.proj_dir(pid)
    idx = open(os.path.join(d, "MEMORY.md")).read()
    assert "基线分支约定.md" in idx                         # 索引登记了

    r = memory.recall(pid)
    assert "以 main 为基线分支" in r                        # 召回带正文

    # 注入系统提示：原文保留 + 目录路径 + 召回内容都在；重复调用（模拟开新会话）仍拿得到 → 不随 TTL 蒸发
    sp = memory.augment_system_prompt("原始系统提示", pid)
    assert "原始系统提示" in sp and d in sp and "以 main 为基线分支" in sp
    assert "原始系统提示" in memory.augment_system_prompt("原始系统提示", pid)

    # 同一事实再写一次：索引不重复加（订正而非堆叠）
    memory.remember(pid, "基线分支约定", "改：基线分支仍是 main，补充 hotfix 从 tag 拉。")
    assert open(os.path.join(d, "MEMORY.md")).read().count("基线分支约定.md") == 1

    assert memory.recall("pi.lan:3000/team/other") == ""   # 别的项目读不到（不串台）
    assert ".." not in os.path.basename(memory.proj_dir("../../etc/passwd"))  # 路径穿越被消毒

    # 关键：store_path 即便是相对的，记忆目录也必须解析成绝对路径——
    # bot 进程 cwd 是 live dir、Claude 子进程 cwd 是 clone dir，相对路径会让两边读写到不同目录，
    # 跨会话留存直接失效（注入提示里给 Claude 的路径同理）。
    orig_store = settings.store_path
    try:
        settings.store_path = "./store"
        assert os.path.isabs(memory.proj_dir("pi.lan:3000/team/app"))
        assert os.path.isabs(memory._root())
    finally:
        settings.store_path = orig_store

    # 关掉开关时不注入
    orig = settings.memory_enabled
    try:
        settings.memory_enabled = False
        assert memory.augment_system_prompt("X", pid) == "X"
    finally:
        settings.memory_enabled = orig


def _reset_ledger():
    import pr_ledger
    pr_ledger._data = {}
    pr_ledger._loaded = False


# ---------- PR 台账：登记/去重/更新/销账 + 持久化 ----------
def test_pr_ledger():
    import tempfile
    import pr_ledger
    orig = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_ledger()
    try:
        assert pr_ledger.record("h/o/r", 5, "http://x/pulls/5", "!room") is True
        assert pr_ledger.record("h/o/r", 5, "http://x/pulls/5", "!room") is False   # 不重复记
        a = pr_ledger.active()
        assert len(a) == 1 and a[0]["number"] == 5 and a[0]["room"] == "!room"
        pr_ledger.update("h/o/r", 5, branch="claude/x", review_fixes=2)
        assert pr_ledger.active()[0]["branch"] == "claude/x"
        _reset_ledger()                                          # 清内存态 → 必须能从盘恢复
        got = pr_ledger.active()
        assert got and got[0]["number"] == 5 and got[0]["review_fixes"] == 2
        pr_ledger.remove("h/o/r", 5)
        assert pr_ledger.active() == []
    finally:
        settings.store_path = orig
        _reset_ledger()


# ---------- 从回复里抽取本项目的 PR 链接 ----------
def test_extract_pr():
    rec = {"host": "http://pi.lan:3000", "owner": "claude", "repo": "playground"}
    assert bot._extract_pr("搞定，PR：http://pi.lan:3000/claude/playground/pulls/7 看下", rec) \
        == (7, "http://pi.lan:3000/claude/playground/pulls/7")
    assert bot._extract_pr("纯问答没开 PR", rec) is None
    assert bot._extract_pr("http://pi.lan:3000/other/repo/pulls/3", rec) is None   # 别的库不算


# ---------- PR 跟进：合并→销账回报；新评审→派跟进且记 seen/计数 ----------
def test_pr_followup_actions():
    import tempfile
    import pr_ledger
    import gitea
    set_identity()
    orig_store = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent, spawned = [], []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.get_project, bot.client, bot._spawn,
            gitea.pr_info, gitea.pr_reviews, gitea.ci_state)
    bot.projects.get_project = lambda pid: rec if pid == "h/o/r" else None
    bot.client = FC()
    bot._spawn = lambda coro: (spawned.append(1), coro.close())
    try:
        # a) 已合并 → 销账 + 报"已合并"
        pr_ledger.record("h/o/r", 1, "u1", "!room")
        async def merged_info(r, n): return {"state": "closed", "merged": True, "head": {"ref": "b", "sha": "s"}}
        async def no_reviews(r, n): return []
        async def no_ci(r, s): return ""
        gitea.pr_info, gitea.pr_reviews, gitea.ci_state = merged_info, no_reviews, no_ci
        asyncio.run(bot._followup_one([e for e in pr_ledger.active() if e["number"] == 1][0]))
        assert not any(e["number"] == 1 for e in pr_ledger.active())   # 销账
        assert any("已合并" in m for m in sent)

        # b) 新 REQUEST_CHANGES 评审 → 派跟进 + 记 seen_review + review_fixes+1
        pr_ledger.record("h/o/r", 2, "u2", "!room")
        async def open_info(r, n): return {"state": "open", "merged": False, "head": {"ref": "claude/x", "sha": "s2"}}
        async def reviews2(r, n): return [{"id": 10, "state": "REQUEST_CHANGES", "body": "改下 X", "user": {"login": "root"}}]
        gitea.pr_info, gitea.pr_reviews = open_info, reviews2
        asyncio.run(bot._followup_one([e for e in pr_ledger.active() if e["number"] == 2][0]))
        assert spawned                                              # 派了跟进任务
        e2 = [e for e in pr_ledger.active() if e["number"] == 2][0]
        assert e2["seen_review"] == 10 and e2["review_fixes"] == 1 and e2["branch"] == "claude/x"
    finally:
        (bot.projects.get_project, bot.client, bot._spawn,
         gitea.pr_info, gitea.pr_reviews, gitea.ci_state) = orig
        settings.store_path = orig_store
        _reset_ledger()


TESTS = [
    ("启动+群任务全链路", test_startup_and_task_flow),
    ("认 reply / 点名", test_reply_addressing),
    ("引用回退块剥离", test_reply_fallback_strip),
    ("track 门控 + 时间单调", test_track_and_monotonic),
    ("自己账号入上下文不派活", test_own_account_context),
    ("TTL 过期提示", test_ttl_notice),
    ("token 受信主机 + redact", test_security_bits),
    ("访问控制：授权 + 邀请闸", test_authorization_and_invite),
    ("/reset 清空背景上下文", test_reset_clears_context),
    ("重试仅限会话失效", test_retry_only_on_session_error),
    ("分块代码围栏自洽", test_fence_balance_on_split),
    ("/bind 带任务接着派", test_bind_carries_trailing_task),
    ("群分派修复丢失的 checkout", test_group_dispatch_repairs_checkout),
    ("绑定原子写+损坏备份", test_bindings_atomic_and_corrupt_backup),
    ("发送失败有日志不入上下文", test_send_failure_logged),
    ("默认分支选实际存在的", test_detect_base_prefers_existing_branch),
    ("纯链接才自动绑定", test_just_url_autobind_only_for_bare_url),
    ("未同步群不当私聊+剥外链img", test_dm_classification_and_html_hardening),
    ("媒体下载落盘+入上下文+派活", test_media_download_and_dispatch),
    ("媒体超体积跳过", test_media_oversize_skipped),
    ("媒体文件名消毒", test_media_safe_name),
    ("媒体滚动删旧", test_media_prune),
    ("DM 分诊路由过 ensure", test_dm_routing_ensures_checkout),
    ("裸仓库名兜底在分诊之后", test_dm_loose_name_after_triage),
    ("主动 PASS 只占短冷却", test_proactive_pass_keeps_short_cooldown),
    ("已绑群裸 URL 给换绑提示", test_group_rebind_hint),
    ("分块续块保留语言标记", test_fence_language_preserved),
    ("DM 分诊失败沿用上次项目", test_dm_last_project_fallback),
    ("群 URL+任务同条消息先绑再派", test_group_url_with_task_binds),
    ("_is_dm 只认恰好 2 人", test_is_dm_requires_exactly_two),
    ("_human_gap 25h 不塌成 1 天", test_human_gap_precision),
    ("CONTEXT_LINES=0 不带背景", test_context_lines_zero_means_none),
    ("发送限流退避重试", test_send_retries_on_rate_limit),
    ("会话失效匹配收紧", test_session_error_matching_tightened),
    ("媒体行不在 prompt 重复", test_run_on_project_skips_media_line),
    ("未声明大文件按真实大小拦下", test_media_oversize_undeclared_streamed),
    ("残留目录自愈重 clone", test_ensure_cleans_residual_dir),
    ("会话 key 按项目+房间隔离", test_session_key_per_room),
    ("默认分支带斜杠不被截断", test_detect_base_slash_branch),
    ("派活前清回干净 base", test_prepare_worktree_resets),
    ("触发词按词边界匹配", test_trigger_word_boundary),
    ("会话落盘重启可恢复", test_sessions_persisted_across_restart),
    ("DM 一般性问题当通用助手答", test_dm_general_question),
    ("项目长期记忆 跨会话留存", test_project_memory),
    ("PR 台账 登记/持久化/销账", test_pr_ledger),
    ("从回复抽取本项目 PR 链接", test_extract_pr),
    ("PR 跟进 合并销账/评审派活", test_pr_followup_actions),
]


def main():
    import traceback
    import tempfile
    # 把状态目录指到临时目录，别让自检把 sessions.json / last_projects.json 写进真实 store
    settings.store_path = tempfile.mkdtemp(prefix="mxbot-smoke-store-")
    failed = 0
    for name, fn in TESTS:
        try:
            fn()
            print(f"  ✅ {name}")
        except Exception as e:
            failed += 1
            print(f"  ❌ {name}: {e}")
            traceback.print_exc()
    print()
    if failed:
        print(f"FAILED: {failed}/{len(TESTS)}")
        sys.exit(1)
    print(f"ALL {len(TESTS)} SMOKE TESTS PASSED ✅")


if __name__ == "__main__":
    main()
