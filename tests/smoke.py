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
import state                                # noqa: E402
import dispatch, proactive, tasks, heartbeat, media  # noqa: E402,E401
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
    state.MY_ID, state.MY_NAME, state.MY_LOCAL = "@claudebot:ex.org", "claude-bot", "claudebot"


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

    async def fake_ask(key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                       on_delta=None, cancel_key=None):
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

    orig_pf = settings.pr_followup_enabled               # 关掉守护循环（PR 跟进 / 自驱心跳 / 工单接活），否则会 sleep 住任务回收
    orig_hb = settings.proactive_heartbeat_enabled
    orig_ii = settings.issue_intake_enabled
    settings.pr_followup_enabled = False
    settings.proactive_heartbeat_enabled = False
    settings.issue_intake_enabled = False
    bot._context["!g:ex.org"].clear()
    try:
        asyncio.run(bot.main())
    finally:
        settings.pr_followup_enabled = orig_pf
        settings.proactive_heartbeat_enabled = orig_hb
        settings.issue_intake_enabled = orig_ii

    assert state.MY_ID == "@claudebot:ex.org" and state.MY_NAME == "claude-bot"
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

    state.client = FC()
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
    state._synced = True                                        # 初始 sync 之后才处理消息
    room = FakeRoom("!g:ex.org", 3)
    bot._context[room.room_id].clear()
    orig_trigger, orig_spawn = settings.trigger_phrase, state._spawn
    settings.trigger_phrase = ""
    spawned = []
    state._spawn = lambda coro: (spawned.append(1), coro.close())
    try:
        asyncio.run(bot.on_message(
            room, make_event("自言自语", sender="@claudebot:ex.org", event_id="$mine")))
    finally:
        state._spawn, settings.trigger_phrase = orig_spawn, orig_trigger
    assert len(bot._context[room.room_id]) == 1               # 进了上下文
    assert not spawned                                        # 没派活


# ---------- 5b) 编辑消息(m.replace)当新消息进来：不重派活、不进上下文 ----------
def test_edit_event_ignored():
    set_identity()
    state._synced = True
    rid = "!edit:ex.org"
    room = FakeRoom(rid, 2)                    # DM → 否则不必回、测不出"本会派活"
    bot._context[rid].clear()
    spawned = []
    orig = state._spawn
    state._spawn = lambda coro: (spawned.append(1), coro.close())
    try:
        ev = make_event("* 修正后的正文", event_id="$edit1")
        ev.source["content"]["m.relates_to"] = {"rel_type": "m.replace", "event_id": "$orig1"}
        asyncio.run(bot.on_message(room, ev))
    finally:
        state._spawn = orig
        bot._context[rid].clear()
    assert not spawned                          # 编辑事件不派活
    assert len(bot._context[rid]) == 0          # 也不进上下文/逐字记录


# ---------- 6) TTL 过期提示（claude_runner） ----------
def test_ttl_notice():
    async def run():
        r = claude_runner.ClaudeRunner()

        async def fake_run(cmd, cwd=None, on_proc=None):
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


# ---------- 8) 无访问控制：谁邀请都进房、只有真正的 invite 才触发 join ----------
def test_no_access_control_invite_joins():
    set_identity()
    joined = []

    class FC:
        rooms = {}                       # 欢迎语后台任务会探 client.rooms，给个空的即可
        async def join(self, rid):
            joined.append(rid)

    orig_client = state.client
    state.client = FC()
    try:
        mk_inv = lambda s, m="invite": types.SimpleNamespace(
            state_key="@claudebot:ex.org", membership=m, sender=s)
        room = FakeRoom("!r:ex.org", 2)
        asyncio.run(bot.on_invite(room, mk_inv("@eve:ex.org")))     # 陌生人邀请也加入
        asyncio.run(bot.on_invite(room, mk_inv("@alice:ex.org")))
        assert joined == ["!r:ex.org", "!r:ex.org"]
        asyncio.run(bot.on_invite(room, mk_inv("@x:ex.org", "join")))  # 非 invite 成员事件不触发
        assert len(joined) == 2
    finally:
        state.client = orig_client


# ---------- 8a2) 孤儿房间：人走光只剩自己 → 退房+forget ----------
def test_leave_when_alone():
    set_identity()
    left, forgot = [], []

    class FC:
        rooms = {}
        async def room_leave(self, rid):
            left.append(rid)
            self.rooms.pop(rid, None)
        async def room_forget(self, rid):
            forgot.append(rid)

    orig_client, orig_synced = state.client, state._synced
    state.client, state._synced = FC(), True
    try:
        alone = FakeRoom("!alone:ex.org", 0)
        alone.users = {"@claudebot:ex.org": 1}
        both = FakeRoom("!both:ex.org", 0)
        both.users = {"@claudebot:ex.org": 1, "@alice:ex.org": 1}
        FC.rooms = {"!alone:ex.org": alone, "!both:ex.org": both}
        mk = lambda sk, m: types.SimpleNamespace(state_key=sk, membership=m)

        asyncio.run(bot.on_member(both, mk("@bob:ex.org", "leave")))    # 还有人在 → 不退
        assert left == []
        asyncio.run(bot.on_member(alone, mk("@claudebot:ex.org", "leave")))  # 自己的成员事件不触发
        asyncio.run(bot.on_member(alone, mk("@alice:ex.org", "join")))       # 进房类事件不触发
        assert left == []
        state._synced = False                                           # 初始同步期间不动手
        asyncio.run(bot.on_member(alone, mk("@alice:ex.org", "leave")))
        assert left == []
        state._synced = True
        asyncio.run(bot.on_member(alone, mk("@alice:ex.org", "leave")))  # 只剩自己 → 退房+forget
        assert left == ["!alone:ex.org"] and forgot == ["!alone:ex.org"]
        assert asyncio.run(bot._leave_if_alone("!alone:ex.org")) is False  # 已退的房间再查不误报
    finally:
        state.client, state._synced = orig_client, orig_synced


# ---------- 8a3) 退房后清尾巴：绑定/路由被清、在跑任务被取消、聊天记录/媒体被删 ----------
def test_leave_cleans_up_room():
    import tempfile
    import transcript
    set_identity()
    rid = "!dead:ex.org"
    cancelled = []

    class FC:
        rooms = {}
        async def room_leave(self, r):
            self.rooms.pop(r, None)
        async def room_forget(self, r):
            pass

    room = FakeRoom(rid, 0)
    room.users = {"@claudebot:ex.org": 1}          # 只剩自己 → 触发退房

    tmp_media = tempfile.mkdtemp()
    mdir = os.path.join(tmp_media, bot._safe_name(rid, "room"))
    os.makedirs(mdir)
    open(os.path.join(mdir, "f.bin"), "w").close()
    bpath = os.path.join(tempfile.mkdtemp(), "bindings.json")

    orig = (state.client, state._synced, settings.media_root,
            bot.projects.bindings_path, bot.runner.cancel)
    fc = FC(); fc.rooms = {rid: room}
    state.client, state._synced = fc, True
    settings.media_root = tmp_media
    bot.projects.bindings_path = bpath
    bot.runner.cancel = lambda k: (cancelled.append(k) or 1)
    bot.projects._rooms[rid] = "h/o/app"           # 该房间绑着某项目
    bot._last_project_by_room[rid] = "h/o/app"     # 且有路由记忆
    os.makedirs(transcript._root(), exist_ok=True)
    open(transcript.path_for(rid), "w").close()    # 造一份逐字记录
    transcript.mark_backfilled(rid)                # 和回灌标记
    try:
        left = asyncio.run(bot._leave_if_alone(rid))
        still_bound = rid in bot.projects._rooms
        still_routed = rid in bot._last_project_by_room
        tr_gone = not os.path.exists(transcript.path_for(rid)) and not transcript.is_backfilled(rid)
        media_gone = not os.path.exists(mdir)
    finally:
        (state.client, state._synced, settings.media_root,
         bot.projects.bindings_path, bot.runner.cancel) = orig
        bot.projects._rooms.pop(rid, None)
        bot._last_project_by_room.pop(rid, None)
    assert left is True
    assert cancelled == [rid]        # 在跑任务按房间取消（复用 /cancel 路径）
    assert not still_bound           # 绑定被清并落盘
    assert not still_routed          # 路由记忆被清
    assert tr_gone                   # 逐字记录 + 回灌标记被删
    assert media_gone                # 媒体目录被删


# ---------- 8b) 群"对话延续窗口"：点过名后免重复 @ 也算续话 ----------
def test_group_followup_window():
    set_identity()
    rid = "!fw:ex.org"
    room = FakeRoom(rid, 3)                       # 群（非 DM）
    orig_win = settings.group_followup_window
    settings.group_followup_window = 180
    bot._group_engaged.clear()
    try:
        # 没点过名 → 普通消息不算点名
        ok, _ = bot._is_addressed(room, make_event("接着把它改一下", sender="@alice:ex.org"))
        assert not ok

        bot._mark_engaged(rid, "@alice:ex.org")   # alice 点了名

        # 窗口内 alice 的后续消息（没@）→ 算续话
        ok, _ = bot._is_addressed(room, make_event("接着把它改一下", sender="@alice:ex.org"))
        assert ok

        # 窗口内但 @ 了别人 → 不算（在跟别人说话）
        ok, _ = bot._is_addressed(room, make_event(
            "@bob 你看呢", sender="@alice:ex.org", mentions=["@bob:ex.org"]))
        assert not ok

        # 窗口内但这条是回复别人的消息 → 不算续话
        ok, _ = bot._is_addressed(room, make_event(
            "说得对", sender="@alice:ex.org", in_reply_to="$someoneelse"))
        assert not ok

        # 没点过名的 bob → 不算续话
        ok, _ = bot._is_addressed(room, make_event("我也要", sender="@bob:ex.org"))
        assert not ok

        # 窗口过期 → 不再续话
        bot._group_engaged[(rid, "@alice:ex.org")] = time.time() - 1000
        ok, _ = bot._is_addressed(room, make_event("还在吗", sender="@alice:ex.org"))
        assert not ok

        # 开关关掉 → 不续话
        settings.group_followup_window = 0
        bot._mark_engaged(rid, "@alice:ex.org")
        ok, _ = bot._is_addressed(room, make_event("接着改", sender="@alice:ex.org"))
        assert not ok
    finally:
        settings.group_followup_window = orig_win
        bot._group_engaged.clear()


# ---------- 9) /reset 连背景对话一起清空 ----------
def test_reset_clears_context():
    set_identity()
    rid = "!g:ex.org"
    room = FakeRoom(rid, 3)
    bot._context[rid].clear()
    bot._context[rid].append((time.time(), "Alice", "上一轮的旧话题"))   # 预置背景

    rec = {"id": "p1", "owner": "o", "repo": "r", "path": "/tmp", "base": "main"}
    orig_get_room, orig_reset, orig_client = bot.projects.get_room, bot.runner.reset, state.client

    class FC:
        async def room_send(self, r, mt, content, **k):
            return types.SimpleNamespace(event_id="$x")

    bot.projects.get_room = lambda r: rec
    bot.runner.reset = lambda k: None
    state.client = FC()
    try:
        asyncio.run(bot.handle_task(room, make_event("/reset"), "/reset"))
    finally:
        bot.projects.get_room, bot.runner.reset, state.client = orig_get_room, orig_reset, orig_client
    assert len(bot._context[rid]) == 0                        # 背景被清空，不会漏进新会话


# ---------- 10) 重试只在"会话失效"时发生（防重复 PR） ----------
def test_retry_only_on_session_error():
    def run(err_text):
        async def go():
            r = claude_runner.ClaudeRunner()
            calls = {"n": 0}

            async def fake_run(cmd, cwd=None, on_proc=None):
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

    orig = (bot.projects.bind_room, bot.runner.reset, tasks.handle_task, state.client)
    bot.projects.bind_room = fake_bind_room
    bot.runner.reset = lambda k: None
    tasks.handle_task = fake_handle_task
    state.client = FC()
    try:
        ev = make_event("/bind https://gitea.example.com/o/r 修复登录刷新")
        asyncio.run(bot.do_bind(room, repo, ev, "修复登录刷新"))
    finally:
        (bot.projects.bind_room, bot.runner.reset, tasks.handle_task, state.client) = orig
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
    orig_client = state.client
    state.client = FC()
    bot.log.addHandler(h)
    try:
        asyncio.run(bot.send(rid, "答复内容", track=True))
    finally:
        bot.log.removeHandler(h)
        state.client = orig_client
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
    state._synced = True
    room = FakeRoom("!g:ex.org", 3)          # 群、未绑定
    orig_host = settings.gitea_host
    orig = (bot.projects.get_room, bot.do_bind, state._spawn)
    settings.gitea_host = "https://gitea.example.com"
    bot.projects.get_room = lambda rid: None
    bound = []
    bot.do_bind = lambda *a, **k: bound.append(1)
    state._spawn = lambda coro: coro.close() if hasattr(coro, "close") else None
    try:
        bot._context[room.room_id].clear()
        asyncio.run(bot.on_message(room, make_event("https://gitea.example.com/o/r")))
        n_bare = len(bound)
        asyncio.run(bot.on_message(
            room, make_event("https://gitea.example.com/o/r 顺便问下这个咋样啊", event_id="$in2")))
        n_with_task = len(bound) - n_bare
    finally:
        settings.gitea_host = orig_host
        (bot.projects.get_room, bot.do_bind, state._spawn) = orig
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
    state._synced = True
    rid = "!md:ex.org"
    room = FakeRoom(rid, 2)                       # 2 人 → DM → 必回
    bot._context[rid].clear()
    tmp = tempfile.mkdtemp()
    orig = (settings.media_root, settings.media_enabled, state.client, media.handle_task)
    settings.media_root, settings.media_enabled = tmp, True
    captured = {}

    async def fake_handle(rm, ev, text, skip_body=None):
        captured["text"] = text

    class FC:
        async def download(self, mxc=None, save_to=None, **k):   # nio 流式落盘到 save_to
            with open(save_to, "wb") as f:
                f.write(b"hello-log-bytes")
            return types.SimpleNamespace(content_type="text/plain", filename="app.log")

    state.client = FC()
    media.handle_task = fake_handle
    try:
        async def go():
            await bot._process_media(room, make_media_event(body="app.log", event_id="$md1"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_enabled, state.client, media.handle_task) = orig

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
    state._synced = True
    rid = "!mo:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    orig = (settings.media_root, settings.media_max_mb, state.client, media.handle_task)
    settings.media_root, settings.media_max_mb = tempfile.mkdtemp(), 1
    called = {"dl": 0}

    class FC:
        async def download(self, mxc=None, **k):
            called["dl"] += 1
            return types.SimpleNamespace(body=b"x", content_type="", filename="big.bin")

    async def fake_handle(rm, ev, text):
        pass

    state.client, media.handle_task = FC(), fake_handle
    try:
        async def go():
            await bot._process_media(
                room, make_media_event(body="big.bin", size=5 * 1024 * 1024, event_id="$big"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_max_mb, state.client, media.handle_task) = orig
    assert called["dl"] == 0                                       # 超限不下载
    assert any("超过上限" in b for _, _, b in bot._context[rid])   # 上下文有标注


# ---------- 20b) DM 文件处理失败且无 caption：明确回错误，不沉默 return ----------
def test_media_failure_notifies_when_addressed():
    import tempfile
    set_identity()
    state._synced = True
    rid = "!mfail:ex.org"
    room = FakeRoom(rid, 2)                     # DM → 必回
    bot._context[rid].clear()
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")
        async def download(self, mxc=None, save_to=None, **k):   # 不写文件 → size 0 → 判失败
            return types.SimpleNamespace(content_type="", filename="broken.bin")

    async def fake_handle(rm, ev, text, skip_body=None):
        pass

    orig = (settings.media_root, settings.media_enabled, state.client, media.handle_task)
    settings.media_root, settings.media_enabled = tempfile.mkdtemp(), True
    state.client = FC()
    media.handle_task = fake_handle
    try:
        async def go():
            await bot._process_media(
                room, make_media_event(body="broken.bin", event_id="$mfail1"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_enabled, state.client, media.handle_task) = orig
        bot._context[rid].clear()
    assert any("没能处理" in m and "下载失败" in m for m in sent)   # 文件失败且无 caption → 回错误


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
            bot.projects.get_project, dispatch.runner)
    bot.projects.list_projects = lambda: [app]
    bot.projects.ensure_project = fake_ensure
    bot.projects.get_project = lambda pid: app if pid == "h/o/app" else None
    dispatch.runner = R()
    try:
        out = asyncio.run(bot._dispatch(room, make_event("帮我看看那个东西"), "帮我看看那个东西"))
    finally:
        (bot.projects.list_projects, bot.projects.ensure_project,
         bot.projects.get_project, dispatch.runner) = orig
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

    orig = (bot.projects.list_projects, bot.projects.ensure_project, dispatch._triage)
    bot.projects.list_projects = lambda: [app]
    bot.projects.ensure_project = fake_ensure
    dispatch._triage = triage_none
    try:
        out = asyncio.run(bot._dispatch(room, make_event("那个 app 跑不起来了"), "那个 app 跑不起来了"))
    finally:
        (bot.projects.list_projects, bot.projects.ensure_project, dispatch._triage) = orig
    assert out is app                     # 裸仓库名兜底命中
    assert order == ["triage", "ensure"]  # 先分诊、放弃后才用裸名兜底


# ---------- 25) 主动插话判定 PASS 时只占用短冷却，不整段沉默 ----------
def test_proactive_pass_keeps_short_cooldown():
    set_identity()
    rid = "!pc:ex.org"
    room = FakeRoom(rid, 3)
    bot._context[rid].clear()
    bot._last_proactive[rid] = 0.0
    orig = (proactive.runner, state.client, settings.proactive_cooldown)
    settings.proactive_cooldown = 600

    class R:
        async def quick(self, prompt):
            return "__PASS__"

    class FC:
        async def room_send(self, *a, **k):
            return types.SimpleNamespace(event_id="$x")

    proactive.runner, state.client = R(), FC()
    try:
        asyncio.run(bot.maybe_proactive(room, make_event("报错了帮我看看"), "报错了帮我看看"))
    finally:
        (proactive.runner, state.client, settings.proactive_cooldown) = orig
    remaining = 600 - (time.time() - bot._last_proactive[rid])
    assert 0 < remaining <= bot._PROACTIVE_PASS_COOLDOWN + 2, remaining   # PASS 后很快能重判


# ---------- 25b) PROACTIVE_REQUIRE_HINT：关掉后能评估"没人求助但话里有错"的陈述句 ----------
def test_proactive_require_hint_toggle():
    set_identity()
    rid = "!ph:ex.org"
    room = FakeRoom(rid, 3)
    bot._context[rid].clear()
    # 纯陈述句、无任何求助/报错词，但内容是技术错误 → 关键词预筛会拦它
    msg = "这个 list 多线程 append 完全安全，随便并发写都没问题"
    assert not bot._looks_actionable(msg)
    calls = []

    class R:
        async def quick(self, prompt):
            calls.append(prompt)
            return "其实 list 并发写不是线程安全的，高并发 append 可能丢元素，建议加锁或用 queue。"

    class FC:
        async def room_send(self, r, mt, content, **k):
            return types.SimpleNamespace(event_id="$x")

    orig = (proactive.runner, state.client, bot.projects.get_room,
            settings.proactive_require_hint, settings.proactive_cooldown,
            settings.transcript_enabled)
    proactive.runner, state.client = R(), FC()
    bot.projects.get_room = lambda r: None     # 未绑库 → 走 quick 文本判断
    settings.proactive_cooldown = 600
    settings.transcript_enabled = False        # 别在测试里写 store/transcripts
    try:
        settings.proactive_require_hint = True   # 预筛开：非求助句被挡下，不评估
        bot._last_proactive[rid] = 0.0
        asyncio.run(bot.maybe_proactive(room, make_event(msg), msg))
        assert calls == [], "require_hint=True 时不该评估非求助消息"

        settings.proactive_require_hint = False  # 预筛关：每条都评估，judge 纠错 → 会插话
        bot._last_proactive[rid] = 0.0
        asyncio.run(bot.maybe_proactive(room, make_event(msg), msg))
        assert len(calls) == 1, "require_hint=False 时应评估并纠正"
    finally:
        (proactive.runner, state.client, bot.projects.get_room,
         settings.proactive_require_hint, settings.proactive_cooldown,
         settings.transcript_enabled) = orig


# ---------- 26) 已绑定群里再发裸 URL：给换绑提示而非静默无反应 ----------
def test_group_rebind_hint():
    set_identity()
    state._synced = True
    rid = "!gb:ex.org"
    room = FakeRoom(rid, 3)
    bot._context[rid].clear()
    bound = {"id": "gitea.example.com/o/old", "owner": "o", "repo": "old"}
    msgs, pending = [], []
    orig = (settings.gitea_host, bot.projects.get_room, state.client, state._spawn)
    settings.gitea_host = "https://gitea.example.com"
    bot.projects.get_room = lambda r: bound

    class FC:
        async def room_send(self, r, mt, content, **k):
            msgs.append(content["body"])
            return types.SimpleNamespace(event_id="$x")

    state.client = FC()
    state._spawn = lambda coro: pending.append(coro)
    try:
        async def go():
            await bot.on_message(room, make_event("https://gitea.example.com/o/new"))
            for c in pending:
                await c
        asyncio.run(go())
    finally:
        (settings.gitea_host, bot.projects.get_room, state.client, state._spawn) = orig
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
            bot.projects.get_project, dispatch._triage)
    bot.projects.list_projects = lambda: [appA]
    bot.projects.ensure_project = fake_ensure
    bot.projects.get_project = lambda pid: appA if pid == "h/o/appA" else None
    dispatch._triage = triage_none
    bot._last_project_by_room[rid] = "h/o/appA"           # 上一轮已路由到 appA
    try:                                                  # 延续消息没提任何项目名/链接
        out = asyncio.run(bot._dispatch(room, make_event("再补个单元测试吧"), "再补个单元测试吧"))
    finally:
        (bot.projects.list_projects, bot.projects.ensure_project,
         bot.projects.get_project, dispatch._triage) = orig
        bot._last_project_by_room.pop(rid, None)
    assert out is appA and ensured == [appA]              # 沿用上次项目而不是反问


# ---------- 29) 群里"@bot 仓库URL 任务"一条消息：先绑再派，不再答非所问 ----------
def test_group_url_with_task_binds():
    set_identity()
    state._synced = True
    rid = "!gbind:ex.org"
    room = FakeRoom(rid, 3)                               # 群、未绑定
    orig_host = settings.gitea_host
    orig = (bot.projects.get_room, bot.do_bind, state._spawn)
    settings.gitea_host = "https://gitea.example.com"
    bot.projects.get_room = lambda r: None
    captured = {}

    async def fake_do_bind(room, repo, event=None, task_text=""):
        captured["repo"], captured["task"] = repo, task_text

    bot.do_bind = fake_do_bind
    pend = []
    state._spawn = lambda coro: pend.append(coro)
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
        (bot.projects.get_room, bot.do_bind, state._spawn) = orig
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

    orig = state.client
    state.client = FC()
    try:
        asyncio.run(bot.send(rid, "答复", track=True))
    finally:
        state.client = orig
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
        def busy(self, k): return False
        def running(self, k): return 0
        async def ask(self, key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                      on_delta=None, cancel_key=None):
            captured["prompt"] = prompt
            return "ok"

    class FC:
        async def room_send(self, *a, **k):
            return types.SimpleNamespace(event_id="$x")

    rec = {"id": "p", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}
    orig = (tasks.runner, state.client)
    tasks.runner, state.client = R(), FC()
    try:
        ev = make_media_event(body="a.log", event_id="$mm")
        asyncio.run(bot._run_on_project(room, ev, "看看这个文件 /m/a.log", rec, skip_body=line))
    finally:
        (tasks.runner, state.client) = orig
        bot._context[rid].clear()
    assert "[文件] a.log" not in captured["prompt"]        # 媒体行不再重复出现在 prompt
    assert "之前聊的别的事" in captured["prompt"]           # 其它背景仍照常带上


# ---------- 35b) 排队回执：项目锁被占时立即知会"已排队"，空闲时不发 ----------
def test_queue_receipt_when_busy():
    set_identity()
    rid = "!queue:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    sent = []

    class R:
        def __init__(self): self.is_busy = False; self.n_running = 0
        def busy(self, k): return self.is_busy
        def running(self, k): return self.n_running
        async def ask(self, key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                      on_delta=None, cancel_key=None):
            return "ok"

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    rec = {"id": "p", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}
    orig = (tasks.runner, state.client, settings.stream_replies)
    r = R()
    tasks.runner, state.client = r, FC()
    settings.stream_replies = False
    try:
        ev = make_event("先干个活")
        asyncio.run(bot._run_on_project(room, ev, "先干个活", rec))
        assert not any("已排队" in m for m in sent)            # 空闲：不发回执

        r.is_busy, r.n_running = True, 1                       # 忙且本房间在跑 → 回执 + /cancel 提示
        asyncio.run(bot._run_on_project(room, ev, "再来一个", rec))
        assert any("已排队" in m and "/cancel" in m for m in sent)

        sent.clear()
        r.n_running = 0                                        # 忙但占用来自别处 → 说明 /cancel 停不了
        asyncio.run(bot._run_on_project(room, ev, "第三个", rec))
        assert any("已排队" in m and "停不了" in m for m in sent)
    finally:
        (tasks.runner, state.client, settings.stream_replies) = orig
        bot._context[rid].clear()


# ---------- 36) 未声明 size 的大文件：流式落盘后按真实大小拦下，不静默吃内存 ----------
def test_media_oversize_undeclared_streamed():
    import tempfile
    set_identity()
    state._synced = True
    rid = "!mbig:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    orig = (settings.media_root, settings.media_max_mb, settings.media_enabled,
            state.client, media.handle_task)
    settings.media_root = tempfile.mkdtemp()
    settings.media_max_mb, settings.media_enabled = 1, True

    class FC:
        async def download(self, mxc=None, save_to=None, **k):
            with open(save_to, "wb") as f:                # 模拟 nio 流式落盘：写 2MB、不声明 size
                f.write(b"x" * (2 * 1024 * 1024))
            return types.SimpleNamespace(body=save_to, content_type="application/octet-stream")

    async def fake_handle(rm, ev, text, skip_body=None):
        pass

    state.client, media.handle_task = FC(), fake_handle
    try:
        async def go():
            await bot._process_media(room, make_media_event(body="big.bin", event_id="$ub"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_max_mb, settings.media_enabled,
         state.client, bot.handle_task) = orig
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

    orig = (bot.projects.list_projects, dispatch._triage, state.client)
    bot.projects.list_projects = lambda: [app]
    dispatch._triage = triage_general
    state.client = FC()
    try:
        out = asyncio.run(bot._dispatch(room, make_event("用 Python 写个快排"), "用 Python 写个快排"))
    finally:
        (bot.projects.list_projects, dispatch._triage, state.client) = orig
    assert out is not None and out.get("general") is True   # 返回通用助手 rec
    assert out["path"] == settings.claude_workdir            # 在隔离 scratch 目录答
    assert not any("哪个项目" in m for m in asked)           # 没有反问


# ---------- 43b) DM 发 /bind：给引导，不把 "/bind http://…" 当任务喂给 Claude ----------
def test_dm_bind_gets_guidance():
    set_identity()
    state._synced = True
    rid = "!dmbind:ex.org"
    room = FakeRoom(rid, 2)                     # DM
    bot._context[rid].clear()
    sent, task_calls, pend = [], [], []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    async def fake_task(*a, **k):
        task_calls.append(a)

    orig = (state.client, state._spawn, bot.handle_task)
    state.client = FC()
    state._spawn = lambda coro: pend.append(coro)
    bot.handle_task = fake_task
    try:
        async def go():
            await bot.on_message(room, make_event(
                "/bind https://gitea.example.com/o/r", event_id="$dmb"))
            for c in pend:
                await c
        asyncio.run(go())
    finally:
        (state.client, state._spawn, bot.handle_task) = orig
        bot._context[rid].clear()
    assert any("私聊不用绑定" in m for m in sent)   # 得到引导
    assert task_calls == []                          # 没被当任务派给 Claude


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


def _reset_issue_ledger():
    import issue_ledger
    issue_ledger._data = {}
    issue_ledger._loaded = False


# ---------- 自驱心跳：PASS 不打扰；有建议→提议；autopilot→派执行 ----------
def test_heartbeat_propose_and_autopilot():
    set_identity()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent, spawned = [], []

    class R:
        def __init__(self, reply): self.reply = reply
        async def consult(self, prompt, cwd=None, system_prompt=None): return self.reply

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (heartbeat.runner, state.client, state._spawn, settings.proactive_autopilot)
    state.client = FC()
    state._spawn = lambda coro: (spawned.append(1), coro.close())
    try:
        heartbeat.runner = R("__PASS__")                       # 没值得做的 → 不发言、不派活
        settings.proactive_autopilot = False
        asyncio.run(bot._heartbeat_one(rec, "!room"))
        assert not sent and not spawned

        heartbeat.runner = R("建议：给 X 补个单元测试")          # 有建议 + autopilot 关 → 只提议
        asyncio.run(bot._heartbeat_one(rec, "!room"))
        assert any("巡检建议" in m for m in sent) and not spawned

        sent.clear()
        heartbeat.runner = R("建议：给 X 补个单元测试")          # 有建议 + autopilot 开 → 宣布开干并派执行
        settings.proactive_autopilot = True
        asyncio.run(bot._heartbeat_one(rec, "!room"))
        assert spawned and any("自驱" in m for m in sent)
    finally:
        (heartbeat.runner, state.client, state._spawn, settings.proactive_autopilot) = orig


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

    orig = (bot.projects.get_project, state.client, state._spawn,
            gitea.pr_info, gitea.pr_reviews, gitea.ci_state)
    bot.projects.get_project = lambda pid: rec if pid == "h/o/r" else None
    state.client = FC()
    state._spawn = lambda coro: (spawned.append(1), coro.close())
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
        (bot.projects.get_project, state.client, state._spawn,
         gitea.pr_info, gitea.pr_reviews, gitea.ci_state) = orig
        settings.store_path = orig_store
        _reset_ledger()


# ---------- PR 台账：连续 3 轮确切 404 才销账并通知；中途成功清零；网络抖动一轮都不攒 ----------
def test_pr_gone_after_three_404():
    import tempfile
    import pr_ledger
    import gitea
    set_identity()
    orig_store, orig_am = settings.store_path, settings.pr_automerge
    settings.store_path = tempfile.mkdtemp()
    settings.pr_automerge = False                      # 不走合并路径，专测销账逻辑
    _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.get_project, state.client, state._spawn,
            gitea.pr_info, gitea.pr_reviews, gitea.ci_state, gitea.pr_gone)
    bot.projects.get_project = lambda pid: rec if pid == "h/o/r" else None
    state.client = FC()
    state._spawn = lambda coro: coro.close()
    async def none_info(r, n): return None            # pr_info 查不到
    async def is_gone(r, n): return True              # 确切 404
    async def not_gone(r, n): return False            # 网络抖动（查不到但非 404）
    async def no_reviews(r, n): return []
    async def no_ci(r, s): return ""
    e = lambda num: [x for x in pr_ledger.active() if x["number"] == num]
    try:
        gitea.pr_info, gitea.pr_gone = none_info, is_gone
        gitea.pr_reviews, gitea.ci_state = no_reviews, no_ci

        # 连续 3 轮确切 404 → 才销账 + 通知
        pr_ledger.record("h/o/r", 1, "u1", "!room")
        asyncio.run(bot._followup_one(e(1)[0]))
        assert e(1) and e(1)[0]["gone_rounds"] == 1
        asyncio.run(bot._followup_one(e(1)[0]))
        assert e(1) and e(1)[0]["gone_rounds"] == 2 and not any("已不存在" in m for m in sent)
        asyncio.run(bot._followup_one(e(1)[0]))
        assert not e(1) and any("PR #1" in m and "已不存在" in m for m in sent)   # 3 轮 → 销账 + 报

        # 网络抖动（非 404）：一轮都不攒，永远不销
        gitea.pr_gone = not_gone
        pr_ledger.record("h/o/r", 2, "u2", "!room")
        for _ in range(5):
            asyncio.run(bot._followup_one(e(2)[0]))
        assert e(2) and e(2)[0]["gone_rounds"] == 0

        # 中途成功查到一次 → 之前攒的 404 轮数清零
        gitea.pr_gone = is_gone
        pr_ledger.update("h/o/r", 2, gone_rounds=2)
        async def open_info(r, n):
            return {"state": "open", "merged": False, "mergeable": True,
                    "head": {"ref": "b", "sha": "s"}}
        gitea.pr_info = open_info
        asyncio.run(bot._followup_one(e(2)[0]))
        assert e(2) and e(2)[0]["gone_rounds"] == 0
    finally:
        (bot.projects.get_project, state.client, state._spawn,
         gitea.pr_info, gitea.pr_reviews, gitea.ci_state, gitea.pr_gone) = orig
        settings.store_path, settings.pr_automerge = orig_store, orig_am
        _reset_ledger()


# ---------- 工单台账：登记/去重/更新/销账 + 持久化 ----------
def test_issue_ledger():
    import tempfile
    import issue_ledger
    orig = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_issue_ledger()
    try:
        assert issue_ledger.record("h/o/r", 3, "http://x/issues/3", "!room") is True
        assert issue_ledger.record("h/o/r", 3, "http://x/issues/3", "!room") is False   # 不重复接单
        assert issue_ledger.taken("h/o/r", 3) and not issue_ledger.taken("h/o/r", 4)
        issue_ledger.update("h/o/r", 3, pr=9)
        _reset_issue_ledger()                                    # 清内存态 → 必须能从盘恢复
        got = issue_ledger.active()
        assert got and got[0]["number"] == 3 and got[0]["pr"] == 9
        issue_ledger.remove("h/o/r", 3)
        assert issue_ledger.active() == []
    finally:
        settings.store_path = orig
        _reset_issue_ledger()


# ---------- 工单接活：指派的 issue → 登记 + 认领留言 + 房间宣布 + 派执行；已接过不重复 ----------
def test_issue_intake_flow():
    import tempfile
    import issue_ledger
    import gitea
    set_identity()
    orig_store = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_issue_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent, spawned, claimed = [], [], []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (state.client, state._spawn, gitea.assigned_issues, gitea.comment_issue)
    state.client = FC()
    state._spawn = lambda coro: (spawned.append(1), coro.close())
    async def issues(r, login): return [{"number": 3, "title": "登录太慢", "html_url": "http://h/o/r/issues/3"}]
    async def comment(r, n, body): claimed.append((n, body)); return True
    gitea.assigned_issues, gitea.comment_issue = issues, comment
    try:
        asyncio.run(bot._intake_one(rec, "!room", "claudebot"))
        assert issue_ledger.taken("h/o/r", 3)                              # 已登记
        assert claimed and claimed[0][0] == 3 and "认领" in claimed[0][1]   # issue 下留言认领
        assert spawned and any("工单 #3" in m for m in sent)               # 房间宣布 + 派执行
        sent.clear(); spawned.clear()
        asyncio.run(bot._intake_one(rec, "!room", "claudebot"))            # 下轮又轮询到同一单 → 不重复接
        assert not spawned and not sent
    finally:
        (state.client, state._spawn, gitea.assigned_issues, gitea.comment_issue) = orig
        settings.store_path = orig_store
        _reset_issue_ledger()


# ---------- 工单执行：开 PR → 进 PR 台账 + 工单记 PR + issue 贴链接；关单后 sweep 销账 ----------
def test_issue_execute_and_sweep():
    import tempfile
    import issue_intake
    import issue_ledger
    import pr_ledger
    import gitea
    set_identity()
    orig_store = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_issue_ledger(); _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent, comments = [], []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")
        async def room_typing(self, r, *a, **k): return None

    class R:
        async def ask(self, key, prompt, **k):
            assert "Closes #3" in prompt                          # 提示词要求 PR 带关单标记
            return "已修复并开 PR：http://h/o/r/pulls/9"

    orig = (state.client, issue_intake.runner, gitea.issue_comments, gitea.comment_issue,
            bot.projects.get_project, gitea.issue_info)
    state.client = FC()
    issue_intake.runner = R()
    async def no_comments(r, n): return []
    async def comment(r, n, body): comments.append(body); return True
    gitea.issue_comments, gitea.comment_issue = no_comments, comment
    try:
        issue_ledger.record("h/o/r", 3, "http://h/o/r/issues/3", "!room")
        asyncio.run(bot._issue_execute(rec, "!room", {"number": 3, "title": "登录太慢", "body": "太慢了"}))
        assert any("工单 #3" in m and "pulls/9" in m for m in sent)   # 结果回报房间
        assert any(e["number"] == 9 for e in pr_ledger.active())      # PR 进台账 → 跟进循环盯到合并
        assert issue_ledger.active()[0]["pr"] == 9                    # 工单记下 PR 号
        assert any("pulls/9" in c for c in comments)                  # issue 下贴了 PR 链接

        bot.projects.get_project = lambda pid: rec                    # 关单 → sweep 销账
        async def closed(r, n): return {"state": "closed"}
        gitea.issue_info = closed
        asyncio.run(issue_intake._sweep_closed())
        assert issue_ledger.active() == []
    finally:
        (state.client, issue_intake.runner, gitea.issue_comments, gitea.comment_issue,
         bot.projects.get_project, gitea.issue_info) = orig
        settings.store_path = orig_store
        _reset_issue_ledger(); _reset_ledger()


# ---------- 工单台账：连续 3 轮确切 404 才销账并通知；中途成功清零；网络抖动一轮都不攒 ----------
def test_issue_gone_after_three_404():
    import tempfile
    import issue_intake
    import issue_ledger
    import gitea
    set_identity()
    orig_store = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_issue_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (state.client, bot.projects.get_project, gitea.issue_info, gitea.issue_gone)
    state.client = FC()
    bot.projects.get_project = lambda pid: rec if pid == "h/o/r" else None
    async def none_info(r, n): return None
    async def is_gone(r, n): return True
    async def not_gone(r, n): return False
    a = lambda num: [x for x in issue_ledger.active() if x["number"] == num]
    try:
        gitea.issue_info, gitea.issue_gone = none_info, is_gone

        # 连续 3 轮确切 404 → 才销账 + 通知
        issue_ledger.record("h/o/r", 3, "u3", "!room")
        asyncio.run(issue_intake._sweep_closed())
        assert a(3) and a(3)[0]["gone_rounds"] == 1
        asyncio.run(issue_intake._sweep_closed())
        assert a(3) and a(3)[0]["gone_rounds"] == 2 and not any("已不存在" in m for m in sent)
        asyncio.run(issue_intake._sweep_closed())
        assert not a(3) and any("工单 #3" in m and "已不存在" in m for m in sent)

        # 网络抖动（非 404）：一轮都不攒
        gitea.issue_gone = not_gone
        issue_ledger.record("h/o/r", 4, "u4", "!room")
        for _ in range(4):
            asyncio.run(issue_intake._sweep_closed())
        assert a(4) and a(4)[0]["gone_rounds"] == 0

        # 中途成功查到（未关）→ 清零、留在册
        gitea.issue_gone = is_gone
        issue_ledger.update("h/o/r", 4, gone_rounds=2)
        async def open_issue(r, n): return {"state": "open"}
        gitea.issue_info = open_issue
        asyncio.run(issue_intake._sweep_closed())
        assert a(4) and a(4)[0]["gone_rounds"] == 0
    finally:
        (state.client, bot.projects.get_project, gitea.issue_info, gitea.issue_gone) = orig
        settings.store_path = orig_store
        _reset_issue_ledger()


def _reset_gitea_health():
    import gitea
    import gitea_health
    gitea._health.update(consecutive_failures=0, last_success_ts=0.0,
                         last_failure_ts=0.0, last_code=0, last_kind="")
    gitea_health._alerted = False


# ---------- Gitea 健康度埋点：失败累计/成功清零、401 定性 token、404 不计入失败、网络/5xx 区分 ----------
def test_gitea_health_accounting():
    import gitea
    import urllib.error
    _reset_gitea_health()
    mode = {"v": "ok"}

    def fake_get(url):
        m = mode["v"]
        if m == "ok":
            return 200, {}
        if m == "net":
            raise urllib.error.URLError("refused")           # 连不上
        codes = {"notfound": 404, "auth": 401, "forbidden": 403, "server": 502}
        raise urllib.error.HTTPError(url, codes[m], m, None, None)

    orig = gitea._get
    gitea._get = fake_get
    try:
        # 401 连续失败：累计 + 定性 auth（token 问题）+ ok=False
        for _ in range(3):
            mode["v"] = "auth"; asyncio.run(gitea._aget("u"))
        h = gitea.health()
        assert h["consecutive_failures"] == 3 and h["last_kind"] == "auth" and h["last_code"] == 401
        assert h["ok"] is False

        # 403 同样定性成 token 问题
        mode["v"] = "forbidden"; asyncio.run(gitea._aget("u"))
        assert gitea.health()["last_kind"] == "auth" and gitea.health()["last_code"] == 403

        # 一次成功（2xx）→ 清零、记 last_success、ok 恢复
        mode["v"] = "ok"; st, _ = asyncio.run(gitea._aget("u"))
        h = gitea.health()
        assert st == 200 and h["consecutive_failures"] == 0 and h["ok"] is True and h["last_success_ts"] > 0

        # 404 是"对象不存在"的业务答案：不计入失败，反而算"活着"→ 清零
        mode["v"] = "auth"; asyncio.run(gitea._aget("u"))
        assert gitea.health()["consecutive_failures"] == 1
        mode["v"] = "notfound"; st, d = asyncio.run(gitea._aget("u"))
        assert st == 404 and d is None and gitea.health()["consecutive_failures"] == 0

        # 网络层错误 → kind=network、code=0（连不上，与 token 失效区分）
        mode["v"] = "net"; st, _ = asyncio.run(gitea._aget("u"))
        h = gitea.health()
        assert st == 0 and h["last_kind"] == "network" and h["last_code"] == 0 and h["consecutive_failures"] == 1

        # 5xx → kind=http（连上了但服务器不正常），与网络/鉴权都区分
        mode["v"] = "server"; st, _ = asyncio.run(gitea._aget("u"))
        assert st == 502 and gitea.health()["last_kind"] == "http" and gitea.health()["last_code"] == 502
        assert gitea.health()["consecutive_failures"] == 2   # 网络那笔 + 这笔，连续累计
    finally:
        gitea._get = orig
        _reset_gitea_health()


# ---------- Gitea 健康度：/status 两种形态 + 跨阈值告警只发一次 + 恢复通知 ----------
def test_gitea_health_status_and_alert():
    import gitea
    import gitea_health
    set_identity()
    _reset_gitea_health()
    orig_host = settings.gitea_host
    settings.gitea_host = "https://gitea.example.com"
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    # 两个绑了项目的房间收告警（验证"各发一条"）
    orig_rooms = dict(bot.projects._rooms)
    orig_routed = dict(state._last_project_by_room)
    orig_client = state.client
    bot.projects._rooms.clear(); state._last_project_by_room.clear()
    bot.projects._rooms["!ga:ex.org"] = "h/o/r"
    bot.projects._rooms["!gb:ex.org"] = "h/o/r2"
    state.client = FC()
    try:
        # 形态一：健康 → "正常"
        assert gitea_health.status_line(gitea.health()) == "• Gitea：正常"

        # 未跨阈值（4 次）→ 不告警
        for _ in range(4):
            gitea._note_failure(401, "auth")
        asyncio.run(gitea_health.check_and_alert())
        assert not any("连不上" in m for m in sent)     # 4 次还不够阈值，忍住不吵

        # 形态二：连续失败 → 点名 token + 最近成功多久前
        gitea._health["last_success_ts"] = time.time() - 600
        gitea._note_failure(401, "auth")                 # 第 5 次，跨阈值
        line = gitea_health.status_line(gitea.health())
        assert "连续 5 次失败" in line and "token" in line and "前" in line

        # 跨阈值 → 两个房间各告警一次
        asyncio.run(gitea_health.check_and_alert())
        assert sum("Gitea 连不上" in m for m in sent) == 2
        # 仍在失败：再巡检一轮不重复刷屏
        gitea._note_failure(0, "network")
        asyncio.run(gitea_health.check_and_alert())
        assert sum("Gitea 连不上" in m for m in sent) == 2

        # 恢复 → 两个房间各发一条"已恢复"，且只发一次
        gitea._note_alive()
        assert gitea_health.status_line(gitea.health()) == "• Gitea：正常"
        asyncio.run(gitea_health.check_and_alert())
        assert sum("已恢复" in m for m in sent) == 2
        asyncio.run(gitea_health.check_and_alert())
        assert sum("已恢复" in m for m in sent) == 2
    finally:
        settings.gitea_host = orig_host
        bot.projects._rooms.clear(); bot.projects._rooms.update(orig_rooms)
        state._last_project_by_room.clear(); state._last_project_by_room.update(orig_routed)
        state.client = orig_client
        _reset_gitea_health()


# ---------- Gitea 健康度：/status 命令确实带上这条（异常态） ----------
def test_status_shows_gitea_health():
    import tempfile
    import pr_ledger
    import gitea
    set_identity()
    _reset_gitea_health()
    orig_store, orig_host = settings.store_path, settings.gitea_host
    settings.store_path = tempfile.mkdtemp()
    settings.gitea_host = "https://gitea.example.com"
    _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.get_room, state.client)
    bot.projects.get_room = lambda rid: rec
    state.client = FC()
    try:
        gitea._health["last_success_ts"] = time.time() - 600
        for _ in range(5):
            gitea._note_failure(401, "auth")
        asyncio.run(bot.handle_status(FakeRoom("!g:ex.org", 3)))
        out = "\n".join(sent)
        assert "Gitea" in out and "连续 5 次失败" in out and "token" in out   # /status 暴露连通性
    finally:
        bot.projects.get_room, state.client = orig
        settings.store_path, settings.gitea_host = orig_store, orig_host
        _reset_ledger()
        _reset_gitea_health()


# ---------- 模型拆分：干活用 CLAUDE_MODEL，轻判断（quick/consult）优先 CLAUDE_QUICK_MODEL ----------
def test_quick_model_split():
    from claude_runner import runner
    orig = (settings.claude_model, settings.claude_quick_model)
    settings.claude_model, settings.claude_quick_model = "opus", "haiku"
    try:
        agentic = runner._cmd("p", None, agentic=True)
        quick = runner._cmd("p", None, agentic=False)
        ro = runner._cmd_ro("p")
        assert agentic[agentic.index("--model") + 1] == "opus"    # 干活用大模型
        assert quick[quick.index("--model") + 1] == "haiku"       # 轻判断用小模型
        assert ro[ro.index("--model") + 1] == "haiku"             # 只读查证同轻判断
        settings.claude_quick_model = ""                          # 没拆时跟随 CLAUDE_MODEL
        q2 = runner._cmd("p", None, agentic=False)
        assert q2[q2.index("--model") + 1] == "opus"
        settings.claude_model = ""                                # 都空 = 不带 --model
        assert "--model" not in runner._cmd("p", None, agentic=True)
    finally:
        settings.claude_model, settings.claude_quick_model = orig


# ---------- 聊天逐字记录：落盘/回溯指引/保留删旧/开关 ----------
def test_transcript_log_and_recall():
    import tempfile
    import transcript
    orig = (settings.store_path, settings.transcript_enabled,
            settings.transcript_keep_days, settings.transcript_max_lines)
    settings.store_path = tempfile.mkdtemp()
    settings.transcript_enabled = True
    settings.transcript_keep_days = 30
    settings.transcript_max_lines = 5000
    rid = "!room:ex.org"
    try:
        transcript.append(rid, "alice", "前天聊了部署", event_id="$1", ts=time.time() - 2 * 86400)
        transcript.append(rid, "bot", "对，部署到 pi.lan", ts=time.time() - 2 * 86400 + 1)
        transcript.append(rid, "alice", "今天的进展", event_id="$2")
        recs = transcript._read_all(rid)
        assert [r["body"] for r in recs] == ["前天聊了部署", "对，部署到 pi.lan", "今天的进展"]

        # 派活系统提示里要指向真实日志文件，让 Claude 按需读
        sp = transcript.augment_system_prompt("BASE", rid)
        assert sp.startswith("BASE") and transcript.path_for(rid) in sp

        # 保留删旧：超 keep_days 的行被 prune 丢弃，近的留着
        settings.transcript_keep_days = 1
        transcript.append(rid, "old", "很久以前", event_id="$old", ts=time.time() - 5 * 86400)
        transcript._prune(rid)
        bodies = [r["body"] for r in transcript._read_all(rid)]
        assert "很久以前" not in bodies and "今天的进展" in bodies

        # 关闭开关：不再落盘、也不注入指引
        settings.transcript_enabled = False
        before = len(transcript._read_all(rid))
        transcript.append(rid, "alice", "关了不该记", event_id="$3")
        assert len(transcript._read_all(rid)) == before
        assert transcript.augment_system_prompt("BASE", rid) == "BASE"
    finally:
        (settings.store_path, settings.transcript_enabled,
         settings.transcript_keep_days, settings.transcript_max_lines) = orig


# ---------- PR 自动合并：可合并+CI通过(或无CI)+无未决改动 → 合并销账；否则不动 ----------
def test_pr_automerge():
    import tempfile
    import pr_ledger
    import gitea
    set_identity()
    orig_store = settings.store_path
    orig_am = (settings.pr_automerge, settings.pr_merge_method)
    settings.store_path = tempfile.mkdtemp()
    settings.pr_automerge = True
    settings.pr_merge_method = "merge"
    _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent, merged = [], []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.get_project, state.client, state._spawn,
            gitea.pr_info, gitea.pr_reviews, gitea.ci_state, gitea.merge)
    bot.projects.get_project = lambda pid: rec if pid == "h/o/r" else None
    state.client = FC()
    state._spawn = lambda coro: coro.close()

    async def open_mergeable(r, n):
        return {"state": "open", "merged": False, "mergeable": True,
                "head": {"ref": "claude/x", "sha": "s"}}
    async def no_reviews(r, n): return []
    async def no_ci(r, s): return ""
    async def fake_merge(r, n, method="merge", delete_branch=False):
        merged.append((n, method)); return True, ""
    try:
        gitea.pr_info, gitea.pr_reviews, gitea.ci_state, gitea.merge = (
            open_mergeable, no_reviews, no_ci, fake_merge)
        # a) 可合并 + 无 CI + 无评审 → 合并 + 销账 + 报"已自动合并"
        pr_ledger.record("h/o/r", 1, "u1", "!room")
        asyncio.run(bot._followup_one([e for e in pr_ledger.active() if e["number"] == 1][0]))
        assert merged == [(1, "merge")]
        assert not any(e["number"] == 1 for e in pr_ledger.active())   # 销账
        assert any("已自动合并" in m for m in sent)

        # b) 未决 REQUEST_CHANGES（非新评审，不会走派跟进）→ 不合并、仍在册
        merged.clear()
        pr_ledger.record("h/o/r", 2, "u2", "!room")
        pr_ledger.update("h/o/r", 2, seen_review=10)   # 标记已"看过"，section 1 不再当新评审派活
        async def rc_review(r, n): return [{"id": 10, "state": "REQUEST_CHANGES", "body": "改"}]
        gitea.pr_reviews = rc_review
        asyncio.run(bot._followup_one([e for e in pr_ledger.active() if e["number"] == 2][0]))
        assert merged == [] and any(e["number"] == 2 for e in pr_ledger.active())

        # c) 不可合并（有冲突）→ 不合并、仍在册
        merged.clear()
        gitea.pr_reviews = no_reviews
        async def conflict(r, n):
            return {"state": "open", "merged": False, "mergeable": False,
                    "head": {"ref": "claude/x", "sha": "s"}}
        gitea.pr_info = conflict
        pr_ledger.record("h/o/r", 3, "u3", "!room")
        asyncio.run(bot._followup_one([e for e in pr_ledger.active() if e["number"] == 3][0]))
        assert merged == [] and any(e["number"] == 3 for e in pr_ledger.active())
    finally:
        (bot.projects.get_project, state.client, state._spawn,
         gitea.pr_info, gitea.pr_reviews, gitea.ci_state, gitea.merge) = orig
        settings.pr_automerge, settings.pr_merge_method = orig_am
        settings.store_path = orig_store
        _reset_ledger()


# ---------- 自动合并闸：ci_state 查询失败(None) 不当"CI 通过"放行，且不误报 CI 失败 ----------
def test_automerge_skips_on_ci_unknown():
    import tempfile
    import pr_ledger
    import gitea
    set_identity()
    orig_store = settings.store_path
    orig_am = (settings.pr_automerge, settings.pr_merge_method)
    settings.store_path = tempfile.mkdtemp()
    settings.pr_automerge = True
    settings.pr_merge_method = "merge"
    _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent, merged = [], []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.get_project, state.client, state._spawn,
            gitea.pr_info, gitea.pr_reviews, gitea.ci_state, gitea.merge)
    bot.projects.get_project = lambda pid: rec if pid == "h/o/r" else None
    state.client = FC()
    state._spawn = lambda coro: coro.close()

    async def open_mergeable(r, n):
        return {"state": "open", "merged": False, "mergeable": True,
                "head": {"ref": "claude/x", "sha": "s"}}
    async def no_reviews(r, n): return []
    async def ci_unknown(r, s): return None                   # CI 查询失败：状态未知
    async def fake_merge(r, n, method="merge", delete_branch=False):
        merged.append((n, method)); return True, ""
    try:
        gitea.pr_info, gitea.pr_reviews, gitea.ci_state, gitea.merge = (
            open_mergeable, no_reviews, ci_unknown, fake_merge)
        pr_ledger.record("h/o/r", 1, "u1", "!room")
        asyncio.run(bot._followup_one([e for e in pr_ledger.active() if e["number"] == 1][0]))
        assert merged == []                                   # CI 未知 → 绝不自动合并
        assert any(e["number"] == 1 for e in pr_ledger.active())   # 仍在册，等 CI 明朗
        assert not any("CI 失败" in m for m in sent)          # 也不误报 CI 失败
    finally:
        (bot.projects.get_project, state.client, state._spawn,
         gitea.pr_info, gitea.pr_reviews, gitea.ci_state, gitea.merge) = orig
        settings.pr_automerge, settings.pr_merge_method = orig_am
        settings.store_path = orig_store
        _reset_ledger()


# ---------- PR 冲突：首见告警一次，同 sha 不重复刷屏；换了 sha 会再报一次 ----------
def test_conflict_alert_once():
    import tempfile
    import pr_ledger
    import gitea
    set_identity()
    orig_store, orig_am = settings.store_path, settings.pr_automerge
    settings.store_path = tempfile.mkdtemp()
    settings.pr_automerge = True
    _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.get_project, state.client, state._spawn,
            gitea.pr_info, gitea.pr_reviews, gitea.ci_state)
    bot.projects.get_project = lambda pid: rec if pid == "h/o/r" else None
    state.client = FC()
    state._spawn = lambda coro: coro.close()
    async def no_reviews(r, n): return []
    async def no_ci(r, s): return ""
    head = {"sha": "s1"}
    async def conflict(r, n):
        return {"state": "open", "merged": False, "mergeable": False,
                "head": {"ref": "claude/x", "sha": head["sha"]}}
    e = lambda: [x for x in pr_ledger.active() if x["number"] == 1][0]
    try:
        gitea.pr_info, gitea.pr_reviews, gitea.ci_state = conflict, no_reviews, no_ci
        pr_ledger.record("h/o/r", 1, "u1", "!room")
        asyncio.run(bot._followup_one(e()))
        asyncio.run(bot._followup_one(e()))               # 同一 sha 再冲突一轮
        assert sum("有冲突" in m for m in sent) == 1        # 只告警一次，不每 180s 刷屏
        assert e()["conflict_seen"] == "s1"
        head["sha"] = "s2"                                 # 重推了新 commit（换 sha）
        asyncio.run(bot._followup_one(e()))
        assert sum("有冲突" in m for m in sent) == 2        # 新版本 → 允许再报一次
    finally:
        (bot.projects.get_project, state.client, state._spawn,
         gitea.pr_info, gitea.pr_reviews, gitea.ci_state) = orig
        settings.store_path, settings.pr_automerge = orig_store, orig_am
        _reset_ledger()


def _drain_and_run(coro):
    """跑一个协程，并把它 _spawn 出的后台任务收割干净（命令类走 _spawn）。"""
    async def go():
        await coro
        for _ in range(50):
            pending = [t for t in bot._tasks if not t.done()]
            if not pending:
                break
            await asyncio.gather(*pending, return_exceptions=True)
    asyncio.run(go())


class _CapClient:
    """记下所有 room_send 的完整 content（不只 body），供线程/编辑/附件断言。"""
    def __init__(self):
        self.sent = []
        self.uploaded = []
        self.rooms = {}

    async def room_typing(self, *a, **k):
        return None

    async def join(self, rid):
        self.rooms[rid] = types.SimpleNamespace(room_id=rid)   # 模拟 join 后房间进入 client.rooms
        return None

    async def room_send(self, rid, mt, content, **k):
        self.sent.append(content)
        return types.SimpleNamespace(event_id="$e%d" % len(self.sent))

    async def upload(self, provider, content_type="application/octet-stream",
                     filename=None, encrypt=False, filesize=None):
        f = provider(0, 0); f.read(); f.close()      # 确认文件真被读取
        self.uploaded.append((filename, content_type, encrypt))
        return types.SimpleNamespace(content_uri="mxc://ex.org/up%d" % len(self.uploaded)), None


# ---------- 新增能力 1) 线程化回复 ----------
def test_thread_helpers_and_send():
    set_identity()
    assert bot._thread_root_of(make_event("hi", event_id="$root1")) == "$root1"     # 不在线程→自身作根
    in_thread = types.SimpleNamespace(event_id="$x", source={"content": {
        "m.relates_to": {"rel_type": "m.thread", "event_id": "$realroot"}}})
    assert bot._thread_root_of(in_thread) == "$realroot"                            # 已在线程→沿用根
    rel = bot._thread_rel("$r", "$prev")
    assert rel["rel_type"] == "m.thread" and rel["event_id"] == "$r"
    assert rel["m.in_reply_to"]["event_id"] == "$prev" and rel["is_falling_back"] is True
    assert bot._thread_rel(None) is None

    c = _CapClient(); state.client = c
    asyncio.run(bot.send("!r:ex.org", "答复", thread_root="$root"))
    assert c.sent[0]["m.relates_to"]["rel_type"] == "m.thread"                      # 传了 root → 挂线程
    assert c.sent[0]["m.relates_to"]["event_id"] == "$root"
    c.sent.clear()
    asyncio.run(bot.send("!r:ex.org", "答复"))
    assert "m.relates_to" not in c.sent[0]                                          # 不传 → 顶层


def test_group_task_reply_threaded():
    set_identity()
    c = _CapClient(); state.client = c
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}
    bot.projects.get_room = lambda rid: rec
    async def fake_ensure(info):
        return rec
    bot.projects.ensure_project = fake_ensure
    async def fake_ask(key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                       on_delta=None, cancel_key=None):
        return "搞定"
    bot.runner.ask = fake_ask
    room = FakeRoom("!g2:ex.org", 3)
    ev = make_event("@claude-bot 干活", mentions=["@claudebot:ex.org"], event_id="$Q")
    orig = settings.stream_replies; settings.stream_replies = False
    try:
        asyncio.run(bot.handle_task(room, ev, "干活"))
    finally:
        settings.stream_replies = orig
    ans = [m for m in c.sent if m.get("body") == "搞定"]
    assert ans and ans[0]["m.relates_to"]["rel_type"] == "m.thread"
    assert ans[0]["m.relates_to"]["event_id"] == "$Q"                              # 群里挂到提问那条


# ---------- 新增能力 2) /help + 进房欢迎 ----------
def test_help_and_welcome():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    _drain_and_run(bot.on_message(FakeRoom("!h:ex.org", 3), make_event("/help", event_id="$h")))
    assert any("我能干嘛" in (m.get("body") or "") for m in c.sent)
    c.sent.clear()
    inv = types.SimpleNamespace(state_key=state.MY_ID, membership="invite", sender="@x:ex.org")
    _drain_and_run(bot.on_invite(FakeRoom("!inv:ex.org", 2), inv))   # 欢迎语现为后台任务，收割它
    assert any("/help" in (m.get("body") or "") for m in c.sent)                   # 进房打招呼指到 /help


# ---------- 新增能力 3) /summarize ----------
def test_summarize_command():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    cap = {}
    async def fake_quick(prompt):
        cap["p"] = prompt
        return "• 聊了登录\n• 待办：修 token"
    bot.runner.quick = fake_quick
    bot._context["!s:ex.org"].clear()
    bot._context["!s:ex.org"].append((time.time(), "Alice", "登录会掉线"))
    bot._context["!s:ex.org"].append((time.time(), "Bob", "明天修"))
    orig = settings.transcript_enabled; settings.transcript_enabled = False
    try:
        _drain_and_run(bot.on_message(FakeRoom("!s:ex.org", 3), make_event("/summarize 10", event_id="$sm")))
    finally:
        settings.transcript_enabled = orig
    assert any("最近对话小结" in (m.get("body") or "") for m in c.sent)
    assert "登录会掉线" in cap["p"] and "/summarize" not in cap["p"]               # 带上下文、且不含命令本身


# ---------- 新增能力 4) /cancel ----------
def test_cancel_command():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    calls = []
    bot.runner.cancel = lambda rid: (calls.append(rid) or 1)
    _drain_and_run(bot.on_message(FakeRoom("!c:ex.org", 3), make_event("/cancel", event_id="$cx")))
    assert calls == ["!c:ex.org"] and any("已停止" in (m.get("body") or "") for m in c.sent)
    calls.clear(); c.sent.clear()
    bot.runner.cancel = lambda rid: 0
    _drain_and_run(bot.on_message(FakeRoom("!c:ex.org", 3), make_event("/cancel", event_id="$cx2")))
    assert any("没有正在运行" in (m.get("body") or "") for m in c.sent)


# ---------- 新增能力 5) 附件回传 ----------
def test_emit_files_allowed_blocked_stripped():
    set_identity()
    import tempfile
    d = tempfile.mkdtemp(prefix="mxbot-files-")
    fp = os.path.join(d, "chart.png")
    with open(fp, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n")
    c = _CapClient(); state.client = c
    room = FakeRoom("!f:ex.org", 2)
    orig = settings.send_files_back; settings.send_files_back = True
    try:
        ans = asyncio.run(bot._emit_files(
            room, f"做好了\n[[send-file: {fp}]]\n外部 [[send-file: /etc/hostname]]", d, "$root"))
    finally:
        settings.send_files_back = orig
    assert "send-file" not in ans                                                  # 标记被抹掉
    assert c.uploaded and c.uploaded[0][0] == "chart.png" and c.uploaded[0][1].startswith("image/")
    imgs = [m for m in c.sent if m.get("msgtype") == "m.image"]
    assert imgs and imgs[0]["m.relates_to"]["rel_type"] == "m.thread"              # 附件也挂线程
    assert "不在允许目录内" in ans                                                 # /etc/... 被拦
    assert bot._within_allowed(fp, d) and not bot._within_allowed("/etc/hostname", d)


# ---------- 新增能力 6) 流式：占位→编辑→定稿 ----------
def test_live_reply_streams_and_finalizes():
    set_identity()
    c = _CapClient(); state.client = c
    live = bot._LiveReply("!lr:ex.org", thread_root="$root")
    async def go():
        await live.on_delta("正在看代码", None)        # 建占位（带线程）
        await live.finalize("最终答复", track=False)    # 定稿成 m.replace 编辑
    asyncio.run(go())
    assert c.sent[0]["m.relates_to"]["rel_type"] == "m.thread"                     # 占位挂线程
    edits = [m for m in c.sent if m.get("m.relates_to", {}).get("rel_type") == "m.replace"]
    assert edits and edits[-1]["m.new_content"]["body"] == "最终答复"             # 编辑成最终答复


def test_dm_no_projects_falls_to_general():
    """还没有任何已知项目时，DM 的一般性问题也该落到通用助手答，而不是反问"发个仓库地址"。"""
    set_identity()
    room = FakeRoom("!dmnone:ex.org", 2)                   # 2 人 → DM
    sent = []

    async def cap_send(rid, text, *a, **k):
        sent.append(text)

    orig = (bot.projects.list_projects, dispatch.send)
    bot.projects.list_projects = lambda: []                # 零项目
    dispatch.send = cap_send
    try:
        out = asyncio.run(bot._dispatch(room, make_event("怎么用 python 写快速排序"),
                                        "怎么用 python 写快速排序"))
    finally:
        (bot.projects.list_projects, dispatch.send) = orig
    assert out and out.get("general") is True              # 当通用助手答
    assert out["id"] == dispatch._GENERAL_ID
    assert not sent                                        # 不再反问"发个 Gitea 仓库地址"


# ---------- /status：项目 / 任务 / 在跟 PR / 主动性一屏可见 ----------
def test_status_command():
    import tempfile
    import pr_ledger
    set_identity()
    orig_store = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.get_room, state.client)
    bot.projects.get_room = lambda rid: rec
    state.client = FC()
    try:
        pr_ledger.record("h/o/r", 7, "http://h/o/r/pulls/7", "!g:ex.org")
        asyncio.run(bot.handle_status(FakeRoom("!g:ex.org", 3)))
        out = "\n".join(sent)
        assert "o/r" in out and "PR #7" in out            # 项目 + 在跟的 PR
        assert "没有正在跑的任务" in out and "自驱心跳" in out
    finally:
        bot.projects.get_room, state.client = orig
        settings.store_path = orig_store
        _reset_ledger()


# ---------- 流式定稿：编辑失败不吞答案，退回整条新发 ----------
def test_livereply_finalize_edit_fallback():
    set_identity()
    sent = []

    class FC:
        async def room_send(self, rid, mt, content, **k):
            if (content.get("m.relates_to") or {}).get("rel_type") == "m.replace":
                return types.SimpleNamespace(event_id=None, status_code="M_UNKNOWN")  # 编辑一律失败
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$e%d" % len(sent))

    state.client = FC()
    rid = "!lr:ex.org"
    bot._context[rid].clear()
    live = bot._LiveReply(rid)
    asyncio.run(live.on_delta("part", "Bash"))            # 生成占位消息
    asyncio.run(live.finalize("最终答案", track=True))
    assert any("最终答案" in b for b in sent)              # 占位编辑失败 → 答案作为新消息发出
    assert any(b == "最终答案" for _, s, b in bot._context[rid])   # 且照常入上下文


# ---------- 自驱 / PR 跟进任务的取消维度是房间：/cancel 才停得下来 ----------
def test_autonomous_tasks_cancellable_by_room():
    import pr_followup
    set_identity()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    seen, sent = [], []

    class FC:
        async def room_typing(self, *a, **k): return None
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    class R:
        async def ask(self, key, prompt, cwd=None, system_prompt=None, lock_key=None,
                      prepare=None, on_delta=None, cancel_key=None):
            seen.append(cancel_key); return "干完了，没开 PR"

    orig = (heartbeat.runner, pr_followup.runner, state.client)
    heartbeat.runner = pr_followup.runner = R()
    state.client = FC()
    try:
        asyncio.run(heartbeat._heartbeat_execute(rec, "!room", "修个小 bug"))
        asyncio.run(pr_followup._followup_dispatch(
            rec, {"pid": "h/o/r", "number": 3, "room": "!room", "branch": "claude/x"}, "有评审"))
        assert seen == ["!room", "!room"]                  # 取消维度=汇报房间，/cancel(按房间) 能命中
    finally:
        heartbeat.runner, pr_followup.runner, state.client = orig


# ---------- /cancel：排队中的任务被取消后绝不开跑；三种情况文案可区分 ----------
def test_cancel_stops_queued_task():
    cr = claude_runner

    # (a) runner 级：A 在跑占着锁、B 排队等锁；此刻 /cancel → A 被杀、B 拿到锁即了断，绝不偷偷开跑
    async def go():
        r = cr.ClaudeRunner()
        a_in = asyncio.Event()        # A 已进锁并起了子进程
        release_a = asyncio.Event()   # 放行 A（模拟它被杀后返回）
        b_started_proc = {"v": False}
        killed = []
        orig_kill = cr._kill_group
        cr._kill_group = lambda p: (killed.append(p), setattr(p, "returncode", -9))

        class P:                      # 假子进程：只用到 pid / returncode
            def __init__(self): self.pid, self.returncode = 4321, None

        async def fake_run(cmd, cwd=None, on_proc=None):
            first = not a_in.is_set()
            proc = P()
            if on_proc:
                on_proc(proc)                          # 登记进程 → token.started=True
            if first:
                a_in.set()
                await release_a.wait()                 # A 持锁阻塞，其间会被 /cancel 杀
                return proc.returncode or -9, b"", b""
            b_started_proc["v"] = True                 # B 一旦起了子进程就记下——本不该发生
            return 0, json.dumps({"result": "b-ran", "session_id": "sb", "is_error": False}).encode(), b""

        r._run = fake_run
        try:
            ta = asyncio.create_task(r.ask("A", "a", lock_key="proj", cancel_key="room"))
            await a_in.wait()                            # 确保 A 已进锁在跑
            tb = asyncio.create_task(r.ask("B", "b", lock_key="proj", cancel_key="room"))
            for _ in range(200):                         # 等 B 登记令牌并排到锁上
                if len(r._tokens.get("room", ())) == 2:
                    break
                await asyncio.sleep(0)
            running, queued = r.cancel("room")           # /cancel
            release_a.set()
            ra = (await asyncio.gather(ta, return_exceptions=True))[0]
            rb = (await asyncio.gather(tb, return_exceptions=True))[0]
            return running, queued, ra, rb, b_started_proc["v"], killed
        finally:
            cr._kill_group = orig_kill

    running, queued, ra, rb, b_ran, killed = asyncio.run(go())
    assert running == 1 and queued == 1                  # A 运行中、B 排队中，各计一
    assert isinstance(ra, cr.ClaudeCancelled)            # A 按取消路径退（上层回"已停止"而非"出错了"）
    assert isinstance(rb, cr.ClaudeCancelled)            # B 也按取消退
    assert b_ran is False                                # B 根本没起子进程——没有背着用户开跑
    assert len(killed) == 1                              # A 的子进程组被杀

    # (b) handle_cancel 三种情况文案可区分：运行中 / 排队中 / 空场
    orig_runner, orig_client = tasks.runner, state.client
    c = _CapClient(); state.client = c

    class FR:
        def __init__(self, res): self.res = res
        def cancel(self, rid): return self.res
    try:
        for res, expect in (((1, 0), "已停止正在运行"),
                            ((0, 2), "已取消排队"),
                            ((0, 0), "没有正在运行或排队")):
            c.sent.clear()
            tasks.runner = FR(res)
            asyncio.run(bot.handle_cancel(FakeRoom("!cq:ex.org", 3)))
            assert any(expect in (m.get("body") or "") for m in c.sent), (res, expect)
    finally:
        tasks.runner, state.client = orig_runner, orig_client


# ---------- /cancel 空场不留标记：之后新派的任务不会被莫名毒杀 ----------
def test_cancel_empty_no_poison():
    cr = claude_runner

    async def go():
        r = cr.ClaudeRunner()
        assert r.cancel("room") == (0, 0)                # 空场：什么都没停
        assert not r._tokens.get("room")                 # 且没留下任何取消令牌（否则会毒杀下一个任务）

        async def fake_run(cmd, cwd=None, on_proc=None):
            proc = types.SimpleNamespace(pid=1, returncode=None)
            if on_proc:
                on_proc(proc)
            proc.returncode = 0
            return 0, json.dumps({"result": "干完了", "session_id": "s", "is_error": False}).encode(), b""

        r._run = fake_run
        return await r.ask("k", "干活", lock_key="proj", cancel_key="room")   # 之后正常派活
    assert asyncio.run(go()) == "干完了"                  # 不被那条陈旧标记莫名秒杀


# ---------- 流式任务异常：占位收尾成报错，不再永远停在"正在干活"、也不重复报错 ----------
def test_stream_task_error_finalizes_placeholder():
    set_identity()
    rid = "!serr:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    c = _CapClient(); state.client = c

    class R:
        def busy(self, k): return False
        def running(self, k): return 0
        async def ask(self, key, prompt, cwd=None, system_prompt=None, lock_key=None,
                      prepare=None, on_delta=None, cancel_key=None):
            if on_delta:
                await on_delta("看了一半代码", "Bash")     # 先造出占位消息（停在"正在干活"）
            raise RuntimeError("claude 退出码 1: boom")     # 再异常退出

    rec = {"id": "p", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}
    orig = (tasks.runner, settings.stream_replies)
    tasks.runner = R()
    settings.stream_replies = True
    try:
        asyncio.run(bot._run_on_project(room, make_event("干个活"), "干个活", rec))  # 不该往外抛
    finally:
        (tasks.runner, settings.stream_replies) = orig
        bot._context[rid].clear()

    edits = [m for m in c.sent if (m.get("m.relates_to") or {}).get("rel_type") == "m.replace"]
    assert edits and "出错了" in edits[-1]["m.new_content"]["body"]     # 占位被收尾成报错
    assert "boom" in edits[-1]["m.new_content"]["body"]                # 带上具体错因
    top_errs = [m for m in c.sent                                      # 不重复报错：没有另发的顶层"出错了"
                if (m.get("m.relates_to") or {}).get("rel_type") != "m.replace"
                and "出错了" in (m.get("body") or "")]
    assert not top_errs


# ---------- 手动前台 Ctrl-C：协程取消时子进程组被杀，不留孤儿 claude ----------
def test_run_kills_group_on_cancel():
    import tempfile
    cr = claude_runner

    async def go():
        r = cr.ClaudeRunner()
        killed = []
        orig_kill = cr._kill_group
        cr._kill_group = lambda p: killed.append(p)

        class FakeProc:
            def __init__(self): self.pid, self.returncode = 999, None
            async def communicate(self): await asyncio.sleep(3600)   # 永远卡住，等外部取消
            async def wait(self): return self.returncode

        proc = FakeProc()
        orig_exec = asyncio.create_subprocess_exec
        async def fake_exec(*a, **k): return proc
        asyncio.create_subprocess_exec = fake_exec
        started = asyncio.Event()
        try:
            task = asyncio.create_task(
                r._run(["claude", "-p", "hi"], cwd=tempfile.mkdtemp(),
                       on_proc=lambda p: started.set()))
            await started.wait()          # 子进程已登记
            await asyncio.sleep(0)         # 让 _run 进入 communicate 的 await
            task.cancel()                  # 模拟 Ctrl-C 打断协程
            try:
                await task
            except asyncio.CancelledError:
                pass
            return proc, killed
        finally:
            cr._kill_group = orig_kill
            asyncio.create_subprocess_exec = orig_exec

    proc, killed = asyncio.run(go())
    assert proc in killed                  # 取消时子进程组被杀，claude 不会变孤儿继续 push/开 PR


# ---------- 重启后回复 bot 旧消息仍算点名（向服务器补认发送者） ----------
def test_reply_to_bot_after_restart():
    set_identity()
    calls = []

    def make_client(sender):
        class FC:
            async def room_get_event(self, rid, eid):
                calls.append(eid)
                return types.SimpleNamespace(event=types.SimpleNamespace(sender=sender))
        return FC()

    bot._sent_events.clear()
    state._foreign_events.clear()
    room = FakeRoom("!g:ex.org", 3)

    state.client = make_client("@claudebot:ex.org")       # 被回复的是 bot 自己的旧消息
    ev = make_event("> <@claudebot:ex.org> 旧\n\n继续弄", in_reply_to="$old1")
    asyncio.run(bot._resolve_reply_author(room.room_id, ev.source["content"]))
    a, t = bot._is_addressed(room, ev)
    assert a and t == "继续弄"                             # 重启（_sent_events 清空）后仍认得

    state.client = make_client("@bob:ex.org")             # 被回复的是别人 → 不误当点名，且只查一次
    ev2 = make_event("> <@bob:ex.org> x\n\n哈哈", in_reply_to="$old2")
    asyncio.run(bot._resolve_reply_author(room.room_id, ev2.source["content"]))
    asyncio.run(bot._resolve_reply_author(room.room_id, ev2.source["content"]))
    a2, _ = bot._is_addressed(room, ev2)
    assert a2 is False and calls.count("$old2") == 1


# ---------- PR 跟进：bot 自己（同 token）的评论不当新评审，别人的照常派活 ----------
def test_followup_ignores_own_reviews():
    import tempfile
    import pr_ledger
    import gitea
    set_identity()
    orig_store, orig_am = settings.store_path, settings.pr_automerge
    settings.store_path = tempfile.mkdtemp()
    settings.pr_automerge = False
    _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    spawned = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.get_project, state.client, state._spawn,
            gitea.pr_info, gitea.pr_reviews, gitea.ci_state)
    own_backup = dict(gitea._own_user)
    bot.projects.get_project = lambda pid: rec if pid == "h/o/r" else None
    state.client = FC()
    state._spawn = lambda coro: (spawned.append(1), coro.close())
    gitea._own_user.clear(); gitea._own_user["id"] = 42

    async def open_info(r, n):
        return {"state": "open", "merged": False, "mergeable": True,
                "head": {"ref": "claude/x", "sha": "s"}}
    async def own_review(r, n):
        return [{"id": 11, "state": "COMMENT", "body": "我自己的回应", "user": {"id": 42}}]
    async def other_review(r, n):
        return [{"id": 12, "state": "COMMENT", "body": "别人提的意见", "user": {"id": 7}}]
    async def no_ci(r, s): return ""
    try:
        gitea.pr_info, gitea.pr_reviews, gitea.ci_state = open_info, own_review, no_ci
        pr_ledger.record("h/o/r", 4, "u4", "!room")
        entry = [e for e in pr_ledger.active() if e["number"] == 4][0]
        asyncio.run(bot._followup_one(entry))
        e4 = [e for e in pr_ledger.active() if e["number"] == 4][0]
        assert not spawned and e4["review_fixes"] == 0    # 自己的评论：不派活、不烧自动处理次数
        assert e4["seen_review"] == 11                    # 但水位照常推进，下一轮不再重看
        gitea.pr_reviews = other_review                   # 混进别人的新评论 → 照常派跟进
        asyncio.run(bot._followup_one(e4))
        assert spawned
    finally:
        (bot.projects.get_project, state.client, state._spawn,
         gitea.pr_info, gitea.pr_reviews, gitea.ci_state) = orig
        gitea._own_user.clear(); gitea._own_user.update(own_backup)
        settings.store_path, settings.pr_automerge = orig_store, orig_am
        _reset_ledger()


# ---------- DM 分诊：当前这条不重复出现在分诊背景里 ----------
def test_dispatch_triage_skips_current():
    set_identity()
    room = FakeRoom("!dm2:ex.org", 2)
    rid = room.room_id
    bot._context[rid].clear()
    known = [{"id": "h/o/app", "owner": "o", "repo": "app", "host": "http://h", "path": "/x", "base": "main"},
             {"id": "h/o/web", "owner": "o", "repo": "web", "host": "http://h", "path": "/y", "base": "main"}]
    captured = {}

    async def fake_triage(text, kn, context=""):
        captured["ctx"] = context
        return dispatch.TRIAGE_GENERAL

    orig = (dispatch._triage, bot.projects.list_projects)
    dispatch._triage = fake_triage
    bot.projects.list_projects = lambda: known
    try:
        body = "这个流程整体该怎么设计比较好"
        bot._context[rid].append((time.time(), "Alice", "早些的消息"))
        bot._context[rid].append((time.time(), "Alice", body))    # on_message 已把当前消息垫进背景
        rec = asyncio.run(bot._dispatch(room, make_event(body), body))
        assert rec and rec.get("general")
        assert "早些的消息" in captured["ctx"]                    # 背景还在
        assert body not in captured["ctx"]                        # 但当前这条被剔掉，不喂两遍
    finally:
        dispatch._triage, bot.projects.list_projects = orig


# ---------- /summarize、/catchup（含带参数形式）不混进小结素材 ----------
def test_summarize_excludes_command_variants():
    import tasks
    set_identity()
    rid = "!sum:ex.org"
    room = FakeRoom(rid, 3)
    sent, captured = [], {}

    class FC:
        async def room_typing(self, *a, **k): return None
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    async def fake_quick(prompt):
        captured["p"] = prompt; return "小结好了"

    orig = (state.client, tasks.runner.quick, settings.transcript_enabled)
    state.client = FC()
    tasks.runner.quick = fake_quick
    settings.transcript_enabled = False                   # 走内存背景缓冲
    bot._context[rid].clear()
    bot._context[rid].append((time.time(), "Alice", "昨天讨论了部署方案"))
    bot._context[rid].append((time.time(), "Alice", "/catchup 30"))
    try:
        asyncio.run(bot.handle_summarize(room, make_event("/catchup 30"), "/catchup 30"))
        assert "昨天讨论了部署方案" in captured["p"]
        assert "/catchup" not in captured["p"]            # 命令本身（含带参数形式）不进素材
        assert any("小结" in b for b in sent)
    finally:
        state.client, tasks.runner.quick, settings.transcript_enabled = orig


TESTS = [
    ("启动+群任务全链路", test_startup_and_task_flow),
    ("零项目DM落通用助手", test_dm_no_projects_falls_to_general),
    ("线程化回复 helper+send", test_thread_helpers_and_send),
    ("群任务答复挂线程", test_group_task_reply_threaded),
    ("/help + 进房欢迎", test_help_and_welcome),
    ("/summarize 小结最近对话", test_summarize_command),
    ("/cancel 停当前任务", test_cancel_command),
    ("附件回传 允许/拦截/抹标记", test_emit_files_allowed_blocked_stripped),
    ("流式 占位→编辑→定稿", test_live_reply_streams_and_finalizes),
    ("认 reply / 点名", test_reply_addressing),
    ("引用回退块剥离", test_reply_fallback_strip),
    ("track 门控 + 时间单调", test_track_and_monotonic),
    ("自己账号入上下文不派活", test_own_account_context),
    ("编辑消息 m.replace 不重派活", test_edit_event_ignored),
    ("TTL 过期提示", test_ttl_notice),
    ("token 受信主机 + redact", test_security_bits),
    ("无访问控制：谁邀请都进房", test_no_access_control_invite_joins),
    ("孤儿房间 人走光自动退", test_leave_when_alone),
    ("退房清尾巴 绑定/路由/任务/记录", test_leave_cleans_up_room),
    ("群对话延续窗口", test_group_followup_window),
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
    ("媒体失败无caption 明确回错误", test_media_failure_notifies_when_addressed),
    ("媒体文件名消毒", test_media_safe_name),
    ("媒体滚动删旧", test_media_prune),
    ("DM 分诊路由过 ensure", test_dm_routing_ensures_checkout),
    ("裸仓库名兜底在分诊之后", test_dm_loose_name_after_triage),
    ("主动 PASS 只占短冷却", test_proactive_pass_keeps_short_cooldown),
    ("主动插话预筛开关", test_proactive_require_hint_toggle),
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
    ("DM /bind 给引导不派活", test_dm_bind_gets_guidance),
    ("项目长期记忆 跨会话留存", test_project_memory),
    ("PR 台账 登记/持久化/销账", test_pr_ledger),
    ("从回复抽取本项目 PR 链接", test_extract_pr),
    ("PR 跟进 合并销账/评审派活", test_pr_followup_actions),
    ("PR 连续3轮404才销账 抖动不销", test_pr_gone_after_three_404),
    ("工单台账 登记/持久化/销账", test_issue_ledger),
    ("工单接活 认领/宣布/派执行/防重", test_issue_intake_flow),
    ("工单执行 开PR进台账/贴链接/关单销账", test_issue_execute_and_sweep),
    ("工单连续3轮404才销账 抖动不销", test_issue_gone_after_three_404),
    ("Gitea健康度 失败累计/成功清零/401定性/404不计", test_gitea_health_accounting),
    ("Gitea健康度 status两态+告警一次+恢复", test_gitea_health_status_and_alert),
    ("/status 暴露Gitea连通性", test_status_shows_gitea_health),
    ("排队回执 忙时知会/空闲不发", test_queue_receipt_when_busy),
    ("模型拆分 干活大/轻判断小", test_quick_model_split),
    ("聊天逐字记录 落盘/回溯/删旧/开关", test_transcript_log_and_recall),
    ("PR 自动合并 条件满足才合并", test_pr_automerge),
    ("CI查询失败不放行自动合并", test_automerge_skips_on_ci_unknown),
    ("PR 冲突只告警一次不刷屏", test_conflict_alert_once),
    ("自驱心跳 提议/autopilot", test_heartbeat_propose_and_autopilot),
    ("/status 状态一屏可见", test_status_command),
    ("流式定稿 编辑失败退回新发", test_livereply_finalize_edit_fallback),
    ("自驱/跟进任务可被 /cancel", test_autonomous_tasks_cancellable_by_room),
    ("/cancel 停排队任务+三种文案", test_cancel_stops_queued_task),
    ("/cancel 空场不毒杀下个任务", test_cancel_empty_no_poison),
    ("流式异常 占位收尾不重复报错", test_stream_task_error_finalizes_placeholder),
    ("取消协程 子进程组被杀", test_run_kills_group_on_cancel),
    ("重启后回复 bot 仍算点名", test_reply_to_bot_after_restart),
    ("PR 跟进忽略自己的评论", test_followup_ignores_own_reviews),
    ("分诊背景剔除当前消息", test_dispatch_triage_skips_current),
    ("/summarize 剔除命令变体", test_summarize_excludes_command_variants),
]


def main():
    import traceback
    import tempfile
    import gitea
    # 把状态目录指到临时目录，别让自检把 sessions.json / last_projects.json 写进真实 store
    settings.store_path = tempfile.mkdtemp(prefix="mxbot-smoke-store-")
    # 预置"bot 自己的 Gitea 用户 id/登录名"缓存：自检必须离线，不许真去查 /api/v1/user
    gitea._own_user["id"] = -1
    gitea._own_user["login"] = "claudebot"
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
