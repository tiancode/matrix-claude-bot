"""冒烟：启动全链路·群任务答复/线程/回执·runner 会话分叉·命令与流式回复"""
from _helpers import (
    FakeRoom, RoomMessageText, _CapClient, _drain_and_run, _task_fixtures, asyncio, bot, claude_runner, dispatch, json, make_event, matrix_io, os, set_identity, settings, state, time, types)

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

        def add_to_device_callback(self, cb, ev_type):   # 开 E2E 后 main() 会注册 SAS 验证回调
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
                pending = [t for t in state._tasks if not t.done()]
                if not pending:
                    break
                await asyncio.gather(*pending, return_exceptions=True)

    async def fake_ask(key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                       on_delta=None, cancel_key=None, **_kw):
        captured["prompt"], captured["key"], captured["lock_key"] = prompt, key, lock_key
        return "已改好并提了 PR：https://gitea.example.com/team/app/pulls/7"

    async def fake_login():
        return None

    rec = {"id": "gitea.example.com/team/app", "owner": "team", "repo": "app",
           "path": "/tmp", "base": "main", "host": "https://gitea.example.com"}
    async def fake_ensure(info):   # checkout 视作已在，直接返回（群分派会过 ensure 校验）
        return rec

    # 这些全局补丁必须在 finally 里还原：否则 get_room 一直被钉成返回 rec，
    # 会污染后续 DM 用例（DM 分派现在也查 get_room）。
    orig_bot = (bot.AsyncClient, bot._login, bot.projects.get_room,
                bot.projects.ensure_project, bot.runner.ask)
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
        (bot.AsyncClient, bot._login, bot.projects.get_room,
         bot.projects.ensure_project, bot.runner.ask) = orig_bot

    assert state.MY_ID == "@claudebot:ex.org" and state.MY_NAME == "claude-bot"
    assert captured["key"] == "gitea.example.com/team/app|!g:ex.org"    # 会话按项目+房间（不串台）
    assert captured["lock_key"] == "gitea.example.com/team/app"         # checkout 仍按项目串行
    assert sent and "PR" in sent[0][1]                                  # 答复发回房间
    assert "【当前要你处理的任务】" in captured["prompt"]               # 任务带上下文区块
    assert "Alice" in captured["prompt"] and "[" in captured["prompt"]  # 带时间戳的群上下文
    assert "修一下登录 token" not in captured["prompt"].split("【当前要你处理的任务】")[0]  # 当前任务不在背景里重复
    assert any(s == "claude-bot" for _, s, *_ in bot._context["!g:ex.org"])  # bot 回复入上下文

# ---------- 新增能力 1) 线程 / 引用回复 helper ----------
def test_thread_helpers_and_send():
    set_identity()
    assert bot._thread_of(make_event("hi", event_id="$root1")) is None              # 顶层消息不算在线程里
    assert matrix_io._thread_root_of(make_event("hi", event_id="$root1")) == "$root1"     # 旧式模式：自身作根
    in_thread = types.SimpleNamespace(event_id="$x", source={"content": {
        "m.relates_to": {"rel_type": "m.thread", "event_id": "$realroot"}}})
    assert bot._thread_of(in_thread) == "$realroot"                                 # 已在线程→沿用根
    assert matrix_io._thread_root_of(in_thread) == "$realroot"
    rel = matrix_io._thread_rel("$r", "$prev")
    assert rel["rel_type"] == "m.thread" and rel["event_id"] == "$r"
    assert rel["m.in_reply_to"]["event_id"] == "$prev" and rel["is_falling_back"] is True
    assert matrix_io._thread_rel(None) is None
    assert matrix_io._reply_rel("$q") == {"m.in_reply_to": {"event_id": "$q"}}            # 引用回复关系
    assert matrix_io._reply_rel(None) is None

    c = _CapClient(); state.client = c
    asyncio.run(bot.send("!r:ex.org", "答复", thread_root="$root"))
    assert c.sent[0]["m.relates_to"]["rel_type"] == "m.thread"                      # 传了 root → 挂线程
    assert c.sent[0]["m.relates_to"]["event_id"] == "$root"
    c.sent.clear()
    asyncio.run(bot.send("!r:ex.org", "答复", reply_to="$q"))                       # 引用回复：非线程
    assert c.sent[0]["m.relates_to"] == {"m.in_reply_to": {"event_id": "$q"}}
    c.sent.clear()
    asyncio.run(bot.send("!r:ex.org", "答复"))
    assert "m.relates_to" not in c.sent[0]                                          # 不传 → 顶层

def test_group_task_reply_threaded():
    """REPLY_IN_THREAD=1 旧式模式：群任务答复挂进提问那条的线程。"""
    set_identity()
    c = _CapClient(); state.client = c
    _task_fixtures()
    room = FakeRoom("!g2:ex.org", 3)
    ev = make_event("@claude-bot 干活", mentions=["@claudebot:ex.org"], event_id="$Q")
    orig = (settings.stream_replies, settings.reply_in_thread)
    settings.stream_replies = False; settings.reply_in_thread = True
    try:
        asyncio.run(bot.handle_task(room, ev, "干活"))
    finally:
        settings.stream_replies, settings.reply_in_thread = orig
    ans = [m for m in c.sent if m.get("body") == "搞定"]
    assert ans and ans[0]["m.relates_to"]["rel_type"] == "m.thread"
    assert ans[0]["m.relates_to"]["event_id"] == "$Q"                              # 群里挂到提问那条

def test_group_flat_reply_and_smart_quote():
    """默认（REPLY_IN_THREAD=0）：安静群顶层直答不 fork 线程；发送时群里已插进新消息 → 自动改
    引用回复指向触发消息；用户自己在线程里说话 → 跟进该线程。"""
    set_identity()
    c = _CapClient(); state.client = c
    _task_fixtures()
    room = FakeRoom("!flat:ex.org", 3)
    rid = room.room_id
    orig = (settings.stream_replies, settings.reply_in_thread)
    settings.stream_replies = False; settings.reply_in_thread = False
    try:
        # ① 安静群：任务期间没人插话 → 顶层直答，无任何 m.relates_to。
        # 上下文里存的是 on_message 落的【原始正文】（带@），与 _run_on_project 里 cur_body 一致，
        # 于是 _mark_dispatched 会命中这条尾消息、就地换成带 dispatched 标记的新元组——回归点：引用回复
        # 决策器若用对象身份比对尾条，会因这次换元组误判成「群里插了话」而错误引用回复。
        bot._context[rid].clear()
        bot._context[rid].append((time.time(), "Alice", "@claude-bot 干活A"))   # on_message 存的原始正文
        asyncio.run(bot.handle_task(
            room, make_event("@claude-bot 干活A", mentions=[state.MY_ID], event_id="$QA"), "干活A"))
        ans = [m for m in c.sent if m.get("body") == "搞定"]
        assert ans and "m.relates_to" not in ans[0]   # 安静群仍顶层直答（_mark_dispatched 换元组不该触发引用）

        # ② 任务期间群里插进了别的消息 → 答复改用引用回复指明在回哪条
        c.sent.clear()
        async def ask_interrupted(key, prompt, cwd=None, system_prompt=None, lock_key=None,
                                  prepare=None, on_delta=None, cancel_key=None, **_kw):
            bot._context[rid].append((time.time(), "Bob", "插一句别的"))
            return "搞定"
        bot.runner.ask = ask_interrupted
        bot._context[rid].append((time.time(), "Alice", "干活B"))
        asyncio.run(bot.handle_task(
            room, make_event("@claude-bot 干活B", mentions=[state.MY_ID], event_id="$QB"), "干活B"))
        ans = [m for m in c.sent if m.get("body") == "搞定"]
        assert ans and ans[0]["m.relates_to"] == {"m.in_reply_to": {"event_id": "$QB"}}

        # ③ 用户自己在线程里说话 → 答复跟进该线程（不看 REPLY_IN_THREAD）
        c.sent.clear()
        _task_fixtures()
        in_thread = types.SimpleNamespace(
            body="@claude-bot 干活C", sender="@alice:ex.org", event_id="$QC",
            server_timestamp=int(time.time() * 1000),
            source={"content": {"m.relates_to": {"rel_type": "m.thread", "event_id": "$TR"},
                                "m.mentions": {"user_ids": [state.MY_ID]}}})
        asyncio.run(bot.handle_task(room, in_thread, "干活C"))
        ans = [m for m in c.sent if m.get("body") == "搞定"]
        assert ans and ans[0]["m.relates_to"]["rel_type"] == "m.thread"
        assert ans[0]["m.relates_to"]["event_id"] == "$TR"
    finally:
        settings.stream_replies, settings.reply_in_thread = orig
        bot._context[rid].clear()

def test_group_unbound_chat_general():
    """未绑定仓库的群：不再回绝"还没绑定仓库"，落到通用助手照聊；系统提示带绑定指引，会话按房间隔离。"""
    set_identity()
    c = _CapClient(); state.client = c
    bot.projects.get_room = lambda rid: None                       # 未绑定
    captured = {}
    async def fake_ask(key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                       on_delta=None, cancel_key=None, **_kw):
        captured["key"], captured["sp"] = key, system_prompt
        return "哈哈，好的"
    bot.runner.ask = fake_ask
    room = FakeRoom("!ub:ex.org", 3)
    ev = make_event("@claude-bot 讲个笑话", mentions=[state.MY_ID], event_id="$ub1")
    orig = (settings.stream_replies, settings.reply_in_thread)
    settings.stream_replies = False; settings.reply_in_thread = False
    try:
        asyncio.run(bot.handle_task(room, ev, "讲个笑话"))
    finally:
        settings.stream_replies, settings.reply_in_thread = orig
    assert any((m.get("body") or "") == "哈哈，好的" for m in c.sent)               # 真的答了
    assert not any("还没绑定仓库" in (m.get("body") or "") for m in c.sent)         # 不再一口回绝
    assert captured["key"].startswith(dispatch._GENERAL_ID)                        # 通用助手会话、按房间隔离
    assert "还没绑定仓库" in captured["sp"] and "/bind" in captured["sp"]           # 指引进了系统提示

def test_task_ack_reaction():
    """收到任务先给触发消息打 👀 reaction 回执，处理完 redact 撤掉。"""
    set_identity()
    c = _CapClient(); state.client = c
    _task_fixtures()
    room = FakeRoom("!ack:ex.org", 3)
    ev = make_event("@claude-bot 干活", mentions=[state.MY_ID], event_id="$ACK")
    orig = (settings.stream_replies, settings.reply_in_thread)
    settings.stream_replies = False; settings.reply_in_thread = False
    try:
        asyncio.run(bot.handle_task(room, ev, "干活"))
    finally:
        settings.stream_replies, settings.reply_in_thread = orig
    reacts = [m for m in c.sent if (m.get("m.relates_to") or {}).get("rel_type") == "m.annotation"]
    assert reacts and reacts[0]["m.relates_to"]["event_id"] == "$ACK"              # 打在触发消息上
    assert reacts[0]["m.relates_to"]["key"] == "👀"
    assert c.redacted == ["$e1"]                                                   # 处理完撤掉（回执是第 1 条发送）

def test_runner_fork_session():
    """runner 层的会话分叉：key 无会话且给了 fork_from → --resume 父会话 + --fork-session，
    新会话存线程 key、父会话不动；第二次直接 resume 线程会话；父会话失效 → 清父、全新开。"""
    set_identity()
    rec = {"id": "h/o/r"}
    assert state._sess_key(rec, "!r") == "h/o/r|!r"                    # 不带线程 = 老格式
    assert state._sess_key(rec, "!r", "$T") == "h/o/r|!r|$T"           # 线程维度
    cmd = claude_runner.runner._cmd("hi", "SID", True, fork=True)
    assert cmd[cmd.index("--resume") + 1] == "SID" and "--fork-session" in cmd
    assert "--fork-session" not in claude_runner.runner._cmd("hi", "SID", True)    # 不 fork 不带

    r = claude_runner.ClaudeRunner()
    r._sessions["proj|room"] = ("SID-PARENT", time.time())
    cmds = []

    async def fake_run(cmd, cwd=None, sema=None, env=None, timeout=None, on_proc=None):
        cmds.append(cmd)
        return 0, json.dumps({"result": "ok", "session_id": "SID-FORKED"}).encode(), b""
    r._run = fake_run
    assert asyncio.run(r.ask("proj|room|$T", "hi", fork_from="proj|room")) == "ok"
    c = cmds[0]
    assert c[c.index("--resume") + 1] == "SID-PARENT" and "--fork-session" in c    # 从父分叉
    assert r._sessions["proj|room|$T"][0] == "SID-FORKED"              # 新会话存线程 key
    assert r._sessions["proj|room"][0] == "SID-PARENT"                 # 父会话原样
    cmds.clear()
    asyncio.run(r.ask("proj|room|$T", "again", fork_from="proj|room"))
    c = cmds[0]
    assert c[c.index("--resume") + 1] == "SID-FORKED" and "--fork-session" not in c  # 第二次直接续
    cmds.clear()
    asyncio.run(r.ask("p2|room|$T", "hi", fork_from="p2|room"))        # 父不存在 → 全新开
    assert "--resume" not in cmds[0] and "--fork-session" not in cmds[0]

    r2 = claude_runner.ClaudeRunner()                                  # 父会话失效：清父、全新开
    r2._sessions["p3|room"] = ("SID-DEAD", time.time())

    async def fail_then_ok(cmd, cwd=None, sema=None, env=None, timeout=None, on_proc=None):
        if "--resume" in cmd:
            return 1, b"", b"No conversation found with session ID SID-DEAD"
        return 0, json.dumps({"result": "ok", "session_id": "SID-NEW"}).encode(), b""
    r2._run = fail_then_ok
    assert asyncio.run(r2.ask("p3|room|$T", "hi", fork_from="p3|room")) == "ok"
    assert "p3|room" not in r2._sessions                               # 失效的是父，父被清
    assert r2._sessions["p3|room|$T"][0] == "SID-NEW"

def test_runner_on_reset_fires_on_expired_session():
    """会话在拿到锁前已过 TTL → ask 全新开（sid 一开始就是 None，走不到「resume 被拒」分支）；
    on_reset 仍须回调，否则调用方按「续接」预判裁掉的背景消息两头落空。全新 key（本无会话）则不回调。"""
    set_identity()
    r = claude_runner.ClaudeRunner()
    r._sessions["proj|room"] = ("SID-OLD", time.time() - 10 ** 9)      # 早已过期的会话

    async def fake_run(cmd, cwd=None, sema=None, env=None, timeout=None, on_proc=None):
        return 0, json.dumps({"result": "ok", "session_id": "SID-NEW"}).encode(), b""
    r._run = fake_run

    fired = {"n": 0}
    ans = asyncio.run(r.ask("proj|room", "hi",
                            on_reset=lambda: fired.__setitem__("n", fired["n"] + 1)))
    assert "ok" in ans                                                # 过期会前缀「已开启新对话」，故只查包含
    assert fired["n"] == 1                                             # 过期→全新开 → on_reset 触发一次
    assert r._sessions["proj|room"][0] == "SID-NEW"                    # 新会话已存下

    fresh = {"n": 0}                                                  # 全新 key：本就没会话可续，不该回调
    assert asyncio.run(r.ask("brandnew|room", "hi",
                             on_reset=lambda: fresh.__setitem__("n", fresh["n"] + 1))) == "ok"
    assert fresh["n"] == 0

def test_runner_on_reset_skipped_when_fork_rescues_expired_thread():
    """线程自己的会话过 TTL，但父会话（房间会话）仍有效 → fork 接上父会话，这其实是续接不是
    「全新开」：on_reset 不该触发（父会话历史还在，dispatched 标记仍然有效），答复也不该被前缀
    「已开启新对话」误导用户（fork 明明带着完整历史续上了）。"""
    set_identity()
    r = claude_runner.ClaudeRunner()
    r._sessions["proj|room"] = ("SID-PARENT", time.time())                 # 父会话（房间）仍新鲜
    r._sessions["proj|room|$T"] = ("SID-THREAD-OLD", time.time() - 10 ** 9)  # 线程自己的会话已过 TTL

    async def fake_run(cmd, cwd=None, sema=None, env=None, timeout=None, on_proc=None):
        return 0, json.dumps({"result": "ok", "session_id": "SID-FORKED"}).encode(), b""
    r._run = fake_run

    fired = {"n": 0}
    ans = asyncio.run(r.ask("proj|room|$T", "hi", fork_from="proj|room",
                            on_reset=lambda: fired.__setitem__("n", fired["n"] + 1)))
    assert ans == "ok"                          # 没有「已开启新对话」前缀：fork 接上了父会话历史
    assert fired["n"] == 0                      # fork 成功 → 不算全新开，不该清 dispatched 标记
    assert r._sessions["proj|room|$T"][0] == "SID-FORKED"

def test_thread_scoped_session_forks():
    """用户线程里的任务：会话细分到线程并从房间会话 fork；背景不带房间近况、只补线程起点；
    线程里 /reset 只重置该线程；顶层任务不受影响。"""
    set_identity()
    c = _CapClient(); state.client = c
    rec = _task_fixtures()
    captured = []

    async def fake_ask(key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                       on_delta=None, cancel_key=None, fork_from=None, **_kw):
        captured.append((key, fork_from, prompt))
        return "线程里搞定"
    bot.runner.ask = fake_ask
    room = FakeRoom("!ts:ex.org", 3)
    rid = room.room_id

    def thread_event(body, eid, root="$TROOT"):
        return types.SimpleNamespace(
            body=body, sender="@alice:ex.org", event_id=eid,
            server_timestamp=int(time.time() * 1000),
            source={"content": {"m.relates_to": {"rel_type": "m.thread", "event_id": root},
                                "m.mentions": {"user_ids": [state.MY_ID]}}})
    orig = (settings.stream_replies, settings.reply_in_thread, bot.runner.reset)
    settings.stream_replies = False; settings.reply_in_thread = False
    try:
        bot._context[rid].clear()
        bot._context[rid].append((time.time(), "Bob", "房间里的无关闲聊"))   # 不该漏进线程任务
        asyncio.run(bot.handle_task(room, thread_event("@claude-bot 线程活", "$TQ"), "线程活"))
        key, fork_from, prompt = captured[0]
        assert key == f"{rec['id']}|{rid}|$TROOT"                      # 会话键带线程维度
        assert fork_from == f"{rec['id']}|{rid}"                       # 从房间会话分叉
        assert "root-of-$TROOT" in prompt                              # 背景=线程起点（根消息）
        assert "房间里的无关闲聊" not in prompt                          # 房间近况被隔离

        captured.clear()                                               # 顶层任务：老样子，不带线程不 fork
        asyncio.run(bot.handle_task(
            room, make_event("@claude-bot 顶层活", mentions=[state.MY_ID], event_id="$PQ"), "顶层活"))
        key, fork_from, _ = captured[0]
        assert key == f"{rec['id']}|{rid}" and fork_from is None

        resets = []                                                    # 线程里 /reset 只重置线程会话
        bot.runner.reset = lambda k: resets.append(k)
        asyncio.run(bot.handle_task(room, thread_event("/reset", "$RQ"), "/reset"))
        assert resets == [f"{rec['id']}|{rid}|$TROOT"]
        assert any("已重置本线程" in (m.get("body") or "") for m in c.sent)
        assert bot._context[rid]                                       # 房间背景缓冲没被线程 reset 清掉
    finally:
        settings.stream_replies, settings.reply_in_thread, bot.runner.reset = orig
        bot._context[rid].clear()

# ---------- 「@了 谁」附注贯通：背景/任务正文都看得到点名对象，skip 去重不被附注破坏 ----------
def test_mention_note_in_context_and_prompt():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    _task_fixtures()
    captured = {}
    async def fake_ask(key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                       on_delta=None, cancel_key=None, **_kw):
        captured["prompt"] = prompt
        return "搞定"
    bot.runner.ask = fake_ask
    room = FakeRoom("!mn:ex.org", 3)
    rid = room.room_id
    orig = (settings.stream_replies, settings.reply_in_thread)
    settings.stream_replies = False; settings.reply_in_thread = False
    try:
        bot._context[rid].clear()
        bot._context[rid].append((time.time(), "Bob", "先垫一条背景"))
        ev = make_event("@claude-bot 问下 Alice 的进度", sender="@bob:ex.org", event_id="$mn1",
                        mentions=["@claudebot:ex.org", "@alice:ex.org"])
        _drain_and_run(bot.on_message(room, ev))
        # ① 落进背景的这条正文尾部带附注（Claude 拼背景时能看到谁被点名，@bot 自己不进附注）
        stored = [b for _, s, b, *_ in bot._context[rid] if s == "@bob:ex.org"]
        assert stored and stored[0].endswith("〔@了 Alice〕") and "claudebot" not in stored[0].split("〔")[-1]
        # ② 派给 Claude 的当前任务正文也带附注
        task_part = captured["prompt"].split("【当前要你处理的任务】")[-1]
        assert "问下 Alice 的进度〔@了 Alice〕" in task_part
        # ③ skip/dispatched 的原文匹配按同样规则拼过：当前消息没在背景区重复出现
        assert "问下 Alice 的进度" not in captured["prompt"].split("【当前要你处理的任务】")[0]
    finally:
        settings.stream_replies, settings.reply_in_thread = orig
        bot._context[rid].clear()

# ---------- 附注抗显示名漂移：接收时算一次随派活透传，派活前改名不破坏去重/正文 ----------
def test_mention_note_immune_to_displayname_drift():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    _task_fixtures()
    captured = {}
    async def fake_ask(key, prompt, **_kw):
        captured["prompt"] = prompt
        return "搞定"
    bot.runner.ask = fake_ask

    class DriftRoom(FakeRoom):
        def __init__(self, rid, n):
            super().__init__(rid, n)
            self.names = {"@alice:ex.org": "Alice", "@claudebot:ex.org": "claude-bot"}
        def user_name(self, uid):
            return self.names.get(uid, uid)

    room = DriftRoom("!drift:ex.org", 3)
    rid = room.room_id
    orig = (settings.stream_replies, settings.reply_in_thread)
    settings.stream_replies = False; settings.reply_in_thread = False
    try:
        bot._context[rid].clear()
        bot._context[rid].append((time.time(), "Bob", "垫一条背景"))
        ev = make_event("@claude-bot 问下 Alice 的进度", sender="@bob:ex.org", event_id="$dr1",
                        mentions=["@claudebot:ex.org", "@alice:ex.org"])
        async def go():
            await bot.on_message(room, ev)                    # 接收：存 ctx_body、spawn 派活（尚未跑）
            room.names["@alice:ex.org"] = "Alice·改名了"       # 派活跑起来之前显示名变了
            for _ in range(50):
                pend = [t for t in state._tasks if not t.done()]
                if not pend:
                    break
                await asyncio.gather(*pend, return_exceptions=True)
        asyncio.run(go())
        head, _, task_part = captured["prompt"].partition("【当前要你处理的任务】")
        assert "〔@了 Alice〕" in task_part                    # 用的是接收时的附注，不是派活时重算的
        assert "Alice·改名了" not in captured["prompt"]
        assert "问下 Alice 的进度" not in head                 # 去重没被漂移打破：当前消息未混进背景区
    finally:
        settings.stream_replies, settings.reply_in_thread = orig
        bot._context[rid].clear()

# ---------- 连发合并：短窗内同一人连发的消息并成一个任务，去重/附注不变形 ----------
def test_debounce_merges_rapid_messages():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    _task_fixtures()
    captured = {"n": 0}
    async def fake_ask(key, prompt, **_kw):
        captured["n"] += 1
        captured["prompt"] = prompt
        return "搞定"
    bot.runner.ask = fake_ask
    room = FakeRoom("!db:ex.org", 3)
    rid = room.room_id
    orig = (settings.stream_replies, settings.reply_in_thread, settings.message_debounce)
    settings.stream_replies = False; settings.reply_in_thread = False
    settings.message_debounce = 0.2
    try:
        bot._context[rid].clear()
        bot._context[rid].append((time.time(), "Bob", "垫一条背景"))
        async def go():
            # 第一条点名 + 紧接一条没点名的补充（一句话分两条打）；第二条若不被合并，
            # 会因 _mark_engaged 落进续话弱信号路径去打语义闸（这里未打桩，走到就会炸）
            await bot.on_message(room, make_event(
                "@claude-bot 修一下登录", mentions=[state.MY_ID],
                sender="@alice:ex.org", event_id="$db1"))
            await bot.on_message(room, make_event(
                "顺便把注册流程也看看", sender="@alice:ex.org", event_id="$db2"))
            for _ in range(80):
                pend = [t for t in state._tasks if not t.done()]
                if not pend:
                    break
                await asyncio.gather(*pend, return_exceptions=True)
        asyncio.run(go())
        assert captured["n"] == 1                                     # 两条并成一个任务，只派一次
        head, _, task = captured["prompt"].partition("【当前要你处理的任务】")
        assert "修一下登录" in task and "顺便把注册流程也看看" in task
        assert "修一下登录" not in head and "顺便把注册流程" not in head  # 多条 skip 全部剔出背景
        # 两条原文事后都标 dispatched（续接轮背景不再重复喂）
        marked = [it for it in bot._context[rid] if state._ctx_dispatched(it)]
        assert len(marked) == 2
        # 合并窗期间不冷场：👀 在等待前就打在首条触发消息上，任务收尾时照常撤掉
        acks = [m for m in c.sent if (m.get("m.relates_to") or {}).get("key") == "👀"
                and m["m.relates_to"].get("event_id") == "$db1"]
        assert len(acks) == 1 and c.redacted        # 只打一次（handle_task 不重复打），且已撤
    finally:
        settings.stream_replies, settings.reply_in_thread, settings.message_debounce = orig
        bot._pending_dispatch.clear()
        bot._context[rid].clear()


# ---------- 合并窗不吞控制消息：重置/取消/换绑/引用回复各走各的路 ----------
def test_debounce_guards_commands():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    _task_fixtures()
    asked = {"n": 0, "prompts": []}
    async def fake_ask(key, prompt, **_kw):
        asked["n"] += 1
        asked["prompts"].append(prompt)
        return "搞定"
    bot.runner.ask = fake_ask
    resets = []
    orig_reset = bot.runner.reset
    bot.runner.reset = lambda k: resets.append(k)
    room = FakeRoom("!dg:ex.org", 3)
    rid = room.room_id
    orig = (settings.stream_replies, settings.reply_in_thread, settings.message_debounce,
            settings.gitea_host)
    settings.stream_replies = False; settings.reply_in_thread = False
    settings.message_debounce = 0.2
    settings.gitea_host = "https://gitea.example.com"   # parse_repo_url fail-closed，认 URL 需受信主机
    try:
        # ① 窗内发重置词：不被吸收进任务正文，正常翻篇；待派的半截话一并作废（翻篇的意图涵盖它）
        bot._context[rid].clear()
        _drain_and_run(bot.on_message(room, make_event(
            "@claude-bot 修登录", mentions=[state.MY_ID], sender="@alice:ex.org", event_id="$g1")))
        # ↑ drain 会把 waiter 也等完——所以这里用手工时序：重开一个缓冲再立刻发 /reset
        async def go():
            await bot.on_message(room, make_event(
                "@claude-bot 改注册", mentions=[state.MY_ID], sender="@alice:ex.org", event_id="$g2"))
            # 重置词由另一人发（engaged 用户的裸 reset 会先过续话语义闸，测试里别去碰真 quick 调用；
            # 作废合并窗对全房间生效，谁发的都一样）
            await bot.on_message(room, make_event(
                "/reset", sender="@bob:ex.org", event_id="$g3"))
            for _ in range(80):
                pend = [t for t in state._tasks if not t.done()]
                if not pend:
                    break
                await asyncio.gather(*pend, return_exceptions=True)
        n0 = asked["n"]                     # ①里第一条正常派掉的不算
        asyncio.run(go())
        assert resets                                        # reset 真的执行了
        assert asked["n"] == n0                              # "改注册"被翻篇作废，没派
        assert not any("改注册" in p for p in asked["prompts"])
        assert any("已开启新对话" in (m.get("body") or "") for m in c.sent)

        # ② 窗内 /cancel：作废待派缓冲并告知，任务不再开跑
        c.sent.clear()
        async def go2():
            await bot.on_message(room, make_event(
                "@claude-bot 跑个大活", mentions=[state.MY_ID], sender="@alice:ex.org", event_id="$g4"))
            await bot.on_message(room, make_event(
                "/cancel", sender="@alice:ex.org", event_id="$g5"))
            for _ in range(80):
                pend = [t for t in state._tasks if not t.done()]
                if not pend:
                    break
                await asyncio.gather(*pend, return_exceptions=True)
        n1 = asked["n"]
        asyncio.run(go2())
        assert asked["n"] == n1                              # 被取消，没派
        assert any("已取消排队中的任务" in (m.get("body") or "") for m in c.sent)

        # ③ 窗内粘仓库 URL：不被吸收，走换绑提示分支
        c.sent.clear()
        async def go3():
            await bot.on_message(room, make_event(
                "@claude-bot 看下代码", mentions=[state.MY_ID], sender="@alice:ex.org", event_id="$g6"))
            await bot.on_message(room, make_event(
                "https://gitea.example.com/other/repo2", sender="@alice:ex.org", event_id="$g7"))
            for _ in range(80):
                pend = [t for t in state._tasks if not t.done()]
                if not pend:
                    break
                await asyncio.gather(*pend, return_exceptions=True)
        asyncio.run(go3())
        assert any("换绑" in (m.get("body") or "") for m in c.sent)   # URL 到达了绑定分支
        # URL 没被并进任何任务的正文（出现在背景区是正常的——它照常入上下文）
        assert not any("other/repo2" in p.partition("【当前要你处理的任务】")[2]
                       for p in asked["prompts"])

        # ④ 窗内的引用回复（strong）：不并入缓冲，独立成任务并解析引文
        async def go4():
            await bot.on_message(room, make_event(
                "@claude-bot 修这个", mentions=[state.MY_ID], sender="@alice:ex.org", event_id="$g8"))
            await bot.on_message(room, make_event(
                "@claude-bot 就是这条", mentions=[state.MY_ID], sender="@alice:ex.org",
                event_id="$g9", in_reply_to="$q1"))
            for _ in range(80):
                pend = [t for t in state._tasks if not t.done()]
                if not pend:
                    break
                await asyncio.gather(*pend, return_exceptions=True)
        n2 = asked["n"]
        asyncio.run(go4())
        assert asked["n"] == n2 + 2                          # 两个独立任务
        assert any("root-of-$q1" in p for p in asked["prompts"])   # 引文被解析（没有丢）
    finally:
        (settings.stream_replies, settings.reply_in_thread, settings.message_debounce,
         settings.gitea_host) = orig
        bot.runner.reset = orig_reset
        bot._pending_dispatch.clear()
        bot._context[rid].clear()

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
        ans = asyncio.run(matrix_io._emit_files(
            room, f"做好了\n[[send-file: {fp}]]\n外部 [[send-file: /etc/hostname]]", d, "$root"))
    finally:
        settings.send_files_back = orig
    assert "send-file" not in ans                                                  # 标记被抹掉
    assert c.uploaded and c.uploaded[0][0] == "chart.png" and c.uploaded[0][1].startswith("image/")
    imgs = [m for m in c.sent if m.get("msgtype") == "m.image"]
    assert imgs and imgs[0]["m.relates_to"]["rel_type"] == "m.thread"              # 附件也挂线程
    assert "不在允许目录内" in ans                                                 # /etc/... 被拦
    assert matrix_io._within_allowed(fp, d) and not matrix_io._within_allowed("/etc/hostname", d)

# ---------- 附件上传：nio 不会替我们关文件句柄，_send_file 自己必须收尾，否则每发一个附件漏一个 fd ----------
def test_send_file_closes_opened_handle():
    set_identity()
    import tempfile
    d = tempfile.mkdtemp(prefix="mxbot-files-fd-")
    fp = os.path.join(d, "a.png")
    with open(fp, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n")
    opened = []

    class FC:                          # 仿真实 nio：只读数据，从不替调用方关闭文件对象
        async def upload(self, provider, content_type=None, filename=None,
                         encrypt=False, filesize=None):
            f = provider(0, 0)
            opened.append(f)
            f.read()
            return types.SimpleNamespace(content_uri="mxc://ex.org/up1"), None
        async def room_send(self, *a, **k):
            return types.SimpleNamespace(event_id="$x")
    state.client = FC()
    room = FakeRoom("!fd:ex.org", 2)
    ok, _ = asyncio.run(matrix_io._send_file(room, fp, None))
    assert ok
    assert opened and opened[0].closed                  # _send_file 必须自己关掉，别指望 nio 关

# ---------- 附件回传超过单次上限（10 个）：多出的标记别悄悄消失，得让用户知道少发了几个 ----------
def test_emit_files_over_cap_notifies():
    set_identity()
    c = _CapClient(); state.client = c
    room = FakeRoom("!f11:ex.org", 2)
    orig = settings.send_files_back; settings.send_files_back = True
    markers = "\n".join(f"[[send-file: /no/such/file{i}]]" for i in range(11))
    try:
        ans = asyncio.run(matrix_io._emit_files(room, f"搞定\n{markers}", "/tmp", None))
    finally:
        settings.send_files_back = orig
    assert "send-file" not in ans                        # 标记文字本身总会被抹掉
    assert "超过单次上限" in ans and "1 个" in ans        # 但得告诉用户还有 1 个没尝试发送

# ---------- 新增能力 6) 流式：占位→编辑→定稿 ----------
def test_live_reply_streams_and_finalizes():
    set_identity()
    c = _CapClient(); state.client = c
    live = matrix_io._LiveReply("!lr:ex.org", thread_root="$root")
    async def go():
        await live.on_delta("正在看代码", None)        # 建占位（带线程）
        await live.finalize("最终答复", track=False)    # 定稿成 m.replace 编辑
    asyncio.run(go())
    assert c.sent[0]["m.relates_to"]["rel_type"] == "m.thread"                     # 占位挂线程
    edits = [m for m in c.sent if m.get("m.relates_to", {}).get("rel_type") == "m.replace"]
    assert edits and edits[-1]["m.new_content"]["body"] == "最终答复"             # 编辑成最终答复


TESTS = [
    ('启动+群任务全链路', test_startup_and_task_flow),
    ('线程/引用回复 helper+send', test_thread_helpers_and_send),
    ('群任务答复挂线程(旧式)', test_group_task_reply_threaded),
    ('群答复默认平铺+插话时引用回复', test_group_flat_reply_and_smart_quote),
    ('未绑定群当通用助手聊', test_group_unbound_chat_general),
    ('任务回执 👀 打上/撤掉', test_task_ack_reaction),
    ('runner 会话分叉 fork/续/父失效', test_runner_fork_session),
    ('runner 会话过期全新开触发 on_reset', test_runner_on_reset_fires_on_expired_session),
    ('runner fork 接上父会话时不误触发 on_reset', test_runner_on_reset_skipped_when_fork_rescues_expired_thread),
    ('线程会话细分+起点背景+线程reset', test_thread_scoped_session_forks),
    ('「@了 谁」附注贯通背景/任务正文', test_mention_note_in_context_and_prompt),
    ('「@了 谁」附注抗显示名漂移', test_mention_note_immune_to_displayname_drift),
    ('连发合并 一个任务/去重/标记/提前回执', test_debounce_merges_rapid_messages),
    ('合并窗不吞 重置/取消/换绑/引用回复', test_debounce_guards_commands),
    ('/help + 进房欢迎', test_help_and_welcome),
    ('/summarize 小结最近对话', test_summarize_command),
    ('/cancel 停当前任务', test_cancel_command),
    ('附件回传 允许/拦截/抹标记', test_emit_files_allowed_blocked_stripped),
    ('附件上传后关闭文件句柄', test_send_file_closes_opened_handle),
    ('附件回传超上限 提示未发送数量', test_emit_files_over_cap_notifies),
    ('流式 占位→编辑→定稿', test_live_reply_streams_and_finalizes),
]
