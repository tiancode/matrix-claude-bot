"""冒烟：背景上下文分范围/剔重·续接 resume·发送退避·会话失效匹配·媒体行去重"""
from _helpers import (
    FakeRoom, _drain_tasks, asyncio, bot, claude_runner, make_event, make_media_event, media, set_identity, settings, state, tasks, time, types)

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

# ---------- 32b) 背景缓冲按线程分范围：线程的话不串进主时间线，反之亦然 ----------
def test_context_thread_scoping():
    import state
    set_identity()
    rid = "!ctxthr:ex.org"
    root = "$thread_root"
    bot._context[rid].clear()
    orig = settings.context_lines
    settings.context_lines = 20
    try:
        # 顶层三条 + 线程内两条（第 4 元 = 线程根）+ 一条老式 3 元组（应按顶层算）
        bot._context[rid].append((time.time(), "Alice", "顶层：讲个故事", None))
        bot._context[rid].append((time.time(), "claude-bot", "从前有座山", None))
        bot._context[rid].append((time.time(), "Alice", "线程里：改下这个函数", root))
        bot._context[rid].append((time.time(), "claude-bot", "改好了", root))
        bot._context[rid].append((time.time(), "Bob", "老式三元组算顶层"))   # 3 元组 → _ctx_thread=None

        # 顶层范围（默认）：只见顶层，绝不含线程里的话
        top = bot._format_context(rid)
        assert "讲个故事" in top and "从前有座山" in top and "老式三元组算顶层" in top
        assert "改下这个函数" not in top and "改好了" not in top   # ← 漏补上了：线程的话不进主时间线背景

        # 线程范围：只见该线程，不含顶层
        thr = bot._format_context(rid, thread=root)
        assert "改下这个函数" in thr and "改好了" in thr
        assert "讲个故事" not in thr and "老式三元组算顶层" not in thr

        # _ctx_thread 容忍 3 元组（无标记）→ None
        assert state._ctx_thread((0, "x", "y")) is None
        assert state._ctx_thread((0, "x", "y", root)) == root

        # 续话窗口的"第三人插话"只看顶层：线程里 Bob 说话不该作废主聊天的续话窗口
        import addressing
        bot._context[rid].clear()
        t0 = time.time()
        bot._context[rid].append((t0 + 1, "Bob", "线程里插一句", root))   # 线程内第三人
        assert addressing._third_party_spoke_since(rid, t0, "Alice") is False
        bot._context[rid].append((t0 + 2, "Bob", "主时间线插一句", None))  # 顶层第三人
        assert addressing._third_party_spoke_since(rid, t0, "Alice") is True

        # bot 自己的答复也要带线程标记（_track_reply 走 send/流式定稿两条路都传 thread_root），
        # 否则线程里的答复被当顶层、串进主时间线背景，且线程范围反而看不到它
        import matrix_io
        bot._context[rid].clear()
        matrix_io._track_reply(rid, "线程里的答复", "$root2")
        matrix_io._track_reply(rid, "顶层的答复")            # 缺省 → None
        assert state._ctx_thread(bot._context[rid][0]) == "$root2"
        assert state._ctx_thread(bot._context[rid][1]) is None
        assert "线程里的答复" not in bot._format_context(rid)              # 顶层背景不含线程答复
        assert "线程里的答复" in bot._format_context(rid, thread="$root2")  # 线程范围能看到
    finally:
        settings.context_lines = orig
        bot._context[rid].clear()

# ---------- 32c) 续接时把「以前派过的用户消息」从背景剔掉（已在 --resume 里，别重复喂）----------
def test_context_drop_dispatched():
    import state
    set_identity()
    rid = "!disp:ex.org"
    bot._context[rid].clear()
    orig = settings.context_lines
    settings.context_lines = 20
    try:
        t = time.time()
        bot._context[rid].append((t, "Alice", "以前派过的活", None))
        bot._context[rid].append((t, "Bob", "路过没派的闲聊", None))
        state._mark_dispatched(rid, "Alice", "以前派过的活")   # 标记「这条派过给 Claude 了」
        assert state._ctx_dispatched(bot._context[rid][0]) is True    # 就近命中并置真
        assert state._ctx_dispatched(bot._context[rid][1]) is False
        assert state._ctx_dispatched((0, "x", "y")) is False          # 短元组容忍 → False
        assert state._ctx_dispatched((0, "x", "y", None)) is False

        # 续接（drop_dispatched=True）：派过的消失，没派的闲聊仍在
        resumed = bot._format_context(rid, drop_dispatched=True)
        assert "以前派过的活" not in resumed and "路过没派的闲聊" in resumed
        # 全新/首轮（默认 False）：背景是唯一来源，派过的也照常带
        fresh = bot._format_context(rid, drop_dispatched=False)
        assert "以前派过的活" in fresh and "路过没派的闲聊" in fresh
    finally:
        settings.context_lines = orig
        bot._context[rid].clear()

# ---------- 32d) 端到端：续接轮的 prompt 背景里不再重复「上一轮派过的消息」 ----------
def test_run_drops_dispatched_on_resume():
    import types
    set_identity()
    rid = "!dispe2e:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    caught = []

    class R:
        def __init__(self): self.live = False
        def busy(self, k): return False
        def running(self, k): return 0
        def session_ts(self, k): return 111.0 if self.live else None   # 首轮无会话，派完才有
        async def ask(self, key, prompt, cwd=None, system_prompt=None, lock_key=None,
                      prepare=None, on_delta=None, cancel_key=None, **_kw):
            caught.append(prompt)
            self.live = True
            return "ok"

    class FC:
        async def room_send(self, *a, **k): return types.SimpleNamespace(event_id="$x")

    rec = {"id": "p", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}
    orig = (tasks.runner, state.client)
    tasks.runner, state.client = R(), FC()
    try:
        bot._context[rid].append((time.time(), "Bob", "路过没派的闲聊", None))
        bot._context[rid].append((time.time(), "Alice", "改 A", None))
        asyncio.run(tasks._run_on_project(room, make_event("改 A", event_id="$e1"), "改 A", rec, skip_body="改 A"))
        bot._context[rid].append((time.time(), "Alice", "改 B", None))
        asyncio.run(tasks._run_on_project(room, make_event("改 B", event_id="$e2"), "改 B", rec, skip_body="改 B"))
    finally:
        (tasks.runner, state.client) = orig
        bot._context[rid].clear()
    # 首轮无会话 → 背景照常带上闲聊；次轮续接 → 上一轮派过的「改 A」不再进背景，闲聊仍在
    assert "路过没派的闲聊" in caught[0]
    assert "改 A" not in caught[1]           # ← 残留去掉了：派过的消息不再从背景重复喂
    assert "路过没派的闲聊" in caught[1]      # 没派过的仍照常带

# ---------- 32e) resume 运行时失效回退全新开：清 dispatched 标记，别让被剔的消息永久两头落空 ----------
def test_dispatched_cleared_on_resume_failure():
    import types
    set_identity()
    rid = "!dispfail:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    caught = []

    class R:
        def __init__(self): self.turn = 0
        def busy(self, k): return False
        def running(self, k): return 0
        def session_ts(self, k): return 111.0 if self.turn > 0 else None
        async def ask(self, key, prompt, cwd=None, system_prompt=None, lock_key=None,
                      prepare=None, on_delta=None, cancel_key=None, on_reset=None, **_kw):
            caught.append(prompt)
            self.turn += 1
            if self.turn == 2 and on_reset:   # 第二轮模拟 --resume 被判失效 → runner 回退全新开、回调 on_reset
                on_reset()
            return "ok"

    class FC:
        async def room_send(self, *a, **k): return types.SimpleNamespace(event_id="$x")

    rec = {"id": "p", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}
    orig = (tasks.runner, state.client)
    tasks.runner, state.client = R(), FC()
    try:
        bot._context[rid].append((time.time(), "Alice", "改 A", None))
        asyncio.run(tasks._run_on_project(room, make_event("改 A", event_id="$e1"), "改 A", rec, skip_body="改 A"))
        assert state._ctx_dispatched(bot._context[rid][0]) is True     # 改 A 已标 dispatched

        bot._context[rid].append((time.time(), "Alice", "改 B", None))
        # 第二轮续接：拼背景时 drop_dispatched 会剔「改 A」（单轮降级不可免）；但 ask 里 --resume 失效
        # → on_reset 触发 → _clear_dispatched 把标记清掉
        asyncio.run(tasks._run_on_project(room, make_event("改 B", event_id="$e2"), "改 B", rec, skip_body="改 B"))
        assert state._ctx_dispatched(bot._context[rid][0]) is False    # on_reset 已清掉改 A 的标记

        bot._context[rid].append((time.time(), "Alice", "改 C", None))
        asyncio.run(tasks._run_on_project(room, make_event("改 C", event_id="$e3"), "改 C", rec, skip_body="改 C"))
    finally:
        (tasks.runner, state.client) = orig
        bot._context[rid].clear()
    assert "改 A" not in caught[1]   # 第二轮（on_reset 之前拼的）确实剔了改 A —— 单轮降级
    assert "改 A" in caught[2]       # ★关键：resume 失败没让改 A 永久丢，第三轮背景把它带回来了

# ---------- 32f) dispatched 只在派活【成功】后才标：取消/报错没进会话的消息不该被标（否则下轮误剔）----------
def test_dispatched_not_marked_on_cancel():
    import types, claude_runner
    set_identity()
    rid = "!dispcancel:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()

    class R:
        def busy(self, k): return False
        def running(self, k): return 0
        def session_ts(self, k): return None
        async def ask(self, key, prompt, **_kw):
            raise claude_runner.ClaudeCancelled()   # 模拟 /cancel：ask 抛取消，这条根本没进会话

    class FC:
        async def room_send(self, *a, **k): return types.SimpleNamespace(event_id="$x")

    rec = {"id": "p", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}
    orig = (tasks.runner, state.client, settings.stream_replies)
    tasks.runner, state.client = R(), FC()
    settings.stream_replies = False
    try:
        bot._context[rid].append((time.time(), "Alice", "改 X", None))
        asyncio.run(tasks._run_on_project(room, make_event("改 X", event_id="$e1"), "改 X", rec, skip_body="改 X"))
        marked = state._ctx_dispatched(bot._context[rid][0])   # 取消后先读，别等 finally 清了再读
    finally:
        (tasks.runner, state.client, settings.stream_replies) = orig
        bot._context[rid].clear()
    assert marked is False   # 被取消、没进会话 → 不该标 dispatched

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
    assert "答复" in [b for _, _, b, *_ in bot._context[rid]]  # 最终成功 → 入上下文
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
        def session_ts(self, k): return None      # 无续接会话 → 背景照常全带（不剔派过的）
        async def ask(self, key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                      on_delta=None, cancel_key=None, **_kw):
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
        asyncio.run(tasks._run_on_project(room, ev, "看看这个文件 /m/a.log", rec, skip_body=line))
    finally:
        (tasks.runner, state.client) = orig
        bot._context[rid].clear()
    assert "[文件] a.log" not in captured["prompt"]        # 媒体行不再重复出现在 prompt
    assert "之前聊的别的事" in captured["prompt"]           # 其它背景仍照常带上

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
            await media._process_media(room, make_media_event(body="big.bin", event_id="$ub"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_max_mb, settings.media_enabled,
         state.client, bot.handle_task) = orig
    assert any("超过上限" in b for _, _, b, *_ in bot._context[rid])
    bot._context[rid].clear()


TESTS = [
    ('CONTEXT_LINES=0 不带背景', test_context_lines_zero_means_none),
    ('背景缓冲按线程分范围', test_context_thread_scoping),
    ('续接剔掉派过的用户消息', test_context_drop_dispatched),
    ('端到端 续接背景不重复派过的', test_run_drops_dispatched_on_resume),
    ('resume 失效回退清 dispatched 不丢上下文', test_dispatched_cleared_on_resume_failure),
    ('dispatched 只在成功后标 取消不标', test_dispatched_not_marked_on_cancel),
    ('发送限流退避重试', test_send_retries_on_rate_limit),
    ('会话失效匹配收紧', test_session_error_matching_tightened),
    ('媒体行不在 prompt 重复', test_run_on_project_skips_media_line),
    ('未声明大文件按真实大小拦下', test_media_oversize_undeclared_streamed),
]
