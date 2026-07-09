"""冒烟：用户/管理员删消息(m.room.redaction) → 清本地留存（逐字记录/媒体文件/待派缓冲）。"""
import tempfile

from _helpers import (
    FakeRoom, asyncio, bot, claude_runner, fmt, make_event, os, set_identity, settings, state,
    tasks, time, types, _CapClient, _task_fixtures)
import transcript


def _redaction(redacts, sender="@alice:ex.org"):
    """构造一条 m.room.redaction 事件（nio RedactionEvent 的最小替身）：redacts 指向被删消息。"""
    return types.SimpleNamespace(redacts=redacts, sender=sender)


# ---------- 1) 文本删除：物理删掉逐字记录里那一行，别的行不动 ----------
def test_redaction_removes_transcript_line():
    set_identity(); state._synced = True
    rid = "!rd:ex.org"
    room = FakeRoom(rid, 3)
    orig = (settings.store_path, settings.transcript_enabled)
    settings.store_path = tempfile.mkdtemp(prefix="mxbot-rd-")
    settings.transcript_enabled = True
    try:
        transcript.append(rid, "Alice", "第一条", event_id="$r1")
        transcript.append(rid, "Alice", "第二条", event_id="$r2")
        transcript.append(rid, "Alice", "第三条", event_id="$r3")
        asyncio.run(bot.on_redaction(room, _redaction("$r2")))
        ids = [r.get("id") for r in transcript._read_all(rid)]
        assert ids == ["$r1", "$r3"]                         # 被删那行没了，其余保留
        bodies = [r.get("body") for r in transcript._read_all(rid)]
        assert "第二条" not in bodies
    finally:
        transcript.discard(rid)
        settings.store_path, settings.transcript_enabled = orig


# ---------- 2) 媒体删除：删掉以 event_id 为前缀的本地文件 ----------
def test_redaction_removes_media_file():
    set_identity(); state._synced = True
    rid = "!rdm:ex.org"
    room = FakeRoom(rid, 3)
    orig = settings.media_root
    settings.media_root = tempfile.mkdtemp(prefix="mxbot-rdm-")
    try:
        room_dir = os.path.join(settings.media_root, fmt._safe_name(rid, "room"))
        os.makedirs(room_dir)
        # 与 media._save_media 同款命名：<safe(event_id)>__<safe(文件名)>
        victim = os.path.join(room_dir, fmt._safe_name("$m1", "ev") + "__app.log")
        keep = os.path.join(room_dir, fmt._safe_name("$m2", "ev") + "__other.log")
        open(victim, "w").close()
        open(keep, "w").close()
        asyncio.run(bot.on_redaction(room, _redaction("$m1")))
        assert not os.path.exists(victim)                    # 被删消息的文件没了
        assert os.path.exists(keep)                          # 别的文件不动
    finally:
        import shutil
        shutil.rmtree(settings.media_root, ignore_errors=True)
        settings.media_root = orig


# ---------- 3) 待派缓冲：删的是唯一一条 → 整个缓冲摘掉，任务不派 ----------
def test_redaction_drops_pending_single():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    _task_fixtures()
    asked = {"n": 0}
    async def fake_ask(key, prompt, **_kw):
        asked["n"] += 1
        return "搞定"
    bot.runner.ask = fake_ask
    room = FakeRoom("!rdp:ex.org", 3)
    rid = room.room_id
    orig = (settings.stream_replies, settings.reply_in_thread, settings.message_debounce)
    settings.stream_replies = False; settings.reply_in_thread = False
    settings.message_debounce = 0.5
    try:
        async def go():
            await bot.on_message(room, make_event(
                "@claude-bot 修登录", mentions=[state.MY_ID], sender="@alice:ex.org", event_id="$p1"))
            key = (rid, "@alice:ex.org", None)
            assert bot._pending_dispatch.get(key) is not None    # 缓冲已建、waiter 在睡
            await bot.on_redaction(room, _redaction("$p1"))       # 这条被删
            assert bot._pending_dispatch.get(key) is None         # 单条 → 整个缓冲摘掉
            for _ in range(80):                                   # 收割 waiter（醒来发现缓冲没了自退）
                pend = [t for t in state._tasks if not t.done()]
                if not pend:
                    break
                await asyncio.gather(*pend, return_exceptions=True)
        asyncio.run(go())
        assert asked["n"] == 0                                    # 被删的消息没被派活
    finally:
        (settings.stream_replies, settings.reply_in_thread, settings.message_debounce) = orig
        bot._pending_dispatch.clear()
        bot._context[rid].clear()


# ---------- 4) 待派缓冲：删掉【补充】那条 → 缓冲留下 anchor，anchor 照常派 ----------
def test_redaction_drops_supplement_keeps_anchor():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    _task_fixtures()
    asked = {"n": 0, "prompts": []}
    async def fake_ask(key, prompt, **_kw):
        asked["n"] += 1
        asked["prompts"].append(prompt)
        return "搞定"
    bot.runner.ask = fake_ask
    room = FakeRoom("!rdp2:ex.org", 3)
    rid = room.room_id
    orig = (settings.stream_replies, settings.reply_in_thread, settings.message_debounce)
    settings.stream_replies = False; settings.reply_in_thread = False
    settings.message_debounce = 0.5
    try:
        async def go():
            await bot.on_message(room, make_event(          # 首条 strong = anchor，建缓冲
                "@claude-bot 修登录", mentions=[state.MY_ID], sender="@alice:ex.org", event_id="$p1"))
            await bot.on_message(room, make_event(          # 没点名的补充 → 并入同一缓冲
                "顺便看下注册", sender="@alice:ex.org", event_id="$p2"))
            await bot.on_redaction(room, _redaction("$p2"))  # 删掉【补充】，anchor 还在
            key = (rid, "@alice:ex.org", None)
            assert bot._pending_dispatch.get(key) is not None  # 缓冲还在（anchor 未删）
            for _ in range(80):
                pend = [t for t in state._tasks if not t.done()]
                if not pend:
                    break
                await asyncio.gather(*pend, return_exceptions=True)
        asyncio.run(go())
        assert asked["n"] == 1                              # anchor 照常派了一次
        prompt = asked["prompts"][0]
        assert "修登录" in prompt                            # anchor 进了任务
        assert "顺便看下注册" not in prompt                  # 被删的补充：既不在任务正文、也（被 _context 剔除后）不在背景
    finally:
        (settings.stream_replies, settings.reply_in_thread, settings.message_debounce) = orig
        bot._pending_dispatch.clear()
        bot._context[rid].clear()


# ---------- 5) 待派缓冲：删掉 anchor（根消息）→ 整个缓冲作废，无独立寻址的补充不单独派 ----------
def test_redaction_anchor_drops_whole_buffer():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    _task_fixtures()
    asked = {"n": 0}
    async def fake_ask(key, prompt, **_kw):
        asked["n"] += 1
        return "搞定"
    bot.runner.ask = fake_ask
    room = FakeRoom("!rdp3:ex.org", 3)
    rid = room.room_id
    orig = (settings.stream_replies, settings.reply_in_thread, settings.message_debounce)
    settings.stream_replies = False; settings.reply_in_thread = False
    settings.message_debounce = 0.5
    try:
        async def go():
            await bot.on_message(room, make_event(          # anchor（strong）
                "@claude-bot 上线到生产", mentions=[state.MY_ID], sender="@alice:ex.org", event_id="$p1"))
            await bot.on_message(room, make_event(          # 无点名补充，被吸收
                "顺便清下缓存", sender="@alice:ex.org", event_id="$p2"))
            key = (rid, "@alice:ex.org", None)
            assert bot._pending_dispatch.get(key) is not None
            await bot.on_redaction(room, _redaction("$p1"))  # 删掉 anchor（反悔了这次上线）
            assert bot._pending_dispatch.get(key) is None    # 整个缓冲作废，补充不落单
            for _ in range(80):
                pend = [t for t in state._tasks if not t.done()]
                if not pend:
                    break
                await asyncio.gather(*pend, return_exceptions=True)
        asyncio.run(go())
        assert asked["n"] == 0                              # 什么都没派——补充"顺便清下缓存"没被当独立任务
    finally:
        (settings.stream_replies, settings.reply_in_thread, settings.message_debounce) = orig
        bot._pending_dispatch.clear()
        bot._context[rid].clear()


# ---------- 5) 边界：不存在的目标 / bot 自撤 reaction / 积压期，都安全不误伤 ----------
def test_redaction_noops_safely():
    set_identity(); state._synced = True
    rid = "!rdn:ex.org"
    room = FakeRoom(rid, 3)
    orig = (settings.store_path, settings.transcript_enabled, settings.process_backlog)
    settings.store_path = tempfile.mkdtemp(prefix="mxbot-rdn-")
    settings.transcript_enabled = True
    settings.process_backlog = False
    try:
        transcript.append(rid, "Alice", "别动我", event_id="$keep")
        # a) 删一个不存在的 event_id：不报错，什么都不删
        asyncio.run(bot.on_redaction(room, _redaction("$nope")))
        assert [r.get("id") for r in transcript._read_all(rid)] == ["$keep"]
        # b) bot 自己发的 redaction（撤 reaction 会回灌成这个）：即便目标真存在也提前挡掉、不清
        asyncio.run(bot.on_redaction(room, _redaction("$keep", sender=state.MY_ID)))
        assert [r.get("id") for r in transcript._read_all(rid)] == ["$keep"]
        # c) 初始 sync 积压期（未 synced）：跳过不处理
        state._synced = False
        asyncio.run(bot.on_redaction(room, _redaction("$keep")))
        assert [r.get("id") for r in transcript._read_all(rid)] == ["$keep"]
    finally:
        state._synced = True
        transcript.discard(rid)
        (settings.store_path, settings.transcript_enabled, settings.process_backlog) = orig


# ---------- 6) runner 排队队列：cancel_event 只撤【还没起进程】的那一个 ----------
def test_cancel_event_only_queued():
    r = claude_runner.ClaudeRunner()
    T = claude_runner._CancelToken
    rid = "!q:ex.org"
    queued = T("$q1")                              # 排队中（started=False）
    running = T("$q1"); running.started = True     # 同 event_id 但已起进程在跑
    other = T("$q2")                               # 别条消息触发的排队任务
    r._tokens[rid] = {queued, running, other}
    n = r.cancel_event(rid, "$q1")
    assert n == 1                                  # 只撤了排队中的那一个
    assert queued.cancelled and queued.silent      # 置了取消 + 静默
    assert not running.cancelled                   # 在跑的不碰（删除不中断在跑任务）
    assert not other.cancelled                     # 别条消息不误伤
    assert r.cancel_event(rid, "") == 0            # 空 event_id 不做
    assert r.cancel_event(rid, "$none") == 0       # 不存在的 event_id


# ---------- 7) on_redaction 接线：删消息 → 撤 runner 里那条排队任务 ----------
def test_redaction_cancels_queued_task():
    set_identity(); state._synced = True
    rid = "!rdq:ex.org"
    room = FakeRoom(rid, 3)
    tok = claude_runner._CancelToken("$q1")        # 一条已派进 runner、还在排队等锁的任务
    bot.runner._tokens.setdefault(rid, set()).add(tok)
    try:
        asyncio.run(bot.on_redaction(room, _redaction("$q1")))
        assert tok.cancelled and tok.silent        # 删消息把它撤了，且静默（不回"已停止"）
    finally:
        bot.runner._tokens.pop(rid, None)


# ---------- 8) 静默收尾：silent 取消不回"🛑 已停止"、且撤掉「⏳ 已排队」回执；普通 /cancel 照回、留回执 ----------
def test_silent_cancel_sends_no_message():
    set_identity()
    rid = "!rdsilent:ex.org"
    room = FakeRoom(rid, 2)
    rec = {"id": "p", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}

    class FC:
        def __init__(self):
            self.sent = []
            self.redacted = []
        async def room_send(self, r_, mt, content, **k):
            self.sent.append(content.get("body") or "")
            return types.SimpleNamespace(event_id="$x%d" % len(self.sent))
        async def room_typing(self, *a, **k):
            return None
        async def room_redact(self, r_, eid, **k):
            self.redacted.append(eid)
            return types.SimpleNamespace(event_id="$rd%d" % len(self.redacted))

    class R:
        def __init__(self, silent):
            self._silent = silent
        def busy(self, k):
            return True    # 让派活层先发一条「⏳ 已排队」回执（本用例要验证它被撤掉）
        def running(self, k):
            return 0
        def session_ts(self, k):
            return None
        async def ask(self, key, prompt, **_kw):
            raise claude_runner.ClaudeCancelled(silent=self._silent)

    orig = (tasks.runner, state.client, settings.stream_replies)
    settings.stream_replies = False
    try:
        # a) silent=True（删消息撤排队任务）→ 不回"已停止"，且「⏳ 已排队」回执被撤掉
        fc = FC(); tasks.runner, state.client = R(True), fc
        asyncio.run(tasks._run_on_project(
            room, make_event("干活", event_id="$e1"), "干活", rec, skip_body="干活"))
        assert any("已排队" in s for s in fc.sent)       # 确实发过排队回执
        assert not any("已停止" in s for s in fc.sent)   # 但不回"已停止"（静默）
        assert fc.redacted == ["$x1"]                     # 排队回执（首条 send）被撤掉
        # b) silent=False（真 /cancel）→ 照回"🛑 已停止"，排队回执保留不撤
        fc2 = FC(); tasks.runner, state.client = R(False), fc2
        asyncio.run(tasks._run_on_project(
            room, make_event("干活2", event_id="$e2"), "干活2", rec, skip_body="干活2"))
        assert any("已停止" in s for s in fc2.sent)
        assert fc2.redacted == []                         # 普通取消不撤排队回执
    finally:
        (tasks.runner, state.client, settings.stream_replies) = orig
        bot._context[rid].clear()


# ---------- 9) sema 竞态：等 sema 时被静默撤 → 子进程一起来就被杀，绝不背着用户跑完 ----------
def test_silent_cancel_kills_proc_spawned_after_cancel():
    cr = claude_runner

    async def go():
        r = cr.ClaudeRunner()
        killed = []
        orig_kill = cr._kill_group
        cr._kill_group = lambda p: (killed.append(p), setattr(p, "returncode", -9))

        class P:
            def __init__(self):
                self.pid, self.returncode = 4321, None

        async def fake_run(cmd, cwd=None, on_proc=None):
            # 模拟：B 已过锁 / prepare 检查点，正卡在 sema 上（token.started 仍 False）。此刻删消息静默撤它。
            r.cancel_event("room", "$b")
            proc = P()
            if on_proc:
                on_proc(proc)              # _reg：应发现已 cancelled → 立刻杀掉刚起的 proc
            return proc.returncode or -9, b"", b""

        r._run = fake_run
        try:
            res = await asyncio.gather(
                r.ask("B", "b", lock_key="proj", cancel_key="room", trigger_eid="$b"),
                return_exceptions=True)
            return killed, res[0]
        finally:
            cr._kill_group = orig_kill

    killed, exc = asyncio.run(go())
    assert isinstance(exc, cr.ClaudeCancelled)        # 按取消退
    assert getattr(exc, "silent", False) is True      # 且静默（删消息撤的）
    assert len(killed) == 1                            # 刚起的子进程被立刻杀，没背着用户跑完（push/开 PR）


# ---------- 10) 派活链间隙删消息：ask 登记 token 后查"已删名单"，静默了断、不进锁不开跑 ----------
def test_mark_redacted_aborts_before_run():
    cr = claude_runner

    async def go():
        r = cr.ClaudeRunner()
        ran = {"v": False}

        async def fake_run(cmd, cwd=None, on_proc=None):
            ran["v"] = True   # 不该走到这——ask 应在进锁前就了断
            proc = types.SimpleNamespace(pid=1, returncode=0)
            if on_proc:
                on_proc(proc)
            return 0, b'{"result":"x","session_id":"s","is_error":false}', b""

        r._run = fake_run
        r.mark_redacted("$gap")   # 消息在 clone/fetch 间隙就被删了（那时 token 还没登记）
        res = await asyncio.gather(
            r.ask("k", "干活", lock_key="proj", cancel_key="room", trigger_eid="$gap"),
            return_exceptions=True)
        return ran["v"], res[0]

    ran, exc = asyncio.run(go())
    assert ran is False                                # 根本没起子进程
    assert isinstance(exc, cr.ClaudeCancelled)         # 静默了断
    assert getattr(exc, "silent", False) is True


# ---------- 11) 内存背景：删消息也从 _context 剔掉（默认关 transcript 也生效，堵误粘密钥仍进模型）----------
def test_redaction_purges_context():
    set_identity(); state._synced = True
    c = _CapClient(); state.client = c
    rid = "!rdctx:ex.org"
    room = FakeRoom(rid, 3)
    orig = (settings.transcript_enabled, settings.proactive)
    settings.transcript_enabled = False   # 默认就是关的：内存背景剔除不依赖 transcript
    settings.proactive = False            # 没点名的消息别去走主动插话判断
    bot._context[rid].clear(); state._ctx_recent.clear()
    try:
        # 走真实入口 on_message：它同时落 _context 和 _ctx_recent 索引
        asyncio.run(bot.on_message(room, make_event(
            "我的密钥 sk-secret-xyz", sender="@alice:ex.org", event_id="$sec")))
        asyncio.run(bot.on_message(room, make_event(
            "另一句无关的话", sender="@alice:ex.org", event_id="$keep")))
        assert any("sk-secret-xyz" in it[2] for it in bot._context[rid])   # 先确认进了背景
        asyncio.run(bot.on_redaction(room, _redaction("$sec")))
        bodies = [it[2] for it in bot._context[rid]]
        assert not any("sk-secret-xyz" in b for b in bodies)   # 删后从内存背景剔掉，不再喂给下一个任务
        assert any("另一句无关的话" in b for b in bodies)        # 其它不动
    finally:
        (settings.transcript_enabled, settings.proactive) = orig
        bot._context[rid].clear(); state._ctx_recent.clear()


TESTS = [
    ('删消息 清逐字记录那一行', test_redaction_removes_transcript_line),
    ('删消息 剔内存背景 _context', test_redaction_purges_context),
    ('删消息 清本地媒体文件', test_redaction_removes_media_file),
    ('删消息 摘待派缓冲(唯一条不派)', test_redaction_drops_pending_single),
    ('删消息 摘待派缓冲(删补充留 anchor)', test_redaction_drops_supplement_keeps_anchor),
    ('删消息 删 anchor 整个缓冲作废', test_redaction_anchor_drops_whole_buffer),
    ('删消息 边界(不存在/自撤/积压)安全', test_redaction_noops_safely),
    ('删消息 cancel_event 只撤排队中的', test_cancel_event_only_queued),
    ('删消息 撤 runner 排队任务(接线)', test_redaction_cancels_queued_task),
    ('删消息 静默收尾不回"已停止"', test_silent_cancel_sends_no_message),
    ('删消息 等sema被撤 子进程即起即杀', test_silent_cancel_kills_proc_spawned_after_cancel),
    ('删消息 派活间隙删 进锁前静默了断', test_mark_redacted_aborts_before_run),
]
