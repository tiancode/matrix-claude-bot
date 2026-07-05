"""冒烟：任务取消/停排队/子进程组·流式异常收尾·重启后回复·引用回复回捞·忽略自评·/summarize 剔命令"""
from _helpers import (
    FakeRoom, _CapClient, _reset_ledger, addressing, asyncio, bot, claude_runner, heartbeat, json, make_event, pr_followup, set_identity, settings, state, tasks, time, types)

# ---------- 2.5) 引用回复：拉回被引用消息的内容（真实回归） ----------
def test_quoted_reply_fetches_referenced_message():
    """真实回归（!bBJJRELZSgyuyJQbum 复现）：用户引用回复某条消息只 @ 了 bot，客户端只发
    m.in_reply_to 指针、不内联引文，本条正文剥完只剩空——被引用的内容对 bot 不可见，于是回空话。
    修法：_quoted_subject 按 event_id 向服务器拉回原消息内容当主题；线程回退不当"引用"。"""
    set_identity()
    room = FakeRoom("!q:ex.org", 3)
    quoted = make_event("给我讲一个笑话", sender="@alice:ex.org", event_id="$q1")

    class FakeClient:
        rooms = {}
        async def room_get_event(self, rid, eid):
            assert eid == "$q1"
            return types.SimpleNamespace(event=quoted)

    orig = getattr(state, "client", None)
    state.client = FakeClient()
    try:
        # ① 真·引用回复（只带 in_reply_to 指针）→ 拉回被引用内容
        ev = make_event("claude", in_reply_to="$q1", mentions=["@claudebot:ex.org"])
        got = asyncio.run(tasks._quoted_subject(room, ev))
        assert "给我讲一个笑话" in got and "Alice" in got
        # ② 线程回退（rel_type=m.thread / is_falling_back）不是主动引用 → 不取
        ev2 = make_event("接着改", in_reply_to="$q1")
        ev2.source["content"]["m.relates_to"].update(rel_type="m.thread", is_falling_back=True)
        assert asyncio.run(tasks._quoted_subject(room, ev2)) == ""
        # ③ 压根不是引用 → 空串
        assert asyncio.run(tasks._quoted_subject(room, make_event("hi"))) == ""
    finally:
        state.client = orig

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
                      prepare=None, on_delta=None, cancel_key=None, **_kw):
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
        def session_ts(self, k): return None
        async def ask(self, key, prompt, cwd=None, system_prompt=None, lock_key=None,
                      prepare=None, on_delta=None, cancel_key=None, **_kw):
            if on_delta:
                await on_delta("看了一半代码", "Bash")     # 先造出占位消息（停在"正在干活"）
            raise RuntimeError("claude 退出码 1: boom")     # 再异常退出

    rec = {"id": "p", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}
    orig = (tasks.runner, settings.stream_replies)
    tasks.runner = R()
    settings.stream_replies = True
    try:
        asyncio.run(tasks._run_on_project(room, make_event("干个活"), "干个活", rec))  # 不该往外抛
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
    a, t = addressing._is_addressed(room, ev)
    assert a and t == "继续弄"                             # 重启（_sent_events 清空）后仍认得

    state.client = make_client("@bob:ex.org")             # 被回复的是别人 → 不误当点名，且只查一次
    ev2 = make_event("> <@bob:ex.org> x\n\n哈哈", in_reply_to="$old2")
    asyncio.run(bot._resolve_reply_author(room.room_id, ev2.source["content"]))
    asyncio.run(bot._resolve_reply_author(room.room_id, ev2.source["content"]))
    a2, _ = addressing._is_addressed(room, ev2)
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
        asyncio.run(pr_followup._followup_one(entry))
        e4 = [e for e in pr_ledger.active() if e["number"] == 4][0]
        assert not spawned and e4["review_fixes"] == 0    # 自己的评论：不派活、不烧自动处理次数
        assert e4["seen_review"] == 11                    # 但水位照常推进，下一轮不再重看
        gitea.pr_reviews = other_review                   # 混进别人的新评论 → 照常派跟进
        asyncio.run(pr_followup._followup_one(e4))
        assert spawned
    finally:
        (bot.projects.get_project, state.client, state._spawn,
         gitea.pr_info, gitea.pr_reviews, gitea.ci_state) = orig
        gitea._own_user.clear(); gitea._own_user.update(own_backup)
        settings.store_path, settings.pr_automerge = orig_store, orig_am
        _reset_ledger()

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

# ---------- /summarize 0（关闭逐字记录、退回内存背景）不该把整段背景当成"最近 0 条" ----------
def test_summarize_zero_does_not_dump_full_context():
    import tasks
    set_identity()
    rid = "!sum0:ex.org"
    room = FakeRoom(rid, 3)
    captured = {}

    class FC:
        async def room_typing(self, *a, **k): return None
        async def room_send(self, r, mt, content, **k):
            return types.SimpleNamespace(event_id="$x")

    async def fake_quick(prompt):
        captured["p"] = prompt; return "小结好了"

    orig = (state.client, tasks.runner.quick, settings.transcript_enabled)
    state.client = FC()
    tasks.runner.quick = fake_quick
    settings.transcript_enabled = False                   # 走内存背景缓冲（-0 == 0 的坑在这条路径上）
    bot._context[rid].clear()
    bot._context[rid].append((time.time(), "Alice", "很久以前聊的无关内容"))
    bot._context[rid].append((time.time(), "Alice", "最近这条才该被带上"))
    try:
        asyncio.run(bot.handle_summarize(room, make_event("/summarize 0"), "/summarize 0"))
        # n=0 时 list[-0:] 会变成整个列表；期望 max(1, n) 兜底成"最近 1 条"，不该把全部背景喂进去
        assert "很久以前聊的无关内容" not in captured.get("p", "")
        assert "最近这条才该被带上" in captured.get("p", "")
    finally:
        state.client, tasks.runner.quick, settings.transcript_enabled = orig


TESTS = [
    ('自驱/跟进任务可被 /cancel', test_autonomous_tasks_cancellable_by_room),
    ('/cancel 停排队任务+三种文案', test_cancel_stops_queued_task),
    ('/cancel 空场不毒杀下个任务', test_cancel_empty_no_poison),
    ('流式异常 占位收尾不重复报错', test_stream_task_error_finalizes_placeholder),
    ('取消协程 子进程组被杀', test_run_kills_group_on_cancel),
    ('重启后回复 bot 仍算点名', test_reply_to_bot_after_restart),
    ('引用回复拉回被引用消息内容', test_quoted_reply_fetches_referenced_message),
    ('PR 跟进忽略自己的评论', test_followup_ignores_own_reviews),
    ('/summarize 剔除命令变体', test_summarize_excludes_command_variants),
    ('/summarize 0 不当成"全部背景"', test_summarize_zero_does_not_dump_full_context),
]
