"""冒烟：排队回执·模型拆分·逐字记录·日摘要·PR 自动合并/冲突告警·心跳提议·/status·流式定稿回退"""
from _helpers import (
    FakeRoom, _reset_ledger, asyncio, bot, heartbeat, make_event, matrix_io, os, pr_followup, set_identity, settings, state, tasks, time, types)

# ---------- 35b) 排队回执：项目锁被占/并发额度占满时立即知会"已排队"，空闲时不发 ----------
def test_queue_receipt_when_busy():
    set_identity()
    rid = "!queue:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    sent = []

    class R:
        def __init__(self):
            self.is_busy = False; self.n_running = 0; self.cap_full = False; self.n_pending = 0
        def busy(self, k): return self.is_busy
        def running(self, k): return self.n_running
        def capacity_full(self): return self.cap_full
        def pending(self, k): return self.n_pending
        def session_ts(self, k): return None
        async def ask(self, key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                      on_delta=None, cancel_key=None, **_kw):
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
        asyncio.run(tasks._run_on_project(room, ev, "先干个活", rec))
        assert not any("已排队" in m for m in sent)            # 空闲：不发回执

        r.is_busy, r.n_running = True, 1                       # 忙且本房间在跑 → 回执 + /cancel 提示
        asyncio.run(tasks._run_on_project(room, ev, "再来一个", rec))
        assert any("已排队" in m and "/cancel" in m for m in sent)

        sent.clear()
        r.n_running = 0                                        # 忙但占用来自别处 → 说明 /cancel 停不了
        asyncio.run(tasks._run_on_project(room, ev, "第三个", rec))
        assert any("已排队" in m and "停不了" in m for m in sent)

        sent.clear()
        r.is_busy, r.cap_full = False, True                    # 锁空闲但全局并发额度占满 → 也要知会
        asyncio.run(tasks._run_on_project(room, ev, "第四个", rec))
        assert any("并发额度已满" in m and "已排队" in m for m in sent)

        sent.clear()
        r.cap_full = False                                     # 两层都空闲 → 不发回执
        asyncio.run(tasks._run_on_project(room, ev, "第五个", rec))
        assert not any("已排队" in m for m in sent)

        sent.clear()
        r.is_busy = r.cap_full = True                          # 两闸同时关 → 只发一条，锁文案优先
        asyncio.run(tasks._run_on_project(room, ev, "第六个", rec))
        assert len([m for m in sent if "已排队" in m]) == 1
        assert any("上一个任务" in m for m in sent)

        sent.clear()
        r.is_busy, r.cap_full, r.n_pending = False, True, 1    # 占额度的是本房间自己排队的任务
        asyncio.run(tasks._run_on_project(room, ev, "第七个", rec))  # → 给 /cancel 提示而非怪罪别处
        assert any("并发额度已满" in m and "/cancel" in m for m in sent)
    finally:
        (tasks.runner, state.client, settings.stream_replies) = orig
        bot._context[rid].clear()

# ---------- capacity_full：真 runner 的全局并发信号量占满判定 ----------
def test_capacity_full_semaphore():
    from claude_runner import ClaudeRunner

    async def go():
        real = ClaudeRunner()
        assert not real.capacity_full()          # 初始有空位
        # 按观测排空而非复制 __init__ 的 max(1, MAX_CONCURRENCY) 容量公式：
        # 公式将来变了这里也不会多 acquire 一次把无超时的套件挂死
        while not real.capacity_full():
            await real._sema.acquire()
        assert real.capacity_full()              # 占满即判满
        real._sema.release()
        assert not real.capacity_full()          # 释放一个空位即不再判满
    asyncio.run(go())


# ---------- 自驱心跳：巡检时段只在工作日白天（默认东八区 9~19 点） ----------
def test_heartbeat_schedule_window():
    orig = (settings.proactive_heartbeat_weekdays_only, settings.proactive_heartbeat_start_hour,
            settings.proactive_heartbeat_end_hour, settings.proactive_heartbeat_tz_hours)
    settings.proactive_heartbeat_weekdays_only = True
    settings.proactive_heartbeat_start_hour = 9
    settings.proactive_heartbeat_end_hour = 19
    settings.proactive_heartbeat_tz_hours = 8
    try:
        assert heartbeat._in_heartbeat_window(1704074400.0)        # 周一 10:00 → 在时段内
        assert not heartbeat._in_heartbeat_window(1704592800.0)    # 周日 10:00 → 周末，跳过
        assert not heartbeat._in_heartbeat_window(1704121200.0)    # 周一 23:00 → 夜里，跳过

        settings.proactive_heartbeat_weekdays_only = False
        assert heartbeat._in_heartbeat_window(1704592800.0)        # 关掉工作日限制 → 周日也算

        settings.proactive_heartbeat_weekdays_only = True          # 跨零点时段（如夜班 22~6）
        settings.proactive_heartbeat_start_hour = 22
        settings.proactive_heartbeat_end_hour = 6
        assert heartbeat._in_heartbeat_window(1704121200.0)        # 周一 23:00 → 落在 22~6 内
        assert not heartbeat._in_heartbeat_window(1704074400.0)    # 周一 10:00 → 不在 22~6 内
    finally:
        (settings.proactive_heartbeat_weekdays_only, settings.proactive_heartbeat_start_hour,
         settings.proactive_heartbeat_end_hour, settings.proactive_heartbeat_tz_hours) = orig

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
        asyncio.run(heartbeat._heartbeat_one(rec, "!room"))
        assert not sent and not spawned

        heartbeat.runner = R("建议：给 X 补个单元测试")          # 有建议 + autopilot 关 → 只提议
        asyncio.run(heartbeat._heartbeat_one(rec, "!room"))
        assert any("巡检建议" in m for m in sent) and not spawned

        sent.clear()
        heartbeat.runner = R("建议：给 X 补个单元测试")          # 有建议 + autopilot 开 → 宣布开干并派执行
        settings.proactive_autopilot = True
        asyncio.run(heartbeat._heartbeat_one(rec, "!room"))
        assert spawned and any("自驱" in m for m in sent)
    finally:
        (heartbeat.runner, state.client, state._spawn, settings.proactive_autopilot) = orig

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

def test_model_only_on_fresh_session():
    """--model 只在开新会话时传：--resume 时再带会把恢复出的会话（连同被中断的子代理）
    整体强制成 CLAUDE_MODEL，覆盖子代理各自的模型路由，续跑烧错额度。"""
    from claude_runner import runner
    orig = (settings.claude_model, settings.claude_extra_args)
    settings.claude_model = "fable"
    settings.claude_extra_args = None   # 别把真实部署的额外参数混进命令（同 test_persistent）
    try:
        fresh = runner._cmd("p", None, agentic=True)
        assert fresh[fresh.index("--model") + 1] == "fable"       # 新会话：正常传
        resumed = runner._cmd("p", "sid-1", agentic=True)
        assert "--model" not in resumed and "--resume" in resumed  # 续会话：不传，沿用会话记录的模型
        forked = runner._cmd("p", "sid-1", agentic=True, fork=True)
        assert "--model" not in forked                             # fork 继承父会话，同 resume
        # 常驻进程模式同一规则
        p_fresh = runner._cmd_persistent(None, None, fork=False)
        assert p_fresh[p_fresh.index("--model") + 1] == "fable"
        p_resumed = runner._cmd_persistent("sid-1", None, fork=False)
        assert "--model" not in p_resumed and "--resume" in p_resumed
        # 会话模型记录（sessions.json 第三元，观测用）：旧版二元条目兼容读、三元正常读，落盘不炸
        runner._sessions["m2|room"] = ("SID-2", time.time())          # 旧版格式（无模型元）
        runner._sessions["m3|room"] = ("SID-3", time.time(), "opus")
        assert runner.session_model("m2|room") == ""
        assert runner.session_model("m3|room") == "opus"
        assert runner.session_model("不存在") == ""
        runner._save_sessions()                                       # 混合格式可序列化
        assert runner._load_sessions()["m3|room"][2] == "opus"        # 落盘回读模型仍在
    finally:
        runner._sessions.pop("m2|room", None); runner._sessions.pop("m3|room", None)
        runner._save_sessions()
        settings.claude_model, settings.claude_extra_args = orig

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

# ---------- 聊天日摘要 + 主题索引：触发判断 / 东八区分桶 / 落盘 / 水位线 / 合并去重 / 注入 ----------
def test_digest_layer():
    import tempfile
    import datetime as dt
    import digest
    import transcript

    orig = (settings.store_path, settings.transcript_enabled, settings.digest_enabled,
            settings.digest_min_lines, settings.digest_min_interval,
            settings.digest_keep_days, settings.digest_tz_hours, settings.transcript_keep_days)
    orig_quick = digest.runner.quick
    settings.store_path = tempfile.mkdtemp()
    settings.transcript_enabled = True
    # 固定用 2026-03 的历史日期测东八区分桶，得把逐字记录保留期放大，否则 append 的自动 prune
    # 会按真实时钟（远晚于 2026-03）把这些"过期"行删掉，读回来就凑不齐两行了。
    settings.transcript_keep_days = 10 ** 6
    settings.digest_enabled = True
    settings.digest_min_lines = 30
    settings.digest_min_interval = 7200
    settings.digest_keep_days = 180
    settings.digest_tz_hours = 8
    rid = "!dg:ex.org"

    # 假 quick：返回固定格式（条目 + 末行 INDEX）；主题清单可切换以验证合并去重
    topics = {"line": "INDEX: 部署；登录"}
    async def fake_quick(prompt):
        return ("- **10:00–10:30** 部署：把服务部署到 pi.lan（ts 1—2）\n"
                "- **11:00–11:10** 登录：token 自动刷新（ts 3—4）\n" + topics["line"])
    digest.runner.quick = fake_quick
    digest._last_run.pop(rid, None)      # _last_run 是内存态，清一下防跨用例串
    digest._last_prune.pop(rid, None)
    try:
        # ---- should_digest：不够行数 → False ----
        for i in range(5):
            transcript.append(rid, "alice", f"闲聊{i}", event_id=f"$c{i}")
        assert digest.should_digest(rid) is False        # 5 行 < 30、全今天、无跨天残留

        # ---- should_digest：够行数 + 够间隔 → True ----
        transcript.discard(rid)
        digest._last_run.pop(rid, None)
        for i in range(30):
            transcript.append(rid, "alice", f"活跃{i}", event_id=f"$m{i}")
        assert digest.should_digest(rid) is True         # 30 ≥ 30 且上次运行=0（间隔够）

        # ---- should_digest：跨天残留 + 600s 后 → True（行数不够也收尾昨天）----
        transcript.discard(rid)
        transcript.append(rid, "alice", "昨天的尾巴", event_id="$y1", ts=time.time() - 86400)
        digest._last_run[rid] = time.time() - 100        # 距上次 100s < 600 → 先不收
        assert digest.should_digest(rid) is False
        digest._last_run[rid] = time.time() - 700        # 700s ≥ 600 → 收尾昨天
        assert digest.should_digest(rid) is True

        # ---- digest_room：东八区分桶（UTC 20:00 → +8 次日）+ 落盘 + 水位线 ----
        transcript.discard(rid)
        digest.discard(rid)
        digest._last_run.pop(rid, None)
        # 用 20:00 UTC 而非 15:00：15:00 UTC +8=23:00 仍同日、跨不过午夜；≥16:00 才真进次日，
        # 测才有意义。2026-03-10 20:00 UTC → 东八区 2026-03-11 04:00 → 落到 03-11.md
        cross = dt.datetime(2026, 3, 10, 20, 0, tzinfo=dt.timezone.utc).timestamp()
        transcript.append(rid, "alice", "半夜聊部署", event_id="$x1", ts=cross)
        transcript.append(rid, "bot", "好的部署到 pi", event_id="$x2", ts=cross + 60)
        assert asyncio.run(digest.digest_room(rid)) == 2        # 覆盖 2 行
        ddir = digest._room_dir(rid)
        day = open(os.path.join(ddir, "2026-03-11.md")).read()  # +8 次日的日文件
        assert day.startswith("# 2026-03-11") and "部署" in day
        idx = open(os.path.join(ddir, "INDEX.md")).read()
        assert idx.startswith("2026-03-11:") and "部署" in idx and "登录" in idx
        assert digest._read_wm(rid) == round(cross + 60, 3)     # 水位线推到该批最大 ts

        # ---- 第二次调用：旧行都在水位线下 → 不重复处理 ----
        assert asyncio.run(digest.digest_room(rid)) == 0

        # ---- 同日期两批 INDEX 主题合并去重 ----
        transcript.append(rid, "alice", "同天又聊", event_id="$x3", ts=cross + 120)
        topics["line"] = "INDEX: 登录；测试"                     # 与上批「登录」重叠、新增「测试」
        assert asyncio.run(digest.digest_room(rid)) == 1
        line = [l for l in open(os.path.join(ddir, "INDEX.md")).read().splitlines()
                if l.startswith("2026-03-11:")][0]
        assert line.count("登录") == 1                           # 去重：登录只一次
        assert "部署" in line and "测试" in line                 # 合并：老部署 + 新测试都在

        # ---- augment_system_prompt：含目录路径 + 原始日志 + 近 7 天索引行全文 ----
        rid2 = "!dg2:ex.org"
        transcript.append(rid2, "alice", "今天聊架构", event_id="$t1")   # 今天 → 落在近 7 天窗口
        topics["line"] = "INDEX: 架构选型"
        asyncio.run(digest.digest_room(rid2))
        sp = digest.augment_system_prompt("BASE", rid2)
        assert sp.startswith("BASE")
        assert digest._room_dir(rid2) in sp and transcript.path_for(rid2) in sp
        assert digest._today() in sp and "架构选型" in sp        # 今天这行索引被注入
        assert "查询协议" in sp

        # ---- runner.quick 抛异常：不崩、水位线不动、不落盘 ----
        rid3 = "!dg3:ex.org"
        transcript.append(rid3, "alice", "会失败的一批", event_id="$f1")
        async def boom(prompt):
            raise RuntimeError("quick 挂了")
        digest.runner.quick = boom
        assert asyncio.run(digest.digest_room(rid3)) == 0       # 吞异常、返回已完成 0
        assert digest._read_wm(rid3) == 0.0                     # 水位线没动
        assert not os.path.exists(os.path.join(digest._room_dir(rid3), "INDEX.md"))

        # ---- 批切分：行数或正文字符量任一到顶就断批（防把整段粘贴日志一次喂爆 quick）----
        big = [{"ts": i, "body": "x" * 4000} for i in range(31)]
        old_cap = digest._MAX_BATCH_CHARS
        digest._MAX_BATCH_CHARS = 10000
        try:
            chunks = list(digest._batches(big))
        finally:
            digest._MAX_BATCH_CHARS = old_cap
        assert all(len(c) <= 2 for c in chunks)         # 10000/4000 → 每批最多 2 行
        assert sum(len(c) for c in chunks) == 31        # 切批不丢行

        # ---- discard 幂等 ----
        digest.discard(rid)
        digest.discard(rid)                                     # 再删一次不报错
        assert not os.path.exists(digest._room_dir(rid))

        # ---- 未开启：augment 原样返回 ----
        settings.digest_enabled = False
        assert digest.augment_system_prompt("BASE", rid2) == "BASE"
    finally:
        (settings.store_path, settings.transcript_enabled, settings.digest_enabled,
         settings.digest_min_lines, settings.digest_min_interval,
         settings.digest_keep_days, settings.digest_tz_hours, settings.transcript_keep_days) = orig
        digest.runner.quick = orig_quick
        for r in ("!dg:ex.org", "!dg2:ex.org", "!dg3:ex.org"):
            digest._last_run.pop(r, None)
            digest._last_prune.pop(r, None)

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
        asyncio.run(pr_followup._followup_one([e for e in pr_ledger.active() if e["number"] == 1][0]))
        assert merged == [(1, "merge")]
        assert not any(e["number"] == 1 for e in pr_ledger.active())   # 销账
        assert any("已自动合并" in m for m in sent)

        # b) 未决 REQUEST_CHANGES（非新评审，不会走派跟进）→ 不合并、仍在册
        merged.clear()
        pr_ledger.record("h/o/r", 2, "u2", "!room")
        pr_ledger.update("h/o/r", 2, seen_review=10)   # 标记已"看过"，section 1 不再当新评审派活
        async def rc_review(r, n): return [{"id": 10, "state": "REQUEST_CHANGES", "body": "改"}]
        gitea.pr_reviews = rc_review
        asyncio.run(pr_followup._followup_one([e for e in pr_ledger.active() if e["number"] == 2][0]))
        assert merged == [] and any(e["number"] == 2 for e in pr_ledger.active())

        # c) 不可合并（有冲突）→ 不合并、仍在册
        merged.clear()
        gitea.pr_reviews = no_reviews
        async def conflict(r, n):
            return {"state": "open", "merged": False, "mergeable": False,
                    "head": {"ref": "claude/x", "sha": "s"}}
        gitea.pr_info = conflict
        pr_ledger.record("h/o/r", 3, "u3", "!room")
        asyncio.run(pr_followup._followup_one([e for e in pr_ledger.active() if e["number"] == 3][0]))
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
        asyncio.run(pr_followup._followup_one([e for e in pr_ledger.active() if e["number"] == 1][0]))
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
        asyncio.run(pr_followup._followup_one(e()))
        asyncio.run(pr_followup._followup_one(e()))               # 同一 sha 再冲突一轮
        assert sum("有冲突" in m for m in sent) == 1        # 只告警一次，不每 180s 刷屏
        assert e()["conflict_seen"] == "s1"
        head["sha"] = "s2"                                 # 重推了新 commit（换 sha）
        asyncio.run(pr_followup._followup_one(e()))
        assert sum("有冲突" in m for m in sent) == 2        # 新版本 → 允许再报一次
    finally:
        (bot.projects.get_project, state.client, state._spawn,
         gitea.pr_info, gitea.pr_reviews, gitea.ci_state) = orig
        settings.store_path, settings.pr_automerge = orig_store, orig_am
        _reset_ledger()

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

    orig = (bot.projects.get_room, state.client, settings.proactive_heartbeat_weekdays_only,
            settings.proactive_heartbeat_start_hour, settings.proactive_heartbeat_end_hour)
    bot.projects.get_room = lambda rid: rec
    state.client = FC()
    try:
        pr_ledger.record("h/o/r", 7, "http://h/o/r/pulls/7", "!g:ex.org")
        asyncio.run(bot.handle_status(FakeRoom("!g:ex.org", 3)))
        out = "\n".join(sent)
        assert "o/r" in out and "PR #7" in out            # 项目 + 在跟的 PR
        assert "没有正在跑的任务" in out and "自驱心跳" in out

        settings.proactive_heartbeat_weekdays_only = False   # 强制"在时段内" → 不该带提示
        settings.proactive_heartbeat_start_hour, settings.proactive_heartbeat_end_hour = 0, 24
        sent.clear()
        asyncio.run(bot.handle_status(FakeRoom("!g:ex.org", 3)))
        assert "当前不在巡检时段" not in "\n".join(sent)

        settings.proactive_heartbeat_start_hour, settings.proactive_heartbeat_end_hour = 0, 0  # 恒不在时段内
        sent.clear()
        asyncio.run(bot.handle_status(FakeRoom("!g:ex.org", 3)))
        assert "当前不在巡检时段" in "\n".join(sent)

        # 模型配置漂移：会话记录的模型 ≠ 当前 CLAUDE_MODEL → 提示"对当前会话不生效"；一致则不提
        orig_model = settings.claude_model
        tasks.runner._sessions["h/o/r|!g:ex.org"] = ("SID-M", time.time(), "opus")
        try:
            settings.claude_model = "haiku"
            sent.clear()
            asyncio.run(bot.handle_status(FakeRoom("!g:ex.org", 3)))
            out = "\n".join(sent)
            assert "会话模型：opus" in out and "haiku" in out and "/reset" in out
            settings.claude_model = "opus"
            sent.clear()
            asyncio.run(bot.handle_status(FakeRoom("!g:ex.org", 3)))
            assert "会话模型" not in "\n".join(sent)
        finally:
            settings.claude_model = orig_model
            tasks.runner._sessions.pop("h/o/r|!g:ex.org", None)
    finally:
        (bot.projects.get_room, state.client, settings.proactive_heartbeat_weekdays_only,
         settings.proactive_heartbeat_start_hour, settings.proactive_heartbeat_end_hour) = orig
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
    live = matrix_io._LiveReply(rid)
    asyncio.run(live.on_delta("part", "Bash"))            # 生成占位消息
    asyncio.run(live.finalize("最终答案", track=True))
    assert any("最终答案" in b for b in sent)              # 占位编辑失败 → 答案作为新消息发出
    assert any(b == "最终答案" for _, s, b, *_ in bot._context[rid])   # 且照常入上下文


TESTS = [
    ('排队回执 忙时知会/空闲不发', test_queue_receipt_when_busy),
    ('capacity_full 信号量占满判定', test_capacity_full_semaphore),
    ('模型拆分 干活大/轻判断小', test_quick_model_split),
    ('--model 只开新会话传 resume不覆盖', test_model_only_on_fresh_session),
    ('聊天逐字记录 落盘/回溯/删旧/开关', test_transcript_log_and_recall),
    ('聊天日摘要 触发/东八区分桶/落盘/水位线/合并/注入', test_digest_layer),
    ('PR 自动合并 条件满足才合并', test_pr_automerge),
    ('CI查询失败不放行自动合并', test_automerge_skips_on_ci_unknown),
    ('PR 冲突只告警一次不刷屏', test_conflict_alert_once),
    ('自驱心跳 巡检时段（工作日白天）', test_heartbeat_schedule_window),
    ('自驱心跳 提议/autopilot', test_heartbeat_propose_and_autopilot),
    ('/status 状态一屏可见', test_status_command),
    ('流式定稿 编辑失败退回新发', test_livereply_finalize_edit_fallback),
]
