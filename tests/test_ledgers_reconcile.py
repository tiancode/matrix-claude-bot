"""冒烟：PR/工单/在途台账登记销账·启动对账"""
from _helpers import (
    _reset_inflight, _reset_issue_ledger, _reset_ledger, asyncio, bot, issue_intake, pr_followup, set_identity, settings, state, tasks, types)

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
    assert tasks._extract_pr("搞定，PR：http://pi.lan:3000/claude/playground/pulls/7 看下", rec) \
        == (7, "http://pi.lan:3000/claude/playground/pulls/7")
    assert tasks._extract_pr("纯问答没开 PR", rec) is None
    assert tasks._extract_pr("http://pi.lan:3000/other/repo/pulls/3", rec) is None   # 别的库不算

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
        asyncio.run(pr_followup._followup_one([e for e in pr_ledger.active() if e["number"] == 1][0]))
        assert not any(e["number"] == 1 for e in pr_ledger.active())   # 销账
        assert any("已合并" in m for m in sent)

        # b) 新 REQUEST_CHANGES 评审 → 派跟进 + 记 seen_review + review_fixes+1
        pr_ledger.record("h/o/r", 2, "u2", "!room")
        async def open_info(r, n): return {"state": "open", "merged": False, "head": {"ref": "claude/x", "sha": "s2"}}
        async def reviews2(r, n): return [{"id": 10, "state": "REQUEST_CHANGES", "body": "改下 X", "user": {"login": "root"}}]
        gitea.pr_info, gitea.pr_reviews = open_info, reviews2
        asyncio.run(pr_followup._followup_one([e for e in pr_ledger.active() if e["number"] == 2][0]))
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
        asyncio.run(pr_followup._followup_one(e(1)[0]))
        assert e(1) and e(1)[0]["gone_rounds"] == 1
        asyncio.run(pr_followup._followup_one(e(1)[0]))
        assert e(1) and e(1)[0]["gone_rounds"] == 2 and not any("已不存在" in m for m in sent)
        asyncio.run(pr_followup._followup_one(e(1)[0]))
        assert not e(1) and any("PR #1" in m and "已不存在" in m for m in sent)   # 3 轮 → 销账 + 报

        # 网络抖动（非 404）：一轮都不攒，永远不销
        gitea.pr_gone = not_gone
        pr_ledger.record("h/o/r", 2, "u2", "!room")
        for _ in range(5):
            asyncio.run(pr_followup._followup_one(e(2)[0]))
        assert e(2) and e(2)[0]["gone_rounds"] == 0

        # 中途成功查到一次 → 之前攒的 404 轮数清零
        gitea.pr_gone = is_gone
        pr_ledger.update("h/o/r", 2, gone_rounds=2)
        async def open_info(r, n):
            return {"state": "open", "merged": False, "mergeable": True,
                    "head": {"ref": "b", "sha": "s"}}
        gitea.pr_info = open_info
        asyncio.run(pr_followup._followup_one(e(2)[0]))
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
        asyncio.run(issue_intake._intake_one(rec, "!room", "claudebot"))
        assert issue_ledger.taken("h/o/r", 3)                              # 已登记
        assert claimed and claimed[0][0] == 3 and "认领" in claimed[0][1]   # issue 下留言认领
        assert spawned and any("工单 #3" in m for m in sent)               # 房间宣布 + 派执行
        sent.clear(); spawned.clear()
        asyncio.run(issue_intake._intake_one(rec, "!room", "claudebot"))            # 下轮又轮询到同一单 → 不重复接
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

# ---------- 在途登记簿：登记/补录占位eid/摘除/清空 + 持久化往返 ----------
def test_inflight_ledger():
    import tempfile
    import inflight
    orig = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_inflight()
    try:
        k = inflight.record("!room", "修一下登录 token 刷新", inflight.KIND_CHAT)
        e = inflight.active()[0]
        assert k and e["room"] == "!room" and e["kind"] == "chat" and e["eid"] == ""
        inflight.attach_eid(k, "$placeholder1")                       # 占位创建后补录 eid
        _reset_inflight()                                             # 清内存态 → 必须能从盘恢复
        got = inflight.active()
        assert len(got) == 1 and got[0]["eid"] == "$placeholder1"     # 落盘往返：eid 还在
        inflight.record("!r2", "工单 #7", inflight.KIND_ISSUE, issue=7)
        assert any(x["kind"] == "issue" and x["issue"] == 7 for x in inflight.active())
        inflight.remove(k)                                            # 摘除聊天那条
        assert [x["kind"] for x in inflight.active()] == ["issue"]
        inflight.clear()                                             # 对账后清空整簿
        assert inflight.active() == []
    finally:
        settings.store_path = orig
        _reset_inflight()

# ---------- 启动对账·在途：占位收尾成中断提示 / 排队条目补说明 / 已退房间跳过 / 工单不催重发 ----------
def test_reconcile_inflight():
    import tempfile
    import inflight
    set_identity()
    orig_store = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_inflight()
    sent, edits = [], []

    class FC:
        rooms = {"!live:ex.org": object()}     # 只有这个房间还在；!dead 已退（不在 client.rooms）
        async def room_send(self, r, mt, content, **k):
            rel = content.get("m.relates_to") or {}
            if rel.get("rel_type") == "m.replace":
                edits.append((r, rel.get("event_id"), content["m.new_content"]["body"]))
            else:
                sent.append((r, content["body"]))
            return types.SimpleNamespace(event_id="$x")

    orig_client = state.client
    state.client = FC()
    try:
        ka = inflight.record("!live:ex.org", "修登录 token", inflight.KIND_CHAT)
        inflight.attach_eid(ka, "$ph1")                         # a) 有占位 + 房间还在 → 编辑占位
        inflight.record("!live:ex.org", "排队的活", inflight.KIND_CHAT)   # b) 无占位 → 补发说明
        inflight.record("!dead:ex.org", "死房间的活", inflight.KIND_CHAT) # c) 房间已退 → 跳过
        inflight.record("!live:ex.org", "工单活", inflight.KIND_ISSUE, issue=9)  # d) 工单 → 不催重发

        asyncio.run(bot._reconcile_inflight())

        assert any(r == "!live:ex.org" and t == "$ph1" and "中断" in b for r, t, b in edits)  # a)
        assert any(r == "!live:ex.org" and "重新发一遍" in b for r, b in sent)                # b)
        assert not any(r == "!dead:ex.org" for r, _, _ in edits)                              # c)
        assert not any(r == "!dead:ex.org" for r, _ in sent)                                  # c)
        assert not any("工单活" in b for _, b in sent)                                        # d)
        assert inflight.active() == []                          # 处理完清空整簿
    finally:
        state.client = orig_client
        settings.store_path = orig_store
        _reset_inflight()

# ---------- 启动对账·工单：pr==0 且无对应 open PR → 重新派执行 ----------
def test_reconcile_issues_redispatches_pr_zero():
    import tempfile
    import issue_ledger
    import gitea
    set_identity()
    orig_store = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_issue_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent, dispatched, pend = [], [], []

    class FC:
        rooms = {"!room": object()}
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    async def no_open_pulls(r): return []                        # 仓库没有任何 open PR
    async def open_issue(r, n):
        return {"number": n, "state": "open", "title": "登录太慢", "body": "太慢了"}

    async def fake_execute(rec_, room_, issue_):
        dispatched.append((room_, issue_["number"]))

    orig = (bot.projects.get_project, state.client, state._spawn,
            gitea.open_pulls, gitea.issue_info, bot._issue_execute)
    bot.projects.get_project = lambda pid: rec if pid == "h/o/r" else None
    state.client = FC()
    state._spawn = lambda coro: pend.append(coro)
    gitea.open_pulls, gitea.issue_info = no_open_pulls, open_issue
    bot._issue_execute = fake_execute
    try:
        issue_ledger.record("h/o/r", 5, "http://h/o/r/issues/5", "!room")   # pr==0（接了单没开 PR）
        async def go():
            await bot._reconcile_issues()
            for c in pend:
                await c
        asyncio.run(go())
        assert dispatched == [("!room", 5)]                     # 重派执行
        assert any("工单 #5" in m and "重新接手" in m for m in sent)   # 房间告知重启后重接
        assert issue_ledger.active()[0]["pr"] == 0              # 没找到 PR → 仍 pr==0，靠重派产出
    finally:
        (bot.projects.get_project, state.client, state._spawn,
         gitea.open_pulls, gitea.issue_info, bot._issue_execute) = orig
        settings.store_path = orig_store
        _reset_issue_ledger()

# ---------- 启动对账·工单：已有正文 Closes #N 的 open PR → 只补账继续跟进，不重复执行（防重复开 PR） ----------
def test_reconcile_issues_skips_when_pr_exists():
    import tempfile
    import issue_ledger
    import pr_ledger
    import gitea
    set_identity()
    orig_store = settings.store_path
    settings.store_path = tempfile.mkdtemp()
    _reset_issue_ledger(); _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    dispatched, pend, info_calls = [], [], []

    class FC:
        rooms = {"!room": object()}
        async def room_send(self, r, mt, content, **k):
            return types.SimpleNamespace(event_id="$x")

    async def pulls_with_closes(r):     # 崩在"PR 已开、台账没记 pr 号"之间：open PR 正文带 Closes #5
        return [{"number": 12, "body": "修复登录刷新\n\nCloses #5", "html_url": "http://h/o/r/pulls/12"},
                {"number": 8, "body": "无关 PR，Closes #99", "html_url": "http://h/o/r/pulls/8"}]

    async def issue_info(r, n):
        info_calls.append(n); return {"number": n, "state": "open", "title": "登录太慢", "body": "太慢"}

    async def fake_execute(rec_, room_, issue_):
        dispatched.append(issue_["number"])

    # 匹配器本身：命中 Closes/fixes，按 #N 边界收口，不误命中 #50/无关键词
    assert bot._pr_body_closes_issue("干完了 Closes #5", 5)
    assert bot._pr_body_closes_issue("fixes #5 done", 5)
    assert not bot._pr_body_closes_issue("Closes #50", 5)
    assert not bot._pr_body_closes_issue("提到 #5 但没关键词", 5)

    orig = (bot.projects.get_project, state.client, state._spawn,
            gitea.open_pulls, gitea.issue_info, bot._issue_execute)
    bot.projects.get_project = lambda pid: rec if pid == "h/o/r" else None
    state.client = FC()
    state._spawn = lambda coro: pend.append(coro)
    gitea.open_pulls, gitea.issue_info = pulls_with_closes, issue_info
    bot._issue_execute = fake_execute
    try:
        issue_ledger.record("h/o/r", 5, "http://h/o/r/issues/5", "!room")   # pr==0
        async def go():
            await bot._reconcile_issues()
            for c in pend:
                await c
        asyncio.run(go())
        assert dispatched == []                                  # 不重复执行（PR 已在开）
        assert info_calls == []                                  # 命中 open PR 即短路，不再查 issue
        assert issue_ledger.active()[0]["pr"] == 12              # 台账补记已开的 PR 号
        assert any(e["number"] == 12 for e in pr_ledger.active())   # PR 进跟进台账盯到合并
    finally:
        (bot.projects.get_project, state.client, state._spawn,
         gitea.open_pulls, gitea.issue_info, bot._issue_execute) = orig
        settings.store_path = orig_store
        _reset_issue_ledger(); _reset_ledger()


TESTS = [
    ('PR 台账 登记/持久化/销账', test_pr_ledger),
    ('从回复抽取本项目 PR 链接', test_extract_pr),
    ('PR 跟进 合并销账/评审派活', test_pr_followup_actions),
    ('PR 连续3轮404才销账 抖动不销', test_pr_gone_after_three_404),
    ('工单台账 登记/持久化/销账', test_issue_ledger),
    ('工单接活 认领/宣布/派执行/防重', test_issue_intake_flow),
    ('工单执行 开PR进台账/贴链接/关单销账', test_issue_execute_and_sweep),
    ('工单连续3轮404才销账 抖动不销', test_issue_gone_after_three_404),
    ('在途登记簿 登记/补录eid/摘除/落盘', test_inflight_ledger),
    ('启动对账·在途 占位收尾/排队补说明/退房跳过', test_reconcile_inflight),
    ('启动对账·工单 pr==0 重派执行', test_reconcile_issues_redispatches_pr_zero),
    ('启动对账·工单 已有CloseS PR不重派只补账', test_reconcile_issues_skips_when_pr_exists),
]
