"""冒烟：群对话延续窗口·强弱信号分级·语义闸·/reset·会话失效重试"""
from _helpers import (
    FakeRoom, addressing, asyncio, bot, claude_runner, json, make_event, set_identity, settings, state, time, types)

# ---------- 8b) 群"对话延续窗口"：点过名后免重复 @ 也算续话 ----------
def test_group_followup_window():
    set_identity()
    rid = "!fw:ex.org"
    room = FakeRoom(rid, 3)                       # 群（非 DM）
    orig_win = settings.group_followup_window
    orig_soft = settings.followup_semantic_window
    settings.group_followup_window = 180
    settings.followup_semantic_window = 0     # 本例只验硬窗口；软窗口另有专测
    state._group_engaged.clear()
    try:
        # 没点过名 → 普通消息不算点名
        ok, _ = addressing._is_addressed(room, make_event("接着把它改一下", sender="@alice:ex.org"))
        assert not ok

        bot._mark_engaged(rid, "@alice:ex.org")   # alice 点了名

        # 窗口内 alice 的后续消息（没@）→ 算续话
        ok, _ = addressing._is_addressed(room, make_event("接着把它改一下", sender="@alice:ex.org"))
        assert ok

        # 窗口内但 @ 了别人 → 不算（在跟别人说话）
        ok, _ = addressing._is_addressed(room, make_event(
            "@bob 你看呢", sender="@alice:ex.org", mentions=["@bob:ex.org"]))
        assert not ok

        # 窗口内但这条是回复别人的消息 → 不算续话
        ok, _ = addressing._is_addressed(room, make_event(
            "说得对", sender="@alice:ex.org", in_reply_to="$someoneelse"))
        assert not ok

        # 没点过名的 bob → 不算续话
        ok, _ = addressing._is_addressed(room, make_event("我也要", sender="@bob:ex.org"))
        assert not ok

        # 窗口过期（软窗口也关着）→ 不再续话
        state._group_engaged[(rid, "@alice:ex.org")] = time.time() - 1000
        ok, _ = addressing._is_addressed(room, make_event("还在吗", sender="@alice:ex.org"))
        assert not ok

        # 开关关掉 → 不续话
        settings.group_followup_window = 0
        bot._mark_engaged(rid, "@alice:ex.org")
        ok, _ = addressing._is_addressed(room, make_event("接着改", sender="@alice:ex.org"))
        assert not ok
    finally:
        settings.group_followup_window = orig_win
        settings.followup_semantic_window = orig_soft
        state._group_engaged.clear()

# ---------- 8b) 续话窗口：第三人插话作废 + 强/弱信号分级 ----------
def test_followup_window_third_party_invalidates():
    set_identity()
    rid = "!fw3:ex.org"
    room = FakeRoom(rid, 3)
    orig_win = settings.group_followup_window
    settings.group_followup_window = 180
    state._group_engaged.clear()
    bot._context[rid].clear()
    try:
        t0 = time.time()
        state._group_engaged[(rid, "@alice:ex.org")] = t0        # alice 在 t0 点过名

        # 窗口内只有 alice 自己 + bot 说话 → 续话仍成立（弱信号）
        bot._context[rid].append((t0 + 1, "claude-bot", "好的，已处理"))
        bot._context[rid].append((t0 + 2, "Alice", "接着再改一处"))
        kind, _ = bot._address_kind(room, make_event("接着再改一处", sender="@alice:ex.org"))
        assert kind == "weak"

        # 窗口内 bob 插了话 → 对话转向别人，续话作废（零成本规则堵最常见误触发）
        bot._context[rid].append((t0 + 3, "Bob", "对了你们看球了吗"))
        kind, _ = bot._address_kind(room, make_event("哈哈是啊", sender="@alice:ex.org"))
        assert kind == ""
    finally:
        settings.group_followup_window = orig_win
        state._group_engaged.clear()
        bot._context[rid].clear()

def test_address_kind_strong_vs_weak():
    set_identity()
    rid = "!ak:ex.org"
    room = FakeRoom(rid, 3)
    orig_win = settings.group_followup_window
    settings.group_followup_window = 180
    state._group_engaged.clear()
    bot._context[rid].clear()
    bot._sent_events.clear()
    try:
        # @我 → strong
        k, _ = bot._address_kind(room, make_event(
            "@claude-bot 帮我看看", sender="@alice:ex.org", mentions=["@claudebot:ex.org"]))
        assert k == "strong"
        # 回复 bot 的旧消息 → strong
        bot._sent_events.append("$mybot1")
        k, _ = bot._address_kind(room, make_event(
            "再改一下", sender="@alice:ex.org", in_reply_to="$mybot1"))
        assert k == "strong"
        # 点过名 + 窗口内无旁人插话 → weak
        bot._mark_engaged(rid, "@alice:ex.org")
        k, _ = bot._address_kind(room, make_event("接着改", sender="@alice:ex.org"))
        assert k == "weak"
        # 没点名、没续话窗口 → ""
        state._group_engaged.clear()
        k, _ = bot._address_kind(room, make_event("今天天气不错", sender="@bob:ex.org"))
        assert k == ""
    finally:
        settings.group_followup_window = orig_win
        state._group_engaged.clear()
        bot._context[rid].clear()
        bot._sent_events.clear()

# ---------- 8b') 续话软窗口：硬窗口外仍放给语义闸（不再按死时间/旁人插话一刀切） ----------
def test_followup_semantic_window():
    set_identity()
    rid = "!fwsoft:ex.org"
    room = FakeRoom(rid, 3)
    orig_win = settings.group_followup_window
    orig_soft = settings.followup_semantic_window
    orig_gate = settings.followup_semantic_gate
    settings.group_followup_window = 180
    settings.followup_semantic_window = 1800
    settings.followup_semantic_gate = True
    state._group_engaged.clear()
    bot._context[rid].clear()
    try:
        now = time.time()
        # alice 20 分钟前点过名：超硬窗口(180s) 但在软窗口(1800s) 内
        state._group_engaged[(rid, "@alice:ex.org")] = now - 1200
        # 期间 bob 还插过话——硬窗口会因此作废，但软窗口不预筛，交给语义闸 → 仍算 weak
        bot._context[rid].append((now - 600, "Bob", "你们看球了吗"))
        kind, _ = bot._address_kind(room, make_event("再讲一个鬼故事", sender="@alice:ex.org"))
        assert kind == "weak"        # 隔几十分钟+中途旁人插话，仍放给语义闸定夺（而非硬时间切掉）

        # @了别人：软窗口里也直接不认（明摆着在跟别人说）
        kind, _ = bot._address_kind(room, make_event(
            "@bob 是啊", sender="@alice:ex.org", mentions=["@bob:ex.org"]))
        assert kind == ""

        # 超过软窗口 → 彻底过期，不再续话
        state._group_engaged[(rid, "@alice:ex.org")] = now - 5000
        kind, _ = bot._address_kind(room, make_event("还在吗", sender="@alice:ex.org"))
        assert kind == ""

        # 语义闸关掉 → 软窗口不生效（软窗口全靠语义闸兜底，没它就不敢放这么长）
        settings.followup_semantic_gate = False
        state._group_engaged[(rid, "@alice:ex.org")] = now - 1200
        kind, _ = bot._address_kind(room, make_event("再讲一个", sender="@alice:ex.org"))
        assert kind == ""

        # 硬窗口内不受影响：语义闸关着也照常 weak（老行为）
        state._group_engaged[(rid, "@alice:ex.org")] = now - 10
        bot._context[rid].clear()     # 清掉 bob 的插话，硬窗口预筛才不作废
        kind, _ = bot._address_kind(room, make_event("接着改", sender="@alice:ex.org"))
        assert kind == "weak"
    finally:
        settings.group_followup_window = orig_win
        settings.followup_semantic_window = orig_soft
        settings.followup_semantic_gate = orig_gate
        state._group_engaged.clear()
        bot._context[rid].clear()

# ---------- 8b'') 线程里的消息不当顶层 weak 续话（哪怕客户端没发 m.in_reply_to 回退块）----------
def test_thread_msg_not_top_level_weak():
    import types
    set_identity()
    rid = "!thrfollow:ex.org"
    room = FakeRoom(rid, 3)
    orig = (settings.group_followup_window, settings.followup_semantic_window)
    settings.group_followup_window = 180
    settings.followup_semantic_window = 1800
    state._group_engaged.clear()
    bot._context[rid].clear()
    try:
        bot._mark_engaged(rid, "@alice:ex.org")   # alice 在主时间线刚 @ 过
        # 顶层裸消息 → weak（对照组）
        k, _ = bot._address_kind(room, make_event("接着讲", sender="@alice:ex.org"))
        assert k == "weak"
        # 线程里的消息（m.thread，且【不带】m.in_reply_to 回退）：老代码只看 in_reply_to 会漏判成顶层 weak，
        # 拿顶层上下文误判「是不是找我」。现在用线程关系稳健排除 → 归 ""（不当顶层续话）
        thr_ev = types.SimpleNamespace(
            body="接着讲", sender="@alice:ex.org", event_id="$t1",
            server_timestamp=int(time.time() * 1000),
            source={"content": {"m.relates_to": {"rel_type": "m.thread", "event_id": "$root"}}})
        k, _ = bot._address_kind(room, thr_ev)
        assert k == ""
    finally:
        settings.group_followup_window, settings.followup_semantic_window = orig
        state._group_engaged.clear()
        bot._context[rid].clear()

# ---------- 8c) 续话语义闸：__NO__ 拦下、__YES__/出错放行 ----------
def test_followup_semantic_gate():
    import proactive
    rid = "!gate:ex.org"
    bot._context[rid].clear()
    orig_quick = proactive.runner.quick
    try:
        async def q_no(prompt):
            return "__NO__"
        proactive.runner.quick = q_no
        assert asyncio.run(proactive.followup_is_for_me(rid, "Alice", "该睡了")) is False

        async def q_yes(prompt):
            return "__YES__"
        proactive.runner.quick = q_yes
        assert asyncio.run(proactive.followup_is_for_me(rid, "Alice", "那再加个测试")) is True

        async def q_boom(prompt):
            raise RuntimeError("claude 挂了")
        proactive.runner.quick = q_boom          # 判断出错 → 放行（宁可多接，不漏真续话）
        assert asyncio.run(proactive.followup_is_for_me(rid, "Alice", "继续")) is True
    finally:
        proactive.runner.quick = orig_quick
        bot._context[rid].clear()

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


TESTS = [
    ('群对话延续窗口', test_group_followup_window),
    ('续话窗口第三人插话作废+强弱分级', test_followup_window_third_party_invalidates),
    ('_address_kind 强/弱信号分级', test_address_kind_strong_vs_weak),
    ('续话软窗口 硬窗外交给语义闸', test_followup_semantic_window),
    ('线程消息不当顶层weak续话', test_thread_msg_not_top_level_weak),
    ('续话语义闸 NO拦下/YES/出错放行', test_followup_semantic_gate),
    ('/reset 清空背景上下文', test_reset_clears_context),
    ('重试仅限会话失效', test_retry_only_on_session_error),
]
