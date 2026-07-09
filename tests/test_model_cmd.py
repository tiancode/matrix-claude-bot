"""冒烟：/model 房间级模型——命令识别·查看/设置/恢复默认·派活透传·--model 只在新会话带·持久化"""
from _helpers import (
    FakeRoom, _CapClient, _task_fixtures, asyncio, bot, claude_runner, json, make_event,
    set_identity, settings, state, tasks, time)

import dispatch
import projects as projects_mod


# ---------- /model 命令识别：斜杠按前缀、中文「模型」严进（别吞普通聊天） ----------
def test_model_cmd_recognition():
    ok = tasks._is_model_cmd
    assert ok("/model")
    assert ok("/model opus")
    assert ok("/Model fable")          # 大小写不敏感（低层比对用 low）
    assert ok("模型")
    assert ok("当前模型")
    assert ok("模型 opus")
    assert ok("模型 claude-opus-4-8")
    assert ok("模型 默认")
    assert ok("模型 reset")
    # 中文入口严进：参数不像模型名/恢复默认词的，当普通聊天放行
    assert not ok("模型是什么")
    assert not ok("模型 是什么意思")
    assert not ok("模型 这个词咋理解")
    assert not ok("聊聊模型")


# ---------- /model：查看 / 设置 / 非法名 / 重复设置 / reset / 中文入口 ----------
def test_model_cmd_view_set_reset():
    set_identity()
    c = _CapClient(); state.client = c
    rid = "!mcmd:ex.org"
    room = FakeRoom(rid, 3)
    orig_get = projects_mod.projects.get_room
    orig_model = settings.claude_model
    projects_mod.projects.get_room = lambda r: None   # 未绑定房间：会话按通用助手算
    settings.claude_model = ""
    state._room_model.pop(rid, None)
    try:
        # ① 不带参数 → 直接返回当前用的模型（没配置也没覆盖 → CLI 默认）
        asyncio.run(tasks.handle_model(room, "/model"))
        assert "默认" in c.sent[-1]["body"]
        # ①' 配了全局 CLAUDE_MODEL → 显示它
        settings.claude_model = "conf-model"
        asyncio.run(tasks.handle_model(room, "/model"))
        assert "conf-model" in c.sent[-1]["body"]
        # ② 设置本房间覆盖
        asyncio.run(tasks.handle_model(room, "/model opus"))
        assert state._room_model.get(rid) == "opus" and "opus" in c.sent[-1]["body"]
        # ③ 再查看 → 显示覆盖值
        asyncio.run(tasks.handle_model(room, "/model"))
        assert "opus" in c.sent[-1]["body"]
        # ④ 非法名字：拒绝且不写入
        asyncio.run(tasks.handle_model(room, "/model 这不是模型"))
        assert state._room_model.get(rid) == "opus"
        # ⑤ 重复设置同名 → 提示无需改动，不变
        asyncio.run(tasks.handle_model(room, "/model opus"))
        assert state._room_model.get(rid) == "opus"
        # ⑥ 会话漂移提示：存活会话记录的模型 ≠ 当前设置 → 查看时点出来
        skey = state._sess_key(dispatch._general_rec(), rid)
        claude_runner.runner._sessions[skey] = ("sid-x", time.time(), "old-model")
        asyncio.run(tasks.handle_model(room, "/model"))
        assert "old-model" in c.sent[-1]["body"]
        claude_runner.runner._sessions.pop(skey, None)
        # ⑦ reset 清掉覆盖
        asyncio.run(tasks.handle_model(room, "/model reset"))
        assert rid not in state._room_model
        # ⑧ 中文入口：「模型 <名字>」设置、「模型 默认」恢复
        asyncio.run(tasks.handle_model(room, "模型 sonnet"))
        assert state._room_model.get(rid) == "sonnet"
        asyncio.run(tasks.handle_model(room, "模型 默认"))
        assert rid not in state._room_model
    finally:
        projects_mod.projects.get_room = orig_get
        settings.claude_model = orig_model
        state._room_model.pop(rid, None)


# ---------- 派活透传：设了房间模型的任务把 model 递进 runner.ask，没设不带 ----------
def test_room_model_passed_to_ask():
    set_identity()
    c = _CapClient(); state.client = c
    _task_fixtures()
    seen = {}

    async def fake_ask(key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                       on_delta=None, cancel_key=None, fork_from=None, **kw):
        seen.update(kw)
        return "搞定"
    bot.runner.ask = fake_ask
    orig_stream = settings.stream_replies
    settings.stream_replies = False
    rid = "!mpass:ex.org"
    state._room_model[rid] = "opus"
    try:
        asyncio.run(tasks.handle_task(FakeRoom(rid, 3), make_event("干个活", event_id="$mt1"), "干个活"))
        assert seen.get("model") == "opus"
        seen.clear()
        # 没设置的房间：不带 model kwarg（runner 回落 CLAUDE_MODEL）
        asyncio.run(tasks.handle_task(FakeRoom("!mpass2:ex.org", 3),
                                      make_event("干个活", event_id="$mt2"), "干个活"))
        assert "model" not in seen
    finally:
        settings.stream_replies = orig_stream
        state._room_model.pop(rid, None)


# ---------- runner：--model 覆盖只在开新会话时带；ask 把生效模型记进会话第三元 ----------
def test_runner_model_override_cmdline_and_session_record():
    r = claude_runner.runner
    orig = settings.claude_model
    settings.claude_model = "conf-model"
    try:
        c1 = r._cmd("p", None, True)                       # 无覆盖 → 全局配置
        assert c1[c1.index("--model") + 1] == "conf-model"
        c2 = r._cmd("p", None, True, model="opus")         # 覆盖生效
        assert c2[c2.index("--model") + 1] == "opus"
        assert "--model" not in r._cmd("p", "sid1", True, model="opus")   # --resume 不带 --model
        p1 = r._cmd_persistent(None, None, False, "opus")  # 常驻进程同语义
        assert p1[p1.index("--model") + 1] == "opus"
        assert "--model" not in r._cmd_persistent("sid1", None, False, "opus")
    finally:
        settings.claude_model = orig

    # ask 全新开会话：sessions 第三元记的是本轮生效的覆盖模型（/status、/model 漂移提示的数据源）
    async def go():
        rr = claude_runner.ClaudeRunner()

        async def fake_run(cmd, cwd=None, sema=None, env=None, timeout=None, on_proc=None):
            out = json.dumps({"result": "ok", "session_id": "S1", "is_error": False}).encode()
            return 0, out, b""
        rr._run = fake_run
        assert await rr.ask("proj|!mrec:ex.org", "hi", model="opus") == "ok"
        assert rr._sessions["proj|!mrec:ex.org"][2] == "opus"
        assert rr.session_model("proj|!mrec:ex.org") == "opus"
    asyncio.run(go())


# ---------- 持久化：房间模型落盘、重启（重新加载）不丢 ----------
def test_room_model_persistence():
    import tempfile
    orig_store = settings.store_path
    settings.store_path = tempfile.mkdtemp(prefix="mxbot-model-store-")
    try:
        state._room_model.clear()
        state._room_model["!p1:ex.org"] = "opus"
        state._save_room_models()
        state._room_model.clear()
        state._load_room_models()
        assert state._room_model == {"!p1:ex.org": "opus"}
    finally:
        state._room_model.clear()
        settings.store_path = orig_store


TESTS = [
    ('/model 命令识别·中文严进不吞聊天', test_model_cmd_recognition),
    ('/model 查看/设置/恢复默认/漂移提示', test_model_cmd_view_set_reset),
    ('房间模型随派活透传 runner.ask', test_room_model_passed_to_ask),
    ('--model 只在新会话带·会话记录生效模型', test_runner_model_override_cmdline_and_session_record),
    ('房间模型持久化 重载不丢', test_room_model_persistence),
]
