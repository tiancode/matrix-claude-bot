"""冒烟：私聊绑定/路由/解绑·自驱汇报口选群·项目长期记忆"""
from _helpers import (
    FakeRoom, asyncio, bot, dispatch, make_event, os, set_identity, settings, state, types)

# ---------- 43) 未绑定私聊 → 通用助手，且带"引导绑定"标记（私聊不再按内容自动分诊） ----------
def test_dm_unbound_is_general_with_bind_hint():
    set_identity()
    room = FakeRoom("!dmgen:ex.org", 2)                   # 未绑定 DM
    orig = bot.projects.get_room
    bot.projects.get_room = lambda r: None
    try:
        out = asyncio.run(dispatch._dispatch(room))
    finally:
        bot.projects.get_room = orig
    assert out.get("general") is True                     # 当通用助手答
    assert out.get("unbound_room") is True                # 系统提示会引导 /bind（私聊和群一个待遇）
    assert out["path"] == settings.claude_workdir          # 在隔离 scratch 目录答

# ---------- 43b) DM 发 /bind <URL>：真绑定（私聊也能绑）；/bind 不带地址：给用法引导 ----------
def test_dm_bind_binds_and_needs_url():
    set_identity()
    state._synced = True
    rid = "!dmbind:ex.org"
    room = FakeRoom(rid, 2)                     # DM
    bot._context[rid].clear()
    sent, bound, pend = [], [], []
    orig_host = settings.gitea_host

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (state.client, state._spawn, bot.do_bind, bot.projects.get_room)
    settings.gitea_host = "https://gitea.example.com"
    state.client = FC()
    state._spawn = lambda coro: pend.append(coro)
    bot.do_bind = lambda room, repo, ev, task: bound.append((repo, task))   # 同步桩：记 (仓库, 任务正文)
    bot.projects.get_room = lambda rid: None
    try:
        async def go(body, eid):
            del pend[:]; del sent[:]
            await bot.on_message(room, make_event(body, event_id=eid))
            for c in pend:
                if hasattr(c, "__await__"):
                    await c
                elif hasattr(c, "close"):
                    c.close()
        asyncio.run(go("/bind https://gitea.example.com/o/r", "$dmb1"))
        assert bound and bound[-1][0]["repo"] == "r"        # DM /bind 真的绑定了
        assert not any("私聊不用绑定" in m for m in sent)   # 不再是"私聊不用绑定"

        del bound[:]
        asyncio.run(go("/bind", "$dmb2"))
        assert bound == []                                  # 没带仓库地址 → 不绑
        assert any("仓库地址" in m for m in sent)           # 给出用法引导

        # 私聊里 URL 混在句子里（分诊已删）也要绑到它，并把剩下的话当任务派下去
        del bound[:]
        asyncio.run(go("帮我看看 https://gitea.example.com/o/r 有什么问题", "$dmb3"))
        assert bound and bound[-1][0]["repo"] == "r"        # 嵌在句中的 URL 也绑
        assert "有什么问题" in bound[-1][1]                 # URL 后的话作为任务带下去
    finally:
        (state.client, state._spawn, bot.do_bind, bot.projects.get_room) = orig
        settings.gitea_host = orig_host
        bot._context[rid].clear()

# ---------- 43c) 私聊绑定后：直接落到绑定项目（过 ensure）；/unbind 解绑回到未绑定闲聊 ----------
def test_dm_binding_routes_and_unbinds():
    set_identity()
    rid = "!dmbound:ex.org"
    room = FakeRoom(rid, 2)                     # DM
    app = {"id": "h/o/app", "owner": "o", "repo": "app",
           "path": "/x", "base": "main", "host": "https://h"}
    ensured = []

    async def fake_ensure(info):
        ensured.append(info)
        return info

    orig = (bot.projects.get_room, bot.projects.ensure_project)
    bot.projects.get_room = lambda r: app if r == rid else None
    bot.projects.ensure_project = fake_ensure
    try:
        # 绑定了 → 直接落到 app（过 ensure 校验/修复 checkout），私聊不再按内容分诊
        out = asyncio.run(dispatch._dispatch(room))
        assert out is app and ensured == [app]
    finally:
        (bot.projects.get_room, bot.projects.ensure_project) = orig

    # /unbind：解绑并给回执
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    unbound = []

    async def fake_unbind(r):
        unbound.append(r); return True

    orig2 = (state.client, bot.projects.get_room, bot.projects.unbind)
    state.client = FC()
    bot.projects.get_room = lambda r: app if r == rid else None
    bot.projects.unbind = fake_unbind
    bot._last_project_by_room[rid] = app["id"]            # 登记簿里也留着（handle_task 会写）
    try:
        asyncio.run(bot.handle_unbind(room))
    finally:
        (state.client, bot.projects.get_room, bot.projects.unbind) = orig2
        bot._last_project_by_room.pop(rid, None)
    assert unbound == [rid]                               # 真的解绑了这条私聊
    assert any("解绑" in m for m in sent)                 # 有回执
    assert rid not in bot._last_project_by_room           # 登记簿一并清掉：解绑后不再被心跳/健康度推消息

# ---------- 43d) do_bind 幂等：重复绑同一个仓库不重置会话（重发 URL 别清掉多轮上下文）----------
def test_do_bind_same_repo_keeps_session():
    set_identity()
    rid = "!dmidem:ex.org"
    room = FakeRoom(rid, 2)                     # DM
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "base": "main", "path": "/tmp"}
    now = {"bound": None}
    resets = []

    async def fake_bind_room(r, info):
        now["bound"] = rec
        return rec

    class FC:
        async def room_send(self, *a, **k):
            return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.get_room, bot.projects.bind_room, bot.runner.reset, state.client)
    bot.projects.get_room = lambda r: now["bound"]          # 反映当前绑定
    bot.projects.bind_room = fake_bind_room
    bot.runner.reset = lambda k: resets.append(k)
    state.client = FC()
    try:
        asyncio.run(bot.do_bind(room, {"owner": "o", "repo": "r"}))   # 首次绑 → 重置
        asyncio.run(bot.do_bind(room, {"owner": "o", "repo": "r"}))   # 再绑同一个 → 不重置
    finally:
        (bot.projects.get_room, bot.projects.bind_room, bot.runner.reset, state.client) = orig
    assert len(resets) == 1, resets            # 只有真正建立绑定那次重置了会话，重发同仓库不清上下文

# ---------- 43e) 自驱汇报口：同一仓库既绑群又绑私聊时优先群，别把团队汇报塞进私聊 ----------
def test_heartbeat_home_room_prefers_group():
    import heartbeat
    set_identity()
    pid = "h/o/r"
    dm_rid, grp_rid = "!hbdm:ex.org", "!hbgrp:ex.org"

    class FC:
        def __init__(self):
            self.rooms = {dm_rid: FakeRoom(dm_rid, 2), grp_rid: FakeRoom(grp_rid, 5)}

    orig_rooms = dict(bot.projects._rooms)
    orig_client = state.client
    bot.projects._rooms.clear()
    bot.projects._rooms[dm_rid] = pid           # 私聊先入（dict 顺序排在群前面）
    bot.projects._rooms[grp_rid] = pid
    state.client = FC()
    try:
        home = heartbeat._project_home_room(pid)
    finally:
        bot.projects._rooms.clear(); bot.projects._rooms.update(orig_rooms)
        state.client = orig_client
    assert home == grp_rid                      # 尽管私聊在前，仍选群（非私聊）作汇报口

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


TESTS = [
    ('未绑定私聊=通用助手+引导绑定', test_dm_unbound_is_general_with_bind_hint),
    ('DM /bind 真绑定·无地址给引导', test_dm_bind_binds_and_needs_url),
    ('私聊绑定后直接路由+/unbind 解绑', test_dm_binding_routes_and_unbinds),
    ('重复绑同仓库不重置会话', test_do_bind_same_repo_keeps_session),
    ('自驱汇报口优先群不塞私聊', test_heartbeat_home_room_prefers_group),
    ('项目长期记忆 跨会话留存', test_project_memory),
]
