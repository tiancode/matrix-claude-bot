"""冒烟：代码围栏·/bind 派活·群分派修复·绑定原子写·默认分支·自动绑定·DM 分类与 HTML 加固"""
from _helpers import (
    FakeRoom, asyncio, bot, dispatch, fmt, json, make_event, os, set_identity, settings, state, tasks, types)
import gitea

# ---------- 11) 分块后代码围栏自洽（每块 ``` 成对） ----------
def test_fence_balance_on_split():
    block = "```python\n" + ("x = 1\n" * 1500) + "```\n"      # 远超 4000 字节的代码块
    chunks = fmt._split("先说明一句\n" + block + "结尾一句")
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
        out = asyncio.run(dispatch._dispatch(room))
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
    assert "答复内容" not in [b for _, _, b, *_ in bot._context[rid]]  # 没发出去就不进上下文

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

# ---------- 19) /new-project 建仓成功后走 do_bind 同一条路，且能带上后续任务 ----------
def test_new_project_creates_and_binds():
    set_identity()
    room = FakeRoom("!g:ex.org", 3)
    ev = make_event("/new-project foo 顺便加个 CI")
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (settings.gitea_host, settings.gitea_token, gitea.create_repo, tasks.do_bind, state.client)
    settings.gitea_host, settings.gitea_token = "https://gitea.example.com", "tok"
    bound = {}

    async def fake_create_repo(name, private=True):
        return {"html_url": f"https://gitea.example.com/bot/{name}",
                "name": name, "owner": {"login": "bot"}}, ""

    async def fake_do_bind(rm, repo, event, task_text):
        bound["repo"], bound["task_text"] = repo, task_text
    gitea.create_repo = fake_create_repo
    tasks.do_bind = fake_do_bind
    state.client = FC()
    try:
        asyncio.run(tasks.handle_new_project(room, ev, "/new-project foo 顺便加个 CI"))
    finally:
        (settings.gitea_host, settings.gitea_token, gitea.create_repo,
         tasks.do_bind, state.client) = orig
    assert bound.get("repo", {}).get("repo") == "foo"    # 直接用 API 返回的 owner/name 构造，没经过 parse_repo_url
    assert bound.get("repo", {}).get("owner") == "bot"
    assert bound.get("task_text") == "顺便加个 CI"        # 名字后的剩余文本当任务接着派

# ---------- 19b) 建仓账号名命中 Gitea 保留字（如 admin）也不影响自动绑定 ----------
def test_new_project_binds_even_with_reserved_owner_name():
    set_identity()
    room = FakeRoom("!g:ex.org", 3)
    bound = {}

    class FC:
        async def room_send(self, r, mt, content, **k):
            return types.SimpleNamespace(event_id="$x")

    async def fake_create_repo(name, private=True):
        return {"html_url": f"https://gitea.example.com/admin/{name}",
                "name": name, "owner": {"login": "admin"}}, ""

    async def fake_do_bind(rm, repo, event, task_text):
        bound["repo"] = repo
    orig = (settings.gitea_host, settings.gitea_token, gitea.create_repo, tasks.do_bind, state.client)
    settings.gitea_host, settings.gitea_token = "https://gitea.example.com", "tok"
    gitea.create_repo = fake_create_repo
    tasks.do_bind = fake_do_bind
    state.client = FC()
    try:
        asyncio.run(tasks.handle_new_project(room, make_event("/new-project foo"), "/new-project foo"))
    finally:
        (settings.gitea_host, settings.gitea_token, gitea.create_repo,
         tasks.do_bind, state.client) = orig
    # trusted_repo_info 不套用 parse_repo_url 的 _RESERVED_OWNERS 拒绝名单，owner=admin 照样绑定成功
    assert bound.get("repo", {}).get("owner") == "admin"

# ---------- 19c) 中文命令别名带参数也能建仓（此前只有裸词才匹配，带名字反而落空） ----------
def test_new_project_chinese_alias_with_name_dispatches():
    set_identity()
    state._synced = True
    room = FakeRoom("!g:ex.org", 3)
    called = []
    orig = (bot.handle_new_project, state._spawn)
    bot.handle_new_project = lambda rm, ev, body: called.append(body)
    state._spawn = lambda coro: coro.close() if hasattr(coro, "close") else None
    try:
        asyncio.run(bot.on_message(room, make_event("新建项目 foo")))
    finally:
        (bot.handle_new_project, state._spawn) = orig
    assert called == ["新建项目 foo"]   # 带名字的中文命令能匹配到 handle_new_project（此前会落空到普通消息处理）

# ---------- 20) /new-project 建仓失败 / 名字非法都有明确提示，不静默 ----------
def test_new_project_rejects_bad_name_and_reports_failure():
    set_identity()
    room = FakeRoom("!g:ex.org", 3)
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (settings.gitea_host, settings.gitea_token, gitea.create_repo, state.client)
    settings.gitea_host, settings.gitea_token = "https://gitea.example.com", "tok"
    state.client = FC()
    try:
        asyncio.run(tasks.handle_new_project(room, make_event("/new-project ../evil"),
                                              "/new-project ../evil"))
        assert any("用法" in s for s in sent)             # 非法名字：提示用法，不去调 Gitea

        async def fake_fail(name, private=True):
            return None, "HTTP 409 name already exists"
        gitea.create_repo = fake_fail
        sent.clear()
        asyncio.run(tasks.handle_new_project(room, make_event("/new-project foo"),
                                              "/new-project foo"))
        assert any("建仓库失败" in s and "409" in s for s in sent)
    finally:
        (settings.gitea_host, settings.gitea_token, gitea.create_repo, state.client) = orig


# ---------- 18) 成员未同步的群不被当私聊 + 外发富文本剥外链 img ----------
def test_dm_classification_and_html_hardening():
    # 成员还没同步（users 空、member_count 0/缺）→ 不当 DM，否则 REPLY_IN_DM_ALWAYS 会逐条乱回
    unsynced = types.SimpleNamespace(users={})
    assert bot._is_dm(unsynced) is False
    assert bot._is_dm(types.SimpleNamespace(users={"@a:ex.org": 1, "@b:ex.org": 1})) is True
    assert bot._is_dm(types.SimpleNamespace(
        users={f"@u{i}:ex.org": 1 for i in range(9)}, member_count=0)) is False

    # 外链 <img src> 不得出现在外发 HTML（追踪像素 / 查看者 IP 泄露）；普通链接保留
    html = fmt._to_html("![x](http://attacker.example/track.png)") or ""
    assert "<img" not in html and "attacker.example" not in html
    assert '<a href="https://gitea.example.com/team/app/pulls/7">' in (
        fmt._to_html("[PR](https://gitea.example.com/team/app/pulls/7)") or "")


TESTS = [
    ('分块代码围栏自洽', test_fence_balance_on_split),
    ('/bind 带任务接着派', test_bind_carries_trailing_task),
    ('群分派修复丢失的 checkout', test_group_dispatch_repairs_checkout),
    ('绑定原子写+损坏备份', test_bindings_atomic_and_corrupt_backup),
    ('发送失败有日志不入上下文', test_send_failure_logged),
    ('默认分支选实际存在的', test_detect_base_prefers_existing_branch),
    ('纯链接才自动绑定', test_just_url_autobind_only_for_bare_url),
    ('/new-project 建仓成功后走 do_bind+带任务接着派', test_new_project_creates_and_binds),
    ('/new-project owner 命中保留字仍能绑定', test_new_project_binds_even_with_reserved_owner_name),
    ('/new-project 中文别名带参数也能派发', test_new_project_chinese_alias_with_name_dispatches),
    ('/new-project 非法名字/建仓失败均有提示', test_new_project_rejects_bad_name_and_reports_failure),
    ('未同步群不当私聊+剥外链img', test_dm_classification_and_html_hardening),
]
