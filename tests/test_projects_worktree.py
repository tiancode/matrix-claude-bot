"""冒烟：worktree 自愈/重置/寄存/仅新会话·会话 key 隔离·默认分支·触发词边界·会话落盘"""
from _helpers import (
    addressing, asyncio, bot, claude_runner, json, os, settings, state, time)

# ---------- 37) 残留目录/半截 .git：clone 前清掉并重 clone，不再永久失败 ----------
def test_ensure_cleans_residual_dir():
    import tempfile
    import projects as pmod
    root = tempfile.mkdtemp()
    P = pmod.Projects()
    P.root = root
    P.bindings_path = os.path.join(root, "bindings.json")
    P._projects, P._rooms = {}, {}
    info = {"host": "https://gitea.example.com", "owner": "o", "repo": "r",
            "clone_url": "https://gitea.example.com/o/r.git",
            "web_url": "https://gitea.example.com/o/r"}
    local = os.path.join(root, "gitea.example.com", "o", "r")
    os.makedirs(local, exist_ok=True)
    with open(os.path.join(local, "leftover.txt"), "w") as f:   # 残留的非 git 脏目录
        f.write("junk")

    async def fake_git(*args, cwd=None):
        if args[0] == "rev-parse" and "--is-inside-work-tree" in args:
            ok = os.path.isdir(os.path.join(cwd, ".git"))
            return (0, "true", "") if ok else (1, "", "not a git repo")
        if args[0] == "clone":
            dest = args[-1]
            assert not os.path.exists(os.path.join(dest, "leftover.txt")), "clone 前没清掉残留"
            os.makedirs(os.path.join(dest, ".git"), exist_ok=True)
            return 0, "", ""
        if args[0] == "symbolic-ref":
            return 0, "refs/remotes/origin/main", ""
        return 0, "", ""

    orig_git = pmod._git
    orig_host, orig_tok = settings.gitea_host, settings.gitea_token
    pmod._git = fake_git
    settings.gitea_host, settings.gitea_token = "https://gitea.example.com", ""
    try:
        rec = asyncio.run(P.ensure_project(info))
    finally:
        pmod._git = orig_git
        settings.gitea_host, settings.gitea_token = orig_host, orig_tok
    assert rec["path"] == local
    assert os.path.isdir(os.path.join(local, ".git"))               # 重新 clone 出了 .git
    assert not os.path.exists(os.path.join(local, "leftover.txt"))  # 残留被清掉

# ---------- 38) 会话 key 按项目+房间隔离：不同房间不串台 ----------
def test_session_key_per_room():
    rec = {"id": "h/o/r"}
    k1 = state._sess_key(rec, "!a:ex.org")
    k2 = state._sess_key(rec, "!b:ex.org")
    assert k1 != k2                                   # 两个房间 → 两条会话
    assert "h/o/r" in k1 and "!a:ex.org" in k1        # 仍包含项目维度

# ---------- 39) 默认分支名带斜杠（release/2.0）不被 rsplit 截成 2.0 ----------
def test_detect_base_slash_branch():
    import projects as pmod

    async def fake_git(*args, cwd=None):
        if args[0] == "symbolic-ref":
            return 0, "refs/remotes/origin/release/2.0", ""
        return 1, "", ""

    orig = pmod._git
    pmod._git = fake_git
    try:
        base = asyncio.run(pmod.Projects._detect_base("/x"))
    finally:
        pmod._git = orig
    assert base == "release/2.0"                       # 不再被截成 "2.0"

# ---------- 40) 派活前把工作树拉回干净 base：fetch + checkout -B + reset --hard + clean ----------
def test_prepare_worktree_resets():
    import tempfile
    import projects as pmod
    d = tempfile.mkdtemp()
    os.makedirs(os.path.join(d, ".git"))             # 装成一个 git 工作树
    calls = []

    async def fake_git(*args, cwd=None):
        calls.append(args)
        return 0, "", ""                             # rev-parse 也回 0 → origin/base 存在

    orig = pmod._git
    pmod._git = fake_git
    try:
        asyncio.run(pmod.projects.prepare_worktree({"path": d, "base": "main"}))
    finally:
        pmod._git = orig
    ops = [a[0] for a in calls]
    assert "fetch" in ops                                            # 先 fetch 更新远端
    assert any(a[0] == "checkout" and "-f" in a and "-B" in a for a in calls)  # 强制切回 base
    assert any(a[0] == "reset" and "--hard" in a for a in calls)     # 对齐 origin/base
    assert any(a[0] == "clean" for a in calls)                       # 清掉未跟踪残留
    assert not any(a[0] == "stash" for a in calls)                   # 干净树（status 回空）不寄存

# ---------- 40b) 脏树派活前先 auto-stash 寄存（含未跟踪），且发生在 reset --hard 之前 ----------
def test_prepare_worktree_stashes_dirty():
    import tempfile
    import projects as pmod
    d = tempfile.mkdtemp()
    os.makedirs(os.path.join(d, ".git"))
    calls = []

    async def fake_git(*args, cwd=None):
        calls.append(args)
        if args[:2] == ("status", "--porcelain"):
            return 0, " M foo.py\n?? bar.txt\n", ""   # 装成脏树 + 未跟踪文件
        if args[:2] == ("rev-parse", "--abbrev-ref"):
            return 0, "claude/wip\n", ""
        return 0, "", ""

    orig = pmod._git
    pmod._git = fake_git
    try:
        asyncio.run(pmod.projects.prepare_worktree({"path": d, "base": "main"}))
    finally:
        pmod._git = orig
    # 寄存发生了，且带 -u（含未跟踪）
    assert any(a[0] == "stash" and "push" in a and "--include-untracked" in a for a in calls)
    # 且寄存在 reset --hard 之前——先停住脏活，再清干净
    stash_i = next(i for i, a in enumerate(calls) if a[0] == "stash")
    reset_i = next(i for i, a in enumerate(calls) if a[0] == "reset" and "--hard" in a)
    assert stash_i < reset_i

# ---------- 40c) prepare（拉回干净 base）只在新会话跑一次，续接同一场对话时不再 reset ----------
def test_prepare_runs_only_on_fresh_session():
    async def run():
        r = claude_runner.ClaudeRunner()
        r._sessions.clear()          # 从干净会话表起，别被别的用例落盘到 store 的会话串进来

        async def fake_run(cmd, cwd=None, on_proc=None):
            return 0, json.dumps({"result": "ok", "session_id": "sid1",
                                  "is_error": False}).encode(), b""
        r._run = fake_run
        n = {"prep": 0}

        async def prep():
            n["prep"] += 1

        await r.ask("pk", "hi", prepare=prep)       # 第一轮：本 key 无会话 → 新任务 → 该跑 prepare
        first = n["prep"]
        await r.ask("pk", "again", prepare=prep)    # 第二轮：上轮已存 sid（--resume 续接）→ 不该再 reset
        second = n["prep"]
        # 第三轮：fork 出的新线程（新 key、无自身会话，但从 pk 分叉）——续接父对话上下文，不该 reset
        await r.ask("pk|thread", "fork it", prepare=prep, fork_from="pk")
        return first, second, n["prep"]

    first, second, forked = asyncio.run(run())
    assert first == 1        # 新会话跑了一次 prepare
    assert second == 1       # 续接轮没再跑（还是 1，不是 2）——工作树不在对话中途被抽走
    assert forked == 1       # fork 新线程也没跑（还是 1）——分叉续接父上下文，别把父没提交的活 reset 掉

# ---------- 41) 触发词按词边界匹配（claude 不命中 claudette），CJK 词按子串 ----------
def test_trigger_word_boundary():
    orig = settings.trigger_phrase
    try:
        settings.trigger_phrase = "claude"
        assert bot._has_trigger("用 claude 跑一下") is True
        assert bot._has_trigger("claudette 来了") is False           # 子串不误命中
        assert addressing._strip_trigger("claude 修一下").strip() == "修一下"  # 去掉时也按边界
        settings.trigger_phrase = "小助手"                            # 非 ASCII → 子串
        assert bot._has_trigger("小助手帮我看看") is True
        settings.trigger_phrase = ""
        assert bot._has_trigger("随便一句话") is False                # 空触发词从不命中
    finally:
        settings.trigger_phrase = orig

# ---------- 42) 会话 session_id 落盘：重启（新 runner）后仍能恢复，多轮不断 ----------
def test_sessions_persisted_across_restart():
    import tempfile
    import claude_runner as cr
    d = tempfile.mkdtemp()
    orig = settings.store_path
    settings.store_path = d
    try:
        r1 = cr.ClaudeRunner()
        r1._sessions["h/o/r|!room:ex.org"] = ("sid-123", time.time())
        r1._save_sessions()
        r2 = cr.ClaudeRunner()                       # 模拟重启：新实例从盘上加载
        assert r2._sessions.get("h/o/r|!room:ex.org", (None,))[0] == "sid-123"
    finally:
        settings.store_path = orig


TESTS = [
    ('残留目录自愈重 clone', test_ensure_cleans_residual_dir),
    ('会话 key 按项目+房间隔离', test_session_key_per_room),
    ('默认分支带斜杠不被截断', test_detect_base_slash_branch),
    ('派活前清回干净 base', test_prepare_worktree_resets),
    ('脏树派活前先 auto-stash 寄存', test_prepare_worktree_stashes_dirty),
    ('prepare 只在新会话跑·续接不重置', test_prepare_runs_only_on_fresh_session),
    ('触发词按词边界匹配', test_trigger_word_boundary),
    ('会话落盘重启可恢复', test_sessions_persisted_across_restart),
]
