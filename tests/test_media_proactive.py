"""冒烟：媒体下载/超体积/失败/消毒/滚删·主动插话冷却与预筛·换绑提示·is_dm·human_gap"""
from _helpers import (
    FakeRoom, _drain_tasks, addressing, asyncio, bot, fmt, make_event, make_media_event, media, os, proactive, set_identity, settings, state, time, types)

# ---------- 19) 媒体：下载落盘 + 入上下文 + 被点名(DM)派活带上文件路径 ----------
def test_media_download_and_dispatch():
    import tempfile
    import glob
    set_identity()
    state._synced = True
    rid = "!md:ex.org"
    room = FakeRoom(rid, 2)                       # 2 人 → DM → 必回
    bot._context[rid].clear()
    tmp = tempfile.mkdtemp()
    orig = (settings.media_root, settings.media_enabled, state.client, media.handle_task)
    settings.media_root, settings.media_enabled = tmp, True
    captured = {}

    async def fake_handle(rm, ev, text, skip_body=None):
        captured["text"] = text

    class FC:
        async def download(self, mxc=None, save_to=None, **k):   # nio 流式落盘到 save_to
            with open(save_to, "wb") as f:
                f.write(b"hello-log-bytes")
            return types.SimpleNamespace(content_type="text/plain", filename="app.log")

    state.client = FC()
    media.handle_task = fake_handle
    try:
        async def go():
            await media._process_media(room, make_media_event(body="app.log", event_id="$md1"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_enabled, state.client, media.handle_task) = orig

    files = glob.glob(os.path.join(tmp, "*", "*"))
    assert files, "媒体没落盘"
    with open(files[0], "rb") as f:
        assert f.read() == b"hello-log-bytes"                      # 内容正确写盘
    assert any(files[0] in b for _, _, b, *_ in bot._context[rid])     # 上下文带本地路径
    assert files[0] in captured.get("text", "")                    # 派活时把路径喂给 Claude

# ---------- 20) 媒体：声明体积超限则不下载，只在上下文标注 ----------
def test_media_oversize_skipped():
    import tempfile
    set_identity()
    state._synced = True
    rid = "!mo:ex.org"
    room = FakeRoom(rid, 2)
    bot._context[rid].clear()
    orig = (settings.media_root, settings.media_max_mb, state.client, media.handle_task)
    settings.media_root, settings.media_max_mb = tempfile.mkdtemp(), 1
    called = {"dl": 0}

    class FC:
        async def download(self, mxc=None, **k):
            called["dl"] += 1
            return types.SimpleNamespace(body=b"x", content_type="", filename="big.bin")

    async def fake_handle(rm, ev, text):
        pass

    state.client, media.handle_task = FC(), fake_handle
    try:
        async def go():
            await media._process_media(
                room, make_media_event(body="big.bin", size=5 * 1024 * 1024, event_id="$big"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_max_mb, state.client, media.handle_task) = orig
    assert called["dl"] == 0                                       # 超限不下载
    assert any("超过上限" in b for _, _, b, *_ in bot._context[rid])   # 上下文有标注

# ---------- 20b) DM 文件处理失败且无 caption：明确回错误，不沉默 return ----------
def test_media_failure_notifies_when_addressed():
    import tempfile
    set_identity()
    state._synced = True
    rid = "!mfail:ex.org"
    room = FakeRoom(rid, 2)                     # DM → 必回
    bot._context[rid].clear()
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")
        async def download(self, mxc=None, save_to=None, **k):   # 不写文件 → size 0 → 判失败
            return types.SimpleNamespace(content_type="", filename="broken.bin")

    async def fake_handle(rm, ev, text, skip_body=None):
        pass

    orig = (settings.media_root, settings.media_enabled, state.client, media.handle_task)
    settings.media_root, settings.media_enabled = tempfile.mkdtemp(), True
    state.client = FC()
    media.handle_task = fake_handle
    try:
        async def go():
            await media._process_media(
                room, make_media_event(body="broken.bin", event_id="$mfail1"), False)
            await _drain_tasks()
        asyncio.run(go())
    finally:
        (settings.media_root, settings.media_enabled, state.client, media.handle_task) = orig
        bot._context[rid].clear()
    assert any("没能处理" in m and "下载失败" in m for m in sent)   # 文件失败且无 caption → 回错误

# ---------- 21) 媒体文件名消毒：挡掉 ../ 路径穿越 ----------
def test_media_safe_name():
    s = fmt._safe_name("../../etc/passwd", "f")
    assert "/" not in s and not s.startswith(".")                  # 无分隔符、不以点开头
    assert fmt._safe_name("", "fallback") == "fallback"            # 空 → 兜底名

# ---------- 22) 媒体滚动删旧：保留最近 N 个；keep=0 不会因 [:-0] 反而全留 ----------
def test_media_prune():
    import tempfile
    d = tempfile.mkdtemp()
    for i in range(5):
        p = os.path.join(d, f"f{i}")
        with open(p, "w") as f:
            f.write("x")
        os.utime(p, (1000 + i, 1000 + i))         # 递增 mtime，f0 最旧
    media._prune_dir(d, 2)
    left = sorted(os.listdir(d))
    assert left == ["f3", "f4"], left              # 只留最近 2 个

    for i in range(3):
        p = os.path.join(d, f"g{i}")
        with open(p, "w") as f:
            f.write("x")
        os.utime(p, (2000 + i, 2000 + i))
    media._prune_dir(d, 0)                            # keep=0 被钳到 1，不该一个都不删
    assert os.listdir(d) == ["g2"], os.listdir(d)

# ---------- 25) 主动插话判定 PASS 时只占用短冷却，不整段沉默 ----------
def test_proactive_pass_keeps_short_cooldown():
    set_identity()
    rid = "!pc:ex.org"
    room = FakeRoom(rid, 3)
    bot._context[rid].clear()
    state._last_proactive[rid] = 0.0
    orig = (proactive.runner, state.client, settings.proactive_cooldown)
    settings.proactive_cooldown = 600

    class R:
        async def quick(self, prompt):
            return "__PASS__"

    class FC:
        async def room_send(self, *a, **k):
            return types.SimpleNamespace(event_id="$x")

    proactive.runner, state.client = R(), FC()
    try:
        asyncio.run(bot.maybe_proactive(room, make_event("报错了帮我看看"), "报错了帮我看看"))
    finally:
        (proactive.runner, state.client, settings.proactive_cooldown) = orig
    remaining = 600 - (time.time() - state._last_proactive[rid])
    assert 0 < remaining <= proactive._PROACTIVE_PASS_COOLDOWN + 2, remaining   # PASS 后很快能重判

# ---------- 25b) PROACTIVE_REQUIRE_HINT：关掉后能评估"没人求助但话里有错"的陈述句 ----------
def test_proactive_require_hint_toggle():
    set_identity()
    rid = "!ph:ex.org"
    room = FakeRoom(rid, 3)
    bot._context[rid].clear()
    # 纯陈述句、无任何求助/报错词，但内容是技术错误 → 关键词预筛会拦它
    msg = "这个 list 多线程 append 完全安全，随便并发写都没问题"
    assert not addressing._looks_actionable(msg)
    calls = []

    class R:
        async def quick(self, prompt):
            calls.append(prompt)
            return "其实 list 并发写不是线程安全的，高并发 append 可能丢元素，建议加锁或用 queue。"

    class FC:
        async def room_send(self, r, mt, content, **k):
            return types.SimpleNamespace(event_id="$x")

    orig = (proactive.runner, state.client, bot.projects.get_room,
            settings.proactive_require_hint, settings.proactive_cooldown,
            settings.transcript_enabled)
    proactive.runner, state.client = R(), FC()
    bot.projects.get_room = lambda r: None     # 未绑库 → 走 quick 文本判断
    settings.proactive_cooldown = 600
    settings.transcript_enabled = False        # 别在测试里写 store/transcripts
    try:
        settings.proactive_require_hint = True   # 预筛开：非求助句被挡下，不评估
        state._last_proactive[rid] = 0.0
        asyncio.run(bot.maybe_proactive(room, make_event(msg), msg))
        assert calls == [], "require_hint=True 时不该评估非求助消息"

        settings.proactive_require_hint = False  # 预筛关：每条都评估，judge 纠错 → 会插话
        state._last_proactive[rid] = 0.0
        asyncio.run(bot.maybe_proactive(room, make_event(msg), msg))
        assert len(calls) == 1, "require_hint=False 时应评估并纠正"
    finally:
        (proactive.runner, state.client, bot.projects.get_room,
         settings.proactive_require_hint, settings.proactive_cooldown,
         settings.transcript_enabled) = orig

# ---------- 26) 已绑定群里再发裸 URL：给换绑提示而非静默无反应 ----------
def test_group_rebind_hint():
    set_identity()
    state._synced = True
    rid = "!gb:ex.org"
    room = FakeRoom(rid, 3)
    bot._context[rid].clear()
    bound = {"id": "gitea.example.com/o/old", "owner": "o", "repo": "old"}
    msgs, pending = [], []
    orig = (settings.gitea_host, bot.projects.get_room, state.client, state._spawn)
    settings.gitea_host = "https://gitea.example.com"
    bot.projects.get_room = lambda r: bound

    class FC:
        async def room_send(self, r, mt, content, **k):
            msgs.append(content["body"])
            return types.SimpleNamespace(event_id="$x")

    state.client = FC()
    state._spawn = lambda coro: pending.append(coro)
    try:
        async def go():
            await bot.on_message(room, make_event("https://gitea.example.com/o/new"))
            for c in pending:
                await c
        asyncio.run(go())
    finally:
        (settings.gitea_host, bot.projects.get_room, state.client, state._spawn) = orig
    assert any("换绑" in m for m in msgs)        # 裸 URL 撞上已绑仓库 → 提示换绑

# ---------- 27) 代码块跨分块续块时保留语言标记（语法高亮不丢） ----------
def test_fence_language_preserved():
    block = "```python\n" + ("x = 1\n" * 1500) + "```\n"
    chunks = fmt._split("说明\n" + block + "结尾")
    assert len(chunks) >= 2
    cont = [c for c in chunks if c.startswith("```")]      # 续块开头即重开的围栏
    assert cont and all(c.startswith("```python") for c in cont), \
        "分块续块重开围栏时丢了语言标记"

# ---------- 29) 群里"@bot 仓库URL 任务"一条消息：先绑再派，不再答非所问 ----------
def test_group_url_with_task_binds():
    set_identity()
    state._synced = True
    rid = "!gbind:ex.org"
    room = FakeRoom(rid, 3)                               # 群、未绑定
    orig_host = settings.gitea_host
    orig = (bot.projects.get_room, bot.do_bind, state._spawn)
    settings.gitea_host = "https://gitea.example.com"
    bot.projects.get_room = lambda r: None
    captured = {}

    async def fake_do_bind(room, repo, event=None, task_text=""):
        captured["repo"], captured["task"] = repo, task_text

    bot.do_bind = fake_do_bind
    pend = []
    state._spawn = lambda coro: pend.append(coro)
    try:
        async def go():
            await bot.on_message(room, make_event(
                "@claude-bot https://gitea.example.com/team/app 帮我修登录刷新",
                mentions=["@claudebot:ex.org"]))
            for c in pend:
                await c
        asyncio.run(go())
    finally:
        settings.gitea_host = orig_host
        (bot.projects.get_room, bot.do_bind, state._spawn) = orig
        bot._context[rid].clear()
    assert captured.get("repo", {}).get("repo") == "app"  # 同条消息里的 URL 被识别并绑定
    assert captured.get("task") == "帮我修登录刷新"        # @bot 与 URL 都剥掉，只剩任务正文

# ---------- 30) _is_dm 只认恰好 2 人：只同步到 bot 自己(1) 不当私聊 ----------
def test_is_dm_requires_exactly_two():
    assert bot._is_dm(types.SimpleNamespace(users={"@bot:ex.org": 1})) is False
    assert bot._is_dm(types.SimpleNamespace(users={"@a:ex.org": 1, "@b:ex.org": 1})) is True
    assert bot._is_dm(types.SimpleNamespace(users={"@bot:ex.org": 1}, member_count=5)) is False

# ---------- 31) _human_gap：25 小时不再塌成"约 1 天" ----------
def test_human_gap_precision():
    assert fmt._human_gap(25 * 3600) == "约 25 小时"
    assert "天" in fmt._human_gap(50 * 3600)             # 超过 48h 才进"天"


TESTS = [
    ('媒体下载落盘+入上下文+派活', test_media_download_and_dispatch),
    ('媒体超体积跳过', test_media_oversize_skipped),
    ('媒体失败无caption 明确回错误', test_media_failure_notifies_when_addressed),
    ('媒体文件名消毒', test_media_safe_name),
    ('媒体滚动删旧', test_media_prune),
    ('主动 PASS 只占短冷却', test_proactive_pass_keeps_short_cooldown),
    ('主动插话预筛开关', test_proactive_require_hint_toggle),
    ('已绑群裸 URL 给换绑提示', test_group_rebind_hint),
    ('分块续块保留语言标记', test_fence_language_preserved),
    ('群 URL+任务同条消息先绑再派', test_group_url_with_task_binds),
    ('_is_dm 只认恰好 2 人', test_is_dm_requires_exactly_two),
    ('_human_gap 25h 不塌成 1 天', test_human_gap_precision),
]
