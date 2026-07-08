"""冒烟：寻址(回复/点名/track)·上下文与编辑忽略·TTL·安全·孤儿房退房·加密提示"""
from _helpers import (
    FakeRoom, addressing, asyncio, bot, claude_runner, fmt, json, make_event, os, projects, redact, set_identity, settings, state, time, types)

# ---------- 2) 认 reply / 点名 ----------
def test_reply_addressing():
    set_identity()
    bot._sent_events.append("$botmsg1")
    room = FakeRoom("!g:ex.org", 3)

    a, t = addressing._is_addressed(room, make_event(
        "> <@claudebot:ex.org> 旧消息\n\n再处理下边界", in_reply_to="$botmsg1"))
    assert a and t == "再处理下边界"                                   # 回复 bot → 点名 + 去引用

    a2, _ = addressing._is_addressed(room, make_event(
        "> <@bob:ex.org> 闲聊\n\n哈哈", in_reply_to="$notbot"))
    assert a2 is False                                                 # 回复别人 → 不触发

    fb = ('<mx-reply><blockquote><a href="x">In reply to</a> '
          '<a href="y">@claudebot:ex.org</a><br>旧话</blockquote></mx-reply>看天气')
    a3, _ = addressing._is_addressed(room, make_event(
        "> <@claudebot:ex.org> 旧话\n\n看天气", in_reply_to="$notbot", formatted_body=fb))
    assert a3 is False                                                 # 引用块含 bot id → 不误触发

    a4, t4 = addressing._is_addressed(room, make_event(
        "@claude-bot 看下 CI", mentions=["@claudebot:ex.org"]))
    assert a4 and t4 == "看下 CI"                                      # 普通 @pill 仍有效

# ---------- 3) 引用回退块剥离（不误伤用户自己的 >） ----------
def test_reply_fallback_strip():
    c = {"m.relates_to": {"m.in_reply_to": {"event_id": "$x"}}}
    assert bot._strip_reply_fallback("> <@bot:ex.org> 旧\n\n新问题", c) == "新问题"
    assert bot._strip_reply_fallback("> 我引用一句\n继续", c) == "> 我引用一句\n继续"

# ---------- 4) track 门控 + 上下文时间戳单调 ----------
def test_track_and_monotonic():
    set_identity()
    sent = []

    class FC:
        async def room_send(self, rid, mt, content, **k):
            sent.append(content["body"])
            return types.SimpleNamespace(event_id="$e%d" % len(sent))

    state.client = FC()
    rid = "!r:ex.org"
    bot._context[rid].clear()
    bot._context[rid].append((time.time() + 5, "Alice", "更晚的消息"))  # 制造钟差

    asyncio.run(bot.send(rid, "状态：⏳ 绑定中"))             # 默认不 track
    asyncio.run(bot.send(rid, "真正的答复", track=True))      # track

    bodies = [b for _, _, b, *_ in bot._context[rid]]
    assert "状态：⏳ 绑定中" not in bodies                     # 状态消息不进上下文
    assert "真正的答复" in bodies                             # 答复进上下文
    ts = [t for t, *_ in bot._context[rid]]
    assert ts == sorted(ts)                                   # 时间单调，不倒挂

# ---------- 5) 自己账号：手打入上下文但不派活 ----------
def test_own_account_context():
    set_identity()
    state._synced = True                                        # 初始 sync 之后才处理消息
    room = FakeRoom("!g:ex.org", 3)
    bot._context[room.room_id].clear()
    orig_trigger, orig_spawn = settings.trigger_phrase, state._spawn
    settings.trigger_phrase = ""
    spawned = []
    state._spawn = lambda coro: (spawned.append(1), coro.close())
    try:
        asyncio.run(bot.on_message(
            room, make_event("自言自语", sender="@claudebot:ex.org", event_id="$mine")))
    finally:
        state._spawn, settings.trigger_phrase = orig_spawn, orig_trigger
    assert len(bot._context[room.room_id]) == 1               # 进了上下文
    assert not spawned                                        # 没派活

# ---------- 5b) 编辑消息(m.replace)当新消息进来：不重派活、不进上下文 ----------
def test_edit_event_ignored():
    set_identity()
    state._synced = True
    rid = "!edit:ex.org"
    room = FakeRoom(rid, 2)                    # DM → 否则不必回、测不出"本会派活"
    bot._context[rid].clear()
    spawned = []
    orig = state._spawn
    state._spawn = lambda coro: (spawned.append(1), coro.close())
    try:
        ev = make_event("* 修正后的正文", event_id="$edit1")
        ev.source["content"]["m.relates_to"] = {"rel_type": "m.replace", "event_id": "$orig1"}
        asyncio.run(bot.on_message(room, ev))
    finally:
        state._spawn = orig
        bot._context[rid].clear()
    assert not spawned                          # 编辑事件不派活
    assert len(bot._context[rid]) == 0          # 也不进上下文/逐字记录

# ---------- 6) TTL 过期提示（claude_runner） ----------
def test_ttl_notice():
    async def run():
        r = claude_runner.ClaudeRunner()

        async def fake_run(cmd, cwd=None, on_proc=None):
            return 0, json.dumps({"result": "ok", "session_id": "s",
                                  "is_error": False}).encode(), b""

        r._run = fake_run
        r._sessions["k"] = ("old", time.time() - 10 ** 9)     # 注入早已过期的会话
        return await r.ask("k", "hi"), await r.ask("k", "hi2")

    o1, o2 = asyncio.run(run())
    assert o1.startswith("（距上次较久")                       # 过期首条提示
    assert not o2.startswith("（距上次")                       # 紧接着不再提示

# ---------- 7) 安全：token 只注入受信主机 + 出口 redact ----------
def test_security_bits():
    orig_host, orig_tok = settings.gitea_host, settings.gitea_token
    settings.gitea_host, settings.gitea_token = "https://gitea.example.com", "secrettok123"
    try:
        assert "secrettok123@gitea.example.com" in projects._auth_url(
            "https://gitea.example.com/o/r.git")               # 受信主机 → 注入 token
        assert "secrettok123" not in projects._auth_url(
            "https://evil.com/o/r.git")                        # 第三方 → 绝不注入
        assert projects.parse_repo_url("看 https://evil.com/a/b") is None      # 非受信主机不当仓库
        assert projects.parse_repo_url("https://gitea.example.com/team/app")["repo"] == "app"
        assert projects.parse_repo_url(                        # 默认端口(:443/https)视为同一主机
            "https://gitea.example.com:443/team/app")["repo"] == "app"
        assert projects.parse_repo_url(                        # userinfo 诡计：host 实为 evil → 拒绝
            "https://gitea.example.com@evil.com/o/r") is None
        assert projects.parse_repo_url(                        # 反向 userinfo：netloc 带 @ 一律可疑 → 拒绝
            "https://evil.com@gitea.example.com/o/r") is None
        assert projects.parse_repo_url(                        # 路径穿越的 owner/repo 一律拒绝
            "https://gitea.example.com/../../etc") is None
        assert projects.parse_repo_url(                        # Gitea 保留路由不当 repo
            "https://gitea.example.com/explore/repos") is None
        assert projects.parse_repo_url(                        # PR 链接仍能路由到 owner/repo
            "看 https://gitea.example.com/team/app/pulls/7")["repo"] == "app"
        assert projects.parse_repo_url(                        # 前面先出现不相关链接也别漏掉受信仓库
            "参考 https://github.com/foo/bar 然后改 https://gitea.example.com/team/app")["repo"] == "app"
        assert redact("tok=secrettok123") == "tok=***"          # 外发出口抹掉凭证

        # scheme 降级：配的是 https，同 host 的 http 变体不该被当成同一受信主机（否则 token 明文上路）
        assert projects.parse_repo_url("http://gitea.example.com/team/app") is None
        assert "secrettok123" not in projects._auth_url("http://gitea.example.com/o/r.git")

        settings.gitea_host = ""                                # fail-closed：没配受信主机就不认任何仓库
        assert projects.parse_repo_url("https://gitea.example.com/team/app") is None

        import config                                           # 运行期登记的 Matrix token 也要被 redact
        config.register_secret("matrixtok999")
        assert redact("token=matrixtok999 done") == "token=*** done"
    finally:
        settings.gitea_host, settings.gitea_token = orig_host, orig_tok

# ---------- 8) 无访问控制：谁邀请都进房、只有真正的 invite 才触发 join ----------
def test_no_access_control_invite_joins():
    set_identity()
    joined = []

    class FC:
        rooms = {}                       # 欢迎语后台任务会探 client.rooms，给个空的即可
        async def join(self, rid):
            joined.append(rid)

    orig_client = state.client
    state.client = FC()
    try:
        mk_inv = lambda s, m="invite": types.SimpleNamespace(
            state_key="@claudebot:ex.org", membership=m, sender=s)
        room = FakeRoom("!r:ex.org", 2)
        asyncio.run(bot.on_invite(room, mk_inv("@eve:ex.org")))     # 陌生人邀请也加入
        asyncio.run(bot.on_invite(room, mk_inv("@alice:ex.org")))
        assert joined == ["!r:ex.org", "!r:ex.org"]
        asyncio.run(bot.on_invite(room, mk_inv("@x:ex.org", "join")))  # 非 invite 成员事件不触发
        assert len(joined) == 2
    finally:
        state.client = orig_client

# ---------- 8a2) 孤儿房间：人走光只剩自己 → 退房+forget ----------
def test_leave_when_alone():
    set_identity()
    left, forgot = [], []

    class FC:
        rooms = {}
        async def room_leave(self, rid):
            left.append(rid)
            self.rooms.pop(rid, None)
        async def room_forget(self, rid):
            forgot.append(rid)

    orig_client, orig_synced = state.client, state._synced
    state.client, state._synced = FC(), True
    try:
        alone = FakeRoom("!alone:ex.org", 0)
        alone.users = {"@claudebot:ex.org": 1}
        both = FakeRoom("!both:ex.org", 0)
        both.users = {"@claudebot:ex.org": 1, "@alice:ex.org": 1}
        FC.rooms = {"!alone:ex.org": alone, "!both:ex.org": both}
        mk = lambda sk, m: types.SimpleNamespace(state_key=sk, membership=m)

        asyncio.run(bot.on_member(both, mk("@bob:ex.org", "leave")))    # 还有人在 → 不退
        assert left == []
        asyncio.run(bot.on_member(alone, mk("@claudebot:ex.org", "leave")))  # 自己的成员事件不触发
        asyncio.run(bot.on_member(alone, mk("@alice:ex.org", "join")))       # 进房类事件不触发
        assert left == []
        state._synced = False                                           # 初始同步期间不动手
        asyncio.run(bot.on_member(alone, mk("@alice:ex.org", "leave")))
        assert left == []
        state._synced = True
        asyncio.run(bot.on_member(alone, mk("@alice:ex.org", "leave")))  # 只剩自己 → 退房+forget
        assert left == ["!alone:ex.org"] and forgot == ["!alone:ex.org"]
        assert asyncio.run(bot._leave_if_alone("!alone:ex.org")) is False  # 已退的房间再查不误报
    finally:
        state.client, state._synced = orig_client, orig_synced

# ---------- 8a3) 退房后清尾巴：绑定/路由被清、在跑任务被取消、聊天记录/媒体被删 ----------
def test_leave_cleans_up_room():
    import tempfile
    import transcript
    set_identity()
    rid = "!dead:ex.org"
    cancelled = []

    class FC:
        rooms = {}
        async def room_leave(self, r):
            self.rooms.pop(r, None)
        async def room_forget(self, r):
            pass

    room = FakeRoom(rid, 0)
    room.users = {"@claudebot:ex.org": 1}          # 只剩自己 → 触发退房

    tmp_media = tempfile.mkdtemp()
    mdir = os.path.join(tmp_media, fmt._safe_name(rid, "room"))
    os.makedirs(mdir)
    open(os.path.join(mdir, "f.bin"), "w").close()
    bpath = os.path.join(tempfile.mkdtemp(), "bindings.json")

    orig = (state.client, state._synced, settings.media_root,
            bot.projects.bindings_path, bot.runner.cancel)
    fc = FC(); fc.rooms = {rid: room}
    state.client, state._synced = fc, True
    settings.media_root = tmp_media
    bot.projects.bindings_path = bpath
    bot.runner.cancel = lambda k: (cancelled.append(k) or 1)
    bot.projects._rooms[rid] = "h/o/app"           # 该房间绑着某项目
    bot._last_project_by_room[rid] = "h/o/app"     # 且有路由记忆
    os.makedirs(transcript._root(), exist_ok=True)
    open(transcript.path_for(rid), "w").close()    # 造一份逐字记录
    transcript.mark_backfilled(rid)                # 和回灌标记
    try:
        left = asyncio.run(bot._leave_if_alone(rid))
        still_bound = rid in bot.projects._rooms
        still_routed = rid in bot._last_project_by_room
        tr_gone = not os.path.exists(transcript.path_for(rid)) and not transcript.is_backfilled(rid)
        media_gone = not os.path.exists(mdir)
    finally:
        (state.client, state._synced, settings.media_root,
         bot.projects.bindings_path, bot.runner.cancel) = orig
        bot.projects._rooms.pop(rid, None)
        bot._last_project_by_room.pop(rid, None)
    assert left is True
    assert cancelled == [rid]        # 在跑任务按房间取消（复用 /cancel 路径）
    assert not still_bound           # 绑定被清并落盘
    assert not still_routed          # 路由记忆被清
    assert tr_gone                   # 逐字记录 + 回灌标记被删
    assert media_gone                # 媒体目录被删

# ---------- 8a4) 加密消息解不开：要密钥 + 明文提示，同房限流、积压期不提示 ----------
def test_undecryptable_notifies_and_rate_limits():
    from nio import LocalProtocolError
    set_identity()
    rid = "!enc:ex.org"
    room = FakeRoom(rid, 2)
    sent, key_reqs = [], []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"])
            return types.SimpleNamespace(event_id="$x%d" % len(sent))

        async def request_room_key(self, event, tx_id=None):
            key_reqs.append(event)
            return types.SimpleNamespace()          # 成功响应即可

    ev = types.SimpleNamespace(session_id="sess1", room_id=rid)
    orig = (state.client, state._synced, settings.process_backlog)
    state.client, settings.process_backlog = FC(), False
    bot._last_undecrypt_notice.pop(rid, None)
    try:
        state._synced = False                       # 初始同步期间：积压解密失败不动手
        asyncio.run(bot.on_undecrypted(room, ev))
        assert sent == [] and key_reqs == []        # 既不要密钥也不提示

        state._synced = True
        asyncio.run(bot.on_undecrypted(room, ev))   # 首条解不开 → 要密钥 + 明文提示
        assert len(key_reqs) == 1
        assert len(sent) == 1 and "解不开" in sent[0] and "密钥" in sent[0]

        asyncio.run(bot.on_undecrypted(room, ev))   # 限流窗口内第二条：仍试要密钥，但不再提示
        assert len(sent) == 1                       # 提示没被刷屏
        assert len(key_reqs) == 2                   # 密钥请求每条都试（nio 自身按 session 去重）

        # 补救抛 LocalProtocolError（同 session 已在要密钥）要被接住，且不拦住提示
        bot._last_undecrypt_notice.pop(rid, None)   # 放开限流，验证提示仍发得出

        class FC2(FC):
            async def request_room_key(self, event, tx_id=None):
                raise LocalProtocolError("already requested")

        state.client = FC2()
        asyncio.run(bot.on_undecrypted(room, ev))
        assert len(sent) == 2                       # 抛错被吞，提示照发
    finally:
        (state.client, state._synced, settings.process_backlog) = orig
        bot._last_undecrypt_notice.pop(rid, None)


# ---------- 「@了 谁」附注：@pill 纯文本里只剩显示名，靠元数据补出点名对象 ----------
def test_mention_note():
    set_identity()
    room = FakeRoom("!g:ex.org", 3)
    note = addressing._mention_note(
        room, {"m.mentions": {"user_ids": ["@claudebot:ex.org", "@alice:ex.org"]}})
    assert note == "〔@了 Alice〕"                                     # bot 自己不进附注，MXID 解析成显示名
    assert addressing._mention_note(
        room, {"m.mentions": {"user_ids": ["@claudebot:ex.org"]}}) == ""   # 只 @了 bot → 无附注
    assert addressing._mention_note(room, {}) == ""                    # 没 @ 人 → 空串
    assert addressing._mention_note(
        room, {"m.mentions": {"user_ids": ["@bob:ex.org"]}}) == "〔@了 @bob:ex.org〕"  # 查不到显示名退回 MXID

    # 老客户端只发富文本 pill：引用块里的 pill 不算，正文里的算；百分号转义的 MXID 也能解
    fb = ('<mx-reply><blockquote><a href="https://matrix.to/#/@bob:ex.org">Bob</a> 旧话'
          '</blockquote></mx-reply>问下 <a href="https://matrix.to/#/%40alice%3Aex.org">Alice</a> 的进度')
    assert addressing._mention_note(room, {"formatted_body": fb}) == "〔@了 Alice〕"
    # 房间/事件链接不是人，不进附注；m.mentions 与 pill 同指一人不重复
    assert addressing._mention_note(
        room, {"formatted_body": '<a href="https://matrix.to/#/%23room%3Aex.org">房间</a>'}) == ""
    assert addressing._mention_note(
        room, {"m.mentions": {"user_ids": ["@alice:ex.org"]},
               "formatted_body": '<a href="https://matrix.to/#/@alice:ex.org">Alice</a>'}) == "〔@了 Alice〕"

    # 畸形内容（event content 发送方任意可控）：不崩、当没有。回归点：user_ids 里混非字符串
    # 条目曾让 `uid in seen` / join 抛 TypeError，异常穿透 sync 循环，一条恶意消息打死整个 bot。
    for bad in ({"m.mentions": None}, {"m.mentions": "x"}, {"m.mentions": {"user_ids": 5}},
                {"m.mentions": {"user_ids": "abc"}}, {"m.mentions": {"user_ids": None}},
                {"formatted_body": {"a": 1}}, {"formatted_body": 7}):
        assert addressing._mention_note(room, bad) == ""
    assert addressing._mention_note(
        room, {"m.mentions": {"user_ids": [123, {"x": 1}, None, "", ["y"], "@alice:ex.org"]}}
    ) == "〔@了 Alice〕"                                                # 垃圾条目剔掉，合法的照常


# ---------- KNOWN_BOTS：名单匹配（完整 MXID / localpart 简写 / 空名单） ----------
def test_known_bot_matching():
    orig = (settings.known_bots_full, settings.known_bots_local)
    try:
        settings.known_bots_full = frozenset(("@weather:ex.org",))
        settings.known_bots_local = frozenset(("rss",))
        assert addressing._is_known_bot("@weather:ex.org")           # 完整 MXID 精确匹配
        assert not addressing._is_known_bot("@weather:other.org")    # 完整 MXID 不跨 homeserver
        assert addressing._is_known_bot("@rss:ex.org")               # localpart 简写不限 homeserver
        assert addressing._is_known_bot("@rss:another.org")
        assert not addressing._is_known_bot("@rss2:ex.org")          # localpart 全等，不是前缀匹配
        assert not addressing._is_known_bot("")                      # 空 sender 不误伤
        settings.known_bots_full = frozenset()
        settings.known_bots_local = frozenset()
        assert not addressing._is_known_bot("@weather:ex.org")       # 空名单=没有已知机器人
    finally:
        settings.known_bots_full, settings.known_bots_local = orig

# ---------- KNOWN_BOTS：名单 bot 全静默——消息进上下文，但 DM 必回/元命令都不应答 ----------
def test_known_bot_silent_but_in_context():
    set_identity()
    state._synced = True
    rid = "!dmbot:ex.org"
    room = FakeRoom(rid, 2)                      # DM → 人发的话必回，静默只可能来自名单闸
    bot._context[rid].clear()
    spawned = []
    orig_spawn = state._spawn
    orig_bots = (settings.known_bots_full, settings.known_bots_local)
    settings.known_bots_full = frozenset(("@weather:ex.org",))
    settings.known_bots_local = frozenset()
    state._spawn = lambda coro: (spawned.append(1), coro.close())
    try:
        asyncio.run(bot.on_message(room, make_event(
            "现在气温 3℃", sender="@weather:ex.org", event_id="$kb1")))
        asyncio.run(bot.on_message(room, make_event(
            "/status", sender="@weather:ex.org", event_id="$kb2")))   # 元命令也不认
        assert not spawned                                            # DM 里 bot 说啥都不应答
        assert len(bot._context[rid]) == 2                            # 但消息照常进上下文
        asyncio.run(bot.on_message(room, make_event(
            "帮我看下这个报错", sender="@alice:ex.org", event_id="$kb3")))
        assert len(spawned) == 1                                      # 真人照常派活，名单不误伤
    finally:
        state._spawn = orig_spawn
        settings.known_bots_full, settings.known_bots_local = orig_bots
        bot._context[rid].clear()


TESTS = [
    ('认 reply / 点名', test_reply_addressing),
    ('引用回退块剥离', test_reply_fallback_strip),
    ('track 门控 + 时间单调', test_track_and_monotonic),
    ('自己账号入上下文不派活', test_own_account_context),
    ('编辑消息 m.replace 不重派活', test_edit_event_ignored),
    ('TTL 过期提示', test_ttl_notice),
    ('token 受信主机 + redact', test_security_bits),
    ('无访问控制：谁邀请都进房', test_no_access_control_invite_joins),
    ('孤儿房间 人走光自动退', test_leave_when_alone),
    ('退房清尾巴 绑定/路由/任务/记录', test_leave_cleans_up_room),
    ('加密解不开 要密钥+提示+限流', test_undecryptable_notifies_and_rate_limits),
    ('「@了 谁」附注 解析/排己/去重', test_mention_note),
    ('KNOWN_BOTS 名单匹配', test_known_bot_matching),
    ('KNOWN_BOTS 全静默但进上下文', test_known_bot_silent_but_in_context),
]
