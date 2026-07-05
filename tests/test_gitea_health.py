"""冒烟：Gitea 健康度埋点/状态告警/status 暴露连通性"""
from _helpers import (
    FakeRoom, _reset_gitea_health, _reset_ledger, asyncio, bot, set_identity, settings, state, time, types)

# ---------- Gitea 健康度埋点：失败累计/成功清零、401 定性 token、404 不计入失败、网络/5xx 区分 ----------
def test_gitea_health_accounting():
    import gitea
    import urllib.error
    _reset_gitea_health()
    mode = {"v": "ok"}

    def fake_get(url):
        m = mode["v"]
        if m == "ok":
            return 200, {}
        if m == "net":
            raise urllib.error.URLError("refused")           # 连不上
        codes = {"notfound": 404, "auth": 401, "forbidden": 403, "server": 502}
        raise urllib.error.HTTPError(url, codes[m], m, None, None)

    orig = gitea._get
    gitea._get = fake_get
    try:
        # 401 连续失败：累计 + 定性 auth（token 问题）+ ok=False
        for _ in range(3):
            mode["v"] = "auth"; asyncio.run(gitea._aget("u"))
        h = gitea.health()
        assert h["consecutive_failures"] == 3 and h["last_kind"] == "auth" and h["last_code"] == 401
        assert h["ok"] is False

        # 403 同样定性成 token 问题
        mode["v"] = "forbidden"; asyncio.run(gitea._aget("u"))
        assert gitea.health()["last_kind"] == "auth" and gitea.health()["last_code"] == 403

        # 一次成功（2xx）→ 清零、记 last_success、ok 恢复
        mode["v"] = "ok"; st, _ = asyncio.run(gitea._aget("u"))
        h = gitea.health()
        assert st == 200 and h["consecutive_failures"] == 0 and h["ok"] is True and h["last_success_ts"] > 0

        # 404 是"对象不存在"的业务答案：不计入失败，反而算"活着"→ 清零
        mode["v"] = "auth"; asyncio.run(gitea._aget("u"))
        assert gitea.health()["consecutive_failures"] == 1
        mode["v"] = "notfound"; st, d = asyncio.run(gitea._aget("u"))
        assert st == 404 and d is None and gitea.health()["consecutive_failures"] == 0

        # 网络层错误 → kind=network、code=0（连不上，与 token 失效区分）
        mode["v"] = "net"; st, _ = asyncio.run(gitea._aget("u"))
        h = gitea.health()
        assert st == 0 and h["last_kind"] == "network" and h["last_code"] == 0 and h["consecutive_failures"] == 1

        # 5xx → kind=http（连上了但服务器不正常），与网络/鉴权都区分
        mode["v"] = "server"; st, _ = asyncio.run(gitea._aget("u"))
        assert st == 502 and gitea.health()["last_kind"] == "http" and gitea.health()["last_code"] == 502
        assert gitea.health()["consecutive_failures"] == 2   # 网络那笔 + 这笔，连续累计
    finally:
        gitea._get = orig
        _reset_gitea_health()

# ---------- Gitea 健康度：/status 两种形态 + 跨阈值告警只发一次 + 恢复通知 ----------
def test_gitea_health_status_and_alert():
    import gitea
    import gitea_health
    set_identity()
    _reset_gitea_health()
    orig_host = settings.gitea_host
    settings.gitea_host = "https://gitea.example.com"
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    # 两个绑了项目的房间收告警（验证"各发一条"）
    orig_rooms = dict(bot.projects._rooms)
    orig_routed = dict(state._last_project_by_room)
    orig_client = state.client
    bot.projects._rooms.clear(); state._last_project_by_room.clear()
    bot.projects._rooms["!ga:ex.org"] = "h/o/r"
    bot.projects._rooms["!gb:ex.org"] = "h/o/r2"
    state.client = FC()
    try:
        # 形态一：健康 → "正常"
        assert gitea_health.status_line(gitea.health()) == "• Gitea：正常"

        # 未跨阈值（4 次）→ 不告警
        for _ in range(4):
            gitea._note_failure(401, "auth")
        asyncio.run(gitea_health.check_and_alert())
        assert not any("连不上" in m for m in sent)     # 4 次还不够阈值，忍住不吵

        # 形态二：连续失败 → 点名 token + 最近成功多久前
        gitea._health["last_success_ts"] = time.time() - 600
        gitea._note_failure(401, "auth")                 # 第 5 次，跨阈值
        line = gitea_health.status_line(gitea.health())
        assert "连续 5 次失败" in line and "token" in line and "前" in line

        # 跨阈值 → 两个房间各告警一次
        asyncio.run(gitea_health.check_and_alert())
        assert sum("Gitea 连不上" in m for m in sent) == 2
        # 仍在失败：再巡检一轮不重复刷屏
        gitea._note_failure(0, "network")
        asyncio.run(gitea_health.check_and_alert())
        assert sum("Gitea 连不上" in m for m in sent) == 2

        # 恢复 → 两个房间各发一条"已恢复"，且只发一次
        gitea._note_alive()
        assert gitea_health.status_line(gitea.health()) == "• Gitea：正常"
        asyncio.run(gitea_health.check_and_alert())
        assert sum("已恢复" in m for m in sent) == 2
        asyncio.run(gitea_health.check_and_alert())
        assert sum("已恢复" in m for m in sent) == 2
    finally:
        settings.gitea_host = orig_host
        bot.projects._rooms.clear(); bot.projects._rooms.update(orig_rooms)
        state._last_project_by_room.clear(); state._last_project_by_room.update(orig_routed)
        state.client = orig_client
        _reset_gitea_health()

# ---------- Gitea 健康度：/status 命令确实带上这条（异常态） ----------
def test_status_shows_gitea_health():
    import tempfile
    import gitea
    set_identity()
    _reset_gitea_health()
    orig_store, orig_host = settings.store_path, settings.gitea_host
    settings.store_path = tempfile.mkdtemp()
    settings.gitea_host = "https://gitea.example.com"
    _reset_ledger()
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "host": "http://h", "path": "/x", "base": "main"}
    sent = []

    class FC:
        async def room_send(self, r, mt, content, **k):
            sent.append(content["body"]); return types.SimpleNamespace(event_id="$x")

    orig = (bot.projects.get_room, state.client)
    bot.projects.get_room = lambda rid: rec
    state.client = FC()
    try:
        gitea._health["last_success_ts"] = time.time() - 600
        for _ in range(5):
            gitea._note_failure(401, "auth")
        asyncio.run(bot.handle_status(FakeRoom("!g:ex.org", 3)))
        out = "\n".join(sent)
        assert "Gitea" in out and "连续 5 次失败" in out and "token" in out   # /status 暴露连通性
    finally:
        bot.projects.get_room, state.client = orig
        settings.store_path, settings.gitea_host = orig_store, orig_host
        _reset_ledger()
        _reset_gitea_health()


# ---------- create_repo：新建仓库成功解析 / 失败带上原因（HTTP 错误 / 网络错误）----------
def test_create_repo_success_and_failure():
    import gitea
    import json
    import urllib.error
    orig_post = gitea._post
    orig_host = settings.gitea_host
    settings.gitea_host = "https://gitea.example.com"
    try:
        gitea._post = lambda url, payload: (201, json.dumps(
            {"html_url": "https://gitea.example.com/bot/foo", "clone_url": "https://gitea.example.com/bot/foo.git"}))
        data, err = asyncio.run(gitea.create_repo("foo", private=True))
        assert err == "" and data["html_url"] == "https://gitea.example.com/bot/foo"

        def raise_conflict(url, payload):
            raise urllib.error.HTTPError(url, 409, "name already exists", None, None)
        gitea._post = raise_conflict
        data, err = asyncio.run(gitea.create_repo("foo"))
        assert data is None and "409" in err

        def raise_net(url, payload):
            raise urllib.error.URLError("refused")
        gitea._post = raise_net
        data, err = asyncio.run(gitea.create_repo("foo"))
        assert data is None and err
    finally:
        gitea._post = orig_post
        settings.gitea_host = orig_host


TESTS = [
    ('Gitea健康度 失败累计/成功清零/401定性/404不计', test_gitea_health_accounting),
    ('Gitea健康度 status两态+告警一次+恢复', test_gitea_health_status_and_alert),
    ('/status 暴露Gitea连通性', test_status_shows_gitea_health),
    ('create_repo 成功解析/HTTP失败/网络失败', test_create_repo_success_and_failure),
]
