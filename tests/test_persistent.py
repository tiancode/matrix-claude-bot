"""常驻进程模式（CLAUDE_PERSISTENT）：跨轮复用、后台自发产出投递、reset 收尾、死后重生。

用 tests/_stub_claude.py 假 CLI 走真子进程 + 真 stream-json 协议，不 stub runner 内部。
"""
import os
import stat
import tempfile

from _helpers import asyncio, claude_runner as cr, settings

_STUB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_stub_claude.py")


def _with_persistent(fn):
    """临时切到常驻模式 + 假 CLI 跑一个协程用例，退出时恢复设置并清掉残留进程。"""
    orig = (settings.claude_bin, settings.claude_persistent, settings.claude_extra_args)
    st = os.stat(_STUB)
    os.chmod(_STUB, st.st_mode | stat.S_IEXEC)
    settings.claude_bin, settings.claude_persistent = _STUB, True
    settings.claude_extra_args = None   # 别把真实部署的额外参数喂给假 CLI
    r = cr.ClaudeRunner()
    r._sessions = {}                    # 不吃落盘会话，也不往盘上写真数据
    r._save_sessions = lambda: None

    async def go():
        try:
            await fn(r)
        finally:
            for ps in list(r._persist.values()):   # 收尾残留常驻进程，别泄漏到后续用例
                cr._kill_group(ps.proc)
            await asyncio.sleep(0.1)
    try:
        asyncio.run(go())
    finally:
        settings.claude_bin, settings.claude_persistent, settings.claude_extra_args = orig


def test_persistent_reuse_across_turns():
    """两轮 ask 复用同一个常驻进程；上下文进程内延续；session_id 照常记账。"""
    async def go(r):
        d = tempfile.mkdtemp()
        a1 = await r.ask("k1", "hello", cwd=d)
        ps1 = r._persist.get("k1")
        assert ps1 is not None and ps1.alive(), "首轮后常驻进程应存活"
        assert "ok:" in a1 and "hello" in a1
        a2 = await r.ask("k1", "again", cwd=d)
        ps2 = r._persist.get("k1")
        assert ps1 is ps2, "第二轮应复用同一常驻进程而非新起"
        assert "again" in a2
        assert r._sessions.get("k1", (None,))[0] == "S1", "session_id 应从事件流记账"
    _with_persistent(go)


def test_persistent_spontaneous_notify():
    """回合结束后进程自发再产出（模拟后台任务完成续跑）→ 经 on_notify 投递。"""
    async def go(r):
        d = tempfile.mkdtemp()
        got = []

        async def notify(text):
            got.append(text)

        a = await r.ask("k2", "do SPONT thing", cwd=d, on_notify=notify)
        assert "SPONT" in a          # 回合本身正常返回
        for _ in range(60):          # 自发产出在 0.2s 后到
            if got:
                break
            await asyncio.sleep(0.05)
        assert got and "bg-done" in got[0], f"自发产出没投递到：{got}"
    _with_persistent(go)


def test_persistent_reset_kills_process():
    """/reset 语义：会话重置时常驻进程一并杀掉、从表中摘除。"""
    async def go(r):
        d = tempfile.mkdtemp()
        await r.ask("k3", "x", cwd=d)
        ps = r._persist.get("k3")
        assert ps is not None and ps.alive()
        r.reset("k3")
        assert "k3" not in r._persist, "reset 后不应残留登记"
        for _ in range(40):
            if ps.proc.returncode is not None:
                break
            await asyncio.sleep(0.05)
        assert ps.proc.returncode is not None, "reset 后常驻进程应已被杀"
    _with_persistent(go)


def test_persistent_respawn_after_death():
    """进程死掉（DIE 模拟崩溃）后：登记被读取循环摘除，下一轮凭落盘 sid --resume 重生。"""
    async def go(r):
        d = tempfile.mkdtemp()
        a1 = await r.ask("k4", "please DIE now", cwd=d)
        assert "sid=S1" in a1
        for _ in range(60):          # 等 reader 发现 EOF 并摘除登记
            if "k4" not in r._persist:
                break
            await asyncio.sleep(0.05)
        assert "k4" not in r._persist, "死进程应被自动摘除"
        a2 = await r.ask("k4", "hello again", cwd=d)
        assert "sid=S1-r" in a2, f"第二轮应带 --resume S1 重生（拿到 S1-r），实际：{a2}"
    _with_persistent(go)


# ---------- steering：回合进行中把追加消息写进 stdin（像 Claude Code 运行中打字） ----------
def test_try_steer_injects_only_mid_turn():
    import json as _json
    import types as _types
    from _helpers import set_identity
    set_identity()
    r = cr.ClaudeRunner()
    written = []

    class FS:   # 假 stdin：只记写入
        def write(self, b):
            written.append(b)
        async def drain(self):
            pass

    ps = cr._Persist("k|r", _types.SimpleNamespace(returncode=None, stdin=FS()), "/tmp")
    r._persist["k|r"] = ps
    orig = settings.claude_persistent
    settings.claude_persistent = True
    try:
        assert asyncio.run(r.try_steer("k|r", "x")) is False        # 回合外不注入（会被当下一任务）
        ps.turn = {"fut": None}
        assert asyncio.run(r.try_steer("k|r", "追加一句")) is True  # 回合中注入
        obj = _json.loads(written[0].decode())
        assert obj["type"] == "user"
        assert obj["message"]["content"][0]["text"] == "追加一句"
        assert asyncio.run(r.try_steer("nokey|r", "x")) is False    # 没有常驻进程的 key 不注入
        settings.claude_persistent = False
        assert asyncio.run(r.try_steer("k|r", "x")) is False        # 非常驻模式整体关闭
    finally:
        settings.claude_persistent = orig


def test_handle_task_steers_when_turn_running():
    """回合进行中来的新点名：不排队开新回合，递进当前回合 + 📎 回执 + 标 dispatched；
    steering 失败（回合恰好结束/进程刚死）回落正常派活。"""
    from _helpers import (FakeRoom, _CapClient, bot, make_event, set_identity,
                          state, time, _task_fixtures)
    set_identity()
    c = _CapClient(); state.client = c
    _task_fixtures()
    steered, asked = {}, {"n": 0}

    async def fake_steer(key, text):
        steered["key"], steered["text"] = key, text
        return True

    async def fake_ask(*a, **k):
        asked["n"] += 1
        return "搞定"

    orig = (bot.runner.try_steer, bot.runner.ask,
            settings.stream_replies, settings.reply_in_thread, settings.steer_enabled)
    bot.runner.try_steer, bot.runner.ask = fake_steer, fake_ask
    settings.stream_replies = False; settings.reply_in_thread = False
    settings.steer_enabled = True
    room = FakeRoom("!steer:ex.org", 3)
    rid = room.room_id
    try:
        bot._context[rid].clear()
        bot._context[rid].append((time.time(), "Alice", "@claude-bot 顺便加个重试"))
        asyncio.run(bot.handle_task(
            room, make_event("@claude-bot 顺便加个重试", mentions=[state.MY_ID], event_id="$S1"),
            "顺便加个重试", skip_body="@claude-bot 顺便加个重试"))
        assert asked["n"] == 0                                   # 没有排队开新回合
        assert steered["key"] == f"h/o/r|{rid}"                  # 递进的是本房间会话的回合
        assert "顺便加个重试" in steered["text"] and "追加" in steered["text"]
        reacts = [m for m in c.sent
                  if (m.get("m.relates_to") or {}).get("rel_type") == "m.annotation"]
        clips = [m for m in reacts if m["m.relates_to"]["key"] == "📎"]
        assert clips and clips[0]["m.relates_to"]["event_id"] == "$S1"   # 📎 打在触发消息上
        assert any(state._ctx_dispatched(it) for it in bot._context[rid])  # 已进会话 → 标 dispatched

        async def no_steer(key, text):
            return False
        bot.runner.try_steer = no_steer
        bot._context[rid].append((time.time(), "Alice", "@claude-bot 再来一个"))
        asyncio.run(bot.handle_task(
            room, make_event("@claude-bot 再来一个", mentions=[state.MY_ID], event_id="$S2"),
            "再来一个", skip_body="@claude-bot 再来一个"))
        assert asked["n"] == 1                                   # 回落到 runner.ask 正常派活
    finally:
        (bot.runner.try_steer, bot.runner.ask,
         settings.stream_replies, settings.reply_in_thread, settings.steer_enabled) = orig
        bot._context[rid].clear()


TESTS = [
    ('常驻进程 跨轮复用同一进程', test_persistent_reuse_across_turns),
    ('常驻进程 后台自发产出经 on_notify 投递', test_persistent_spontaneous_notify),
    ('常驻进程 reset 杀进程并摘除', test_persistent_reset_kills_process),
    ('常驻进程 死后凭 sid 重生', test_persistent_respawn_after_death),
    ('steering 仅回合中注入 stdin', test_try_steer_injects_only_mid_turn),
    ('steering 点名递进当前回合+📎+标记', test_handle_task_steers_when_turn_running),
]
