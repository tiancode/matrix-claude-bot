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


TESTS = [
    ('常驻进程 跨轮复用同一进程', test_persistent_reuse_across_turns),
    ('常驻进程 后台自发产出经 on_notify 投递', test_persistent_spontaneous_notify),
    ('常驻进程 reset 杀进程并摘除', test_persistent_reset_kills_process),
    ('常驻进程 死后凭 sid 重生', test_persistent_respawn_after_death),
]
