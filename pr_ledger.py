"""PR 台账：bot 开过的 PR 记在册上，跟到合并 / 关闭才销账——"对结果负责"的状态底座。

每条记 PR 来自哪个项目、哪个房间（好把后续进展回报到原处）、已处理到第几条评审、
自动改了几次（设上限防 CI 反复失败时无限重跑）。持久化到 store/pr_ledger.json，重启不丢账。
进程内单线程访问（asyncio），改完即原子落盘。共享的 load/save/active/update/remove 见 ledger.JsonLedger。
"""
import os
import time

from config import settings
from ledger import JsonLedger


def _path() -> str:
    return os.path.join(settings.store_path, "pr_ledger.json")


def _key(pid: str, number: int) -> str:
    return f"{pid}#{number}"


_led = JsonLedger(_path, "PR 台账", "matrix-claude.pr_ledger")


def record(pid: str, number: int, url: str, room: str) -> bool:
    """登记一条新开的 PR；已在册返回 False（不重复记），新登记返回 True。"""
    # gone_rounds: 连续查到 404 的轮数（成功一次即清零，≥3 轮才销账，防抖动误销）；
    # conflict_seen / merge_fail_seen: 冲突 / 自动合并失败告警的水位（记 head sha，同一版本只吭一次）。
    return _led.add(_key(pid, number), {
        "pid": pid, "number": number, "url": url, "room": room,
        "branch": "", "seen_review": 0, "review_fixes": 0, "ci_fixes": 0,
        "ci_seen": "", "gone_rounds": 0, "conflict_seen": "", "merge_fail_seen": "",
        "created_ts": time.time(), "last_check_ts": 0.0})


def active() -> list[dict]:
    """在册（未销账）的全部 PR。"""
    return _led.active()


def update(pid: str, number: int, **fields) -> None:
    _led.update(_key(pid, number), **fields)


def remove(pid: str, number: int) -> None:
    """销账（合并 / 关闭后）。"""
    _led.remove(_key(pid, number))
