"""工单台账：Gitea 上指派给 bot 且已接下的 issue 记在册上，issue 关闭才销账。

作用是"防重复接单"（重启 / 每轮轮询不重复开 PR）：接单即登记，记来自哪个项目、
回报到哪个房间、后来开的 PR 号。持久化到 store/issue_ledger.json，重启不丢账。
想对同一单重派：把 issue 关了再重开（关单销账后重新可接）。
进程内单线程访问（asyncio），改完即原子落盘。共享底座与 pr_ledger 同——见 ledger.JsonLedger。
"""
import os
import time

from config import settings
from ledger import JsonLedger


def _path() -> str:
    return os.path.join(settings.store_path, "issue_ledger.json")


def _key(pid: str, number: int) -> str:
    return f"{pid}#{number}"


_led = JsonLedger(_path, "工单台账", "matrix-claude.issue_ledger")


def record(pid: str, number: int, url: str, room: str) -> bool:
    """登记一条接下的工单；已在册返回 False（不重复接单）。"""
    # gone_rounds: 连续查到 404 的轮数（成功一次即清零，≥3 轮才销账，防抖动误销）。
    return _led.add(_key(pid, number), {
        "pid": pid, "number": number, "url": url, "room": room,
        "pr": 0, "gone_rounds": 0, "created_ts": time.time()})


def taken(pid: str, number: int) -> bool:
    return _led.has(_key(pid, number))


def update(pid: str, number: int, **fields) -> None:
    _led.update(_key(pid, number), **fields)


def remove(pid: str, number: int) -> None:
    """销账（issue 关闭后）。"""
    _led.remove(_key(pid, number))


def active() -> list[dict]:
    """在册（未销账）的全部工单。"""
    return _led.active()
