"""工单台账：Gitea 上指派给 bot 且已接下的 issue 记在册上，issue 关闭才销账。

作用是"防重复接单"（重启 / 每轮轮询不重复开 PR）：接单即登记，记来自哪个项目、
回报到哪个房间、后来开的 PR 号。持久化到 store/issue_ledger.json，重启不丢账。
想对同一单重派：把 issue 关了再重开（关单销账后重新可接）。
进程内单线程访问（asyncio），改完即原子落盘。与 pr_ledger 同构。
"""
import json
import logging
import os
import time

from config import settings
from storage import atomic_write_json

log = logging.getLogger("matrix-claude.issue_ledger")

_data: dict[str, dict] = {}
_loaded = False


def _path() -> str:
    return os.path.join(settings.store_path, "issue_ledger.json")


def _key(pid: str, number: int) -> str:
    return f"{pid}#{number}"


def _load() -> None:
    global _data, _loaded
    _loaded = True
    try:
        with open(_path()) as f:
            d = json.load(f)
        if isinstance(d, dict):
            _data = {k: v for k, v in d.items() if isinstance(v, dict)}
    except (OSError, json.JSONDecodeError):
        _data = {}


def _save() -> None:
    try:
        atomic_write_json(_path(), _data)
    except OSError as e:
        log.warning("工单台账落盘失败: %s", e)


def _ensure() -> None:
    if not _loaded:
        _load()


def record(pid: str, number: int, url: str, room: str) -> bool:
    """登记一条接下的工单；已在册返回 False（不重复接单）。"""
    _ensure()
    k = _key(pid, number)
    if k in _data:
        return False
    _data[k] = {"pid": pid, "number": number, "url": url, "room": room,
                "pr": 0, "created_ts": time.time()}
    _save()
    return True


def taken(pid: str, number: int) -> bool:
    _ensure()
    return _key(pid, number) in _data


def update(pid: str, number: int, **fields) -> None:
    _ensure()
    k = _key(pid, number)
    if k in _data:
        _data[k].update(fields)
        _save()


def remove(pid: str, number: int) -> None:
    """销账（issue 关闭后）。"""
    _ensure()
    if _data.pop(_key(pid, number), None) is not None:
        _save()


def active() -> list[dict]:
    """在册（未销账）的全部工单。"""
    _ensure()
    return list(_data.values())
