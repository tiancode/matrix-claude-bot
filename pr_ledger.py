"""PR 台账：bot 开过的 PR 记在册上，跟到合并 / 关闭才销账——"对结果负责"的状态底座。

每条记 PR 来自哪个项目、哪个房间（好把后续进展回报到原处）、已处理到第几条评审、
自动改了几次（设上限防 CI 反复失败时无限重跑）。持久化到 store/pr_ledger.json，重启不丢账。
进程内单线程访问（asyncio），改完即原子落盘。
"""
import json
import logging
import os
import time

from config import settings
from storage import atomic_write_json

log = logging.getLogger("matrix-claude.pr_ledger")

_data: dict[str, dict] = {}
_loaded = False


def _path() -> str:
    return os.path.join(settings.store_path, "pr_ledger.json")


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
        log.warning("PR 台账落盘失败: %s", e)


def _ensure() -> None:
    if not _loaded:
        _load()


def record(pid: str, number: int, url: str, room: str) -> bool:
    """登记一条新开的 PR；已在册返回 False（不重复记），新登记返回 True。"""
    _ensure()
    k = _key(pid, number)
    if k in _data:
        return False
    _data[k] = {"pid": pid, "number": number, "url": url, "room": room,
                "branch": "", "seen_review": 0, "review_fixes": 0, "ci_fixes": 0,
                "ci_seen": "", "created_ts": time.time(), "last_check_ts": 0.0}
    _save()
    return True


def active() -> list[dict]:
    """在册（未销账）的全部 PR。"""
    _ensure()
    return list(_data.values())


def update(pid: str, number: int, **fields) -> None:
    _ensure()
    k = _key(pid, number)
    if k in _data:
        _data[k].update(fields)
        _save()


def remove(pid: str, number: int) -> None:
    """销账（合并 / 关闭后）。"""
    _ensure()
    if _data.pop(_key(pid, number), None) is not None:
        _save()
