"""在途登记簿：跑 / 排队中的任务落一条到 store/inflight.json，重启后据此对账。

在跑 / 排队的活只存在于进程内存，重启即蒸发，用户端零提示——流式占位（「⏳ 正在干活…」）
永远挂着没人收尾；排队回执（「已排队…」）发出后队列（等锁的 waiter）没了，用户无限等；
工单执行到一半崩了，issue_ledger 挡着重接、_sweep_closed 只清已关闭的，该单永不被做也不报告。

这里记一笔「我正在做什么、占位消息是哪条、是聊天任务还是工单」，重启对账（bot.main）据此：
聊天任务无法自动续（prompt/上下文已丢）→ 有占位就编辑成中断提示、没占位就补发一条催重发；
工单能自动重派（issue 仍在 Gitea 上）→ 交给 issue_ledger 那条路，这里不催重发。

任务进入执行 / 排队即登记，结束（成功 / 取消 / 报错）即摘除——登记 / 摘除必须覆盖所有退出路径
（放 finally）。占位 eid 在占位创建后再补录（attach_eid）。与 pr_ledger / issue_ledger 同构：
进程内单线程访问（asyncio），改完即原子落盘。
"""
import json
import logging
import os
import time
import uuid

from config import settings
from storage import atomic_write_json

log = logging.getLogger("matrix-claude.inflight")

KIND_CHAT = "chat"     # 聊天任务（群 @ / 私聊派活）
KIND_ISSUE = "issue"   # Gitea 工单接活

_data: dict[str, dict] = {}
_loaded = False


def _path() -> str:
    return os.path.join(settings.store_path, "inflight.json")


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
        log.warning("在途登记簿落盘失败: %s", e)


def _ensure() -> None:
    if not _loaded:
        _load()


def record(room: str, summary: str, kind: str, issue: int = 0) -> str:
    """登记一条在途任务，返回其 key（供后续补录占位 eid / 摘除）。
    summary 截断，别把整段任务正文塞进登记簿。"""
    _ensure()
    key = uuid.uuid4().hex
    _data[key] = {"room": room, "summary": (summary or "")[:200], "eid": "",
                  "kind": kind, "issue": issue, "ts": time.time()}
    _save()
    return key


def attach_eid(key: str, eid: str) -> None:
    """占位消息创建后补录它的 event_id——对账时据此把占位编辑成中断提示。"""
    _ensure()
    if key and eid and key in _data and _data[key].get("eid") != eid:
        _data[key]["eid"] = eid
        _save()


def remove(key: str) -> None:
    """任务结束（成功 / 取消 / 报错）摘除。"""
    _ensure()
    if key and _data.pop(key, None) is not None:
        _save()


def active() -> list[dict]:
    """在册（未摘除）的全部在途任务。"""
    _ensure()
    return list(_data.values())


def clear() -> None:
    """启动对账处理完后清空整簿（残留条目都对过账了）。"""
    global _data
    _ensure()
    if _data:
        _data = {}
        _save()
