"""在途登记簿：跑 / 排队中的任务落一条到 store/inflight.json，重启后据此对账。

在跑 / 排队的活只存在于进程内存，重启即蒸发，用户端零提示——流式占位（「⏳ 正在干活…」）
永远挂着没人收尾；排队回执（「已排队…」）发出后队列（等锁的 waiter）没了，用户无限等；
工单执行到一半崩了，issue_ledger 挡着重接、_sweep_closed 只清已关闭的，该单永不被做也不报告。

这里记一笔「我正在做什么、占位消息是哪条、是聊天任务还是工单」，重启对账（bot.main）据此：
聊天任务无法自动续（prompt/上下文已丢）→ 有占位就编辑成中断提示、没占位就补发一条催重发；
工单能自动重派（issue 仍在 Gitea 上）→ 交给 issue_ledger 那条路，这里不催重发。

任务进入执行 / 排队即登记，结束（成功 / 取消 / 报错）即摘除——登记 / 摘除必须覆盖所有退出路径
（放 finally）。占位 eid 在占位创建后再补录（attach_eid）。与 pr_ledger / issue_ledger 同构：
进程内单线程访问（asyncio），改完即原子落盘。键用随机 uuid（一次一活，无天然主键）。
共享的 load/save/active/remove/clear 见 ledger.JsonLedger。
"""
import os
import time
import uuid

from config import settings
from ledger import JsonLedger

KIND_CHAT = "chat"     # 聊天任务（群 @ / 私聊派活）
KIND_ISSUE = "issue"   # Gitea 工单接活


def _path() -> str:
    return os.path.join(settings.store_path, "inflight.json")


_led = JsonLedger(_path, "在途登记簿", "matrix-claude.inflight")


def record(room: str, summary: str, kind: str, issue: int = 0) -> str:
    """登记一条在途任务，返回其 key（供后续补录占位 eid / 摘除）。
    summary 截断，别把整段任务正文塞进登记簿。"""
    key = uuid.uuid4().hex
    _led.add(key, {"room": room, "summary": (summary or "")[:200], "eid": "",
                   "kind": kind, "issue": issue, "ts": time.time()})
    return key


def attach_eid(key: str, eid: str) -> None:
    """占位消息创建后补录它的 event_id——对账时据此把占位编辑成中断提示。"""
    rec = _led.get(key) if key and eid else None
    if rec is not None and rec.get("eid") != eid:
        _led.update(key, eid=eid)


def remove(key: str) -> None:
    """任务结束（成功 / 取消 / 报错）摘除。"""
    _led.remove(key)


def active() -> list[dict]:
    """在册（未摘除）的全部在途任务。"""
    return _led.active()


def clear() -> None:
    """启动对账处理完后清空整簿（残留条目都对过账了）。"""
    _led.clear()
