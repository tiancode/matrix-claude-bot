"""Gitea 连通性主动告警：连续失败跨过阈值时向绑了项目的房间各发一条告警，恢复后再发一条。

埋点在 gitea._aget（见 gitea.py），这里只做"跨阈值 → 告警 / 恢复 → 通知"，用水位防重复刷屏
（同 pr_followup 里 conflict_seen 的手法）。搭 pr_followup / issue_intake 循环的节奏驱动，
不单起循环。告警发送本身失败只记日志，绝不反噬后台巡检。
"""
import logging
import time

from config import settings
from matrix_io import send
from projects import projects
from state import _last_project_by_room
from fmt import _human_gap
import gitea

log = logging.getLogger("matrix-claude.gitea-health")

_ALERT_THRESHOLD = 5     # 连续失败到这个数才首次告警（低于此当抖动，别一断网就吵）
_alerted = False         # 水位：本轮故障是否已告过警——防每 180s 刷屏；恢复后清零，下次故障能再报


def _alert_rooms() -> set[str]:
    """该向谁告警：所有绑了项目的群房间 + 曾路由到某项目的 DM 房间（有项目关联才配收 Gitea 告警）。"""
    rooms = set(projects._rooms.keys())
    rooms |= {r for r, p in _last_project_by_room.items() if p}
    return rooms


def _reason(h: dict) -> str:
    """把最近一次失败定性成一句人话（与 /status 那行口径一致）。"""
    kind, code = h.get("last_kind"), h.get("last_code")
    if kind == "auth":
        return f"疑似 token 失效（HTTP {code}）"
    if kind == "network":
        return "连不上（网络 / 服务未响应）"
    return f"服务器异常（HTTP {code}）"


def status_line(h: dict) -> str | None:
    """/status 里的一行 Gitea 连通性；没配 Gitea 返回 None（那条不显示）。"""
    if not settings.gitea_host:
        return None
    if h.get("ok"):
        return "• Gitea：正常"
    ago = (_human_gap(max(0.0, time.time() - h["last_success_ts"])) + "前"
           if h.get("last_success_ts") else "启动以来无成功记录")
    return f"• Gitea：连续 {h['consecutive_failures']} 次失败（最近成功 {ago}；{_reason(h)}）"


async def _broadcast(text: str) -> None:
    for room in _alert_rooms():
        try:
            await send(room, text)
        except Exception:
            log.exception("Gitea 告警发送失败：%s", room)


async def check_and_alert() -> None:
    """搭车后台循环每轮调一次：跨阈值首次告警、恢复后通知。水位 _alerted 防重复。"""
    global _alerted
    if not settings.gitea_host:
        return
    h = gitea.health()
    fails = h["consecutive_failures"]
    if fails >= _ALERT_THRESHOLD and not _alerted:
        _alerted = True   # 先落水位再发送：无 await 间隙，两个循环并发也不会重复告警
        log.warning("Gitea 连续 %d 次失败，告警（%s）", fails, _reason(h))
        await _broadcast(f"⚠️ Gitea 连不上了：连续 {fails} 次失败（{_reason(h)}）。"
                         f"PR 跟进 / 工单接活会暂停，恢复后自动继续。")
    elif fails == 0 and _alerted:
        _alerted = False
        log.info("Gitea 已恢复，发送恢复通知")
        await _broadcast("✅ Gitea 已恢复连通，PR 跟进 / 工单接活继续。")
