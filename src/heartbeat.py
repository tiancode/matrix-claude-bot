"""自驱心跳：没人派活时巡检各项目找值得做的事；autopilot 直接认领开 PR。"""
import asyncio
import logging
import os
import time
from datetime import datetime

from config import settings, fixed_tz
import state
from state import _sess_key, _last_project_by_room, _project_last_active
from matrix_io import send, _typing, _is_dm
from tasks import _employee_prompt, _extract_pr, _transient_blurb
from projects import projects
from claude_runner import runner, ClaudeCancelled
import pr_ledger
import memory

log = logging.getLogger("matrix-claude.heartbeat")


def _project_home_room(pid: str) -> str | None:
    """自驱心跳的"汇报口"：绑该项目的房间里【优先群】——团队仓库的自驱汇报别塞进某个私聊
    （同一仓库可能既绑了群又绑了私聊）；没有群就退回任一绑定房间，再没有就退回登记簿里的房间。"""
    def _is_group(rid: str) -> bool:
        room = state.client.rooms.get(rid) if state.client else None
        return bool(room) and not _is_dm(room)
    bound = projects.rooms_for(pid)
    home = next((r for r in bound if _is_group(r)), None)
    if home:
        return home
    if bound:
        return bound[0]
    for r, p in _last_project_by_room.items():
        if p == pid:
            return r
    return None



async def _heartbeat_execute(rec: dict, room: str, proposal: str):
    """autopilot：把巡检挑中的事真正做完并开 PR，结果回报、PR 进台账（→由跟进循环盯到合并）。"""
    prompt = (
        f"你刚巡检后决定主动推进这件事：\n{proposal}\n\n"
        "请像平常派活一样把它做完：从 origin/base 建分支、改代码、commit、push、开 PR，"
        "最终回复附上 PR 链接。用简洁中文回复。"
    )
    sp = memory.augment_system_prompt(_employee_prompt(rec), rec["id"])
    # 与聊天共用同一会话 key：模型也跟房间 /model 设置走，别让自驱新开的会话把模型带偏
    model_kw = {"model": state._room_model[room]} if state._room_model.get(room) else {}
    try:
        async with _typing(room):
            # cancel_key=汇报房间：让房间里的 /cancel 也能停掉自驱任务（不然只能干看着它跑）
            answer = await runner.ask(_sess_key(rec, room), prompt, cwd=rec["path"], system_prompt=sp,
                                      lock_key=rec["id"], prepare=lambda: projects.prepare_worktree(rec),
                                      cancel_key=room, **model_kw)
        _project_last_active[rec["id"]] = time.time()
        await send(room, f"🤖 自驱完成：\n{answer}", track=True)
        pr = _extract_pr(answer, rec)
        if pr and pr_ledger.record(rec["id"], pr[0], pr[1], room):
            log.info("[%s] 自驱开了 PR #%d，进台账", rec["id"], pr[0])
    except ClaudeCancelled:
        await send(room, "🛑 已停止这次自驱任务。")
    except Exception as e:
        log.exception("自驱执行失败")
        await send(room, f"自驱执行出错：{_transient_blurb(e) or e}")



async def _heartbeat_one(rec: dict, room: str):
    """对一个项目只读巡检，挑一件值得主动做的事；autopilot 直接认领去做，否则只提议。"""
    patrol = (
        f"你是负责仓库 {rec['owner']}/{rec['repo']} 的工程师。现在没人给你派活——"
        "主动巡检这个仓库，看有没有**值得现在主动推进、且改动可控**的事："
        "明显的小 bug、缺失的关键测试、陈旧的 TODO/FIXME、文档与代码不符、能小步改进的点。\n"
        "判断标准要高，别为找事而找事。\n"
        "- 不值得打扰就只回一行：__PASS__\n"
        "- 值得就挑**最值得的一件**，简短说清：是什么、为什么值得、打算怎么改（这是只读巡检，先别动手）。"
    )
    try:
        await projects.prepare_worktree(rec)   # 巡检最新的干净 base，而不是上个任务残留的脏树/分支
    except Exception:
        pass
    log.info("[%s] 自驱巡检中…", rec["id"])
    sp = memory.augment_system_prompt(_employee_prompt(rec), rec["id"])
    try:
        proposal = (await runner.consult(patrol, cwd=rec["path"], system_prompt=sp)).strip()
    except Exception:
        log.exception("[%s] 自驱巡检失败", rec["id"])
        return
    if not proposal or "__PASS__" in proposal:
        log.info("[%s] 自驱：本轮没有值得主动做的事（PASS）", rec["id"])
        return
    label = f"{rec['owner']}/{rec['repo']}"
    if settings.proactive_autopilot:
        await send(room, f"🫀 [{label}] 自驱：没人派活，我巡检后打算主动做这件事，开干——\n{proposal}", track=True)
        state._spawn(_heartbeat_execute(rec, room, proposal))
    else:
        await send(room, f"🫀 [{label}] 巡检建议（没人派活时主动看的）：\n{proposal}\n\n"
                         "要做就回我一句；想让我自己认领就开 PROACTIVE_AUTOPILOT。", track=True)



def _in_heartbeat_window(now: float) -> bool:
    """自驱巡检只在配置的工作日 + 白天时段内进行（默认周一~周五 9~19 点、东八区）。
    起止时刻允许跨零点（如 22~6 表示夜间时段）：起 <= 止就是当天区间，起 > 止就是跨天区间。"""
    local = datetime.fromtimestamp(now, fixed_tz(settings.proactive_heartbeat_tz_hours))
    if settings.proactive_heartbeat_weekdays_only and local.weekday() >= 5:   # 5=周六 6=周日
        return False
    start, end, hour = (settings.proactive_heartbeat_start_hour,
                        settings.proactive_heartbeat_end_hour, local.hour)
    if start <= end:
        return start <= hour < end
    return hour >= start or hour < end   # 跨零点时段


# 循环"探测"节奏上限（秒）：即便 PROACTIVE_HEARTBEAT_INTERVAL 配得很大（如 4 小时），也按这个更短
# 的节奏醒来检查是否进入了巡检时段——真正"多久巡检一次"仍由每项目的 _project_last_active 节流
# 保证，这里只管别让大 interval 与巡检时段错峰、导致整段时段被永久错过。
_WINDOW_POLL_INTERVAL = 300


async def _heartbeat_loop():
    """周期巡检有"汇报口"的项目；避让最近在弄的项目，免得打断正在派的活、也别为巡检而 clone。"""
    if not settings.proactive_heartbeat_enabled:
        return
    log.info("自驱心跳已启动（每 %ds 巡检一次，autopilot=%s）",
             settings.proactive_heartbeat_interval, settings.proactive_autopilot)
    while True:
        try:
            await asyncio.sleep(min(settings.proactive_heartbeat_interval, _WINDOW_POLL_INTERVAL))
            now = time.time()
            if not _in_heartbeat_window(now):
                continue   # 非工作时段：跳过本轮，别半夜/周末打扰人
            for rec in projects.list_projects():
                pid = rec["id"]
                room = _project_home_room(pid)
                if not room:
                    continue
                if now - _project_last_active.get(pid, 0) < settings.proactive_heartbeat_interval:
                    continue   # 最近有人在弄 / 刚巡检过：别打扰
                if not os.path.isdir(os.path.join(rec.get("path", ""), ".git")):
                    continue
                _project_last_active[pid] = now   # 占住，避免与真任务/下一轮重叠
                try:
                    await _heartbeat_one(rec, room)
                except Exception:
                    log.exception("[%s] 自驱心跳失败", pid)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("自驱心跳循环异常，继续")

