"""主动插话：不被 @ 时也判断该不该就群消息开口（求助 / 有错可纠正），带冷却防刷屏。"""
import logging
import os
import time

from nio import MatrixRoom, RoomMessageText

from config import settings
from state import _last_proactive
from matrix_io import send, _is_dm, _thread_root_of
from fmt import _format_context
from addressing import _looks_actionable
from projects import projects
from claude_runner import runner

log = logging.getLogger("matrix-claude.proactive")


_PROACTIVE_PASS_COOLDOWN = settings.proactive_pass_cooldown



async def maybe_proactive(room: MatrixRoom, event: RoomMessageText, body: str):
    rid = room.room_id
    now = time.time()
    if now - _last_proactive[rid] < settings.proactive_cooldown:
        return
    # require_hint=True：只评估含求助/报错词的消息（省判断调用）。
    # False：每条都让 Claude 判断——能抓到"没人求助但话里有错"的情形（靠冷却+__PASS__ 倾向防刷屏）。
    if settings.proactive_require_hint and not _looks_actionable(body):
        return
    _last_proactive[rid] = now   # 先占满冷却窗口，防止并发消息同时触发判断
    spoke = False
    try:
        ctx = _format_context(rid)
        prompt = (
            "下面是一个 Matrix 群里最近的对话。你是群里的助手。\n"
            "判断你现在是否应该主动插话：\n"
            "- 该插话：有人在求助，或对话里出现了明显的事实/技术错误、有风险的做法、走错方向，"
            "而你能简短地帮上忙或纠正。\n"
            "- 不该：不关你的事、别人已在处理、只是闲聊、你自己也拿不准——这些一律只回一行：__PASS__\n"
            "- 该插话就直接给要发到群里的简洁中文回复（别解释你为什么发）。宁可克制：拿不准就 __PASS__。\n\n"
            f"最近对话：\n{ctx}"
        )
        # 群已绑仓库且本地 checkout 还在：用只读 consult 让它对着真实代码作答，而不是凭空瞎猜；
        # 否则（未绑/没 clone）退回纯文本 quick。proactive 不主动 clone。
        rec = projects.get_room(rid) if not _is_dm(room) else None
        if rec and os.path.isdir(os.path.join(rec.get("path") or "", ".git")):
            sp = ("你是该仓库的助手。这是主动插话场景：可以只读地查看仓库代码把问题答准、"
                  "或核实对话里的说法对不对，但绝不要修改文件、提交或开 PR。判断标准同上："
                  "有求助或有值得纠正的错误才插话、且简洁；不该插话或拿不准就只回 __PASS__。")
            ans = (await runner.consult(prompt, cwd=rec["path"], system_prompt=sp)).strip()
        else:
            ans = (await runner.quick(prompt)).strip()
        if ans and "__PASS__" not in ans:
            # 主动插话也挂进"被它纠正/回应"的那条消息的线程，群里不打断别的话题。
            thr = _thread_root_of(event) if settings.reply_in_thread and not _is_dm(room) else None
            await send(rid, ans, track=True, thread_root=thr)
            spoke = True
            log.info("[%s] 主动插话 %d 字", rid, len(ans))
    except Exception:
        log.exception("主动判断失败")
    finally:
        if spoke:
            _last_proactive[rid] = time.time()   # 真发言了 → 完整冷却防刷屏
        else:
            # 没发言（PASS/出错）：只占一个短冷却，好让紧接着的真求助能很快重新判断
            short = min(_PROACTIVE_PASS_COOLDOWN, settings.proactive_cooldown)
            _last_proactive[rid] = now - settings.proactive_cooldown + short

