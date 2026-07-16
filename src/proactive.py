"""主动插话：不被 @ 时也判断该不该就群消息开口（求助 / 有错可纠正），带冷却防刷屏。"""
import logging
import time

from nio import MatrixRoom, RoomMessageText

from config import settings
from state import _last_proactive
from matrix_io import send, _is_dm, _thread_of, _thread_root_of
from fmt import _format_context
from addressing import _looks_actionable
from projects import projects, has_local_clone
from claude_runner import runner

log = logging.getLogger("matrix-claude.proactive")


_PROACTIVE_PASS_COOLDOWN = settings.proactive_pass_cooldown



async def followup_is_for_me(rid: str, sender_name: str, text: str) -> bool:
    """续话窗口命中属弱信号：轻量判断这条"没点名"的消息是不是接着刚才的话题继续跟 bot 说，
    而不是转头跟旁人说 / 自言自语 / 与刚才找 bot 的事无关。只在**明确判定不是对我说**时才拦下
    （返回 False）；拿不准、判断出错一律放行（True）——宁可偶尔多接一句，也别把真续话漏掉。"""
    ctx = _format_context(rid)
    prompt = (
        f"下面是一个 Matrix 群里最近的对话（带时间，跨度大处有「间隔」提示）。你是群里的助手，"
        f"刚才在和「{sender_name}」对话。\n"
        f"现在「{sender_name}」又发了一条**没有点名你**的消息（就是最近对话里的最后一条）：\n"
        f"\"\"\"\n{text}\n\"\"\"\n"
        "判断这条是不是接着之前的话题**继续在跟你（助手）说**：\n"
        "- 是，或拿不准 → 只回 __YES__\n"
        "- 明显在跟群里别人说话 / 自言自语 / 与之前找你的事无关 → 只回 __NO__\n"
        "注意：中间隔了很久、或期间有别人插过话，都**不**单独构成「不是跟你说」——只要这句在语义上"
        "顺着你俩之前的话题往下接（例如你在讲故事，他说「再讲一个」），仍算在跟你说；真正判 __NO__ 的是"
        "话题、指向都转向了别人或与你无关。\n\n"
        f"最近对话：\n{ctx}"
    )
    try:
        ans = (await runner.quick(prompt)).strip().upper()
    except Exception:
        log.exception("续话语义闸判断失败，放行")
        return True
    return "__NO__" not in ans



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
        # 按触发消息所在范围取背景：线程里触发就只看该线程、顶层触发就只看主时间线，别互相串台
        ctx = _format_context(rid, thread=_thread_of(event))
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
        if rec and has_local_clone(rec):
            sp = ("你是该仓库的助手。这是主动插话场景：可以只读地查看仓库代码把问题答准、"
                  "或核实对话里的说法对不对，但绝不要修改文件、提交或开 PR。判断标准同上："
                  "有求助或有值得纠正的错误才插话、且简洁；不该插话或拿不准就只回 __PASS__。")
            ans = (await runner.consult(prompt, cwd=rec["path"], system_prompt=sp)).strip()
        else:
            ans = (await runner.quick(prompt)).strip()
        if ans and "__PASS__" not in ans:
            # 主动插话没人点名，必须指明在回哪条：对方在线程里就跟进线程，否则用引用回复
            # （REPLY_IN_THREAD=1 的旧式群保持挂线程）。
            thr = _thread_of(event) if not _is_dm(room) else None
            if thr is None and settings.reply_in_thread and not _is_dm(room):
                thr = _thread_root_of(event)
            reply = getattr(event, "event_id", None) if (thr is None and not _is_dm(room)) else None
            await send(rid, ans, track=True, thread_root=thr, reply_to=reply)
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

