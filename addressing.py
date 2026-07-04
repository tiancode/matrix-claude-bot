"""判定该不该应答：点名 / 回复 bot / 触发词 / 续话窗口 / 剥引用块与自我 @。"""
import re
import time

from nio import MatrixRoom, RoomMessageText

from config import settings
import state
from state import _sent_events, _group_engaged, _ctx_thread
from matrix_io import _is_dm


ACTIONABLE_HINTS = ("?", "？", "帮", "求", "怎么", "如何", "能不能", "可以吗",
                    "报错", "error", "bug", "失败", "搞一下", "弄一下", "处理")



def _addresses_other_user(content: dict) -> bool:
    """这条消息是否 @ 了 bot 以外的人（@别人 → 在跟别人说话，不当成对 bot 的续话）。"""
    ids = content.get("m.mentions", {}).get("user_ids", []) or []
    return any(u and u != state.MY_ID for u in ids)



def _third_party_spoke_since(rid: str, ts: float, sender_name: str) -> bool:
    """窗口起点 ts 之后，群里有没有"第三人"（非本人、非 bot）发过言。有则说明对话已转向别人，
    "还在跟我续话"这个假设不再成立，作废窗口。数据取自内存背景缓冲 _context（存的是显示名）；
    缓冲为空时（如单测直连 _is_addressed）判无第三人，保持原纯时间窗行为。
    续话窗口只在顶层主时间线成立（线程内消息带 in_reply_to，不走续话），故只看顶层消息——
    别让线程里别人说的话（与主时间线隔离）误作废你在主聊天里的续话窗口。"""
    bot_name = state.MY_NAME or "bot"   # 与 _track_reply 落上下文时用的名字一致，别把 bot 自己的答复当第三人
    for item in list(state._context.get(rid, ())):
        t, name = item[0], item[1]
        if t <= ts:
            continue
        if _ctx_thread(item) is not None:   # 线程里的话不算主时间线的第三人插话
            continue
        if name == sender_name or name == bot_name:
            continue
        return True
    return False



def _in_followup_window(rid: str, sender: str, sender_name: str, content: dict) -> bool:
    """群里"对话延续窗口"：sender 近期点过名/续过话、且这条没 @ 别人 → 当成可能接着跟 bot 说
    （弱信号，交由上层再过语义闸后进任务流程）。两级时限，都靠上层语义闸兜底：
      · 硬窗口（group_followup_window，默认 180s）内：再加"窗口内没第三人插话"这道零成本预筛
        （有旁人开口 → 对话多半转向别人，直接不认，省一次判断）。
      · 软窗口（followup_semantic_window，默认 30 min）内、硬窗口外：不再按"有没有旁人""过了几分钟"
        硬性一刀切——全部放行给语义闸，让它结合最近对话（含中途旁人的话、时间间隔）逐条判断"是不是
        还在跟我说"。这样"讲个故事"后隔几十分钟又说"再讲一个"也能被接住。软窗口全靠语义闸把关，
        故仅在 followup_semantic_gate 开着时才放开。"""
    if settings.group_followup_window <= 0:
        return False
    ts = _group_engaged.get((rid, sender), 0.0)
    if ts <= 0:
        return False
    if _addresses_other_user(content):     # @了别人 → 在跟别人说，两级窗口都不认
        return False
    age = time.time() - ts
    if age <= settings.group_followup_window:
        # 硬窗口内有旁人开口 → 对话很可能已转向别人，别再把你这句当成对我说（零成本堵最常见的误触发）
        return not _third_party_spoke_since(rid, ts, sender_name)
    # 硬窗口外：只要语义闸开着且没超软窗口，就放给语义闸结合上下文逐条定夺（不再靠死时间/旁人预筛）
    return settings.followup_semantic_gate and age <= settings.followup_semantic_window



def _mark_engaged(rid: str, sender: str) -> None:
    """记下"这个人刚和 bot 聊过"，并顺手清掉过期项防字典无限增长。清理阈值取两级窗口里更长的那个
    （软窗口），否则记录 180s 就被删、活不到软窗口，跨几十分钟的续话永远走不到语义闸。"""
    now = time.time()
    _group_engaged[(rid, sender)] = now
    horizon = max(settings.group_followup_window, settings.followup_semantic_window, 1)
    cutoff = now - horizon
    for k in [k for k, v in _group_engaged.items() if v < cutoff]:
        del _group_engaged[k]



def _has_trigger(text: str) -> bool:
    """文本里是否出现触发词。拉丁触发词按 token 边界匹配（claude 不命中 claudette），
    CJK 等非 ASCII 触发词没有词边界概念、按子串。空触发词=从不命中。"""
    tp = settings.trigger_phrase
    if not tp:
        return False
    if tp.isascii():
        return re.search(rf"(?<![\w-]){re.escape(tp.lower())}(?![\w-])", text.lower()) is not None
    return tp in text



def _strip_trigger(text: str) -> str:
    """从正文里去掉触发词（与 _has_trigger 同样的边界规则，别把词内的子串也抠掉）。"""
    tp = settings.trigger_phrase
    if not tp:
        return text
    if tp.isascii():
        return re.sub(rf"(?<![\w-]){re.escape(tp)}(?![\w-])", "", text, flags=re.I)
    return text.replace(tp, "")



def _strip_reply_fallback(body: str, content: dict) -> str:
    """去掉回复消息顶部的 `> <@发送者> 原文` 引用回退块，只留用户真正写的内容。
    仅当首行确像引用头才剥，避免误伤用户自己以 `>` 开头的正文。"""
    if not content.get("m.relates_to", {}).get("m.in_reply_to"):
        return body
    lines = body.split("\n")
    if not (lines and re.match(r">\s*<[^>]+>", lines[0])):
        return body
    i = 0
    while i < len(lines) and lines[i].startswith(">"):
        i += 1
    return "\n".join(lines[i:]).strip()



def _strip_self_mentions(text: str) -> str:
    """去掉指向 bot 的各种 @ 形式：完整 MXID、@显示名、@本地名。
    先去完整 MXID，免得只剥掉 @本地名却把 :host 留在正文里。"""
    if state.MY_ID:
        text = text.replace(state.MY_ID, "")
    for tok in (state.MY_NAME, state.MY_LOCAL):
        if tok:  # 带右边界，别把 @bottle 里的 @bot 也剥了
            text = re.sub(rf"@{re.escape(tok)}(?![\w-])", "", text)
    return text



def _address_kind(room: MatrixRoom, event: RoomMessageText) -> tuple[str, str]:
    """返回 (点名强度, 去掉@/触发词/引用块后的正文)。强度：
      "strong" 明确点名（单聊 / 触发词 / @我 / 回复我）——直接派活；
      "weak"   仅靠"对话延续窗口"命中（没点名、只是刚聊过又接着说）——弱信号，上层应再过一道
               轻量语义闸确认确实在跟我说，防"没话找话"的误触发；
      ""       没点名。

    群里认真正的点名（m.mentions / 富文本@pill / 显式 @名字）和直接回复 bot 的消息，
    都不把引用块内容算进点名，避免别人引用你的消息时误触发。
    """
    body = event.body or ""
    content = (event.source or {}).get("content", {})
    task_text = _strip_reply_fallback(body, content)

    # 单聊默认必回
    if settings.reply_in_dm and _is_dm(room):
        return "strong", task_text.strip()

    # 直接回复 bot 之前发的消息 → 视为点名
    in_reply_to = (content.get("m.relates_to", {})
                   .get("m.in_reply_to", {}).get("event_id"))
    replied_to_bot = bool(in_reply_to) and in_reply_to in _sent_events

    # 触发词（拉丁词按词边界，免得 claude 命中 claudette 并把词切碎）
    if _has_trigger(task_text):
        return "strong", _strip_trigger(task_text).strip()

    # 真正的点名（剔除富文本 <mx-reply> 引用块，只看正文 @pill）
    mentioned = state.MY_ID in content.get("m.mentions", {}).get("user_ids", [])
    if not mentioned and state.MY_ID:
        fb = re.sub(r"<mx-reply>.*?</mx-reply>", "",
                    content.get("formatted_body", "") or "", flags=re.S | re.I)
        if state.MY_ID in fb:
            mentioned = True
    if not mentioned:
        low = task_text.lower()
        for tok in (state.MY_NAME, state.MY_LOCAL):  # 要带 @ 且带右边界，@bot 不误命中 @bottle
            if tok and re.search(rf"@{re.escape(tok.lower())}(?![\w-])", low):
                mentioned = True
                break

    if mentioned or replied_to_bot:
        return "strong", _strip_self_mentions(task_text).strip()

    # 对话延续窗口：刚和 bot 聊过的人，短时间内免重复 @ 接着说也当成点名（多轮不必每句点名）。
    # 但若这条是【回复别人的消息】（in_reply_to 非 bot），说明在跟别人说，不算续话。
    sender_name = room.user_name(event.sender) or event.sender
    if not in_reply_to and _in_followup_window(room.room_id, event.sender, sender_name, content):
        return "weak", task_text.strip()

    return "", task_text.strip()



def _is_addressed(room: MatrixRoom, event: RoomMessageText) -> tuple[bool, str]:
    """是否该应答（strong/weak 都算点名）+ 清洗后的正文。区分强弱由 _address_kind 提供，
    media / 兼容调用只需布尔结论走这里。"""
    kind, cleaned = _address_kind(room, event)
    return bool(kind), cleaned



def _looks_actionable(body: str) -> bool:
    low = body.lower()
    return any(h.lower() in low for h in ACTIONABLE_HINTS)

