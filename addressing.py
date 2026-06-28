"""判定该不该应答：点名 / 回复 bot / 触发词 / 续话窗口 / 剥引用块与自我 @。"""
import re
import time

from nio import MatrixRoom, RoomMessageText

from config import settings
import state
from state import _sent_events, _group_engaged
from matrix_io import _is_dm


ACTIONABLE_HINTS = ("?", "？", "帮", "求", "怎么", "如何", "能不能", "可以吗",
                    "报错", "error", "bug", "失败", "搞一下", "弄一下", "处理")



def _addresses_other_user(content: dict) -> bool:
    """这条消息是否 @ 了 bot 以外的人（@别人 → 在跟别人说话，不当成对 bot 的续话）。"""
    ids = content.get("m.mentions", {}).get("user_ids", []) or []
    return any(u and u != state.MY_ID for u in ids)



def _in_followup_window(rid: str, sender: str, content: dict) -> bool:
    """群里"对话延续窗口"：sender 在 group_followup_window 秒内点过名/续过话，且这条没 @ 别人 →
    免重复 @ 也当成接着说，直接进任务流程（不是只读的主动插话判断）。"""
    if settings.group_followup_window <= 0:
        return False
    ts = _group_engaged.get((rid, sender), 0.0)
    if time.time() - ts > settings.group_followup_window:
        return False
    return not _addresses_other_user(content)



def _mark_engaged(rid: str, sender: str) -> None:
    """记下"这个人刚和 bot 聊过"，并顺手清掉过期项防字典无限增长。"""
    now = time.time()
    _group_engaged[(rid, sender)] = now
    cutoff = now - max(settings.group_followup_window, 1)
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



def _is_addressed(room: MatrixRoom, event: RoomMessageText) -> tuple[bool, str]:
    """返回 (是否点名机器人, 去掉@/触发词/引用块后的正文)。

    群里认真正的点名（m.mentions / 富文本@pill / 显式 @名字）和直接回复 bot 的消息，
    都不把引用块内容算进点名，避免别人引用你的消息时误触发。
    """
    body = event.body or ""
    content = (event.source or {}).get("content", {})
    task_text = _strip_reply_fallback(body, content)

    # 单聊默认必回
    if settings.reply_in_dm and _is_dm(room):
        return True, task_text.strip()

    # 直接回复 bot 之前发的消息 → 视为点名
    in_reply_to = (content.get("m.relates_to", {})
                   .get("m.in_reply_to", {}).get("event_id"))
    replied_to_bot = bool(in_reply_to) and in_reply_to in _sent_events

    # 触发词（拉丁词按词边界，免得 claude 命中 claudette 并把词切碎）
    if _has_trigger(task_text):
        return True, _strip_trigger(task_text).strip()

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
        return True, _strip_self_mentions(task_text).strip()

    # 对话延续窗口：刚和 bot 聊过的人，短时间内免重复 @ 接着说也当成点名（多轮不必每句点名）。
    # 但若这条是【回复别人的消息】（in_reply_to 非 bot），说明在跟别人说，不算续话。
    if not in_reply_to and _in_followup_window(room.room_id, event.sender, content):
        return True, task_text.strip()

    return False, task_text.strip()



def _looks_actionable(body: str) -> bool:
    low = body.lower()
    return any(h.lower() in low for h in ACTIONABLE_HINTS)

