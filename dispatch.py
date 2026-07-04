"""把一条消息归到哪个项目：群按绑定（未绑定落通用助手）、DM 按内容分诊（强信号 + 轻量 LLM + 兜底）。"""
import re

from nio import MatrixRoom, RoomMessageText

from config import settings
from state import _last_project_by_room
from matrix_io import send, _is_dm
from fmt import _format_context
from addressing import _strip_reply_fallback
from projects import projects, parse_repo_url
from claude_runner import runner


TRIAGE_GENERAL = "__GENERAL__"   # _triage 判定这是不属于任何项目的一般性问题



_GENERAL_ID = "__general__"      # 通用助手会话 / 串行锁的 key



def _token_match(needle: str, hay_low: str) -> bool:
    """needle 是否作为完整 token 出现在 hay_low（已小写）里：两侧不接 [\\w-]，
    避免裸子串误命中（app 不命中 app-backend）。"""
    return re.search(rf"(?<![\w-]){re.escape(needle.lower())}(?![\w-])", hay_low) is not None



def _match_project_exact(text: str, known: list[dict]) -> dict | None:
    """强信号：文本里出现 owner/repo 或完整 id（token 边界匹配，多命中取最长/最具体）。
    可信，放在 LLM 分诊之前。"""
    low = text.lower()
    best, best_len = None, -1
    for p in known:
        for needle in (f"{p['owner']}/{p['repo']}", p["id"]):
            if len(needle) > best_len and _token_match(needle, low):
                best, best_len = p, len(needle)
    return best



def _match_project_by_repo_name(text: str, known: list[dict]) -> dict | None:
    """弱信号：仅裸仓库名（词边界 + 长度>=3）命中。仓库名常是 app/api/web 这类大众词，
    容易误命中，所以只当分诊失败后的兜底，放在 _triage 之后。"""
    low = text.lower()
    for p in known:
        r = p["repo"].lower()
        if len(r) >= 3 and re.search(rf"(?<![\w-]){re.escape(r)}(?![\w-])", low):
            return p
    return None



async def _triage(text: str, known: list[dict], context: str = "") -> str | None:
    """让 Claude 轻量判断这条 DM 属于哪个项目，返回 proj_id 或 None。
    带上最近对话，"再加个单测吧"这类没点名项目的延续消息才判得准。"""
    lst = "\n".join(f"- {p['id']} ({p['owner']}/{p['repo']})" for p in known)
    ctx_block = (f"\n\n【这个私聊里最近的对话，供判断归属参考】\n{context}" if context else "")
    prompt = (
        "已知项目列表：\n" + lst + ctx_block +
        f"\n\n有人私聊发来一条消息：\n\"\"\"\n{text}\n\"\"\"\n"
        "结合上面的对话判断这条消息：\n"
        "- 若是针对上面某个项目的活，只回复该项目标识（第一列那种 host/owner/repo 形式）；\n"
        "- 若是一般性问题 / 通用编程求助 / 闲聊，不针对任何具体项目，只回复 GENERAL；\n"
        "- 若确实针对某项目但认不出是哪个，只回复 NONE。"
    )
    try:
        ans = (await runner.quick(prompt)).strip()
    except Exception:
        return None
    low = ans.lower()   # 先按 token 边界 + 取最长匹配真实项目（项目名里含 general 也不会被误判成通用）
    best, best_len = None, -1
    for p in known:
        for needle in (p["id"], f"{p['owner']}/{p['repo']}"):
            if len(needle) > best_len and _token_match(needle, low):
                best, best_len = p["id"], len(needle)
    if best:
        return best
    if "GENERAL" in ans.upper():   # 不属于任何项目的一般性问题
        return TRIAGE_GENERAL
    return None



def _general_rec(unbound_room: bool = False) -> dict:
    """不绑项目的"通用助手"伪记录：在隔离的 _scratch 目录里答一般性问题。
    unbound_room=True 表示这是个还没绑定仓库的群：照聊，但系统提示里带上绑定指引（见 tasks）。"""
    rec = {"id": _GENERAL_ID, "general": True, "path": settings.claude_workdir}
    if unbound_room:
        rec["unbound_room"] = True
    return rec



async def _dispatch(room: MatrixRoom, event: RoomMessageText, text: str,
                    skip_body: str | None = None) -> dict | None:
    """决定这条消息归到哪个项目。返回项目记录（general=True 表示当通用助手答）；
    None=已就地回复/提问，不再继续。
    skip_body：当前消息在背景上下文里的原文（媒体与 event.body 不同，由调用方传），
    分诊带背景时把它剔掉，别让 _triage 把同一条消息看两遍。"""
    rid = room.room_id

    if not _is_dm(room):  # 群：绑定了按项目干活；没绑定也别闭门谢客——当通用助手照聊（闲聊/答疑），
        rec = projects.get_room(rid)   # 系统提示里带绑定指引，真要派仓库的活时引导 /bind
        if rec:
            return await projects.ensure_project(rec)  # 校验/按需修复本地 checkout
        return _general_rec(unbound_room=True)

    # DM：自动分诊到对应项目。每条路由都要过 ensure_project——本地 checkout 可能被删/没 clone，
    # 不校验 Claude 会在空目录里干活。
    repo = parse_repo_url(text)                  # 1) 消息里带仓库链接（强信号，能切换/换绑，优先于已绑定）
    if repo:
        return await projects.ensure_project(repo)
    known = projects.list_projects()
    exact = _match_project_exact(text, known)    # 2) 强信号：owner/repo 或完整 id（同样可切换已绑定）
    if exact:
        return await projects.ensure_project(exact)
    bound = projects.get_room(rid)               # 3) 这条私聊显式绑了仓库：无强信号一律落到它、跳过分诊
    if bound:                                    #    （绑定=固定项目；切换发仓库 URL / 点名，或 /unbind 解绑）
        return await projects.ensure_project(bound)
    if not known:                                # 4) 还没有任何已知项目：当通用助手答一般性问题
        return _general_rec()                     #    （要做某仓库的活，欢迎语已指引发 Gitea 地址来绑定）
    sender = room.user_name(event.sender) or event.sender    # 5) 轻量分诊（带最近对话，剔除当前这条）
    cur = (skip_body if skip_body is not None
           else _strip_reply_fallback(event.body or "", (event.source or {}).get("content", {})))
    pid = await _triage(text, known, _format_context(rid, skip=(sender, cur)))
    if pid == TRIAGE_GENERAL:                  # 一般性问题：直接当通用助手答，不必非得挂到某个项目
        return _general_rec()
    if pid:
        rec = projects.get_project(pid)
        if rec:
            return await projects.ensure_project(rec)
    loose = _match_project_by_repo_name(text, known)   # 6) 兜底：裸仓库名（分诊放弃后才用）
    if loose:
        return await projects.ensure_project(loose)
    last_pid = _last_project_by_room.get(rid)    # 7) 仍判不出 → 沿用这个 DM 上次的项目，
    if last_pid:                                 #    别让多轮对话里每条延续消息都被反问"这是关于哪个项目的"
        rec = projects.get_project(last_pid)
        if rec:
            return await projects.ensure_project(rec)
    lst = "\n".join(f"- {p['owner']}/{p['repo']}" for p in known)   # 8) 实在不行才反问
    await send(rid, f"这条是关于哪个项目的？回复项目名或发仓库地址：\n{lst}")
    return None

