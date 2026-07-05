"""把一条消息归到哪个项目：房间（群 / 私聊）绑了就用绑定项目，没绑当通用助手（系统提示里引导绑定）。

私聊过去按内容自动分诊（LLM 判归属 + 裸名兜底 + 沿用上次），现已去掉——私聊就是两人房间，
统一进「房间」模型：绑定即固定项目，切换/换绑走 /bind 或直接发仓库 URL（见 bot.py 绑定分支）。"""
from nio import MatrixRoom

from config import settings
from projects import projects


_GENERAL_ID = "__general__"      # 通用助手会话 / 串行锁的 key



def _general_rec(unbound_room: bool = False) -> dict:
    """不绑项目的"通用助手"伪记录：在隔离的 _scratch 目录里答一般性问题。
    unbound_room=True：这个房间还没绑仓库——照聊，但系统提示里带上绑定指引（见 tasks）。"""
    rec = {"id": _GENERAL_ID, "general": True, "path": settings.claude_workdir}
    if unbound_room:
        rec["unbound_room"] = True
    return rec



async def _dispatch(room: MatrixRoom) -> dict:
    """决定这条消息归到哪个项目：房间绑了就用绑定项目（每次都过 ensure_project 校验/按需修复本地
    checkout——可能被删 / 没 clone，不校验 Claude 会在空目录里干活），没绑就当通用助手照聊
    （系统提示里带绑定指引）。群 / 私聊同一套，私聊不再按内容分诊。"""
    rec = projects.get_room(room.room_id)
    if rec:
        return await projects.ensure_project(rec)
    return _general_rec(unbound_room=True)
