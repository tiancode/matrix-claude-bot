"""进程级共享运行态：Matrix client、本机身份、各房间上下文/历史缓冲、路由记忆等。

这些值会在运行期被重新赋值（client、MY_* 在登录后才有；_synced 同步后转 True），
所以拆模块后一律按 `state.X` 属性访问，别 `from state import client`（会绑死成导入时的旧值）。
in-place 容器（_context/_sent_events/各 dict）可 `from state import` 直接用同一对象。
"""
import asyncio
import json
import os
from collections import deque, defaultdict

from nio import AsyncClient

from config import settings
from storage import atomic_write_json

# E2EE 是否可用（需系统 libolm + matrix-nio[e2e]）
try:
    import olm  # noqa: F401
    OLM_OK = True
except Exception:
    OLM_OK = False

E2E = settings.enable_e2e and OLM_OK

# ---- 运行期会被重新赋值的全局（登录/同步后才有值）：按 state.X 访问 ----
client: AsyncClient | None = None
MY_ID = ""
MY_NAME = ""
MY_LOCAL = ""
_synced = False     # 初始 sync 完成前不处理回放的历史/积压消息

# ---- in-place 容器（不重绑，可被各模块 import 同一对象共享）----
_tasks: set = set()
_context: dict[str, deque] = defaultdict(lambda: deque(maxlen=max(4, settings.context_lines * 2)))
# ↑ 每条是 (ts, sender, body, thread[, dispatched])：
#   thread=线程根 event_id / None=顶层主时间线——会话按线程细分（见 _sess_key），背景也按线程分范围，
#          顶层任务的背景不该串进线程里说的话（取值用 _ctx_thread）。
#   dispatched=这条是否派过给 Claude（第 5 元，缺省即老式 4/3 元组 → False，取值用 _ctx_dispatched）。
#          派过的消息在 --resume 会话里已经有了，续接轮拼背景时会剔掉（见 _format_context 的 drop_dispatched）。
_last_proactive: dict[str, float] = defaultdict(float)
# 最近入背景的消息索引：(event_id, rid, sender, body)。_context 元组本身不存 event_id（见上），
# 删/redact 一条消息时要把它从内存背景里剔掉，靠这份索引按 event_id 定位到 (sender, body) 再删。
# 有界（重启清空无妨，_context 也重启即清）；入背景时（on_message/on_media）同步追加。
_ctx_recent: deque = deque(maxlen=4096)
_sent_events: deque = deque(maxlen=4096)        # 自己发出的 event_id：防自激 + 识别"回复了 bot"（重启清空）
_foreign_events: deque = deque(maxlen=4096)     # 查证过"不是 bot 发的"的 event_id：防重复向服务器拉取
_last_project_by_room: dict[str, str] = {}      # room_id -> proj_id，房间在弄哪个项目（自驱心跳/Gitea 健康度找汇报口）
_room_model: dict[str, str] = {}                # room_id -> /model 给本房间设的模型（覆盖 CLAUDE_MODEL；没设=跟随全局）
_project_last_active: dict[str, float] = defaultdict(float)   # proj_id -> 上次有人派活的时刻，自驱心跳据此避让
_group_engaged: dict[tuple[str, str], float] = {}   # (room_id, user) -> 上次点名/续话时刻：群里"对话延续窗口"用
# KNOWN_BOTS 名单 bot 在各房间用过的【显示名】：rid -> {name}。上下文元组只存显示名不存 MXID，
# 而"第三人插话作废续话窗口"按显示名比对——名单 bot 的定时播报不该算第三人（否则有定时 bot 的
# 房间里真人免 @ 续话永远被掐）。在消息入口记录（与上下文同源同名）。代价：若真人显示名恰好
# 与名单 bot 撞名，其插话也会被当 bot 忽略——比"续话永远失效"轻得多，可接受。
_known_bot_names: dict[str, set[str]] = {}
# 连发合并的待派缓冲：(room_id, sender, 线程) ->
#   {"room","event","msgs":[(text,ctx_body,note,event_id)],"last"}。
# msgs 每条第 4 元是其来源消息的 event_id（redact/删除时据此精确摘掉那条，见 _drop_pending_event）。
# 放 state 而非 bot：/cancel（tasks）、退房清理、消息删除（bot）都要能作废/修改它。
_pending_dispatch: dict[tuple, dict] = {}
# steering 标过 dispatched 的消息：rid -> [(sender, body)]。回合被 /cancel 杀掉时凭它撤销标记，
# 让被丢弃的追加消息回到背景（否则既没被回答、下轮背景又被剔掉，双头落空）。回合正常完成即清。
_steered_dispatched: dict[str, list[tuple[str, str]]] = {}


def _drop_pending(rid: str) -> int:
    """作废某房间全部连发合并缓冲（/cancel、退房时用）。返回作废的条数（按缓冲里的消息数计）。
    等待中的 _debounced_dispatch 醒来后会发现自己的缓冲已被摘除而直接退出（按对象身份复核）。"""
    n = 0
    for k in [k for k in _pending_dispatch if k[0] == rid]:
        pend = _pending_dispatch.pop(k, None)
        if pend:
            n += len(pend["msgs"])
    return n


def _drop_pending_event(rid: str, event_id: str) -> bool:
    """把被 redact / 删除的那条消息从连发合并缓冲里摘掉，别再拿它去派活给 Claude。
    按 msgs 里每条的第 4 元 event_id 精确匹配（见 bot._queue_strong_task/_absorb_into_pending）。
    删空了整个缓冲就连 key 一并摘除——其 waiter(_debounced_dispatch) 醒来发现缓冲没了会自行撤回执退出；
    还剩别的消息则原地保留（哪怕删的恰是首条 anchor，anchor event 至多让回复指向已删消息，无害）。
    返回是否命中；已经派出去（不在缓冲里）的追不回，那是"删除不杀在跑任务"的边界，符合预期。"""
    if not event_id:
        return False
    for key in [k for k in _pending_dispatch if k[0] == rid]:
        pend = _pending_dispatch.get(key)
        if not pend:
            continue
        msgs = pend["msgs"]
        kept = [m for m in msgs if (m[3] if len(m) > 3 else None) != event_id]
        if len(kept) == len(msgs):
            continue   # 这个缓冲里没有这条
        # 删的是 anchor（缓冲的根：_debounced_dispatch 拿 pend["event"] 当回复/线程/发送人锚点，msgs[0] 也是它）
        # → 整个缓冲作废，别把剩下的派出去：那些多是没独立寻址、只作为 anchor 补充被吸收进来的消息
        # （strong 跟评也没留下 event 对象无从重新锚定），单独派会去回一句本不是冲着 bot 说的话。
        anchor_eid = getattr(pend.get("event"), "event_id", None)
        if not kept or event_id == anchor_eid:
            _pending_dispatch.pop(key, None)
        else:
            pend["msgs"] = kept
        return True
    return False


def _drop_context_event(rid: str, event_id: str) -> bool:
    """把被删 / redact 的消息从内存背景 _context 里剔掉（默认关 transcript 也生效）。_context 元组不含
    event_id，靠入背景时同步维护的 _ctx_recent 索引按 event_id 定位到 (sender, body) 再就近删除。
    找到并从 _context 删掉返回 True；索引里没有、或已滚出 _context 返回 False（顺带清掉陈旧索引项）。"""
    if not event_id:
        return False
    found = None
    for rec in _ctx_recent:
        if rec[0] == event_id and rec[1] == rid:
            found = rec
            break
    if found is None:
        return False
    try:
        _ctx_recent.remove(found)
    except ValueError:
        pass
    sender, body = found[2], found[3]
    dq = _context.get(rid)
    if not dq:
        return False
    for i in range(len(dq) - 1, -1, -1):   # 从近往远：同人重复发同一句时删最近那条
        it = dq[i]
        if it[1] == sender and it[2] == body:
            del dq[i]
            return True
    return False


def _unmark_dispatched(rid: str, sender: str, body: str) -> None:
    """撤销一条消息的 dispatched 标记（steering 进的回合被 /cancel 杀掉时用）：
    它没被处理，得回到背景供下轮照常投喂。找不到就算了，尽力而为。"""
    dq = _context.get(rid)
    if not dq:
        return
    for i in range(len(dq) - 1, -1, -1):
        it = dq[i]
        if it[1] == sender and it[2] == body and _ctx_dispatched(it):
            dq[i] = (it[0], sender, body, _ctx_thread(it))
            return


def _ctx_thread(item) -> str | None:
    """取背景条目 _context 的线程标记（第 4 元）。容忍老式 3 元组（没标记）→ 按顶层(None)算。"""
    return item[3] if len(item) > 3 else None


def _ctx_dispatched(item) -> bool:
    """取背景条目「是否派过给 Claude」的标记（第 5 元）。老式短元组 → False（当没派过）。"""
    return item[4] if len(item) > 4 else False


def _mark_dispatched(rid: str, sender: str, body: str) -> None:
    """把「这条消息这轮派给了 Claude」记到背景缓冲上：从右往左（就近）找到匹配 (sender, body) 的
    条目，标 dispatched=True。下轮拼背景时若在续接会话（消息已在 --resume 里），这条就不再重复喂。
    找不到就算了——背景是尽力而为，漏标顶多多喂一条、不出错。"""
    dq = _context.get(rid)
    if not dq:
        return
    for i in range(len(dq) - 1, -1, -1):
        it = dq[i]
        if it[1] == sender and it[2] == body:
            dq[i] = (it[0], sender, body, _ctx_thread(it), True)   # 保留 ts/线程，只把第 5 元置真
            return


def _clear_dispatched(rid: str) -> None:
    """清掉该房间背景里所有 dispatched 标记（还原成不带标记的四元组）。会话被判失效、就地全新开时
    调用：那些「本以为已在会话里」的消息其实随着旧会话没了，标记必须作废，否则续接轮会一直把它们
    从背景剔掉、可它们又不在新会话里 → 永久两头落空。清过后它们照常回到背景（顶多重复喂，不丢）。"""
    dq = _context.get(rid)
    if not dq:
        return
    for i, it in enumerate(dq):
        if _ctx_dispatched(it):
            dq[i] = (it[0], it[1], it[2], _ctx_thread(it))   # 去掉第 5 元 → dispatched 归 False


def _spawn(coro):
    t = asyncio.create_task(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


def _sess_key(rec: dict, rid: str, thread: str | None = None) -> str:
    """Claude 多轮会话的 key：项目 + 房间，用户在线程里说话时再细分到线程。
    只按项目会让不同群 / 不同私聊用户落到同一 repo 时共用同一条会话而互相串台
    （B 接着 A 的对话、看见 A 说过的话）；房间维度隔离各入口。thread=线程根 event_id：
    线程会话在首次派活时从房间会话 fork（继承分叉点前的记忆，之后互相隔离，见 runner.ask）。"""
    base = f"{rec['id']}|{rid}"
    return f"{base}|{thread}" if thread else base


def _last_proj_file() -> str:
    return os.path.join(settings.store_path, "last_projects.json")


def _load_last_projects() -> None:
    """恢复各房间最近一次路由到的项目，让重启后 DM 的 /reset 与多轮延续仍能定位会话。"""
    try:
        with open(_last_proj_file()) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return
    if isinstance(data, dict):
        _last_project_by_room.update({k: v for k, v in data.items() if isinstance(v, str)})


def _save_last_projects() -> None:
    try:
        atomic_write_json(_last_proj_file(), _last_project_by_room)
    except OSError:
        pass


def _room_models_file() -> str:
    return os.path.join(settings.store_path, "room_models.json")


def _load_room_models() -> None:
    """恢复各房间通过 /model 设置的模型（房间属性，随重启保留；解绑仓库不清、退房才清）。"""
    try:
        with open(_room_models_file()) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return
    if isinstance(data, dict):
        _room_model.update({k: v for k, v in data.items() if isinstance(v, str) and v})


def _save_room_models() -> None:
    try:
        atomic_write_json(_room_models_file(), _room_model)
    except OSError:
        pass
