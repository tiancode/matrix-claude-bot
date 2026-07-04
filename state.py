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
# ↑ 每条是 (ts, sender, body, thread)：thread=线程根 event_id / None=顶层主时间线。会话按线程细分
# （见 _sess_key），背景也跟着按线程分范围——顶层任务的背景不该串进线程里说的话（取值用 _ctx_thread）。
_last_proactive: dict[str, float] = defaultdict(float)
_sent_events: deque = deque(maxlen=4096)        # 自己发出的 event_id：防自激 + 识别"回复了 bot"（重启清空）
_foreign_events: deque = deque(maxlen=4096)     # 查证过"不是 bot 发的"的 event_id：防重复向服务器拉取
_last_project_by_room: dict[str, str] = {}      # room_id -> proj_id，房间在弄哪个项目（自驱心跳/Gitea 健康度找汇报口）
_project_last_active: dict[str, float] = defaultdict(float)   # proj_id -> 上次有人派活的时刻，自驱心跳据此避让
_group_engaged: dict[tuple[str, str], float] = {}   # (room_id, user) -> 上次点名/续话时刻：群里"对话延续窗口"用


def _ctx_thread(item) -> str | None:
    """取背景条目 _context 的线程标记（第 4 元）。容忍老式 3 元组（没标记）→ 按顶层(None)算。"""
    return item[3] if len(item) > 3 else None


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
