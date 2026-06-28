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
_last_proactive: dict[str, float] = defaultdict(float)
_sent_events: deque = deque(maxlen=4096)        # 自己发出的 event_id：防自激 + 识别"回复了 bot"（重启清空）
_last_project_by_room: dict[str, str] = {}      # room_id -> proj_id，供 DM /reset 定位会话
_project_last_active: dict[str, float] = defaultdict(float)   # proj_id -> 上次有人派活的时刻，自驱心跳据此避让
_group_engaged: dict[tuple[str, str], float] = {}   # (room_id, user) -> 上次点名/续话时刻：群里"对话延续窗口"用


def _spawn(coro):
    t = asyncio.create_task(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


def _sess_key(rec: dict, rid: str) -> str:
    """Claude 多轮会话的 key：项目 + 房间双维度。
    只按项目会让不同群 / 不同私聊用户落到同一 repo 时共用同一条会话而互相串台
    （B 接着 A 的对话、看见 A 说过的话）。带上房间维度后各入口的会话互相隔离。"""
    return f"{rec['id']}|{rid}"


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
        os.makedirs(settings.store_path, exist_ok=True)
        tmp = _last_proj_file() + ".tmp"
        with open(tmp, "w") as f:
            json.dump(_last_project_by_room, f, ensure_ascii=False)
        os.replace(tmp, _last_proj_file())
    except OSError:
        pass
