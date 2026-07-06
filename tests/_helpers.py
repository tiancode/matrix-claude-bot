"""smoke 冒烟自检的共享夹具与桩。

sys.path 引导（让 `import bot` 找到 src/）+ 通用假对象/假客户端 + 各台账/健康度重置。
各主题模块 `from _helpers import (...)` 取用。产品代码零依赖本文件。
"""
import asyncio  # noqa: F401
import json     # noqa: F401
import os
import sys
import time     # noqa: F401
import types    # noqa: F401

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import bot                                  # noqa: E402,F401
import state                               # noqa: E402,F401
import dispatch, proactive, tasks, heartbeat, media  # noqa: E402,E401,F401
import claude_runner                       # noqa: E402,F401
import projects                            # noqa: E402,F401
import fmt                                 # noqa: E402,F401
import matrix_io                           # noqa: E402,F401
import addressing                          # noqa: E402,F401
import pr_followup                         # noqa: E402,F401
import issue_intake                        # noqa: E402,F401
from config import settings, redact        # noqa: E402,F401
from nio import RoomMessageText            # noqa: E402,F401

# 存量用例都是针对一次性进程路径写的（stub 掉 _run/_run_stream）；常驻进程模式有自己的
# 专门用例（test_persistent.py，自带假 CLI），这里统一关掉，别让两套路径混跑互相踩。
settings.claude_persistent = False

__all__ = [
    "asyncio", "json", "os", "sys", "time", "types",
    "bot", "state", "dispatch", "proactive", "tasks", "heartbeat", "media",
    "claude_runner", "projects", "fmt", "matrix_io", "addressing",
    "pr_followup", "issue_intake", "settings", "redact", "RoomMessageText",
    "FakeRoom", "make_event", "make_media_event", "set_identity",
    "_drain_tasks", "_reset_ledger", "_reset_issue_ledger", "_reset_inflight",
    "_reset_gitea_health", "_drain_and_run", "_CapClient", "_task_fixtures",
]

# ---------- 通用假对象 ----------
class FakeRoom:
    def __init__(self, room_id, n_users):
        self.room_id = room_id
        self.users = {f"@u{i}:ex.org": 1 for i in range(n_users)}

    def user_name(self, uid):
        return {"@alice:ex.org": "Alice", "@claudebot:ex.org": "claude-bot"}.get(uid, uid)

def make_event(body, sender="@alice:ex.org", event_id="$in1", in_reply_to=None,
               mentions=None, formatted_body=None):
    content = {}
    if in_reply_to:
        content["m.relates_to"] = {"m.in_reply_to": {"event_id": in_reply_to}}
    if mentions:
        content["m.mentions"] = {"user_ids": mentions}
    if formatted_body:
        content["formatted_body"] = formatted_body
    return types.SimpleNamespace(
        body=body, sender=sender, event_id=event_id,
        server_timestamp=int(time.time() * 1000), source={"content": content})

def make_media_event(body="app.log", filename=None, mxc="mxc://ex.org/abc",
                     sender="@alice:ex.org", event_id="$m1", size=None, mentions=None):
    content = {"url": mxc, "body": body, "msgtype": "m.file"}
    if filename:
        content["filename"] = filename
    if size is not None:
        content["info"] = {"size": size}
    if mentions:
        content["m.mentions"] = {"user_ids": mentions}
    return types.SimpleNamespace(
        body=body, sender=sender, event_id=event_id, url=mxc,
        server_timestamp=int(time.time() * 1000), source={"content": content})

def set_identity():
    state.MY_ID, state.MY_NAME, state.MY_LOCAL = "@claudebot:ex.org", "claude-bot", "claudebot"

async def _drain_tasks():
    for _ in range(50):
        pend = [t for t in state._tasks if not t.done()]
        if not pend:
            break
        await asyncio.gather(*pend, return_exceptions=True)

def _reset_ledger():
    import pr_ledger
    pr_ledger._led.reset()

def _reset_issue_ledger():
    import issue_ledger
    issue_ledger._led.reset()

def _reset_inflight():
    import inflight
    inflight._led.reset()

def _reset_gitea_health():
    import gitea
    import gitea_health
    gitea._health.update(consecutive_failures=0, last_success_ts=0.0,
                         last_failure_ts=0.0, last_code=0, last_kind="")
    gitea_health._alerted = False

def _drain_and_run(coro):
    """跑一个协程，并把它 _spawn 出的后台任务收割干净（命令类走 _spawn）。"""
    async def go():
        await coro
        for _ in range(50):
            pending = [t for t in state._tasks if not t.done()]
            if not pending:
                break
            await asyncio.gather(*pending, return_exceptions=True)
    asyncio.run(go())

class _CapClient:
    """记下所有 room_send 的完整 content（不只 body）与 redact，供线程/编辑/附件/回执断言。"""
    def __init__(self):
        self.sent = []
        self.uploaded = []
        self.redacted = []
        self.rooms = {}

    async def room_typing(self, *a, **k):
        return None

    async def room_redact(self, rid, eid, **k):
        self.redacted.append(eid)
        return types.SimpleNamespace(event_id="$rd%d" % len(self.redacted))

    async def room_get_event(self, rid, eid):
        # 线程起点回捞用：返回一条可辨认的假根消息
        return types.SimpleNamespace(event=types.SimpleNamespace(
            body=f"root-of-{eid}", sender="@alice:ex.org"))

    async def join(self, rid):
        self.rooms[rid] = types.SimpleNamespace(room_id=rid)   # 模拟 join 后房间进入 client.rooms
        return None

    async def room_send(self, rid, mt, content, **k):
        self.sent.append(content)
        return types.SimpleNamespace(event_id="$e%d" % len(self.sent))

    async def upload(self, provider, content_type="application/octet-stream",
                     filename=None, encrypt=False, filesize=None):
        f = provider(0, 0); f.read(); f.close()      # 确认文件真被读取
        self.uploaded.append((filename, content_type, encrypt))
        return types.SimpleNamespace(content_uri="mxc://ex.org/up%d" % len(self.uploaded)), None

def _task_fixtures():
    """群任务测试共用桩：绑定 rec + ensure + 返回"搞定"的 fake ask。"""
    rec = {"id": "h/o/r", "owner": "o", "repo": "r", "path": "/tmp", "base": "main", "host": "https://h"}
    bot.projects.get_room = lambda rid: rec
    async def fake_ensure(info):
        return rec
    bot.projects.ensure_project = fake_ensure
    async def fake_ask(key, prompt, cwd=None, system_prompt=None, lock_key=None, prepare=None,
                       on_delta=None, cancel_key=None, fork_from=None, **_kw):
        return "搞定"
    bot.runner.ask = fake_ask
    return rec
