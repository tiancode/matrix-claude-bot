#!/usr/bin/env python3
"""假 claude CLI：模拟 --input-format stream-json 的常驻进程协议，供 test_persistent 用。

行为：启动即报 system/init（session_id 由 --resume 派生：无=S1，有=<原sid>-r）；
每收到一条 stdin 用户消息，回一对 assistant/result（正文 `ok:<原文>|sid=<sid>`）。
提示词里含特定标记时模拟特殊行为：
  SPONT —— result 之后 0.2s 再自发吐一对 assistant/result（模拟后台任务完成续跑）
  DIE   —— result 之后立即退出（模拟进程崩溃/被杀）
"""
import json
import sys
import threading
import time

args = sys.argv[1:]
sid = "S1"
if "--resume" in args:
    try:
        sid = args[args.index("--resume") + 1] + "-r"
    except IndexError:
        pass

_wlock = threading.Lock()


def emit(obj):
    with _wlock:
        sys.stdout.write(json.dumps(obj) + "\n")
        sys.stdout.flush()


emit({"type": "system", "subtype": "init", "session_id": sid})

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        msg = json.loads(line)
    except json.JSONDecodeError:
        continue
    content = ((msg.get("message") or {}).get("content")) or []
    txt = "".join(b.get("text", "") for b in content if isinstance(b, dict))
    reply = f"ok:{txt}|sid={sid}"
    emit({"type": "assistant", "session_id": sid,
          "message": {"content": [{"type": "text", "text": reply}]}})
    emit({"type": "result", "session_id": sid, "result": reply, "is_error": False})
    if "SPONT" in txt:
        def _later():
            time.sleep(0.2)
            emit({"type": "assistant", "session_id": sid,
                  "message": {"content": [{"type": "text", "text": "bg-done"}]}})
            emit({"type": "result", "session_id": sid, "result": "bg-done", "is_error": False})
        threading.Thread(target=_later, daemon=True).start()
    if "DIE" in txt:
        time.sleep(0.05)
        sys.exit(0)
