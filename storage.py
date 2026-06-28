"""原子落盘小工具：写 <path>.tmp 再 os.replace，避免崩溃 / 并发读到半截文件。

各持久化模块（projects / pr_ledger / state / claude_runner / transcript / memory）原本各写一份
"建临时文件 → 写 → rename"骨架。这里收成一处，把"原子写"这件事做对一次。
调用方各自保留自己的 try/except（落盘失败按各自策略告警 / 静默 / 上抛），本模块不吞异常。
依赖方向：纯 stdlib 叶子，谁都可 import，不引入环。
"""
import json
import os


def atomic_write_text(path: str, text: str, *, fsync: bool = False) -> None:
    """把 text 原子写到 path。fsync=True 时落盘后强制刷盘（更耐崩溃，略慢）。
    失败抛 OSError，由调用方决定告警 / 静默 / 上抛。"""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
        if fsync:
            f.flush()
            os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_write_json(path: str, obj, *, ensure_ascii: bool = False,
                      indent: int | None = None, fsync: bool = False) -> None:
    """把 obj 序列化为 JSON 后原子写到 path。ensure_ascii / indent 语义同 json.dump。"""
    atomic_write_text(
        path, json.dumps(obj, ensure_ascii=ensure_ascii, indent=indent), fsync=fsync)
