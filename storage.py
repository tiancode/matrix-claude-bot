"""原子落盘小工具：写 <path>.tmp 再 os.replace，避免崩溃 / 并发读到半截文件。

各持久化模块（projects / pr_ledger / state / claude_runner / transcript / memory）原本各写一份
"建临时文件 → 写 → rename"骨架。这里收成一处，把"原子写"这件事做对一次。
调用方各自保留自己的 try/except（落盘失败按各自策略告警 / 静默 / 上抛），本模块不吞异常。
依赖方向：只依赖 stdlib 与同为叶子的 config（config 只读环境变量、不 import 本模块），
谁都可 import，不引入环。
"""
import json
import os
import time

from config import settings


def throttled(last: dict, key: str, interval: float) -> bool:
    """节流闸：距 last[key] 记的上次执行不足 interval 秒 → 返回 False（这次跳过）；
    否则把当前时刻记进 last[key] 并返回 True（这次执行）。

    调用方各自持一个 {key: 上次时刻} 的内存 dict（重启清空无妨——无非早一点多跑一次，
    幂等）。transcript / digest 的每小时 prune 冷却是同一套逻辑，收敛到此只留一份。
    """
    now = time.time()
    if now - last.get(key, 0) < interval:
        return False
    last[key] = now
    return True


def store_root(*parts: str) -> str:
    """store/ 下某子目录的**绝对**路径：store_root("transcripts") → <store>/transcripts。

    跟随 settings.store_path（自检会把它指到临时目录，真实 store 不被污染）。
    必须取绝对路径——bot 进程 cwd 是 live dir、Claude 子进程 cwd 是 clone dir，相对路径会让
    Claude 把记忆/记录写进 clone 的 store、bot 却从 live 的 store 读，永远对不上；这条理由是
    memory / transcript / digest 三处 _root() 共同的根，收敛到此只留一份。
    """
    return os.path.abspath(os.path.join(settings.store_path, *parts))


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
