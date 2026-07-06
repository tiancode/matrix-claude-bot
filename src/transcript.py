"""按房间持久化聊天逐字记录（store/transcripts/<房间>.jsonl），补"回溯更早对话"的短板。

会话有 24h 滑动 TTL、背景缓冲只有最近十几条且重启即清——都答不了"前天我们聊了什么"。
但原始记录其实没丢（在 Synapse 上）。本模块把每条**收到时的明文**按房间落成一行一条的
jsonl（E2EE 下也可靠，不必回解历史密文），并在派活时把文件**路径**注入系统提示——
让 agentic 的 Claude 在被问到更早对话时**自己去读 / grep**（与项目长期记忆同一套玩法：
告诉它存哪、按需取，而不是把整段历史塞进每次 prompt）。

- append():  收到 / 发出一条就追加（带保留天数 + 行数硬上限，滚动删旧）
- backfill(): 从 Matrix 时间线回灌"开启记录之前"的历史（只填 live 区间之前，不与现有重叠）
- augment_system_prompt(): 把"日志在哪、怎么用"拼到系统提示后

注意"明文"不是严格逐字：live 落的 body 可能带接收方补的「〔@了 谁〕」附注（@pill 在纯文本里
只剩显示名，这个附注是唯一的点名线索，见 addressing._mention_note）；backfill 回灌的历史行
没有这个附注。引用"用户原话"时〔〕括起来的附注不是用户打的字。

隐私：只记授权用户的消息（调用方已过权限闸），store/ 已 0700，受保留天数约束。默认关闭。
"""
import json
import logging
import os
import re
import time

from config import settings
from storage import atomic_write_text, store_root, throttled

log = logging.getLogger("matrix-claude.transcript")

_BAD = re.compile(r"[^A-Za-z0-9._-]+")
_MAX_BODY = 4000          # 单条正文上限（比背景缓冲的 1000 宽，留给回溯）
_PRUNE_EVERY = 3600       # 同一房间最多每小时整理一次（按保留天数 / 行数删旧）
_BACKFILL_PAGE = 100      # 回灌单页拉多少条
_MAX_BACKFILL_PAGES = 50  # 回灌翻页上限，兜底防失控

_last_prune: dict[str, float] = {}


def _root() -> str:
    # store/transcripts/ 的绝对路径。为什么必须绝对：见 storage.store_root。
    return store_root("transcripts")


def _safe(room: str) -> str:
    s = _BAD.sub("_", room or "")
    return (s.strip("._") or "room")[:120]


def path_for(room: str) -> str:
    return os.path.join(_root(), _safe(room) + ".jsonl")


def _marker(room: str) -> str:
    return os.path.join(_root(), "." + _safe(room) + ".bf")


def is_backfilled(room: str) -> bool:
    return os.path.exists(_marker(room))


def mark_backfilled(room: str) -> None:
    try:
        os.makedirs(_root(), exist_ok=True)
        open(_marker(room), "w").close()
    except OSError:
        pass


def discard(room: str) -> None:
    """删掉某房间的逐字记录 + 回灌标记（退房清理用）；文件不存在视作已清，不报错。"""
    for p in (path_for(room), _marker(room)):
        try:
            os.remove(p)
        except OSError:
            pass
    _last_prune.pop(room, None)


def _read_all(room: str) -> list[dict]:
    out: list[dict] = []
    try:
        with open(path_for(room)) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(r, dict) and "ts" in r:
                    out.append(r)
    except OSError:
        pass
    return out


def _write_all(room: str, records: list[dict]) -> None:
    path = path_for(room)
    try:
        atomic_write_text(
            path, "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records))
    except OSError as e:
        log.warning("聊天记录落盘失败 %s: %s", path, e)


def _prune(room: str) -> None:
    """按保留天数 + 行数硬上限删旧；只有真删了才重写文件。"""
    recs = _read_all(room)
    if not recs:
        return
    cutoff = time.time() - max(1, settings.transcript_keep_days) * 86400
    kept = [r for r in recs if r.get("ts", 0) >= cutoff]
    cap = max(1, settings.transcript_max_lines)
    if len(kept) > cap:
        kept = kept[-cap:]
    if len(kept) != len(recs):
        _write_all(room, kept)


def _maybe_prune(room: str) -> None:
    if throttled(_last_prune, room, _PRUNE_EVERY):   # 同一房间最多每 _PRUNE_EVERY 秒整理一次
        _prune(room)


def append(room: str, sender: str, body: str,
           event_id: str = "", ts: float | None = None) -> None:
    """追加一条记录（一行 JSON）。关闭 / 空正文直接跳过；best-effort，任何异常不影响主流程。"""
    if not settings.transcript_enabled:
        return
    body = (body or "").strip()
    if not body:
        return
    rec = {"id": event_id or "", "ts": round(ts if ts is not None else time.time(), 3),
           "sender": sender or "?", "body": body[:_MAX_BODY]}
    try:
        os.makedirs(_root(), exist_ok=True)
        with open(path_for(room), "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError as e:
        log.warning("聊天记录追加失败 %s: %s", room, e)
        return
    _maybe_prune(room)


def tail(room: str, n: int) -> list[dict]:
    """返回本房间最近 n 条记录（按时间先后），供 /summarize 追更小结用。无记录返回空表。"""
    recs = _read_all(room)
    return recs[-max(1, n):] if recs else []


def _dedup_sort(records: list[dict]) -> list[dict]:
    records.sort(key=lambda r: r.get("ts", 0))
    seen, out = set(), []
    for r in records:
        rid = r.get("id") or ""
        key = rid or (round(r.get("ts", 0), 1), r.get("sender"), r.get("body"))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


async def backfill(client, room_id: str, days: int | None = None) -> int:
    """从 Matrix 时间线往回翻，把"开启记录之前"的历史灌进本房间日志。返回新增条数。

    只收落在 [now-保留天数, 现有最早一条) 这个区间的文本/媒体事件——即**早于** live 记录的部分，
    天然不与已有重叠、免去跨时钟去重。E2EE 下解不开的历史密文会被跳过（拿不到就读不了）。
    """
    if not settings.transcript_enabled:
        return 0
    from nio import (MessageDirection, RoomMessagesResponse,
                     RoomMessageText, RoomMessageMedia, RoomEncryptedMedia)
    days = days or settings.transcript_backfill_days
    existing = _read_all(room_id)
    earliest = min((r.get("ts", 0) for r in existing), default=time.time()) or time.time()
    cutoff = time.time() - max(1, days) * 86400
    start = getattr(client, "next_batch", "") or ""
    if not start:
        return 0
    seen = {r.get("id") for r in existing if r.get("id")}
    collected: list[dict] = []
    pages, prev_start = 0, None
    while pages < _MAX_BACKFILL_PAGES:
        try:
            resp = await client.room_messages(
                room_id, start=start, direction=MessageDirection.back, limit=_BACKFILL_PAGE)
        except Exception:
            break
        if not isinstance(resp, RoomMessagesResponse) or not resp.chunk:
            break
        stop = False
        for ev in resp.chunk:
            ts = (getattr(ev, "server_timestamp", 0) or 0) / 1000
            if ts and ts < cutoff:        # 翻过保留窗口了，本页处理完就停
                stop = True
                continue
            if ts and ts >= earliest:     # 落在 live 区间，跳过避免重复
                continue
            if isinstance(ev, RoomMessageText):
                body = ev.body or ""
            elif isinstance(ev, (RoomMessageMedia, RoomEncryptedMedia)):
                body = "[文件] " + (ev.body or "")
            else:
                continue                  # 状态事件 / 解不开的密文 → 跳过
            body = body.strip()
            if not body:
                continue
            eid = getattr(ev, "event_id", "") or ""
            if eid and eid in seen:
                continue
            if eid:
                seen.add(eid)
            collected.append({"id": eid, "ts": round(ts, 3) if ts else round(time.time(), 3),
                              "sender": getattr(ev, "sender", "?"), "body": body[:_MAX_BODY]})
        pages += 1
        if stop or not resp.end or resp.end == prev_start:
            break
        prev_start, start = start, resp.end
    if collected:
        merged = _dedup_sort(existing + collected)
        keep_cut = time.time() - max(1, settings.transcript_keep_days) * 86400
        merged = [r for r in merged if r.get("ts", 0) >= keep_cut]
        cap = max(1, settings.transcript_max_lines)
        if len(merged) > cap:
            merged = merged[-cap:]
        _write_all(room_id, merged)
    mark_backfilled(room_id)
    return len(collected)


def augment_system_prompt(system_prompt: str, room: str) -> str:
    """把"本房间历史日志在哪、何时去读"拼到系统提示后。未开启 / 还没有日志就原样返回。"""
    if not settings.transcript_enabled:
        return system_prompt
    path = path_for(room)
    if not os.path.exists(path):
        return system_prompt
    block = (
        "\n\n【本房间完整聊天历史（按需查阅，不随会话 TTL 清空）】\n"
        f"逐行 JSON 日志在：{path}\n"
        "每行 {\"ts\": epoch秒, \"sender\": 说话人, \"body\": 正文}，按时间先后追加。\n"
        "当被问到更早的对话（如\"前天 / 上次我们聊了什么\"）、或需要超出当前上下文的历史时，"
        "直接读取 / grep 这个文件来回溯（用 ts 过滤时间段，单位 epoch 秒；可 `date -d @<ts>` 换算）。"
        "别整篇照搬，挑相关的概括给用户。"
    )
    return system_prompt + block
