"""聊天「日摘要 + 主题索引」层：在原始逐字日志之上再叠一层可 KB 级检索的漏斗。

transcript.py 把每条消息落成一行一条的 jsonl，答得了"前天我们聊了什么"——但代价是
Claude 得把整段原始日志读回来才知道聊过啥。群一活跃，一天就上千行，问一句"以前是不是
讨论过某某问题"要 Claude 整篇扫，既慢又烧 token。

本模块把原始日志**离线预压**成两层结构（都在 store/transcripts/digests/<房间>/ 下，
与原始 jsonl 同级，好按房间对号）：
- <YYYY-MM-DD>.md：每天一份、按话题分条的摘要，每条带 `ts <起>–<止>` 指回原文区间；
- INDEX.md：每天一行 `日期: 主题；主题…` 的话题地图（增量合并去重）；
- .wm：水位线 JSON，记"已摘要到的最大 epoch 秒"，只往前推、断点续做。

于是被问历史时的读取量从"整段原文"降到"近 7 天索引（已注入）+ 按天点开的一小份摘要"：
时间型问题按日期直接开当天摘要、按 HH:MM 过滤；主题型问题先查索引定位到天再读日摘要；
只有需要逐字原话时，才按条目里的 ts 区间回原始 jsonl 切一小段（见 augment_system_prompt）。

风格对齐 transcript：全程 best-effort，任何异常都不该拖累主聊天流程；_root() 跟随
settings.store_path 且取绝对路径（原因见 storage.store_root）。默认开，受保留天数约束。

- should_digest(): 每条消息后被调用的便宜判断，攒够量 / 跨天残留才为真（触发后台生成）
- digest_room(): 真正干活——读水位线之后的新行、按东八区分天喂 runner.quick 压成摘要
- discard(): 退房清理，删该房间整个 digests 子目录
- augment_system_prompt(): 把近 7 天索引 + 查询协议拼进系统提示，引导 Claude 按需检索
"""
import json
import logging
import os
import re
import shutil
import time
from datetime import datetime, timedelta, timezone

from config import settings
from storage import atomic_write_text, store_root, throttled
from claude_runner import runner
import transcript

log = logging.getLogger("matrix-claude.digest")

_INDEX = "INDEX.md"
_WM = ".wm"
_MAX_BATCH = 200        # 单批喂给 runner.quick 的行数上限；超出就同日期内多批循环
_MAX_BATCH_CHARS = 60_000  # 单批正文字符上限：单条正文可达 4000 字（transcript._MAX_BODY），只按行数
                           # 切批最坏 800KB 会喂爆 quick 上下文→该批永久失败、水位线卡死，必须双限
_RECENT_DAYS = 7        # augment 注入的近 N 个自然日索引
_STALE_INTERVAL = 600   # 跨天残留（本地日期早于今天的未摘要行）只要距上次运行 ≥ 这么久就收尾
_PRUNE_EVERY = 3600     # 保留期清理同一房间最多每小时一次（仿 transcript._maybe_prune）

# 上次 digest_room 运行时刻 / 上次保留期清理时刻，纯内存态（仿 transcript._last_prune），
# 重启清空无妨——重启后无非早一点重跑一次摘要，幂等（水位线在盘上，不会重复处理旧行）。
_last_run: dict[str, float] = {}
_last_prune: dict[str, float] = {}
# 并发防抖：正在跑摘要的房间。同房间已在跑就直接返回，别叠第二遍（喂同一批给 quick）。
_in_progress: set[str] = set()


def _root() -> str:
    # 摘要挂在 transcripts/ 下的 digests/ 子目录（与原始 jsonl 同级，好按房间对号）。
    # 绝对路径，为什么必须绝对：见 storage.store_root。
    return store_root("transcripts", "digests")


def _room_dir(room: str) -> str:
    # <safe_room> 直接复用 transcript._safe()，让摘要目录和原始 <safe_room>.jsonl 对得上号。
    return os.path.join(_root(), transcript._safe(room))


# ---- 东八区（可配）日期分桶：不依赖服务器本地时区，跨机器 / 跨部署都一致 ----
def _tz() -> timezone:
    return timezone(timedelta(hours=settings.digest_tz_hours))


def _local_date(ts: float) -> str:
    """epoch 秒 → 东八区自然日 'YYYY-MM-DD'（日期分桶用）。"""
    return datetime.fromtimestamp(ts, _tz()).strftime("%Y-%m-%d")


def _local_hhmm(ts: float) -> str:
    """epoch 秒 → 东八区 'HH:MM'（摘要里显示用）。"""
    return datetime.fromtimestamp(ts, _tz()).strftime("%H:%M")


def _today() -> str:
    return datetime.now(_tz()).strftime("%Y-%m-%d")


# ---- 水位线：已摘要到的最大 ts，只往前推，断点续做的锚 ----
def _wm_path(room: str) -> str:
    return os.path.join(_room_dir(room), _WM)


def _read_wm(room: str) -> float:
    try:
        with open(_wm_path(room)) as f:
            return float((json.load(f) or {}).get("ts", 0))
    except (OSError, json.JSONDecodeError, ValueError, TypeError):
        return 0.0


def _write_wm(room: str, ts: float) -> None:
    try:
        atomic_write_text(_wm_path(room), json.dumps({"ts": ts}))
    except OSError as e:
        log.warning("摘要水位线落盘失败 %s: %s", room, e)


def should_digest(room: str) -> bool:
    """便宜的同步判断：现在值不值得为这个房间跑一次摘要。每条消息后都会被调用，故要省。

    为真的条件（满足其一）：
      a) 水位线之后的新行 ≥ digest_min_lines 且距本房间上次摘要运行 ≥ digest_min_interval 秒；
      b) 存在「本地日期早于今天」的未摘要行，且距上次运行 ≥ 600 秒
         （保证跨天后昨天的尾巴尽快被收掉，不必攒够行数）。
    digest_enabled 或 transcript_enabled 为假时恒 False。
    """
    if not (settings.digest_enabled and settings.transcript_enabled):
        return False
    path = transcript.path_for(room)
    # 先用 jsonl 的 mtime 做粗筛：文件自"已摘要到的时刻"以来没被动过，就必然没有新行——
    # 每条消息（append 到 live 记录）都会把 mtime 推到 now(>水位线)，故这层不会漏掉真新行，
    # 只是把"水位线之后无新增"的绝大多数调用挡在读文件之前（回灌灌进旧 ts 会误过粗筛，
    # 但下面读全量后 ts 过滤会兜住，顶多多读一次，不误判）。
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return False
    wm = _read_wm(room)
    if mtime <= wm:
        return False
    # 过了粗筛才读全量做精确统计。读取量与 transcript 每小时 prune 的 _read_all 同量级
    # （每房间受 transcript_max_lines 封顶），且只有真有新写入时才走到这，可接受。
    recs = [r for r in transcript._read_all(room) if r.get("ts", 0) > wm]
    if not recs:
        return False
    since = time.time() - _last_run.get(room, 0)
    if len(recs) >= settings.digest_min_lines and since >= settings.digest_min_interval:
        return True
    if since >= _STALE_INTERVAL:
        today = _today()
        if any(_local_date(r.get("ts", 0)) < today for r in recs):
            return True
    return False


def _batches(rows: list[dict]):
    """把同一日期的行切成喂 quick 的批：行数、正文字符量任一到顶就断批。
    单条超长（不可再分）也单独成一批——正文本身有 transcript._MAX_BODY 封顶，不会真爆。"""
    batch, chars = [], 0
    for r in rows:
        n = len(r.get("body", "") or "")
        if batch and (len(batch) >= _MAX_BATCH or chars + n > _MAX_BATCH_CHARS):
            yield batch
            batch, chars = [], 0
        batch.append(r)
        chars += n
    if batch:
        yield batch


def _build_prompt(date: str, batch: list[dict]) -> str:
    """把一批（同一东八区日期）对话行拼成喂给 runner.quick 的中文 prompt，严格约束输出格式。"""
    lines = []
    for r in batch:
        ts = int(r.get("ts", 0))
        body = (r.get("body", "") or "").replace("\n", " ")
        lines.append(f"[{_local_hhmm(ts)}|{ts}] {r.get('sender', '?')}: {body}")
    convo = "\n".join(lines)
    return (
        f"下面是某聊天群 {date}（东八区）的一段对话，每行格式 `[HH:MM|epoch秒] 说话人: 正文`。\n"
        "请把它压成**当天的话题摘要**，严格按下述格式输出，不要输出任何额外的标题 / 前言 / "
        "结语 / 代码围栏：\n"
        "1) 每个话题一条，形如：\n"
        "   `- **HH:MM–HH:MM** 主题短语：要点 / 结论 / 为什么（ts <起>–<止>）`\n"
        "   其中 HH:MM 取该话题首尾消息的时间，<起>/<止> 取对应那两条的 epoch 秒整数。\n"
        "2) 所有话题条目之后，另起**单独一行**输出主题清单：\n"
        "   `INDEX: 主题1；主题2；…`（中文分号「；」分隔，与上面各条目的主题短语对应）。\n"
        "闲聊 / 寒暄可合并或略过；只保留值得日后回看的话题。\n\n"
        f"对话：\n{convo}"
    )


def _split_topics(s: str) -> list[str]:
    """把 'a；b; c' 拆成去重且保序的主题列表（兼容中英文分号，去空白）。"""
    out, seen = [], set()
    for p in re.split(r"[；;]", s or ""):
        p = p.strip()
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _parse(text: str) -> tuple[list[str], list[str]]:
    """解析 runner.quick 的返回：拆出条目行（- …）与最后 INDEX: 行里的主题清单。

    宽松解析——模型偶尔多带前言 / 空行都无所谓，只挑以 - / * 开头的条目和 INDEX 行。
    """
    entries, topics = [], []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line[:6].upper() in ("INDEX:", "INDEX："):
            topics = _split_topics(line[6:])
        elif line[0] in "-*":
            entries.append("- " + line[1:].strip())
    return entries, topics


def _append_day(d: str, date: str, entries: list[str]) -> None:
    """把当天条目追加到 <date>.md（文件不存在先写 '# <date>' 标题行）。"""
    path = os.path.join(d, date + ".md")
    try:
        new = not os.path.exists(path)
        with open(path, "a") as f:
            if new:
                f.write(f"# {date}\n\n")
            f.write("\n".join(entries) + "\n")
    except OSError as e:
        log.warning("摘要日文件写入失败 %s: %s", path, e)


def _read_index(d: str) -> dict[str, list[str]]:
    """读 INDEX.md 成 {日期: [主题…]}；忽略不合 '日期: 主题' 形状的行。"""
    rows: dict[str, list[str]] = {}
    try:
        with open(os.path.join(d, _INDEX)) as f:
            for line in f:
                m = re.match(r"^(\d{4}-\d{2}-\d{2})\s*[:：]\s*(.*)$", line.strip())
                if m:
                    rows[m.group(1)] = _split_topics(m.group(2))
    except OSError:
        pass
    return rows


def _write_index(d: str, rows: dict[str, list[str]]) -> None:
    """按日期升序重写 INDEX.md（每天一行 '日期: 主题；主题…'）。"""
    body = "".join(f"{k}: {'；'.join(rows[k])}\n" for k in sorted(rows) if rows[k])
    try:
        atomic_write_text(os.path.join(d, _INDEX), body)
    except OSError as e:
        log.warning("摘要索引写入失败 %s: %s", d, e)


def _merge_index(d: str, date: str, topics: list[str]) -> None:
    """把 topics 合并进 INDEX.md 里 date 那一行（已有该行则按「；」去重后追加）。"""
    rows = _read_index(d)
    merged, seen = [], set()
    for t in rows.get(date, []) + topics:   # 已有的在前，保序去重
        if t and t not in seen:
            seen.add(t)
            merged.append(t)
    rows[date] = merged
    _write_index(d, rows)


def _apply(room: str, date: str, text: str) -> None:
    """把一批的模型输出落盘：条目进 <date>.md，主题进 INDEX.md。"""
    entries, topics = _parse(text)
    if not entries and not topics:
        return
    d = _room_dir(room)
    try:
        os.makedirs(d, exist_ok=True)
    except OSError as e:
        log.warning("建摘要目录失败 %s: %s", room, e)
        return
    if entries:
        _append_day(d, date, entries)
    if topics:
        _merge_index(d, date, topics)


async def digest_room(room: str) -> int:
    """真正干活：读水位线之后的新行、按东八区分天喂 runner.quick 压成摘要。返回覆盖的行数。

    并发防抖 → 按日期分组 → 每批 ≤200 行喂 quick → 落盘 → 每成功一批就把水位线推进到该批最大
    ts。runner.quick 抛异常则记 warning、水位线不动、吞掉异常返回已完成行数（下次触发重试剩余）。
    """
    if not (settings.digest_enabled and settings.transcript_enabled):
        return 0
    if room in _in_progress:
        return 0
    _in_progress.add(room)
    _last_run[room] = time.time()   # 进来就算"跑过一次"，让 should_digest 的间隔 / 600s 冷却生效
    done = 0
    try:
        wm = _read_wm(room)
        recs = [r for r in transcript._read_all(room) if r.get("ts", 0) > wm]
        recs.sort(key=lambda r: r.get("ts", 0))
        if not recs:
            return 0
        by_date: dict[str, list[dict]] = {}
        for r in recs:
            by_date.setdefault(_local_date(r.get("ts", 0)), []).append(r)
        for date in sorted(by_date):        # 日期升序处理，水位线单调前进
            for batch in _batches(by_date[date]):
                try:
                    text = await runner.quick(_build_prompt(date, batch))
                except Exception as e:
                    # 生成失败：水位线不动，返回已完成的行数——下次触发会从这批重试。
                    log.warning("摘要生成失败 %s %s: %s", room, date, e)
                    return done
                _apply(room, date, text)
                _write_wm(room, max(r.get("ts", 0) for r in batch))
                done += len(batch)
        _maybe_prune(room)
        return done
    finally:
        _in_progress.discard(room)


def _prune(room: str) -> None:
    """删掉本地日期早于保留期的日文件，INDEX.md 同步删行。"""
    d = _room_dir(room)
    if not os.path.isdir(d):
        return
    cutoff = _local_date(time.time() - max(1, settings.digest_keep_days) * 86400)
    try:
        names = os.listdir(d)
    except OSError:
        return
    for n in names:
        if re.match(r"^\d{4}-\d{2}-\d{2}\.md$", n) and n[:-3] < cutoff:
            try:
                os.remove(os.path.join(d, n))
            except OSError:
                pass
    rows = _read_index(d)
    kept = {k: v for k, v in rows.items() if k >= cutoff}
    if len(kept) != len(rows):
        _write_index(d, kept)


def _maybe_prune(room: str) -> None:
    if throttled(_last_prune, room, _PRUNE_EVERY):   # 同一房间最多每 _PRUNE_EVERY 秒清理一次
        _prune(room)


def discard(room: str) -> None:
    """删该房间整个 digests 子目录（退房清理用）；不存在不报错。"""
    try:
        shutil.rmtree(_room_dir(room))
    except OSError:
        pass
    _last_run.pop(room, None)
    _last_prune.pop(room, None)
    _in_progress.discard(room)


def augment_system_prompt(system_prompt: str, room: str) -> str:
    """把近 7 天主题索引 + 目录路径 + 查询协议拼到系统提示后。未开启 / 还没索引就原样返回。

    与 transcript.augment_system_prompt 分两层：transcript 给的是原始逐字日志的**指针**
    （逐字回溯的最终落点），本模块给的是**漏斗协议**——先看已注入的近 7 天索引、再按天点开
    日摘要定位，只有需要逐字原话时才回原始日志按 ts 切一小段，避免整篇读。
    """
    if not (settings.digest_enabled and settings.transcript_enabled):
        return system_prompt
    d = _room_dir(room)
    if not os.path.exists(os.path.join(d, _INDEX)):
        return system_prompt
    rows = _read_index(d)
    today = _today()
    start = _local_date(time.time() - (_RECENT_DAYS - 1) * 86400)
    recent = [f"{k}: {'；'.join(rows[k])}" for k in sorted(rows) if start <= k <= today]
    recent_block = "\n".join(recent) if recent else "（近 7 天暂无摘要）"
    jsonl = transcript.path_for(room)
    block = (
        "\n\n【本房间聊天日摘要 + 主题索引（先在这里定位，再按需回原始日志）】\n"
        f"摘要目录：{d}\n"
        "  · <YYYY-MM-DD>.md：每天一份、按话题分条的要点，条目里带 `ts <起>–<止>` 指回原文区间。\n"
        f"  · {os.path.join(d, _INDEX)}：每天一行 `日期: 主题；主题…`，是话题地图。\n"
        f"原始逐字日志（只有需要逐字原话时才切片，别整篇读）：{jsonl}\n"
        "近 7 天主题索引（已为你载入）：\n"
        f"————\n{recent_block}\n————\n"
        "查询协议：\n"
        "· 时间型问题（如「昨天中午 / 前天晚上聊了什么」）→ 直接打开对应日期的 <日期>.md，"
        "按条目开头的 HH:MM 过滤到那个时段。\n"
        "· 主题型问题（如「以前是不是讨论过某某」）→ 先看上面已载入的近 7 天索引；更早的去 "
        f"`grep` {os.path.join(d, _INDEX)} 定位到是哪一天，再打开那天的 <日期>.md。\n"
        "· 只有需要逐字原话时，才按条目里的 ts 区间去原始日志切片（用 ts 过滤，单位 epoch 秒；"
        "可 `date -d @<ts>` 换算），别整篇照搬。"
    )
    return system_prompt + block
