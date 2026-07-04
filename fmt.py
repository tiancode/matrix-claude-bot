"""纯文本/富文本/上下文格式化助手（无 Matrix 调用）。"""
import re
import time

from config import settings
from state import _context, _ctx_thread, _ctx_dispatched


_HTML_TAGS = ["a", "b", "blockquote", "br", "caption", "code", "del", "details",
              "div", "em", "h1", "h2", "h3", "h4", "h5", "h6", "hr", "i",
              "li", "ol", "p", "pre", "span", "strike", "strong", "sub", "summary",
              "sup", "table", "tbody", "td", "th", "thead", "tr", "u", "ul"]



_HTML_ATTRS = {"a": ["href", "title"], "code": ["class"], "ol": ["start"],
               "span": ["data-mx-color", "data-mx-bg-color"]}



_CTX_GAP_SECS = 600     # 相邻消息间隔超过此值，在上下文里插一行"间隔"提示



_CTX_MAX_LINE = 1000    # 单条进上下文的最大字数，防长日志撑爆 prompt



_MEDIA_NAME_RE = re.compile(r"[^A-Za-z0-9._-]")



def _byte_chunks(s: str, size: int) -> list[str]:
    """按 ≤size 字节切块，不切碎多字节字符。"""
    out, buf, blen = [], [], 0
    for ch in s:
        cl = len(ch.encode())
        if blen + cl > size and buf:
            out.append("".join(buf)); buf, blen = [ch], cl
        else:
            buf.append(ch); blen += cl
    if buf:
        out.append("".join(buf))
    return out



def _balance_fences(chunks: list[str]) -> list[str]:
    """跨 chunk 补齐 ``` 代码围栏：块内结束就补 ```、下一块开头重开，避免分块后各自渲染 markdown 错乱。
    续块重开时带上原围栏的语言标记（```python 等），别让分块把语法高亮丢了。"""
    out, inside, lang = [], False, ""
    for ch in chunks:
        body = (f"```{lang}\n" + ch) if inside else ch
        for ln in ch.splitlines():     # 逐行跟踪：离开本块时是否仍在围栏内，以及当前围栏语言
            if ln.lstrip().startswith("```"):
                if inside:
                    inside = False
                else:
                    inside, lang = True, ln.lstrip().lstrip("`").strip()
        if inside:
            body += ("" if body.endswith("\n") else "\n") + "```"
        out.append(body)
    return out



def _split(text: str, size: int = 4000) -> list[str]:
    """按行切分（尽量不破坏代码块），单行超长才按字节硬切。size 单位字节。"""
    if len(text.encode()) <= size:
        return [text or ""]
    chunks, cur, n = [], [], 0
    for line in text.splitlines(keepends=True):
        lb = len(line.encode())
        if lb > size:  # 单行超长，按字节硬切
            if cur:
                chunks.append("".join(cur)); cur, n = [], 0
            chunks.extend(_byte_chunks(line, size))
            continue
        if n + lb > size and cur:
            chunks.append("".join(cur)); cur, n = [line], lb
        else:
            cur.append(line); n += lb
    if cur:
        chunks.append("".join(cur))
    return _balance_fences(chunks) or [""]



def _to_html(text: str) -> str | None:
    """markdown → 按 Matrix 允许标签消毒的 HTML；缺 markdown/bleach 则返回 None 退回纯文本（绝不外发未消毒 HTML）。"""
    try:
        import markdown
        import bleach
    except Exception:
        return None
    html = markdown.markdown(text, extensions=["fenced_code", "tables", "nl2br"])
    return bleach.clean(html, tags=_HTML_TAGS, attributes=_HTML_ATTRS, strip=True)



def _human_gap(sec: float) -> str:
    m = int(sec // 60)
    if m < 60:
        return f"约 {m} 分钟"
    if m < 60 * 48:   # 48 小时内按小时报，避免临界值四舍五入丢精度
        return f"约 {m / 60:.1f} 小时".replace(".0 ", " ")    # 1 位小数，整点去掉 .0（90 分钟→1.5 小时）
    return f"约 {m / 60 / 24:.1f} 天".replace(".0 ", " ")



def _human_bytes(n: float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{int(n)}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024



def _safe_name(s: str, fallback: str) -> str:
    """压成安全的扁平文件名段：非白名单字符换 _、去前导点，挡掉 ../ 路径穿越。"""
    s = _MEDIA_NAME_RE.sub("_", s or "")
    s = s.lstrip(".") or fallback
    return s[:120]



def _format_context(room_id: str, skip: tuple[str, str] | None = None,
                    drop_sender: str | None = None, thread: str | None = None,
                    drop_dispatched: bool = False) -> str:
    """把最近对话渲染成带时间的文本；跨度大处插"间隔"提示，让 Claude 自行判断旧话题是否相关。

    skip=(sender, body)：当前任务会单独给出，这里从背景里剔除它，免得喂两遍。
    drop_sender：派任务时传 bot 自己的名字——它过往的回复在 Claude 的续接会话里已经有了，
    再塞进背景纯属重复投喂、还容易让模型对着自己的旧话打转。
    thread：只渲染该范围的消息——None=顶层主时间线（不含任何线程里说的话），线程根 event_id=该线程内。
    会话按线程细分，背景也按线程隔离：顶层任务的背景不该串进线程的话，反之亦然。先按范围过滤再取末 n 条，
    免得末 n 条里混着别的范围、真正同范围的反而不够。
    drop_dispatched：续接会话时传 True——把「以前派过给 Claude 的用户消息」也剔掉（它们已在 --resume
    里，再喂就重复）。首轮/reset/过期这类会话为空的场景传 False：背景是唯一来源，派过的也得照常带。
    """
    n = settings.context_lines
    if n <= 0:                                              # n<=0 表示不带背景
        return ""
    items = [it for it in _context[room_id]                 # 先按线程范围（+续接时剔派过的）滤，再取末 n 条
             if _ctx_thread(it) == thread and not (drop_dispatched and _ctx_dispatched(it))][-n:]
    if skip and items:
        for i in range(len(items) - 1, -1, -1):   # 从最近往前找到这条任务并删掉
            if items[i][1:3] == skip:             # 只比 (sender, body)，第 4 元线程标记不参与
                del items[i]
                break
    if drop_sender:
        items = [it for it in items if it[1] != drop_sender]
    lines, prev_ts = [], None
    for ts, sender, body, *_ in items:
        if prev_ts is not None and ts - prev_ts > _CTX_GAP_SECS:
            lines.append(f"—— 间隔{_human_gap(ts - prev_ts)} ——")
        body = (body or "").strip()
        if len(body) > _CTX_MAX_LINE:
            body = body[:_CTX_MAX_LINE] + "…（已截断）"
        lines.append(f"[{time.strftime('%m-%d %H:%M', time.localtime(ts))}] {sender}: {body}")
        prev_ts = ts
    return "\n".join(lines)

