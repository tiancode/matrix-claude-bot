"""Matrix × Claude Code 机器人。

监听所在房间的消息：单聊 / 被 @ / 被回复 / 命中触发词时调用 Claude Code 干活并回复；
PROACTIVE 模式下还会对"像在求助"的消息判断要不要主动插话。
登录态与 E2EE 密钥持久化在 store_path，重启复用同一设备。
"""
import asyncio
import json
import logging
import os
import re
import tempfile
import time
from collections import deque, defaultdict
from contextlib import asynccontextmanager

from nio import (
    AsyncClient,
    AsyncClientConfig,
    InviteMemberEvent,
    LoginResponse,
    MatrixRoom,
    RoomEncryptedMedia,
    RoomMessageMedia,
    RoomMessageText,
    WhoamiResponse,
)

from config import settings, redact, register_secret
from claude_runner import runner
from projects import projects, parse_repo_url, proj_id
import memory
import gitea
import pr_ledger
import transcript

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("matrix-claude")

# E2EE 是否可用（需系统 libolm + matrix-nio[e2e]）
try:
    import olm  # noqa: F401
    OLM_OK = True
except Exception:
    OLM_OK = False

E2E = settings.enable_e2e and OLM_OK

RESET_CMDS = {"/reset", "/new", "重置", "新对话", "清空"}
ACTIONABLE_HINTS = ("?", "？", "帮", "求", "怎么", "如何", "能不能", "可以吗",
                    "报错", "error", "bug", "失败", "搞一下", "弄一下", "处理")

_synced = False     # 初始 sync 完成前不处理回放的历史/积压消息
_tasks: set = set()
_context: dict[str, deque] = defaultdict(lambda: deque(maxlen=max(4, settings.context_lines * 2)))
_last_proactive: dict[str, float] = defaultdict(float)
_sent_events: deque = deque(maxlen=4096)        # 自己发出的 event_id：防自激 + 识别"回复了 bot"（重启清空）
_last_project_by_room: dict[str, str] = {}      # room_id -> proj_id，供 DM /reset 定位会话
_project_last_active: dict[str, float] = defaultdict(float)   # proj_id -> 上次有人派活的时刻，自驱心跳据此避让

client: AsyncClient | None = None
MY_ID = ""
MY_NAME = ""
MY_LOCAL = ""


def _spawn(coro):
    t = asyncio.create_task(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


def _authorized(user_id: str) -> bool:
    """谁能驱动机器人及邀请它进房。ALLOW_USERS 空=所有人；非空则须在名单内。"""
    return not settings.allow_users or user_id in settings.allow_users


def _has_trigger(text: str) -> bool:
    """文本里是否出现触发词。拉丁触发词按 token 边界匹配（claude 不命中 claudette），
    CJK 等非 ASCII 触发词没有词边界概念、按子串。空触发词=从不命中。"""
    tp = settings.trigger_phrase
    if not tp:
        return False
    if tp.isascii():
        return re.search(rf"(?<![\w-]){re.escape(tp.lower())}(?![\w-])", text.lower()) is not None
    return tp in text


def _strip_trigger(text: str) -> str:
    """从正文里去掉触发词（与 _has_trigger 同样的边界规则，别把词内的子串也抠掉）。"""
    tp = settings.trigger_phrase
    if not tp:
        return text
    if tp.isascii():
        return re.sub(rf"(?<![\w-]){re.escape(tp)}(?![\w-])", "", text, flags=re.I)
    return text.replace(tp, "")


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


# Matrix 富文本允许的标签/属性子集，用于消毒 Claude 输出夹带的 HTML。
# 不含 img：外发富文本里的 <img src> 会让每个查看者客户端去拉该 URL（追踪像素 / 查看者 IP 泄露），用不到就禁掉。
_HTML_TAGS = ["a", "b", "blockquote", "br", "caption", "code", "del", "details",
              "div", "em", "h1", "h2", "h3", "h4", "h5", "h6", "hr", "i",
              "li", "ol", "p", "pre", "span", "strike", "strong", "sub", "summary",
              "sup", "table", "tbody", "td", "th", "thead", "tr", "u", "ul"]
_HTML_ATTRS = {"a": ["href", "title"], "code": ["class"], "ol": ["start"],
               "span": ["data-mx-color", "data-mx-bg-color"]}


def _to_html(text: str) -> str | None:
    """markdown → 按 Matrix 允许标签消毒的 HTML；缺 markdown/bleach 则返回 None 退回纯文本（绝不外发未消毒 HTML）。"""
    try:
        import markdown
        import bleach
    except Exception:
        return None
    html = markdown.markdown(text, extensions=["fenced_code", "tables", "nl2br"])
    return bleach.clean(html, tags=_HTML_TAGS, attributes=_HTML_ATTRS, strip=True)


_SEND_MAX_TRIES = 3   # 被限流(M_LIMIT_EXCEEDED)时的额外重试次数（nio 自身也会重试若干次，这里再兜一层）


async def _send_chunk(room_id: str, content: dict) -> str | None:
    """发送一块消息；被限流就按 retry_after 退避重试。成功返回 event_id，彻底失败返回 None。"""
    for attempt in range(_SEND_MAX_TRIES):
        resp = await client.room_send(
            room_id, "m.room.message", content, ignore_unverified_devices=True,
        )
        eid = getattr(resp, "event_id", None)
        if eid:
            return eid
        status = getattr(resp, "status_code", "") or ""
        if status == "M_LIMIT_EXCEEDED" and attempt < _SEND_MAX_TRIES - 1:
            ms = getattr(resp, "retry_after_ms", None)
            delay = min((ms / 1000) if isinstance(ms, (int, float)) and ms > 0 else 1.5 * (attempt + 1), 10)
            log.info("发送到 %s 被限流，%.1fs 后重试（%d/%d）", room_id, delay, attempt + 1, _SEND_MAX_TRIES)
            await asyncio.sleep(delay)
            continue
        # 非限流错误，或重试用尽：这块丢了（限流 / 加密房间没配好 / 服务器报错）
        log.warning("发送到 %s 失败（这块消息丢了）: %s", room_id, resp)
        return None
    return None


async def send(room_id: str, text: str, track: bool = False):
    """发消息到房间。track=True 才把这条并入房间上下文（任务答复/主动插话该 track，状态/回执/报错不进）。
    event_id 始终登记，用于防自激与"回复了 bot"识别。"""
    text = redact(text)  # 任何外发文本都抹掉凭证
    any_ok = False
    all_ok = True
    for chunk in _split(text):
        content = {"msgtype": "m.text", "body": chunk}
        html = _to_html(chunk)
        if html:
            content["format"] = "org.matrix.custom.html"
            content["formatted_body"] = html
        eid = await _send_chunk(room_id, content)
        if eid:
            _sent_events.append(eid)
            any_ok = True
        else:
            all_ok = False
    # 投递有丢就发条提示（短消息常比长回复更易发成功）；提示也登记 event_id 防自激回显。
    notice = None
    if any_ok and not all_ok:
        notice = "（部分内容发送失败，上面的回复可能不完整）"
    elif not any_ok and text:
        notice = "（回复发送失败，请稍后重试或检查日志）"
    if notice:
        nid = await _send_chunk(room_id, {"msgtype": "m.text", "body": notice})
        if nid:
            _sent_events.append(nid)
    # 整条都发出去才并入上下文：半截投递不该被当成"已完整说过"喂回后续 prompt
    if any_ok and all_ok and track:
        dq = _context[room_id]
        ts = time.time()
        if dq:
            ts = max(ts, dq[-1][0])  # 防钟差让回复显示得比刚收到的消息还早
        dq.append((ts, MY_NAME or "bot", text))
        transcript.append(room_id, MY_NAME or "bot", text, ts=ts)  # bot 自己的回复也进历史


def _is_dm(room: MatrixRoom) -> bool:
    """恰好 2 人的房间才算单聊。取本地/服务器成员数的较大者。
    只认 ==2：成员未同步(0) 或刚加入只同步到 bot 自己(1) 都不当单聊，
    免得 REPLY_IN_DM_ALWAYS 在群成员还没拉全的窗口里把群当私聊逐条自动回。"""
    try:
        count = max(len(room.users), getattr(room, "member_count", 0) or 0)
        return count == 2
    except Exception:
        return False


def _strip_reply_fallback(body: str, content: dict) -> str:
    """去掉回复消息顶部的 `> <@发送者> 原文` 引用回退块，只留用户真正写的内容。
    仅当首行确像引用头才剥，避免误伤用户自己以 `>` 开头的正文。"""
    if not content.get("m.relates_to", {}).get("m.in_reply_to"):
        return body
    lines = body.split("\n")
    if not (lines and re.match(r">\s*<[^>]+>", lines[0])):
        return body
    i = 0
    while i < len(lines) and lines[i].startswith(">"):
        i += 1
    return "\n".join(lines[i:]).strip()


def _strip_self_mentions(text: str) -> str:
    """去掉指向 bot 的各种 @ 形式：完整 MXID、@显示名、@本地名。
    先去完整 MXID，免得只剥掉 @本地名却把 :host 留在正文里。"""
    if MY_ID:
        text = text.replace(MY_ID, "")
    for tok in (MY_NAME, MY_LOCAL):
        if tok:  # 带右边界，别把 @bottle 里的 @bot 也剥了
            text = re.sub(rf"@{re.escape(tok)}(?![\w-])", "", text)
    return text


def _is_addressed(room: MatrixRoom, event: RoomMessageText) -> tuple[bool, str]:
    """返回 (是否点名机器人, 去掉@/触发词/引用块后的正文)。

    群里认真正的点名（m.mentions / 富文本@pill / 显式 @名字）和直接回复 bot 的消息，
    都不把引用块内容算进点名，避免别人引用你的消息时误触发。
    """
    body = event.body or ""
    content = (event.source or {}).get("content", {})
    task_text = _strip_reply_fallback(body, content)

    # 单聊默认必回
    if settings.reply_in_dm and _is_dm(room):
        return True, task_text.strip()

    # 直接回复 bot 之前发的消息 → 视为点名
    in_reply_to = (content.get("m.relates_to", {})
                   .get("m.in_reply_to", {}).get("event_id"))
    replied_to_bot = bool(in_reply_to) and in_reply_to in _sent_events

    # 触发词（拉丁词按词边界，免得 claude 命中 claudette 并把词切碎）
    if _has_trigger(task_text):
        return True, _strip_trigger(task_text).strip()

    # 真正的点名（剔除富文本 <mx-reply> 引用块，只看正文 @pill）
    mentioned = MY_ID in content.get("m.mentions", {}).get("user_ids", [])
    if not mentioned and MY_ID:
        fb = re.sub(r"<mx-reply>.*?</mx-reply>", "",
                    content.get("formatted_body", "") or "", flags=re.S | re.I)
        if MY_ID in fb:
            mentioned = True
    if not mentioned:
        low = task_text.lower()
        for tok in (MY_NAME, MY_LOCAL):  # 要带 @ 且带右边界，@bot 不误命中 @bottle
            if tok and re.search(rf"@{re.escape(tok.lower())}(?![\w-])", low):
                mentioned = True
                break

    if mentioned or replied_to_bot:
        return True, _strip_self_mentions(task_text).strip()

    return False, task_text.strip()


def _looks_actionable(body: str) -> bool:
    low = body.lower()
    return any(h.lower() in low for h in ACTIONABLE_HINTS)


_CTX_GAP_SECS = 600     # 相邻消息间隔超过此值，在上下文里插一行"间隔"提示
_CTX_MAX_LINE = 1000    # 单条进上下文的最大字数，防长日志撑爆 prompt


def _human_gap(sec: float) -> str:
    m = int(sec // 60)
    if m < 60:
        return f"约 {m} 分钟"
    if m < 60 * 48:   # 48 小时内按小时报，避免临界值四舍五入丢精度
        return f"约 {m / 60:.1f} 小时".replace(".0 ", " ")    # 1 位小数，整点去掉 .0（90 分钟→1.5 小时）
    return f"约 {m / 60 / 24:.1f} 天".replace(".0 ", " ")


def _format_context(room_id: str, skip: tuple[str, str] | None = None,
                    drop_sender: str | None = None) -> str:
    """把最近对话渲染成带时间的文本；跨度大处插"间隔"提示，让 Claude 自行判断旧话题是否相关。

    skip=(sender, body)：当前任务会单独给出，这里从背景里剔除它，免得喂两遍。
    drop_sender：派任务时传 bot 自己的名字——它过往的回复在 Claude 的续接会话里已经有了，
    再塞进背景纯属重复投喂、还容易让模型对着自己的旧话打转。
    """
    n = settings.context_lines
    items = list(_context[room_id])[-n:] if n > 0 else []   # n<=0 表示不带背景（不能用切片，[-0:] 会取到全部）
    if skip and items:
        for i in range(len(items) - 1, -1, -1):   # 从最近往前找到这条任务并删掉
            if items[i][1:] == skip:
                del items[i]
                break
    if drop_sender:
        items = [it for it in items if it[1] != drop_sender]
    lines, prev_ts = [], None
    for ts, sender, body in items:
        if prev_ts is not None and ts - prev_ts > _CTX_GAP_SECS:
            lines.append(f"—— 间隔{_human_gap(ts - prev_ts)} ——")
        body = (body or "").strip()
        if len(body) > _CTX_MAX_LINE:
            body = body[:_CTX_MAX_LINE] + "…（已截断）"
        lines.append(f"[{time.strftime('%m-%d %H:%M', time.localtime(ts))}] {sender}: {body}")
        prev_ts = ts
    return "\n".join(lines)


def _employee_prompt(info: dict) -> str:
    """把 Claude Code 当成负责该仓库的远程工程师的工作流说明。"""
    base, host = info["base"], info["host"]
    owner, repo = info["owner"], info["repo"]
    return (
        f"你是团队的一名远程工程师，通过 Matrix 群接收任务，负责仓库 {owner}/{repo}"
        f"（Gitea: {host}）。当前工作目录就是该仓库的本地 checkout。像真实员工一样把活干完：\n"
        f"1) 先理解任务；信息不足就在回复里直接提问，不要瞎猜。\n"
        f"2) 改代码时：先 git fetch，从 origin/{base} 建分支 claude/<简短任务名>，改完 git add/commit，"
        f"push 到 origin（remote 已配好鉴权）。\n"
        f"3) 然后用 Gitea API 开 PR（token 在环境变量 GITEA_TOKEN）：\n"
        f"   curl -sS -X POST {host}/api/v1/repos/{owner}/{repo}/pulls "
        f"-H \"Authorization: token $GITEA_TOKEN\" -H 'Content-Type: application/json' "
        f"-d '{{\"head\":\"<你的分支>\",\"base\":\"{base}\",\"title\":\"<标题>\",\"body\":\"<说明>\"}}'\n"
        f"   从返回 JSON 取 html_url，并在最终回复里**附上 PR 链接**。\n"
        f"4) 纯问答/查代码/无需改动：直接简洁中文回答，不用建分支或开 PR。\n"
        f"用简洁中文回复，内容会直接发到群里。"
    )


async def do_bind(room: MatrixRoom, repo: dict,
                  event: RoomMessageText | None = None, task_text: str = ""):
    rid = room.room_id
    try:
        await send(rid, f"⏳ 正在绑定并 clone {repo['owner']}/{repo['repo']} …")
        rec = await projects.bind_room(rid, repo)
        runner.reset(_sess_key(rec, rid))  # 换仓库 → 重置本房间在该项目上的会话
        await send(rid, f"✅ 已绑定 {rec['owner']}/{rec['repo']}（base: {rec['base']}）。直接在群里派活就行。")
    except Exception as e:
        log.exception("绑定失败")
        await send(rid, f"绑定失败：{e}")
        return
    if task_text and event is not None:   # 绑定后若还跟了任务，接着派下去
        await handle_task(room, event, task_text)


async def _backfill_cmd(room: MatrixRoom, body: str):
    """/backfill [天数]：从 Matrix 时间线回灌本房间在"开启记录前"的历史。"""
    rid = room.room_id
    if not settings.transcript_enabled:
        await send(rid, "聊天历史记录未开启（在 .env 设 TRANSCRIPT_ENABLED=1 再重启）。")
        return
    parts = body.split()
    days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else settings.transcript_backfill_days
    await send(rid, f"📚 正在从 Matrix 回灌最近 {days} 天的聊天历史…")
    try:
        n = await transcript.backfill(client, rid, days)
        await send(rid, f"✅ 回灌完成，新增 {n} 条历史。之后问我“前天/上次聊了什么”就能回溯了。"
                        if n else "✅ 没有可回灌的更早历史（可能已灌过、或服务器/加密取不到更早的）。")
    except Exception as e:
        log.exception("回灌失败")
        await send(rid, f"回灌出错：{e}")


async def _auto_backfill(room_id: str):
    """开启记录后首次启动时，对还没灌过的房间静默回灌一次历史（不在房间里刷消息）。"""
    try:
        n = await transcript.backfill(client, room_id)
        if n:
            log.info("[%s] 历史回灌 %d 条", room_id, n)
    except Exception:
        log.exception("历史回灌失败 %s", room_id)


async def on_message(room: MatrixRoom, event: RoomMessageText):
    if not settings.process_backlog and not _synced:  # 跳过历史/离线积压
        return
    if event.event_id in _sent_events:  # 防自激
        return
    if settings.room_allowlist and room.room_id not in settings.room_allowlist:
        return

    # 权限闸（在入上下文前）：未授权用户内容既不派活也不进 _context，否则会被当"最近对话"做间接 prompt 注入。
    is_self = event.sender == MY_ID
    if not is_self and not _authorized(event.sender):
        log.debug("忽略无权限用户消息: %s", event.sender)
        return

    body = _strip_reply_fallback(event.body or "", (event.source or {}).get("content", {}))
    sender_name = room.user_name(event.sender) or event.sender
    # 用本地接收时刻而非 event.server_timestamp：与 send() 里 bot 回复同一时钟，
    # _format_context 的"间隔"提示才不会因收/发两端时钟偏差算错。
    _context[room.room_id].append((time.time(), sender_name, body))
    transcript.append(room.room_id, sender_name, body, event_id=event.event_id)  # 落盘逐字记录，供回溯

    # 用自己账号跑时：消息照进上下文，但无触发词不当派活（否则会去回你发给别人的话）。
    if is_self and not _has_trigger(event.body or ""):
        return

    if body.strip().lower().startswith("/backfill"):   # 元命令：从 Matrix 回灌本房间历史
        _spawn(_backfill_cmd(room, body.strip()))
        return

    addressed, cleaned = _is_addressed(room, event)

    # 绑定仓库：仅群聊；/bind 显式，或未绑定群里"仅一条仓库 URL"。DM 交给 handle_task 自动分诊。
    repo = parse_repo_url(body)
    if repo and not _is_dm(room):
        is_bind = body.strip().lower().startswith("/bind")
        rest = re.sub(r"\S*://\S+|git@\S+", "", body.strip(), count=1).strip()
        just_url = len(rest) <= 3   # 去掉 URL 后基本没剩内容才自动绑定，闲聊带链接不算
        bound = projects.get_room(room.room_id)
        # 未绑定群里：纯链接自动绑；被点名时（哪怕同条还带了任务）也先绑再派；/bind 永远显式绑。
        if is_bind or (not bound and (just_url or addressed)):
            task_text = body.strip()
            if is_bind:
                task_text = task_text[len("/bind"):].strip()
            task_text = re.sub(r"\S*://\S+|git@\S+", "", task_text, count=1).strip()
            task_text = _strip_self_mentions(task_text).strip()   # 去掉 @bot，别把点名混进任务正文
            _spawn(do_bind(room, repo, event, task_text))
            return
        # 群已绑别的仓库：裸 URL 不自动换绑（防误触），给个提示而非静默
        if just_url and bound and proj_id(repo) != bound["id"]:
            _spawn(send(room.room_id,
                        f"这个群已绑定 {bound['owner']}/{bound['repo']}；要换绑请用 /bind <仓库URL>。"))
            return

    if addressed:
        _spawn(handle_task(room, event, cleaned))
    elif body.strip() in RESET_CMDS:   # 群里不点名也认重置（重置是元命令，不必 @ 机器人）
        _spawn(handle_task(room, event, body.strip()))
    elif settings.proactive:
        _spawn(maybe_proactive(room, event, body))


def _token_match(needle: str, hay_low: str) -> bool:
    """needle 是否作为完整 token 出现在 hay_low（已小写）里：两侧不接 [\\w-]，
    避免裸子串误命中（app 不命中 app-backend）。"""
    return re.search(rf"(?<![\w-]){re.escape(needle.lower())}(?![\w-])", hay_low) is not None


def _match_project_exact(text: str, known: list[dict]) -> dict | None:
    """强信号：文本里出现 owner/repo 或完整 id（token 边界匹配，多命中取最长/最具体）。
    可信，放在 LLM 分诊之前。"""
    low = text.lower()
    best, best_len = None, -1
    for p in known:
        for needle in (f"{p['owner']}/{p['repo']}", p["id"]):
            if len(needle) > best_len and _token_match(needle, low):
                best, best_len = p, len(needle)
    return best


def _match_project_by_repo_name(text: str, known: list[dict]) -> dict | None:
    """弱信号：仅裸仓库名（词边界 + 长度>=3）命中。仓库名常是 app/api/web 这类大众词，
    容易误命中，所以只当分诊失败后的兜底，放在 _triage 之后。"""
    low = text.lower()
    for p in known:
        r = p["repo"].lower()
        if len(r) >= 3 and re.search(rf"(?<![\w-]){re.escape(r)}(?![\w-])", low):
            return p
    return None


async def _triage(text: str, known: list[dict], context: str = "") -> str | None:
    """让 Claude 轻量判断这条 DM 属于哪个项目，返回 proj_id 或 None。
    带上最近对话，"再加个单测吧"这类没点名项目的延续消息才判得准。"""
    lst = "\n".join(f"- {p['id']} ({p['owner']}/{p['repo']})" for p in known)
    ctx_block = (f"\n\n【这个私聊里最近的对话，供判断归属参考】\n{context}" if context else "")
    prompt = (
        "已知项目列表：\n" + lst + ctx_block +
        f"\n\n有人私聊发来一条消息：\n\"\"\"\n{text}\n\"\"\"\n"
        "结合上面的对话判断这条消息：\n"
        "- 若是针对上面某个项目的活，只回复该项目标识（第一列那种 host/owner/repo 形式）；\n"
        "- 若是一般性问题 / 通用编程求助 / 闲聊，不针对任何具体项目，只回复 GENERAL；\n"
        "- 若确实针对某项目但认不出是哪个，只回复 NONE。"
    )
    try:
        ans = (await runner.quick(prompt)).strip()
    except Exception:
        return None
    low = ans.lower()   # 先按 token 边界 + 取最长匹配真实项目（项目名里含 general 也不会被误判成通用）
    best, best_len = None, -1
    for p in known:
        for needle in (p["id"], f"{p['owner']}/{p['repo']}"):
            if len(needle) > best_len and _token_match(needle, low):
                best, best_len = p["id"], len(needle)
    if best:
        return best
    if "GENERAL" in ans.upper():   # 不属于任何项目的一般性问题
        return TRIAGE_GENERAL
    return None


TRIAGE_GENERAL = "__GENERAL__"   # _triage 判定这是不属于任何项目的一般性问题
_GENERAL_ID = "__general__"      # 通用助手会话 / 串行锁的 key


def _general_rec() -> dict:
    """不绑项目的"通用助手"伪记录：在隔离的 _scratch 目录里答一般性问题。"""
    return {"id": _GENERAL_ID, "general": True, "path": settings.claude_workdir}


async def _dispatch(room: MatrixRoom, event: RoomMessageText, text: str) -> dict | None:
    """决定这条消息归到哪个项目。返回项目记录（general=True 表示当通用助手答）；
    None=已就地回复/提问，不再继续。"""
    rid = room.room_id

    if not _is_dm(room):  # 群：用房间绑定
        rec = projects.get_room(rid)
        if rec:
            return await projects.ensure_project(rec)  # 校验/按需修复本地 checkout
        await send(rid, "这个群还没绑定仓库。发一下 Gitea 仓库地址（或 /bind <仓库URL>），我就开工。")
        return None

    # DM：自动分诊到对应项目。每条路由都要过 ensure_project——本地 checkout 可能被删/没 clone，
    # 不校验 Claude 会在空目录里干活。
    repo = parse_repo_url(text)                  # 1) 消息里带仓库链接
    if repo:
        return await projects.ensure_project(repo)
    known = projects.list_projects()
    exact = _match_project_exact(text, known)    # 2) 强信号：owner/repo 或完整 id
    if exact:
        return await projects.ensure_project(exact)
    if not known:                                # 3) 还没有任何已知项目
        await send(rid, "我还不知道你指的是哪个项目，发个 Gitea 仓库地址给我吧。")
        return None
    pid = await _triage(text, known, _format_context(rid))   # 4) 轻量分诊（带最近对话）
    if pid == TRIAGE_GENERAL:                  # 一般性问题：直接当通用助手答，不必非得挂到某个项目
        return _general_rec()
    if pid:
        rec = projects.get_project(pid)
        if rec:
            return await projects.ensure_project(rec)
    loose = _match_project_by_repo_name(text, known)   # 5) 兜底：裸仓库名（分诊放弃后才用）
    if loose:
        return await projects.ensure_project(loose)
    last_pid = _last_project_by_room.get(rid)    # 6) 仍判不出 → 沿用这个 DM 上次的项目，
    if last_pid:                                 #    别让多轮对话里每条延续消息都被反问"这是关于哪个项目的"
        rec = projects.get_project(last_pid)
        if rec:
            return await projects.ensure_project(rec)
    lst = "\n".join(f"- {p['owner']}/{p['repo']}" for p in known)   # 7) 实在不行才反问
    await send(rid, f"这条是关于哪个项目的？回复项目名或发仓库地址：\n{lst}")
    return None


async def _keep_typing(rid: str, stop: asyncio.Event):
    """任务运行期间周期性续期"正在输入"（单次 30s，任务可长达数百秒）。"""
    try:
        while not stop.is_set():
            await client.room_typing(rid, True, timeout=30000)
            try:
                await asyncio.wait_for(stop.wait(), timeout=25)
            except asyncio.TimeoutError:
                pass
    except Exception:
        pass


@asynccontextmanager
async def _typing(rid: str):
    """进入即开始持续发"正在输入"，退出时停掉并收割续期任务。
    覆盖从 DM 分诊 / clone 到任务执行的整段耗时，别让用户对着空房间干等。"""
    stop = asyncio.Event()
    ticker = asyncio.create_task(_keep_typing(rid, stop))
    try:
        yield
    finally:
        stop.set()
        ticker.cancel()
        try:
            await ticker
        except asyncio.CancelledError:
            pass
        try:
            await client.room_typing(rid, False)
        except Exception:
            pass


async def _run_on_project(room: MatrixRoom, event: RoomMessageText, text: str, rec: dict,
                          skip_body: str | None = None):
    """在某项目上跑任务并回发。"正在输入"由调用方 handle_task 的 _typing 统一负责。
    skip_body：当前这条消息在背景上下文里的原文，用于从背景里剔除它免得重复喂。
    媒体走的是 "[文件]…" 那行，和 event.body（文件名/caption）不一样，必须由调用方显式传。"""
    rid = room.room_id
    sender = room.user_name(event.sender) or event.sender
    cur_body = (skip_body if skip_body is not None
                else _strip_reply_fallback(event.body or "", (event.source or {}).get("content", {})))
    ctx = _format_context(rid, skip=(sender, cur_body), drop_sender=MY_NAME or None)
    if ctx:
        prompt = (
            "【所在会话最近的对话，仅供背景参考；带时间，可能跨较长时间，自行判断哪些与当前任务相关】\n"
            f"{ctx}\n\n"
            f"【当前要你处理的任务】来自 {sender}：{text}"
        )
    else:
        prompt = f"[来自 {sender}] {text}"
    log.info("[%s] 任务@%s: %s", rid, rec["id"], text[:80])
    if rec.get("general"):   # 通用助手：每房间独立 scratch 子目录（互不串文件）、无 employee/Gitea 指引、不碰 git
        sp = settings.claude_system_prompt
        cwd = os.path.join(settings.claude_workdir, _safe_name(rid, "dm"))
        lock_key, prepare = None, None   # lock_key=None → 用会话 key（按房间），各房间可并行
    else:
        # 会话 key 带房间维度（互不串台）；lock_key 用 proj_id（同一 checkout 串行）；
        # 跑任务前先把工作树拉回干净 base，免得上个任务的脏树/残留分支污染这次。
        sp, cwd, lock_key = _employee_prompt(rec), rec["path"], rec["id"]
        sp = memory.augment_system_prompt(sp, rec["id"])   # 注入项目长期记忆（跨会话留存）
        prepare = lambda: projects.prepare_worktree(rec)
        _project_last_active[rec["id"]] = time.time()      # 标记活跃：自驱心跳会避让最近在弄的项目
    sp = transcript.augment_system_prompt(sp, rid)   # 指给它本房间历史日志，便于回溯更早对话
    answer = await runner.ask(_sess_key(rec, rid), prompt, cwd=cwd,
                              system_prompt=sp, lock_key=lock_key, prepare=prepare)
    await send(rid, answer, track=True)
    log.info("[%s] 完成 %d 字", rid, len(answer))
    if not rec.get("general"):   # 回复里若开了本项目的 PR，记进台账，由跟进循环盯到合并
        pr = _extract_pr(answer, rec)
        if pr and pr_ledger.record(rec["id"], pr[0], pr[1], rid):
            log.info("[%s] PR #%d 进台账，开始跟进", rid, pr[0])


# ---- PR 台账跟进："对结果负责"：开了 PR 就盯到合并 ----
def _extract_pr(answer: str, rec: dict) -> tuple[int, str] | None:
    """从回复里抽出本项目刚开的 PR 编号 + 链接（匹配 <host>/<owner>/<repo>/pulls/<n>）。"""
    prefix = f"{(rec.get('host') or '').rstrip('/')}/{rec['owner']}/{rec['repo']}/pulls/"
    m = re.search(re.escape(prefix) + r"(\d+)", answer or "")
    return (int(m.group(1)), prefix + m.group(1)) if m else None


async def _followup_dispatch(rec: dict, entry: dict, detail: str):
    """在该 PR 的分支上处理评审 / CI 并推送，结果回报到原房间；续原会话、不新开 PR。"""
    room, n, branch = entry["room"], entry["number"], entry.get("branch") or ""
    prompt = (
        f"你之前为本仓库开了 PR #{n}（分支 {branch}）。需要你跟进：\n{detail}\n\n"
        f"请先 git fetch、git checkout 分支 {branch}，据此处理：要改代码就改完 commit 并 push 到"
        f"**该分支**（PR 会自动更新）；若是误会或只需回应评审，就在最终回复里说明。"
        f"**不要新开 PR。**用简洁中文回复你做了什么。"
    )
    sp = memory.augment_system_prompt(_employee_prompt(rec), rec["id"])
    try:
        async with _typing(room):
            answer = await runner.ask(_sess_key(rec, room), prompt, cwd=rec["path"],
                                      system_prompt=sp, lock_key=rec["id"],
                                      prepare=lambda: projects.prepare_worktree(rec))
        await send(room, f"🔁 PR #{n} 跟进结果：\n{answer}", track=True)
    except Exception as e:
        log.exception("PR #%s 跟进任务失败", n)
        await send(room, f"PR #{n} 跟进出错：{e}")


async def _followup_one(entry: dict):
    pid, n, room = entry["pid"], entry["number"], entry["room"]
    rec = projects.get_project(pid)
    if not rec:
        return
    info = await gitea.pr_info(rec, n)
    if info is None:
        return   # 查不到（网络抖动 / 被删）：下轮再试，不销账
    pr_ledger.update(pid, n, last_check_ts=time.time())
    if info.get("merged"):
        pr_ledger.remove(pid, n)
        await send(room, f"✅ PR #{n} 已合并：{entry.get('url', '')}")
        return
    if info.get("state") == "closed":
        pr_ledger.remove(pid, n)
        await send(room, f"🚫 PR #{n} 被关闭（未合并）：{entry.get('url', '')}")
        return
    head = info.get("head") or {}
    branch, sha = head.get("ref") or entry.get("branch") or "", head.get("sha") or ""
    if branch and branch != entry.get("branch"):
        pr_ledger.update(pid, n, branch=branch)
    cap = settings.pr_autofix_max

    # 1) 新的评审意见（请求改动 / 评论）—— 评审优先于 CI
    reviews = await gitea.pr_reviews(rec, n)
    fresh = [r for r in reviews if isinstance(r.get("id"), int) and r["id"] > entry.get("seen_review", 0)
             and r.get("state") in ("REQUEST_CHANGES", "COMMENT")]
    if fresh:
        pr_ledger.update(pid, n, seen_review=max(r["id"] for r in fresh))
        if entry.get("review_fixes", 0) < cap:
            pr_ledger.update(pid, n, review_fixes=entry.get("review_fixes", 0) + 1)
            bodies = "\n".join(f"- [{r.get('state')}] {(r.get('body') or '(见行内评论)').strip()[:300]}"
                               for r in fresh)
            await send(room, f"📝 PR #{n} 收到评审意见，我去处理…")
            _spawn(_followup_dispatch(rec, entry, f"PR #{n} 收到评审意见：\n{bodies}"))
        else:
            await send(room, f"📝 PR #{n} 又有评审意见，但已到自动处理上限（{cap} 次），需要人看看：{entry.get('url', '')}")
        return

    # 2) CI 失败
    ci = await gitea.ci_state(rec, sha)
    if ci in ("failure", "error") and entry.get("ci_seen") != sha:
        pr_ledger.update(pid, n, ci_seen=sha)
        if entry.get("ci_fixes", 0) < cap:
            pr_ledger.update(pid, n, ci_fixes=entry.get("ci_fixes", 0) + 1)
            await send(room, f"❌ PR #{n} CI 失败，我去修…")
            _spawn(_followup_dispatch(rec, entry, f"PR #{n} 的 CI（持续集成）检查失败，请定位并修复后推送。"))
        else:
            await send(room, f"❌ PR #{n} CI 还失败，已到自动处理上限（{cap} 次），需要人看看：{entry.get('url', '')}")
        return

    # 3) 没有待处理评审、CI 也不失败：满足条件就自动合并（PR_AUTOMERGE=1 才开）
    if settings.pr_automerge:
        await _maybe_automerge(rec, entry, info, ci, reviews)


async def _maybe_automerge(rec: dict, entry: dict, info: dict, ci: str, reviews: list):
    """followup 末尾的机械合并闸：PR 可合并 + CI 通过(或无 CI 配置) + 无未决"请求改动"评审 →
    直接按 Gitea API 合并、销账、回报。不经 Claude 评审（"只做合并"）；移除人工合并这道闸，
    安全性靠 CI（若配了）+ 快照环境。合并失败保持 PR 开启，下轮再试或等人工。"""
    pid, n, room = entry["pid"], entry["number"], entry["room"]
    if not info.get("mergeable"):
        return                                   # 有冲突 / 暂不可合并：等下一轮
    if ci not in ("", "success"):                # pending / 未知：CI 没跑完就先等（failure 已在上一段处理）
        return
    decisive = [r.get("state") for r in reviews if r.get("state") in ("APPROVED", "REQUEST_CHANGES")]
    if decisive and decisive[-1] == "REQUEST_CHANGES":
        return                                   # 最近一条决定性评审是"请求改动"：别合
    ok, detail = await gitea.merge(rec, n, settings.pr_merge_method,
                                   settings.pr_automerge_delete_branch)
    if ok:
        pr_ledger.remove(pid, n)
        await send(room, f"✅ PR #{n} 已自动合并（{settings.pr_merge_method}）：{entry.get('url', '')}")
        log.info("[%s] PR #%d 自动合并", pid, n)
    else:
        log.warning("[%s] PR #%d 自动合并失败（保持开启，下轮再试或待人工）：%s", pid, n, detail)


async def _pr_followup_loop():
    """周期巡检台账里的 PR：合并/关闭就销账回报；新评审意见 / CI 失败就自动处理（带次数上限）。"""
    if not settings.pr_followup_enabled:
        return
    log.info("PR 跟进循环已启动（每 %ds 巡检一次）", settings.pr_followup_interval)
    while True:
        try:
            await asyncio.sleep(settings.pr_followup_interval)
            for entry in pr_ledger.active():
                try:
                    await _followup_one(entry)
                except Exception:
                    log.exception("PR #%s 跟进失败", entry.get("number"))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("PR 跟进循环异常，继续")


# ---- 主动性·自驱心跳：没人派活时也巡检各项目、主动找值得做的事 ----
def _project_home_room(pid: str) -> str | None:
    """自驱心跳的"汇报口"：优先群绑定房间，否则最近路由到该项目的 DM 房间。"""
    room = projects.first_room_for(pid)
    if room:
        return room
    for r, p in _last_project_by_room.items():
        if p == pid:
            return r
    return None


async def _heartbeat_execute(rec: dict, room: str, proposal: str):
    """autopilot：把巡检挑中的事真正做完并开 PR，结果回报、PR 进台账（→由跟进循环盯到合并）。"""
    prompt = (
        f"你刚巡检后决定主动推进这件事：\n{proposal}\n\n"
        "请像平常派活一样把它做完：从 origin/base 建分支、改代码、commit、push、开 PR，"
        "最终回复附上 PR 链接。用简洁中文回复。"
    )
    sp = memory.augment_system_prompt(_employee_prompt(rec), rec["id"])
    try:
        async with _typing(room):
            answer = await runner.ask(_sess_key(rec, room), prompt, cwd=rec["path"], system_prompt=sp,
                                      lock_key=rec["id"], prepare=lambda: projects.prepare_worktree(rec))
        _project_last_active[rec["id"]] = time.time()
        await send(room, f"🤖 自驱完成：\n{answer}", track=True)
        pr = _extract_pr(answer, rec)
        if pr and pr_ledger.record(rec["id"], pr[0], pr[1], room):
            log.info("[%s] 自驱开了 PR #%d，进台账", rec["id"], pr[0])
    except Exception as e:
        log.exception("自驱执行失败")
        await send(room, f"自驱执行出错：{e}")


async def _heartbeat_one(rec: dict, room: str):
    """对一个项目只读巡检，挑一件值得主动做的事；autopilot 直接认领去做，否则只提议。"""
    patrol = (
        f"你是负责仓库 {rec['owner']}/{rec['repo']} 的工程师。现在没人给你派活——"
        "主动巡检这个仓库，看有没有**值得现在主动推进、且改动可控**的事："
        "明显的小 bug、缺失的关键测试、陈旧的 TODO/FIXME、文档与代码不符、能小步改进的点。\n"
        "判断标准要高，别为找事而找事。\n"
        "- 不值得打扰就只回一行：__PASS__\n"
        "- 值得就挑**最值得的一件**，简短说清：是什么、为什么值得、打算怎么改（这是只读巡检，先别动手）。"
    )
    try:
        await projects.prepare_worktree(rec)   # 巡检最新的干净 base，而不是上个任务残留的脏树/分支
    except Exception:
        pass
    log.info("[%s] 自驱巡检中…", rec["id"])
    sp = memory.augment_system_prompt(_employee_prompt(rec), rec["id"])
    try:
        proposal = (await runner.consult(patrol, cwd=rec["path"], system_prompt=sp)).strip()
    except Exception:
        log.exception("[%s] 自驱巡检失败", rec["id"])
        return
    if not proposal or "__PASS__" in proposal:
        log.info("[%s] 自驱：本轮没有值得主动做的事（PASS）", rec["id"])
        return
    label = f"{rec['owner']}/{rec['repo']}"
    if settings.proactive_autopilot:
        await send(room, f"🫀 [{label}] 自驱：没人派活，我巡检后打算主动做这件事，开干——\n{proposal}", track=True)
        _spawn(_heartbeat_execute(rec, room, proposal))
    else:
        await send(room, f"🫀 [{label}] 巡检建议（没人派活时主动看的）：\n{proposal}\n\n"
                         "要做就回我一句；想让我自己认领就开 PROACTIVE_AUTOPILOT。", track=True)


async def _heartbeat_loop():
    """周期巡检有"汇报口"的项目；避让最近在弄的项目，免得打断正在派的活、也别为巡检而 clone。"""
    if not settings.proactive_heartbeat_enabled:
        return
    log.info("自驱心跳已启动（每 %ds 巡检一次，autopilot=%s）",
             settings.proactive_heartbeat_interval, settings.proactive_autopilot)
    while True:
        try:
            await asyncio.sleep(settings.proactive_heartbeat_interval)
            now = time.time()
            for rec in projects.list_projects():
                pid = rec["id"]
                room = _project_home_room(pid)
                if not room:
                    continue
                if now - _project_last_active.get(pid, 0) < settings.proactive_heartbeat_interval:
                    continue   # 最近有人在弄 / 刚巡检过：别打扰
                if not os.path.isdir(os.path.join(rec.get("path", ""), ".git")):
                    continue
                _project_last_active[pid] = now   # 占住，避免与真任务/下一轮重叠
                try:
                    await _heartbeat_one(rec, room)
                except Exception:
                    log.exception("[%s] 自驱心跳失败", pid)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("自驱心跳循环异常，继续")


async def handle_task(room: MatrixRoom, event: RoomMessageText, text: str,
                      skip_body: str | None = None):
    rid = room.room_id
    try:
        if not text.strip():
            return
        if text.strip() in RESET_CMDS:
            if not _is_dm(room):
                rec = projects.get_room(rid)
            else:  # 私聊没有房间绑定，按这个 DM 最近一次路由到的项目来重置
                pid = _last_project_by_room.get(rid)
                rec = projects.get_project(pid) if pid else None
            if rec:
                runner.reset(_sess_key(rec, rid))   # 只重置本房间的会话，不动别处共用同一 repo 的会话
                _context[rid].clear()   # 连背景一起清，别让旧对话漏进新会话
                await send(rid, "已开启新对话 ✅")
            else:
                await send(rid, "还没有可重置的会话；先发个仓库地址或派个活吧。")
            return

        # 分诊/clone + 跑任务整段都开着"正在输入"，避免 DM 首次路由时房间静默
        async with _typing(rid):
            rec = await _dispatch(room, event, text)
            if rec is None:
                return
            if not rec.get("general"):   # 通用助手不是项目，别记成"上次项目"
                _last_project_by_room[rid] = rec["id"]
                _save_last_projects()   # 落盘：重启后 DM 的 /reset 与多轮延续仍能定位项目
            await _run_on_project(room, event, text, rec, skip_body=skip_body)
    except Exception as e:
        log.exception("处理失败")
        try:
            await send(rid, f"出错了：{e}")
        except Exception:
            pass


# 判定不插话时占用的短冷却（秒）；太小会让活跃群里几乎每条疑问都起一次 Claude 判断（烧钱）。
_PROACTIVE_PASS_COOLDOWN = settings.proactive_pass_cooldown


async def maybe_proactive(room: MatrixRoom, event: RoomMessageText, body: str):
    rid = room.room_id
    now = time.time()
    if now - _last_proactive[rid] < settings.proactive_cooldown:
        return
    if not _looks_actionable(body):
        return
    _last_proactive[rid] = now   # 先占满冷却窗口，防止并发消息同时触发判断
    spoke = False
    try:
        ctx = _format_context(rid)
        prompt = (
            "下面是一个 Matrix 群里最近的对话。你是群里的助手。\n"
            "判断你现在是否应该主动插话帮忙：\n"
            "- 如果不该（比如不关你的事、别人已在处理、信息不足），只回复一行：__PASS__\n"
            "- 如果该，直接给出要发到群里的简洁回复（不要解释你为什么发）。\n\n"
            f"最近对话：\n{ctx}"
        )
        # 群已绑仓库且本地 checkout 还在：用只读 consult 让它对着真实代码作答，而不是凭空瞎猜；
        # 否则（未绑/没 clone）退回纯文本 quick。proactive 不主动 clone。
        rec = projects.get_room(rid) if not _is_dm(room) else None
        if rec and os.path.isdir(os.path.join(rec.get("path") or "", ".git")):
            sp = ("你是该仓库的助手。这是主动插话场景：可以只读地查看仓库代码把问题答准，"
                  "但绝不要修改文件、提交或开 PR。判断标准同上：不该插话就只回 __PASS__，"
                  "该插话就直接给要发到群里的简洁中文回复。")
            ans = (await runner.consult(prompt, cwd=rec["path"], system_prompt=sp)).strip()
        else:
            ans = (await runner.quick(prompt)).strip()
        if ans and "__PASS__" not in ans:
            await send(rid, ans, track=True)
            spoke = True
            log.info("[%s] 主动插话 %d 字", rid, len(ans))
    except Exception:
        log.exception("主动判断失败")
    finally:
        if spoke:
            _last_proactive[rid] = time.time()   # 真发言了 → 完整冷却防刷屏
        else:
            # 没发言（PASS/出错）：只占一个短冷却，好让紧接着的真求助能很快重新判断
            short = min(_PROACTIVE_PASS_COOLDOWN, settings.proactive_cooldown)
            _last_proactive[rid] = now - settings.proactive_cooldown + short


# ---- 媒体（图片 / 文件 / 音视频）：下载到本地、并入上下文，被点名时供 Claude 读取 ----
_MEDIA_NAME_RE = re.compile(r"[^A-Za-z0-9._-]")


def _safe_name(s: str, fallback: str) -> str:
    """压成安全的扁平文件名段：非白名单字符换 _、去前导点，挡掉 ../ 路径穿越。"""
    s = _MEDIA_NAME_RE.sub("_", s or "")
    s = s.lstrip(".") or fallback
    return s[:120]


def _human_bytes(n: float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{int(n)}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024


def _media_meta(event) -> tuple[str, str]:
    """返回 (展示文件名, 说明文字)。带 caption 时 body=说明、filename=真实文件名；否则 body 即文件名。"""
    content = (event.source or {}).get("content", {})
    body = event.body or ""
    fname = content.get("filename") or body or "file"
    caption = body if (content.get("filename") and content["filename"] != body) else ""
    return fname, caption


def _prune_dir(d: str, keep: int) -> None:
    """只保留目录里最近 keep 个文件，按 mtime 删旧，防媒体无限堆积。"""
    keep = max(1, keep)   # keep<=0 时 files[:-0] 会一个都不删，至少保留刚存的那个
    try:
        files = [e.path for e in os.scandir(d) if e.is_file()]
    except OSError:
        return
    if len(files) <= keep:
        return

    def _mtime(p):   # 排序期文件可能被并发 prune 删掉，getmtime 别让它把异常冒到调用方
        try:
            return os.path.getmtime(p)
        except OSError:
            return 0.0
    files.sort(key=_mtime)
    for p in files[:-keep]:
        try:
            os.remove(p)
        except OSError:
            pass


class _MediaTooLarge(Exception):
    """下载后发现实际体积超过上限（多见于不声明 size 的文件）。"""
    def __init__(self, size: int):
        super().__init__(f"{size} 字节超过上限")
        self.size = size


async def _download_media(event, cap: int) -> tuple[bytes, str]:
    """下载（加密房间会解密）媒体，返回 (明文字节, content_type)。

    用 save_to 让 nio 流式落到临时文件，避免不声明 size 的大文件被整块读进内存把进程撑爆；
    落盘后按真实大小兜底，超过 cap 直接丢弃（抛 _MediaTooLarge）。
    """
    # 临时文件落到 media_root（真磁盘）而非系统 /tmp：多数发行版 /tmp 是 tmpfs(走内存)，
    # 放那儿流式落盘就白做了，大文件照样吃内存。
    os.makedirs(settings.media_root, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="mxdl-", dir=settings.media_root)
    os.close(fd)
    resp = None
    try:
        resp = await client.download(mxc=event.url, save_to=tmp)
        size = os.path.getsize(tmp) if os.path.exists(tmp) else 0
        if size == 0:                       # 下载失败时 nio 不写文件，留个空文件
            raise RuntimeError(f"download 未取到内容: {resp}")
        if size > cap:                      # 按落盘后的真实大小兜底，挡住不声明 size 的大文件
            raise _MediaTooLarge(size)
        with open(tmp, "rb") as f:
            data = f.read()
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass
    ctype = getattr(resp, "content_type", "") or ""
    if isinstance(event, RoomEncryptedMedia):  # 加密房间拿到的是密文，本地解密
        from nio.crypto import attachments
        data = attachments.decrypt_attachment(
            data, event.key["k"], event.hashes["sha256"], event.iv)
        ctype = getattr(event, "mimetype", "") or ""   # 密文的 content-type 是 octet-stream，改用声明的真实类型
    return data, ctype


async def _save_media(rid: str, event) -> dict:
    """下载并落盘媒体；返回 {path,name,ctype,human_size} 成功，或 {name,error} 失败/跳过。"""
    fname, _ = _media_meta(event)
    if not settings.media_enabled:
        return {"name": fname, "error": "媒体处理已关闭"}
    cap = settings.media_max_mb * 1024 * 1024
    info = (event.source or {}).get("content", {}).get("info") or {}
    claimed = info.get("size")
    if isinstance(claimed, int) and claimed > cap:
        return {"name": fname, "error": f"{_human_bytes(claimed)} 超过上限 {settings.media_max_mb}MB，未下载"}
    if not getattr(event, "url", ""):
        return {"name": fname, "error": "无下载地址"}
    try:
        data, ctype = await _download_media(event, cap)
    except _MediaTooLarge as e:
        return {"name": fname, "error": f"{_human_bytes(e.size)} 超过上限 {settings.media_max_mb}MB，已丢弃"}
    except Exception as e:
        log.warning("下载媒体失败 %s: %s", getattr(event, "url", "?"), e)
        return {"name": fname, "error": "下载失败"}

    room_dir = os.path.join(settings.media_root, _safe_name(rid, "room"))
    path = os.path.join(room_dir, f"{_safe_name(event.event_id, 'ev')}__{_safe_name(fname, 'file')}")
    try:
        os.makedirs(room_dir, exist_ok=True)
        try:
            os.chmod(settings.media_root, 0o700)  # 媒体可能含私聊内容，收紧权限
        except OSError:
            pass
        with open(path, "wb") as f:
            f.write(data)
        _prune_dir(room_dir, settings.media_keep)
    except OSError as e:
        log.warning("媒体写盘失败 %s: %s", path, e)
        return {"name": fname, "error": "写盘失败"}
    ctype = ctype or info.get("mimetype") or getattr(event, "mimetype", "") or "application/octet-stream"
    return {"path": path, "name": fname, "ctype": ctype, "human_size": _human_bytes(len(data))}


async def _process_media(room: MatrixRoom, event, is_self: bool):
    rid = room.room_id
    try:
        saved = await _save_media(rid, event)
        fname, caption = _media_meta(event)
        sender = room.user_name(event.sender) or event.sender
        if saved.get("path"):
            line = f"[文件] {fname}（{saved['ctype']}, {saved['human_size']}）已存到本地：{saved['path']}"
        else:
            line = f"[文件] {fname}（{saved.get('error', '未处理')}）"
        if caption:
            line += f"\n说明：{caption}"
        _context[rid].append((time.time(), sender, line))   # 本地时钟，与文本消息一致
        transcript.append(rid, sender, line, event_id=getattr(event, "event_id", ""))

        # 与文本相同的派活闸：自己账号无触发词不派活
        if is_self and not _has_trigger(event.body or ""):
            return
        addressed, cleaned = _is_addressed(room, event)
        if not addressed:   # 没点名就只记上下文，不打扰（媒体不走 proactive）
            return
        have_file = bool(saved.get("path"))
        have_caption = bool(cleaned and cleaned != fname)   # 无 caption 时 cleaned 就是文件名
        if not have_file and not have_caption:   # 既没文件又没正文，没什么可干
            return
        parts = []
        if have_caption:
            parts.append(cleaned)
        if have_file:
            parts.append(f"用户发来一个文件：{fname}（{saved['ctype']}），已存到本地 {saved['path']}，"
                         f"需要就直接读取或查看它。")
        else:
            parts.append(f"（用户发来文件 {fname}，但未取到内容：{saved.get('error', '未知')}）")
        # skip_body=line：派活时这个文件已经写进任务正文了，把背景里那行 "[文件]…" 剔掉别重复喂
        _spawn(handle_task(room, event, "\n\n".join(parts), skip_body=line))
    except Exception:
        log.exception("处理媒体失败")


async def on_media(room: MatrixRoom, event):
    # 与 on_message 同样的前置闸：积压门 / 房间白名单 / 用户授权
    if not settings.process_backlog and not _synced:
        return
    if settings.room_allowlist and room.room_id not in settings.room_allowlist:
        return
    is_self = event.sender == MY_ID
    if not is_self and not _authorized(event.sender):
        return
    _spawn(_process_media(room, event, is_self))


async def on_invite(room: MatrixRoom, event: InviteMemberEvent):
    if event.state_key != MY_ID or event.membership != "invite":
        return
    # 只接受授权用户、白名单房间的邀请，否则陌生人能把 bot 拉进房间驱使 Claude。
    if settings.room_allowlist and room.room_id not in settings.room_allowlist:
        log.warning("拒绝非白名单房间的邀请 %s（邀请人 %s）", room.room_id, event.sender)
        return
    if not _authorized(event.sender):
        log.warning("拒绝未授权用户 %s 的房间邀请 %s", event.sender, room.room_id)
        return
    await client.join(room.room_id)
    log.info("已加入房间 %s（邀请人 %s）", room.room_id, event.sender)


def _new_client() -> AsyncClient:
    cfg = AsyncClientConfig(store_sync_tokens=True, encryption_enabled=E2E)
    return AsyncClient(settings.homeserver, settings.user_id,
                       store_path=settings.store_path, config=cfg)


async def _login():
    global client
    os.makedirs(settings.store_path, exist_ok=True)
    try:
        os.chmod(settings.store_path, 0o700)  # store 含 token / E2EE 密钥 / 绑定，收紧权限
    except OSError:
        pass
    creds = None
    if os.path.exists(settings.creds_path):
        try:
            with open(settings.creds_path) as f:
                creds = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.warning("凭证文件 %s 损坏/不可读，改用密码重新登录: %s", settings.creds_path, e)

    # 三个字段缺一不可（缺了 restore_login 取不到键）；不全就回落密码登录
    if creds and all(creds.get(k) for k in ("access_token", "user_id", "device_id")):
        register_secret(creds["access_token"])  # Matrix token 纳入 redact
        client.restore_login(  # 启用 E2E 时会自动 load_store（旧 device 的密钥）
            user_id=creds["user_id"],
            device_id=creds["device_id"],
            access_token=creds["access_token"],
        )
        who = await client.whoami()  # 校验 token 是否仍有效
        if isinstance(who, WhoamiResponse):
            log.info("用已保存的会话登录: %s (device %s)", who.user_id, creds["device_id"])
            return
        # 只有 token 真失效才回落密码登录；网络/服务器抖动时保留已有会话
        if getattr(who, "status_code", "") in ("M_UNKNOWN_TOKEN", "M_MISSING_TOKEN"):
            log.warning("已保存的会话失效（%s），改用密码重新登录", who)
            # 旧 store/olm 绑在旧 device_id 上，nio 不会为新 device 重建 store；
            # 换个干净 client 重登 E2EE 才正常。先关掉旧的 HTTP 会话。
            try:
                await client.close()
            except Exception:
                pass
            client = _new_client()
        else:
            log.warning("whoami 校验未通过（%s），疑似网络/服务器临时问题，暂按已保存会话继续运行", who)
            return
    elif creds:
        log.warning("凭证文件 %s 缺少必要字段（access_token/user_id/device_id），改用密码重新登录",
                    settings.creds_path)

    # 密码登录（首次，或会话失效回落）
    if not settings.password:
        raise SystemExit("需要在 .env 设置 MATRIX_PASSWORD（首次登录或会话失效时）")
    resp = await client.login(settings.password, device_name=settings.device_name)
    if not isinstance(resp, LoginResponse):
        raise SystemExit(f"登录失败: {resp}")
    register_secret(client.access_token)  # Matrix token 纳入 redact
    tmp = settings.creds_path + ".tmp"     # 原子写：临时文件 + chmod 600 + rename
    with open(tmp, "w") as f:
        json.dump({"user_id": client.user_id,
                   "device_id": client.device_id,
                   "access_token": client.access_token}, f)
    os.chmod(tmp, 0o600)
    os.replace(tmp, settings.creds_path)
    log.info("登录成功并保存会话: %s (device %s)", client.user_id, client.device_id)


async def main():
    global client, MY_ID, MY_NAME, MY_LOCAL, _synced
    client = _new_client()
    await _login()

    MY_ID = client.user_id
    MY_LOCAL = MY_ID.lstrip("@").split(":")[0]
    try:
        r = await client.get_displayname(MY_ID)
        MY_NAME = getattr(r, "displayname", "") or MY_LOCAL
    except Exception:
        MY_NAME = MY_LOCAL

    if settings.enable_e2e and not OLM_OK:
        log.warning("MATRIX_ENABLE_E2E=1 但未检测到 olm，已降级为明文模式。"
                    "请先安装 libolm 并 pip install 'matrix-nio[e2e]'。")
    if not settings.allow_users:
        log.info("ALLOW_USERS 未设置：默认所有人都可驱动 Claude（如需收紧在 .env 配置）。")
    os.makedirs(settings.claude_workdir, exist_ok=True)
    _load_last_projects()   # 恢复重启前各房间的项目路由（DM /reset、多轮延续要用）
    log.info("启动: 身份=%s (%s) E2EE=%s 工作目录=%s 主动模式=%s",
             MY_ID, MY_NAME, E2E, settings.claude_workdir, settings.proactive)

    client.add_event_callback(on_message, RoomMessageText)
    client.add_event_callback(on_media, (RoomMessageMedia, RoomEncryptedMedia))
    client.add_event_callback(on_invite, InviteMemberEvent)

    # 初始同步消化积压（此时 _synced 仍 False，被 on_message 挡掉），之后才处理新消息
    await client.sync(timeout=30000, full_state=True)
    _synced = True
    if settings.transcript_enabled:   # 首次启用记录：对没灌过的房间各回灌一次历史（一次性，有标记不重复）
        for rid in list(client.rooms):
            if not transcript.is_backfilled(rid):
                _spawn(_auto_backfill(rid))
    _spawn(_pr_followup_loop())   # 后台盯台账里的 PR，跟到合并
    _spawn(_heartbeat_loop())     # 后台自驱心跳：没人派活时也巡检找事
    await client.sync_forever(timeout=30000, full_state=False)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
