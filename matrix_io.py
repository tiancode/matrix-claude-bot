"""发消息 / 编辑(m.replace) / 线程(m.thread) / 正在输入 / 附件上传 / 流式占位。"""
import asyncio
import logging
import mimetypes
import os
import re
import time
from contextlib import asynccontextmanager

from nio import MatrixRoom

from config import settings, redact
import state
from state import _context, _sent_events, _foreign_events, E2E
from fmt import _split, _to_html, _human_bytes
import transcript

log = logging.getLogger("matrix-claude.io")


_SEND_MAX_TRIES = 3   # 被限流(M_LIMIT_EXCEEDED)时的额外重试次数（nio 自身也会重试若干次，这里再兜一层）



_FILE_MARKER_RE = re.compile(r"\[\[\s*send-file\s*:\s*(.+?)\]\]", re.I)



_FILE_SEND_HINT = (
    "\n要把文件 / 图片发回群里（生成的图表、截图、导出的补丁等），在最终回复里**单独一行**写："
    "[[send-file: 路径]]（每个文件一行，绝对或相对当前工作目录的路径；仅限工作目录内、单个有大小上限）。"
    "我会把对应文件作为附件发出，并从你的文字里去掉该标记。"
)



_STREAM_EDIT_MIN_GAP = 1.2   # 两次流式编辑的最小间隔（秒），别把房间刷爆



_EDIT_MAX_BYTES = 8000        # 编辑事件别太大：超过就只留尾部
_PROGRESS_TAIL_CHARS = 6000   # 进度/流式展示时保留的尾部字符数（编辑事件 + on_delta 共用）



def _thread_root_of(event) -> str | None:
    """这条消息所属的线程根：它本身已在某线程里就沿用那个根，否则用它自己的 event_id 作新线程根。"""
    content = (event.source or {}).get("content", {})
    rel = content.get("m.relates_to") or {}
    if rel.get("rel_type") == "m.thread" and rel.get("event_id"):
        return rel["event_id"]
    return getattr(event, "event_id", None)



def _thread_rel(thread_root: str | None, reply_to: str | None = None) -> dict | None:
    """构造 m.thread 关系；带 is_falling_back + m.in_reply_to，不懂线程的老客户端按普通回复显示。"""
    if not thread_root:
        return None
    return {"rel_type": "m.thread", "event_id": thread_root, "is_falling_back": True,
            "m.in_reply_to": {"event_id": reply_to or thread_root}}



async def _send_chunk(room_id: str, content: dict) -> str | None:
    """发送一块消息；被限流就按 retry_after 退避重试。成功返回 event_id，彻底失败返回 None。"""
    for attempt in range(_SEND_MAX_TRIES):
        resp = await state.client.room_send(
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



def _text_content(text: str) -> dict:
    """构造一条 m.text 消息内容：纯文本 body +（markdown 能渲染时）消毒后的 HTML formatted_body。"""
    content = {"msgtype": "m.text", "body": text}
    html = _to_html(text)
    if html:
        content["format"] = "org.matrix.custom.html"
        content["formatted_body"] = html
    return content



async def _send_and_register(room_id: str, content: dict,
                             rel: dict | None = None) -> str | None:
    """挂上 m.relates_to（线程 / 编辑关系）后发出，成功就登记 event_id（防自激 + 识别"回复了 bot"）。"""
    if rel:
        content["m.relates_to"] = rel
    eid = await _send_chunk(room_id, content)
    if eid:
        _sent_events.append(eid)
    return eid



async def send(room_id: str, text: str, track: bool = False, thread_root: str | None = None):
    """发消息到房间。track=True 才把这条并入房间上下文（任务答复/主动插话该 track，状态/回执/报错不进）。
    thread_root：非空则把这条（含分块）挂进该线程；event_id 始终登记，用于防自激与"回复了 bot"识别。"""
    text = redact(text)  # 任何外发文本都抹掉凭证
    any_ok = False
    all_ok = True
    prev = thread_root
    for chunk in _split(text):
        eid = await _send_and_register(room_id, _text_content(chunk),
                                       _thread_rel(thread_root, prev))
        if eid:
            any_ok = True
            prev = eid           # 同一线程内后续分块回链到上一块
        else:
            all_ok = False
    # 投递有丢就发条提示（短消息常比长回复更易发成功）；提示也登记 event_id 防自激回显。
    notice = None
    if any_ok and not all_ok:
        notice = "（部分内容发送失败，上面的回复可能不完整）"
    elif not any_ok and text:
        notice = "（回复发送失败，请稍后重试或检查日志）"
    if notice:
        await _send_and_register(room_id, {"msgtype": "m.text", "body": notice},
                                 _thread_rel(thread_root, prev))
    # 整条都发出去才并入上下文：半截投递不该被当成"已完整说过"喂回后续 prompt
    if any_ok and all_ok and track:
        _track_reply(room_id, text)



def _track_reply(room_id: str, text: str) -> None:
    """把一条已发出的完整答复并入上下文 + 历史（流式定稿走这里，不再二次发送）。"""
    dq = _context[room_id]
    ts = time.time()
    if dq:
        ts = max(ts, dq[-1][0])
    dq.append((ts, state.MY_NAME or "bot", text))
    transcript.append(room_id, state.MY_NAME or "bot", text, ts=ts)



async def _edit_message(room_id: str, target_eid: str, text: str) -> bool:
    """把已发出的 target_eid 编辑为新内容（m.replace）。流式进度用，过长则只留尾部。"""
    text = redact(text)
    if len(text.encode()) > _EDIT_MAX_BYTES:   # 编辑事件别太大；进度展示留尾部即可
        text = "…" + text[-_PROGRESS_TAIL_CHARS:]
    new_content = _text_content(text)
    content = dict(new_content)                 # 外层 fallback body 用 "* " 前缀，其余（含 html）沿用
    content["body"] = "* " + text
    content["m.new_content"] = new_content
    eid = await _send_and_register(
        room_id, content, {"rel_type": "m.replace", "event_id": target_eid})
    return bool(eid)



class _LiveReply:
    """流式渲染：发一条占位消息，随 Claude 产出节流地编辑它，最后 finalize 成完整答复。"""

    def __init__(self, room_id: str, thread_root: str | None = None):
        self.rid = room_id
        self.thread_root = thread_root
        self.eid: str | None = None
        self.last_edit = 0.0
        self.shown = ""

    async def _ensure(self, initial: str) -> None:
        if self.eid is not None:
            return
        self.eid = await _send_and_register(self.rid, _text_content(initial),
                                            _thread_rel(self.thread_root))
        self.last_edit = time.time()

    async def on_delta(self, text: str, tool: str | None) -> None:
        status = (f"_🔧 {tool}…_" if tool else "_⏳ 正在干活…_")
        tail = (text or "").strip()
        if len(tail) > _PROGRESS_TAIL_CHARS:
            tail = "…" + tail[-_PROGRESS_TAIL_CHARS:]
        shown = (tail + "\n\n" + status) if tail else status
        if self.eid is None:
            await self._ensure(shown)
            self.shown = shown
            return
        now = time.time()
        if now - self.last_edit < _STREAM_EDIT_MIN_GAP or shown == self.shown:
            return
        if await _edit_message(self.rid, self.eid, shown):
            self.shown = shown
            self.last_edit = now

    async def finalize(self, final_text: str, track: bool = False) -> None:
        final_text = final_text or "(空回复)"
        if self.eid is None:           # 没流式出任何东西（极快/无输出）→ 当普通消息发
            await send(self.rid, final_text, track=track, thread_root=self.thread_root)
            return
        chunks = _split(redact(final_text))
        if not await _edit_message(self.rid, self.eid, chunks[0]):   # 占位消息定稿成第一块
            # 定稿编辑失败（限流重试耗尽/服务器错误）：占位消息会永远停在"正在干活"，
            # 答案不能就此吞掉——退回整条新发（send 自带丢块提示）。
            await send(self.rid, final_text, track=track, thread_root=self.thread_root)
            return
        prev, all_ok = self.eid, True
        for c in chunks[1:]:                                  # 超长的余下分块作为线程内续接
            eid = await _send_and_register(self.rid, _text_content(redact(c)),
                                           _thread_rel(self.thread_root, prev))
            if eid:
                prev = eid
            else:
                all_ok = False
        if not all_ok:   # 与 send() 同规矩：有分块丢了要提示，别让读者当成完整回复
            await _send_and_register(self.rid, {"msgtype": "m.text",
                                                "body": "（部分内容发送失败，上面的回复可能不完整）"},
                                     _thread_rel(self.thread_root, prev))
        if track and all_ok:
            _track_reply(self.rid, redact(final_text))



async def _resolve_reply_author(rid: str, content: dict) -> None:
    """这条消息若回复了一条本地不认识的消息，向服务器查一次它是谁发的：是 bot 自己的旧消息
    就补登记进 _sent_events——否则重启后（_sent_events 清空）"回复 bot"就不再被当成点名。
    别人的消息记入 _foreign_events，同一条不重复拉取。"""
    eid = ((content.get("m.relates_to") or {}).get("m.in_reply_to") or {}).get("event_id")
    if not eid or eid in _sent_events or eid in _foreign_events:
        return
    try:
        resp = await state.client.room_get_event(rid, eid)
    except Exception:
        return
    sender = getattr(getattr(resp, "event", None), "sender", "") or ""
    if sender == state.MY_ID:
        _sent_events.append(eid)
    elif sender:
        _foreign_events.append(eid)


def _is_dm(room: MatrixRoom) -> bool:
    """恰好 2 人的房间才算单聊。取本地/服务器成员数的较大者。
    只认 ==2：成员未同步(0) 或刚加入只同步到 bot 自己(1) 都不当单聊，
    免得 REPLY_IN_DM_ALWAYS 在群成员还没拉全的窗口里把群当私聊逐条自动回。"""
    try:
        count = max(len(room.users), getattr(room, "member_count", 0) or 0)
        return count == 2
    except Exception:
        return False



async def _keep_typing(rid: str, stop: asyncio.Event):
    """任务运行期间周期性续期"正在输入"（单次 30s，任务可长达数百秒）。"""
    try:
        while not stop.is_set():
            await state.client.room_typing(rid, True, timeout=30000)
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
            await state.client.room_typing(rid, False)
        except Exception:
            pass



def _within_allowed(path: str, cwd: str | None) -> bool:
    """只允许回传【工作目录 / 媒体目录 / scratch / projects 根】之内的文件，挡掉 /etc/... 这类外泄。"""
    try:
        rp = os.path.realpath(path)
    except OSError:
        return False
    roots = [cwd, settings.media_root, settings.claude_workdir, os.path.abspath(settings.projects_root)]
    for root in roots:
        if not root:
            continue
        rr = os.path.realpath(root)
        if rp == rr or rp.startswith(rr + os.sep):
            return True
    return False



async def _send_file(room: MatrixRoom, path: str, thread_root: str | None) -> tuple[bool, str]:
    """上传并以 m.image/m.video/m.audio/m.file 发出（加密房间走加密上传）。返回 (是否成功, 失败说明)。"""
    name = os.path.basename(path) or "file"
    try:
        size = os.path.getsize(path)
    except OSError:
        return False, f"（附件 {name} 未找到）"
    cap = settings.media_max_mb * 1024 * 1024
    if size > cap:
        return False, f"（附件 {name} {_human_bytes(size)} 超过 {settings.media_max_mb}MB，未发送）"
    ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
    encrypt = bool(getattr(room, "encrypted", False)) and E2E

    def _provider(*_a):
        return open(path, "rb")
    try:
        resp, keys = await state.client.upload(_provider, content_type=ctype, filename=name,
                                         encrypt=encrypt, filesize=size)
    except Exception as e:
        log.warning("上传附件失败 %s: %s", path, e)
        return False, f"（附件 {name} 上传失败）"
    uri = getattr(resp, "content_uri", None)
    if not uri:
        log.warning("上传附件失败 %s: %s", path, resp)
        return False, f"（附件 {name} 上传失败）"
    if ctype.startswith("image/"):
        mt = "m.image"
    elif ctype.startswith("video/"):
        mt = "m.video"
    elif ctype.startswith("audio/"):
        mt = "m.audio"
    else:
        mt = "m.file"
    content = {"msgtype": mt, "body": name, "info": {"mimetype": ctype, "size": size}}
    if encrypt and keys is not None:     # 加密房间：密钥信息放 file，url 也挪进去
        keys["url"] = uri
        content["file"] = keys
    else:
        content["url"] = uri
    eid = await _send_and_register(room.room_id, content, _thread_rel(thread_root))
    if eid:
        return True, ""
    return False, f"（附件 {name} 发送失败）"



async def _emit_files(room: MatrixRoom, answer: str, cwd: str | None,
                      thread_root: str | None) -> str:
    """抽出回复里的 [[send-file: 路径]] 标记，把允许范围内的文件作为附件发出，返回去掉标记后的文字。"""
    if not settings.send_files_back or not answer:
        return answer
    paths = _FILE_MARKER_RE.findall(answer)
    if not paths:
        return answer
    notes = []
    for raw in paths[:10]:               # 一次最多发 10 个，防失控
        p = raw.strip().strip('"').strip("'")
        ap = p if os.path.isabs(p) else os.path.join(cwd or settings.claude_workdir, p)
        base = os.path.basename(p) or "file"
        if not _within_allowed(ap, cwd):
            notes.append(f"（附件 {base} 不在允许目录内，未发送）")
            continue
        if not os.path.isfile(ap):
            notes.append(f"（附件 {base} 未找到）")
            continue
        ok, note = await _send_file(room, ap, thread_root)
        if not ok and note:
            notes.append(note)
    cleaned = _FILE_MARKER_RE.sub("", answer).strip()
    if notes:
        cleaned = (cleaned + "\n" + "\n".join(notes)).strip()
    return cleaned or "（已发送附件）"

