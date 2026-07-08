"""图片/文件/音视频：下载（加密房解密）落盘、并入上下文、被点名时交给 Claude 读取。"""
import logging
import os
import shutil
import tempfile
import time

from nio import MatrixRoom, RoomEncryptedMedia

from config import settings
import state
from state import _context
from fmt import _safe_name, _human_bytes
from matrix_io import send, _is_dm, _resolve_reply_author, _thread_of
from addressing import _has_trigger, _is_addressed, _is_known_bot, _mark_engaged, _mention_note
from tasks import handle_task
import transcript

log = logging.getLogger("matrix-claude.media")


class _MediaTooLarge(Exception):
    """下载后发现实际体积超过上限（多见于不声明 size 的文件）。"""
    def __init__(self, size: int):
        super().__init__(f"{size} 字节超过上限")
        self.size = size



def _media_meta(event) -> tuple[str, str]:
    """返回 (展示文件名, 说明文字)。带 caption 时 body=说明、filename=真实文件名；否则 body 即文件名。"""
    content = (event.source or {}).get("content", {})
    body = event.body or ""
    fname = content.get("filename") or body or "file"
    caption = body if (content.get("filename") and content["filename"] != body) else ""
    return fname, caption



def sweep_stale_downloads() -> None:
    """启动时清掉上次进程被中途杀掉留下的下载临时文件（mkstemp 的 mxdl-*，正常走 finally 删）。
    只扫 media_root 根目录一层——临时文件都建在这里，不进按房间的子目录。"""
    try:
        entries = list(os.scandir(settings.media_root))
    except OSError:
        return
    for e in entries:
        if e.name.startswith("mxdl-") and e.is_file():
            try:
                os.remove(e.path)
            except OSError:
                pass


def discard_room(rid: str) -> None:
    """删掉某房间的媒体目录（退房清理用）；不存在视作已清，不报错。"""
    d = os.path.join(settings.media_root, _safe_name(rid, "room"))
    shutil.rmtree(d, ignore_errors=True)


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
        resp = await state.client.download(mxc=event.url, save_to=tmp)
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
        # 配文里 @ 了谁也补附注（与文本消息一致）。只算这一次，随 skip_body/mention_note 透传：
        # skip_body 传的就是这个 line，去重天然逐字节匹配，派活层不再重算（防显示名漂移）。
        mention_note = _mention_note(room, (event.source or {}).get("content", {}))
        line += mention_note
        _context[rid].append((time.time(), sender, line, _thread_of(event)))   # 本地时钟+线程标记，与文本一致
        transcript.append(rid, sender, line, event_id=getattr(event, "event_id", ""))

        # 与文本相同的派活闸：自己账号无触发词不派活
        if is_self and not _has_trigger(event.body or ""):
            return
        # 已知机器人（KNOWN_BOTS）：文件已落上下文，但绝不应答（与文本入口同一断环闸）
        if not is_self and _is_known_bot(event.sender):
            return
        # 与文本一致：回复的若是 bot 重启前的旧消息，先补认；点名后开/续"对话延续窗口"
        await _resolve_reply_author(rid, (event.source or {}).get("content", {}))
        addressed, cleaned = _is_addressed(room, event)
        if not addressed:   # 没点名就只记上下文，不打扰（媒体不走 proactive）
            return
        if not is_self and not _is_dm(room):
            _mark_engaged(rid, event.sender)
        have_file = bool(saved.get("path"))
        have_caption = bool(cleaned and cleaned != fname)   # 无 caption 时 cleaned 就是文件名
        if not have_file and not have_caption:
            # 走到这说明寻址命中（DM 必回 / 群里被 @），但文件超限/下载失败/媒体功能关，
            # 又没配文字。DM 本该必回——别沉默 return 让用户对着空气等，直接把失败原因回给他。
            await send(rid, f"文件 {fname} 没能处理：{saved.get('error', '未取到内容')}")
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
        state._spawn(handle_task(room, event, "\n\n".join(parts),
                                 skip_body=line, mention_note=mention_note))
    except Exception:
        log.exception("处理媒体失败")



async def on_media(room: MatrixRoom, event):
    if not settings.process_backlog and not state._synced:   # 跳过历史/离线积压
        return
    content = (event.source or {}).get("content", {})
    if (content.get("m.relates_to") or {}).get("rel_type") == "m.replace":
        return   # 编辑事件不重派活（与文本一致；媒体编辑罕见，一并挡掉）
    is_self = event.sender == state.MY_ID
    state._spawn(_process_media(room, event, is_self))

