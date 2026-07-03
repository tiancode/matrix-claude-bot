"""Matrix × Claude Code 机器人 —— 入口与事件回调。

监听房间消息：单聊 / 被 @ / 被回复 / 命中触发词 → 调 Claude Code 干活并回复；
PROACTIVE 下还会对"像在求助/有错可纠正"的群消息判断要不要插话。
登录态与 E2EE 密钥持久化在 store_path，重启复用同一设备。

实现按职责拆到各模块：state(运行态) / fmt(格式化) / matrix_io(收发·线程·流式·附件) /
addressing(寻址) / dispatch(项目分诊) / tasks(跑任务·元命令) / pr_followup / heartbeat /
proactive / media。本文件只留事件回调与启动。下面的 re-export 让 `bot.X` 仍可用（测试/兼容）。
"""
import asyncio
import json
import logging
import os
import re
import time

from nio import (
    AsyncClient,
    AsyncClientConfig,
    InviteMemberEvent,
    LocalProtocolError,
    LoginResponse,
    MatrixRoom,
    MegolmEvent,
    RoomEncryptedMedia,
    RoomMemberEvent,
    RoomMessageMedia,
    RoomMessageText,
    WhoamiResponse,
)

from config import settings, register_secret
import state
from state import (_context, _sent_events, _last_project_by_room, _group_engaged,  # noqa: F401
                   _last_proactive, _tasks, _sess_key)
from claude_runner import runner  # noqa: F401
from projects import projects, parse_repo_url, proj_id
import transcript

# re-export：保持 `bot.X` 可用（测试按这些名字访问内部实现）
from fmt import (_split, _to_html, _format_context, _human_gap, _safe_name)  # noqa: F401
from matrix_io import (send, _is_dm, _LiveReply, _emit_files, _within_allowed,  # noqa: F401
                       _thread_root_of, _thread_rel, _resolve_reply_author)
from addressing import (_is_addressed, _has_trigger, _strip_trigger, _strip_reply_fallback,  # noqa: F401
                        _strip_self_mentions, _mark_engaged, _looks_actionable)
from dispatch import _dispatch, _triage, TRIAGE_GENERAL  # noqa: F401
from tasks import (handle_task, handle_summarize, handle_cancel, handle_status, do_bind,  # noqa: F401
                   _backfill_cmd, _auto_backfill, _run_on_project, _extract_pr,
                   RESET_CMDS, HELP_CMDS, SUMMARY_CMDS, CANCEL_CMDS, STATUS_CMDS,
                   _HELP_TEXT, _WELCOME)
from pr_followup import _followup_one, _pr_followup_loop  # noqa: F401
from heartbeat import _heartbeat_one, _heartbeat_loop  # noqa: F401
from issue_intake import _intake_one, _issue_execute, _issue_intake_loop  # noqa: F401
from proactive import maybe_proactive, _PROACTIVE_PASS_COOLDOWN  # noqa: F401
from media import (on_media, _process_media, _prune_dir,  # noqa: F401
                   discard_room, sweep_stale_downloads)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("matrix-claude")

# 加密消息解不开时的明文提示限流：room_id -> 上次提示时刻。密钥缺失往往一连串消息
# 都解不开，每房间这段窗口内最多提示一次，别刷屏。纯内存态，重启丢了无妨。
_UNDECRYPT_NOTICE_COOLDOWN = 600   # 秒（10 分钟）
_last_undecrypt_notice: dict[str, float] = {}


async def on_message(room: MatrixRoom, event: RoomMessageText):
    if not settings.process_backlog and not state._synced:  # 跳过历史/离线积压
        return
    if event.event_id in _sent_events:  # 防自激
        return
    content = (event.source or {}).get("content", {})
    # 编辑消息(m.replace)会作为新 RoomMessageText 再次进来。当新消息处理会：同一任务带着
    # "* 修正后的正文"重跑，还重复进上下文/逐字记录。取舍：编辑一律不重派活——代价是上下文里
    # 留的是改前的旧文本，可接受。
    if (content.get("m.relates_to") or {}).get("rel_type") == "m.replace":
        return

    # 无访问控制：用这个 bot 的都是可信的人，所有用户的消息都进上下文、都可派活。
    is_self = event.sender == state.MY_ID
    body = _strip_reply_fallback(event.body or "", content)
    sender_name = room.user_name(event.sender) or event.sender
    # 用本地接收时刻而非 event.server_timestamp：与 send() 里 bot 回复同一时钟，
    # _format_context 的"间隔"提示才不会因收/发两端时钟偏差算错。
    _context[room.room_id].append((time.time(), sender_name, body))
    transcript.append(room.room_id, sender_name, body, event_id=event.event_id)  # 落盘逐字记录，供回溯

    # 用自己账号跑时：消息照进上下文，但无触发词不当派活（否则会去回你发给别人的话）。
    if is_self and not _has_trigger(event.body or ""):
        return

    # 元命令：群里不必 @ 也认（与 /reset 同类）。help/summarize/cancel 走各自快路径，不进任务分诊。
    stripped = body.strip()
    low = stripped.lower()
    if low.startswith("/backfill"):       # 从 Matrix 回灌本房间历史
        state._spawn(_backfill_cmd(room, stripped))
        return
    if low in HELP_CMDS:
        state._spawn(send(room.room_id, _HELP_TEXT))
        return
    if low.startswith("/summarize") or low.startswith("/catchup") or stripped in SUMMARY_CMDS:
        state._spawn(handle_summarize(room, event, stripped))
        return
    if low.startswith("/cancel") or low.startswith("/stop") or stripped in CANCEL_CMDS:
        state._spawn(handle_cancel(room))
        return
    if low.startswith("/status") or stripped in STATUS_CMDS:
        state._spawn(handle_status(room))
        return
    # 私聊发 /bind 没意义：DM 不绑房间、按内容自动分诊。别把 "/bind http://…" 放进任务分诊
    # 让 Claude 对着它自由发挥——直接给句引导。
    if _is_dm(room) and low.startswith("/bind"):
        state._spawn(send(room.room_id, "私聊不用绑定，我按内容自动分诊；直接说要干什么就行。"))
        return

    # 重启后 _sent_events 已清空：被回复的若是 bot 的旧消息，先向服务器补认，寻址才认得出
    await _resolve_reply_author(room.room_id, content)
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
            state._spawn(do_bind(room, repo, event, task_text))
            return
        # 群已绑别的仓库：裸 URL 不自动换绑（防误触），给个提示而非静默
        if just_url and bound and proj_id(repo) != bound["id"]:
            state._spawn(send(room.room_id,
                              f"这个群已绑定 {bound['owner']}/{bound['repo']}；要换绑请用 /bind <仓库URL>。"))
            return

    if addressed:
        if not is_self and not _is_dm(room):   # 群里被点名/续话 → 开/续"对话延续窗口"，下条免重复 @
            _mark_engaged(room.room_id, event.sender)
        state._spawn(handle_task(room, event, cleaned))
    elif body.strip() in RESET_CMDS:   # 群里不点名也认重置（重置是元命令，不必 @ 机器人）
        state._spawn(handle_task(room, event, body.strip()))
    elif settings.proactive:
        state._spawn(maybe_proactive(room, event, body))


async def on_invite(room: MatrixRoom, event: InviteMemberEvent):
    if event.state_key != state.MY_ID or event.membership != "invite":
        return
    # 无访问控制：用的人都可信，谁邀请都加入。
    await state.client.join(room.room_id)
    log.info("已加入房间 %s（邀请人 %s）", room.room_id, event.sender)
    # join() 后 nio 还没把房间放进 client.rooms（要等下一次 sync 把 join 消化掉），
    # 立刻 room_send 会 LocalProtocolError("No such room")。后台等房间出现再发欢迎语，
    # 也别阻塞本次 sync 的事件分发。
    state._spawn(_welcome_when_ready(room.room_id))


async def _welcome_when_ready(rid: str):
    """等房间被 sync 收进 client.rooms 后再发欢迎语（长轮询 sync 通常几秒内返回）。"""
    for _ in range(40):
        if rid in state.client.rooms:
            break
        await asyncio.sleep(0.5)
    try:                              # 进房先打个招呼 + 指到 /help，省得新人不知道怎么用
        await send(rid, _WELCOME)
    except Exception:
        log.exception("发送欢迎语失败 %s", rid)


async def on_undecrypted(room: MatrixRoom, event: MegolmEvent):
    """加密消息解不开（megolm 会话密钥缺失）时的兜底：向对方要密钥 + 回一条明文提示，别纯沉默。

    典型诱因：bot 清过 store 换了新 device、或对方客户端没把会话密钥分享到位。这类事件
    以 MegolmEvent 掉地上，用户在加密房 @bot 只会得到纯沉默，无从排查。
    初始 sync 期间的积压解密失败不提示（同 on_message 的手法），免得一启动就对历史消息群发。"""
    if not settings.process_backlog and not state._synced:  # 跳过历史/离线积压
        return
    # ① 先补救：向对方设备请求这条消息的会话密钥。nio 对同一 session 已在请求时会抛
    #    LocalProtocolError（该 session_id 已有 outgoing_key_request），接住即可——本就无需重发。
    try:
        await state.client.request_room_key(event)
    except LocalProtocolError:
        pass                                   # 已在要这把密钥了，别重复请求
    except Exception:
        log.exception("请求会话密钥失败 %s", room.room_id)
    # ② 限流：仅对"明文提示"限流；密钥请求每条都试（nio 自身按 session 去重，不会真刷屏）。
    now = time.time()
    if now - _last_undecrypt_notice.get(room.room_id, 0.0) < _UNDECRYPT_NOTICE_COOLDOWN:
        return
    _last_undecrypt_notice[room.room_id] = now
    # ③ 回一条明文提示，让用户至少知道发生了什么、下一步怎么办。发失败只记日志。
    try:
        await send(room.room_id,
                   "这条加密消息我解不开（密钥缺失），已尝试向你的客户端要密钥——"
                   "稍等重发一次试试；还不行就在客户端里验证一下我这个设备。")
    except Exception:
        log.exception("发送解密失败提示失败 %s", room.room_id)


async def on_member(room: MatrixRoom, event: RoomMemberEvent):
    """有人离开/被踢后房间若只剩自己就退房。进房类事件造不出孤儿房，不用管。"""
    if not state._synced or event.state_key == state.MY_ID:
        return
    if event.membership not in ("leave", "ban"):
        return
    await _leave_if_alone(room.room_id)


async def _leave_if_alone(rid: str) -> bool:
    """房间只剩自己（无他人、也无待接受的邀请）→ 退房并 forget，返回是否退了。

    人散了的房间留着只会攒垃圾（测试遗留的临时房、建完即弃的群）。
    room.users 含已加入+被邀请两类成员，长度为 1 即真·孤儿房。"""
    room = state.client.rooms.get(rid)
    if room is None or len(room.users) != 1 or state.MY_ID not in room.users:
        return False
    log.info("[%s] 房间只剩我自己，退房", rid)
    try:
        await state.client.room_leave(rid)
        await state.client.room_forget(rid)   # 从服务端房间列表一并抹掉，重启不再 sync 到
    except Exception:
        log.exception("退房失败 %s", rid)
        return False
    await _cleanup_room(rid)                   # 退成功后把这个死房间的尾巴一并清掉
    return True


async def _cleanup_room(rid: str):
    """退房后清掉该房间留下的一切尾巴：否则心跳/工单/PR 跟进仍把它当"汇报口"往里发消息、
    房内在跑的任务也白烧完对着死房间回复。每一步失败只记日志——已经退成功了，别让某步崩了
    拖累其它清理，更别把异常冒回退房流程。"""
    try:
        runner.cancel(rid)   # ① 停掉房内在跑的任务（与 /cancel 同一路径，按房间取消）
    except Exception:
        log.exception("退房清理：取消在跑任务失败 %s", rid)
    try:
        await projects.unbind(rid)   # ② 清项目绑定并落盘
    except Exception:
        log.exception("退房清理：解绑失败 %s", rid)
    if _last_project_by_room.pop(rid, None) is not None:   # ③ 清路由记忆并落盘
        try:
            state._save_last_projects()
        except Exception:
            log.exception("退房清理：路由记忆落盘失败 %s", rid)
    try:
        transcript.discard(rid)   # ④ 删逐字记录 + 回灌标记
    except Exception:
        log.exception("退房清理：删聊天记录失败 %s", rid)
    try:
        discard_room(rid)   # ⑤ 删该房间的媒体目录
    except Exception:
        log.exception("退房清理：删媒体目录失败 %s", rid)


def _new_client() -> AsyncClient:
    cfg = AsyncClientConfig(store_sync_tokens=True, encryption_enabled=state.E2E)
    return AsyncClient(settings.homeserver, settings.user_id,
                       store_path=settings.store_path, config=cfg)


async def _login():
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
        state.client.restore_login(  # 启用 E2E 时会自动 load_store（旧 device 的密钥）
            user_id=creds["user_id"],
            device_id=creds["device_id"],
            access_token=creds["access_token"],
        )
        who = await state.client.whoami()  # 校验 token 是否仍有效
        if isinstance(who, WhoamiResponse):
            log.info("用已保存的会话登录: %s (device %s)", who.user_id, creds["device_id"])
            return
        # 只有 token 真失效才回落密码登录；网络/服务器抖动时保留已有会话
        if getattr(who, "status_code", "") in ("M_UNKNOWN_TOKEN", "M_MISSING_TOKEN"):
            log.warning("已保存的会话失效（%s），改用密码重新登录", who)
            # 旧 store/olm 绑在旧 device_id 上，nio 不会为新 device 重建 store；
            # 换个干净 client 重登 E2EE 才正常。先关掉旧的 HTTP 会话。
            try:
                await state.client.close()
            except Exception:
                pass
            state.client = _new_client()
        else:
            log.warning("whoami 校验未通过（%s），疑似网络/服务器临时问题，暂按已保存会话继续运行", who)
            return
    elif creds:
        log.warning("凭证文件 %s 缺少必要字段（access_token/user_id/device_id），改用密码重新登录",
                    settings.creds_path)

    # 密码登录（首次，或会话失效回落）
    if not settings.password:
        raise SystemExit("需要在 .env 设置 MATRIX_PASSWORD（首次登录或会话失效时）")
    resp = await state.client.login(settings.password, device_name=settings.device_name)
    if not isinstance(resp, LoginResponse):
        raise SystemExit(f"登录失败: {resp}")
    register_secret(state.client.access_token)  # Matrix token 纳入 redact
    tmp = settings.creds_path + ".tmp"     # 原子写：临时文件 + chmod 600 + rename
    with open(tmp, "w") as f:
        json.dump({"user_id": state.client.user_id,
                   "device_id": state.client.device_id,
                   "access_token": state.client.access_token}, f)
    os.chmod(tmp, 0o600)
    os.replace(tmp, settings.creds_path)
    log.info("登录成功并保存会话: %s (device %s)", state.client.user_id, state.client.device_id)


async def main():
    state.client = _new_client()
    await _login()

    state.MY_ID = state.client.user_id
    state.MY_LOCAL = state.MY_ID.lstrip("@").split(":")[0]
    try:
        r = await state.client.get_displayname(state.MY_ID)
        state.MY_NAME = getattr(r, "displayname", "") or state.MY_LOCAL
    except Exception:
        state.MY_NAME = state.MY_LOCAL

    if settings.enable_e2e and not state.OLM_OK:
        log.warning("MATRIX_ENABLE_E2E=1 但未检测到 olm，已降级为明文模式。"
                    "请先安装 libolm 并 pip install 'matrix-nio[e2e]'。")
    os.makedirs(settings.claude_workdir, exist_ok=True)
    sweep_stale_downloads()       # 清掉上次被杀留下的下载残件（mxdl-*），启动时扫一遍
    state._load_last_projects()   # 恢复重启前各房间的项目路由（DM /reset、多轮延续要用）
    log.info("启动: 身份=%s (%s) E2EE=%s 工作目录=%s 主动模式=%s",
             state.MY_ID, state.MY_NAME, state.E2E, settings.claude_workdir, settings.proactive)

    state.client.add_event_callback(on_message, RoomMessageText)
    state.client.add_event_callback(on_media, (RoomMessageMedia, RoomEncryptedMedia))
    state.client.add_event_callback(on_invite, InviteMemberEvent)
    state.client.add_event_callback(on_member, RoomMemberEvent)
    state.client.add_event_callback(on_undecrypted, MegolmEvent)  # 解不开的加密消息：要密钥+提示，别沉默

    # 初始同步消化积压（此时 _synced 仍 False，被 on_message 挡掉），之后才处理新消息
    await state.client.sync(timeout=30000, full_state=True)
    state._synced = True
    for rid in list(state.client.rooms):   # 上次运行以来人散了的房间，启动时一并清掉
        await _leave_if_alone(rid)
    if settings.transcript_enabled:   # 首次启用记录：对没灌过的房间各回灌一次历史（一次性，有标记不重复）
        for rid in list(state.client.rooms):
            if not transcript.is_backfilled(rid):
                state._spawn(_auto_backfill(rid))
    state._spawn(_pr_followup_loop())   # 后台盯台账里的 PR，跟到合并
    state._spawn(_heartbeat_loop())     # 后台自驱心跳：没人派活时也巡检找事
    state._spawn(_issue_intake_loop())  # 后台工单接活：Gitea 上指派给 bot 的 issue = 派活
    await state.client.sync_forever(timeout=30000, full_state=False)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
