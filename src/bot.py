"""Matrix × Claude Code 机器人 —— 入口与事件回调。

监听房间消息：单聊 / 被 @ / 被回复 / 命中触发词 → 调 Claude Code 干活并回复；
PROACTIVE 下还会对"像在求助/有错可纠正"的群消息判断要不要插话。
登录态与 E2EE 密钥持久化在 store_path，重启复用同一设备。

实现按职责拆到各模块：state(运行态) / fmt(格式化) / matrix_io(收发·线程·流式·附件) /
addressing(寻址) / dispatch(项目分诊) / tasks(跑任务·元命令) / pr_followup / heartbeat /
proactive / media。本文件只留事件回调与启动。下面从各模块 import 进来的名字，基本都是本文件
自身回调/启动直接要用的（早先一大批纯为测试的 `bot._xxx` 再导出已退役，测试改从来源模块访问）。
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
    KeyVerificationCancel,
    KeyVerificationKey,
    KeyVerificationMac,
    KeyVerificationStart,
    LocalProtocolError,
    LoginResponse,
    MatrixRoom,
    MegolmEvent,
    RoomEncryptedMedia,
    RoomMemberEvent,
    RoomMessageMedia,
    RoomMessageText,
    ToDeviceError,
    WhoamiResponse,
)

from config import settings, register_secret
import state
from state import _context, _sent_events, _last_project_by_room
from claude_runner import runner  # noqa: F401
from projects import projects, parse_repo_url, proj_id
import transcript
import digest
import inflight
import issue_ledger
import pr_ledger
import gitea

# 下面 import 的名字都是本文件回调/启动自己在用的。唯一例外 _format_context 本文件不直接
# 用，仅作 `bot.X` 兼容再导出留着（测试仍按这个名字访问）；其余纯测试用的再导出已退役。
from fmt import _format_context  # noqa: F401  # 测试专用再导出（本文件未直接用）
from matrix_io import (send, _is_dm, _thread_of,
                       _resolve_reply_author, _edit_message)
from addressing import (_address_kind, _has_trigger, _strip_reply_fallback,
                        _strip_self_mentions, _mark_engaged)
from tasks import (handle_task, handle_summarize, handle_cancel, handle_status, do_bind,
                   handle_unbind, _backfill_cmd, _auto_backfill,
                   RESET_CMDS, HELP_CMDS, SUMMARY_CMDS, CANCEL_CMDS, STATUS_CMDS, UNBIND_CMDS,
                   _HELP_TEXT, _WELCOME)
from pr_followup import _pr_followup_loop
from heartbeat import _heartbeat_loop
from issue_intake import _issue_execute, _issue_intake_loop
from proactive import maybe_proactive, followup_is_for_me
from media import on_media, discard_room, sweep_stale_downloads

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
    _context[room.room_id].append((time.time(), sender_name, body, _thread_of(event)))
    transcript.append(room.room_id, sender_name, body, event_id=event.event_id)  # 落盘逐字记录，供回溯
    if digest.should_digest(room.room_id):   # 攒够量 / 跨天残留就后台把原始日志预压成日摘要+主题索引
        state._spawn(digest.digest_room(room.room_id))

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
    if low in UNBIND_CMDS:                 # 解绑（群 / 私聊通用）
        state._spawn(handle_unbind(room))
        return
    # /bind 但没带可解析的仓库地址：无论群 / 私聊都提示怎么用（私聊过去直接拒绝，现在也支持绑定了）
    if low.startswith("/bind") and not parse_repo_url(body):
        state._spawn(send(room.room_id,
                          "发个仓库地址我来绑，比如 `/bind https://gitea.example.com/owner/repo`。"))
        return

    # 重启后 _sent_events 已清空：被回复的若是 bot 的旧消息，先向服务器补认，寻址才认得出
    await _resolve_reply_author(room.room_id, content)
    kind, cleaned = _address_kind(room, event)   # "strong"=明确点名 / "weak"=仅续话窗口命中 / ""=没点名
    addressed = bool(kind)

    # 绑定仓库：/bind 永远显式绑；私聊里【任何】带仓库 URL 的消息都绑到它——私聊已没有别的路由方式
    # （分诊已删），URL 后面若还带话就绑完接着当任务派；群里限"未绑定 + 纯链接/被点名"才自动绑，
    # 混着链接闲聊的不绑（防误触，群是公共的）。
    repo = parse_repo_url(body)
    if repo:
        dm = _is_dm(room)
        is_bind = body.strip().lower().startswith("/bind")
        rest = re.sub(r"\S*://\S+|git@\S+", "", body.strip(), count=1).strip()
        just_url = len(rest) <= 3   # 去掉 URL 后基本没剩内容才自动绑定（群里用；私聊任何带 URL 都绑）
        bound = projects.get_room(room.room_id)
        # /bind 永远绑；私聊里带 URL 就(重)绑（个人房间，换绑不怕误触）；群里未绑定 + 纯链接/被点名才绑。
        if is_bind or dm or (not bound and (just_url or addressed)):
            task_text = body.strip()
            if is_bind:
                task_text = task_text[len("/bind"):].strip()
            task_text = re.sub(r"\S*://\S+|git@\S+", "", task_text, count=1).strip()
            task_text = _strip_self_mentions(task_text).strip()   # 去掉 @bot，别把点名混进任务正文
            state._spawn(do_bind(room, repo, event, task_text))
            return
        # 群已绑别的仓库：裸 URL 不自动换绑（防误触），给个提示而非静默（私聊上面已直接换绑，不到这）
        if not dm and just_url and bound and proj_id(repo) != bound["id"]:
            state._spawn(send(room.room_id,
                              f"这个群已绑定 {bound['owner']}/{bound['repo']}；要换绑请用 /bind <仓库URL>。"))
            return

    if kind == "strong":
        if not is_self and not _is_dm(room):   # 群里被点名 → 开/续"对话延续窗口"，下条免重复 @
            _mark_engaged(room.room_id, event.sender)
        state._spawn(handle_task(room, event, cleaned))
    elif kind == "weak":   # 仅续话窗口命中的弱信号：先过语义闸确认是在跟我说，再决定接不接
        state._spawn(_maybe_followup_task(room, event, cleaned, body, is_self))
    elif body.strip() in RESET_CMDS:   # 群里不点名也认重置（重置是元命令，不必 @ 机器人）
        state._spawn(handle_task(room, event, body.strip()))
    elif settings.proactive:
        state._spawn(maybe_proactive(room, event, body))


async def _maybe_followup_task(room: MatrixRoom, event: RoomMessageText,
                               cleaned: str, body: str, is_self: bool):
    """续话窗口命中（弱信号）时的接活闸：语义闸确认"确实在接着跟我说"才派活并续窗口；
    判为"不是对我说"则不接，转交主动插话闸判断该不该纠错/帮忙（软窗口把这条从"没点名→直接
    进主动判断"的老路截走了，这里补回那次机会），它拿不准会 __PASS__，不会没话找话。"""
    if settings.followup_semantic_gate:
        sender_name = room.user_name(event.sender) or event.sender
        if not await followup_is_for_me(room.room_id, sender_name, body):
            log.info("[%s] 续话窗口命中但语义判断非对我说，跳过", room.room_id)
            # 语义闸判"这句不是在跟我说"——但它可能是在跟旁人说、话里却有值得纠正的错/求助。
            # 别直接沉默：软窗口把这条从"没点名 → 直接进主动判断"的老路上截走了，这里补回那一次机会，
            # 交给主动插话闸（自带冷却+PASS 倾向，不会刷屏）判断该不该插一句。
            if settings.proactive and not is_self:
                await maybe_proactive(room, event, body)
            return
    if not is_self:   # 弱信号只出现在群里（DM 恒 strong），确认接活后再续窗口
        _mark_engaged(room.room_id, event.sender)
    await handle_task(room, event, cleaned)


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


async def on_key_verification(event):
    """自动完成对方发起的 SAS 设备验证（emoji/数字比对），让 bot 这个设备可被验证、
    不再一直转圈。采用"信任优先"策略：不真去核对 emoji，直接确认——bot 是你自有的可信设备，
    在客户端点了验证就视同认可。

    仅覆盖 legacy 的 to-device SAS 流程（start→accept→key→mac）。nio 0.25.2 不认
    m.key.verification.request 起手式、也没有交叉签名能力，所以对方客户端若只走"交叉签名/
    用户验证"而不给 SAS，本回调收不到事件；那种情况开了 E2E 后设备至少不再转圈（有密钥、
    显示为"未验证"而非空转）。失败只记日志，绝不影响正常收发。"""
    client = state.client
    tx = getattr(event, "transaction_id", None)
    try:
        if isinstance(event, KeyVerificationStart):
            if "emoji" not in (event.short_authentication_string or []):
                # 对方不支持 emoji SAS，无法比对，直接取消（reject）而不是悬着让它转圈
                await client.cancel_key_verification(tx, reject=True)
                return
            resp = await client.accept_key_verification(tx)
            if isinstance(resp, ToDeviceError):
                log.warning("接受验证失败: %s", resp)
                return
            sas = client.key_verifications.get(tx)
            if sas is not None:
                await client.to_device(sas.share_key())
        elif isinstance(event, KeyVerificationKey):
            # 收到对方公钥即自动确认短串匹配（信任优先，不做人工 emoji 核对）
            await client.confirm_short_auth_string(tx)
        elif isinstance(event, KeyVerificationMac):
            sas = client.key_verifications.get(tx)
            if sas is None:
                return
            try:
                msg = sas.get_mac()
            except LocalProtocolError:
                return   # SAS 尚未双方确认，还不能发 MAC；等下一步
            await client.to_device(msg)
        elif isinstance(event, KeyVerificationCancel):
            log.info("对方取消了设备验证: %s", getattr(event, "reason", ""))
    except Exception:
        log.exception("自动设备验证处理失败 (tx=%s)", tx)


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
        digest.discard(rid)   # ④' 删该房间的日摘要 + 主题索引目录
    except Exception:
        log.exception("退房清理：删日摘要失败 %s", rid)
    try:
        discard_room(rid)   # ⑤ 删该房间的媒体目录
    except Exception:
        log.exception("退房清理：删媒体目录失败 %s", rid)


def _ssl_param():
    """自建 / 自签名证书 homeserver 的 TLS 策略（详见 config.py）：
    - 指定了 MATRIX_CA_CERT → 用这张证书/CA 做校验（推荐，仍防 MITM）；
    - MATRIX_SSL_VERIFY=0 → 关闭校验（ssl=False，自签名最省事，失去 MITM 防护）；
    - 否则 → None（走系统默认 CA 校验）。
    否则 nio(aiohttp) 默认校验，遇自签名证书会一直失败重试，表现为"连不上"。"""
    if settings.matrix_ca_cert:
        import ssl as _ssl
        ctx = _ssl.create_default_context(cafile=os.path.expanduser(settings.matrix_ca_cert))
        return ctx
    if not settings.matrix_ssl_verify:
        log.warning("MATRIX_SSL_VERIFY=0：已关闭 homeserver TLS 证书校验（无 MITM 防护，仅可信网络可用）")
        return False
    return None


def _new_client() -> AsyncClient:
    cfg = AsyncClientConfig(store_sync_tokens=True, encryption_enabled=state.E2E)
    return AsyncClient(settings.homeserver, settings.user_id,
                       store_path=settings.store_path, config=cfg, ssl=_ssl_param())


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


def _pr_body_closes_issue(body: str, n: int) -> bool:
    """PR 正文是否声明关闭工单 #n。_issue_execute 生成 PR 时要求带一行 `Closes #N`；
    这里也认 Gitea 同样识别的 fixes/resolves 等同义词，大小写不敏感。
    用 #n 后接词边界收口，免得工单 #12 误命中 #123。"""
    return bool(re.search(
        rf"(?i)(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+#{n}\b", body or ""))


async def _find_open_pr_for_issue(rec: dict, n: int) -> tuple[int, str] | None:
    """在仓库 open PR 里找正文 Closes #n 的那条（崩在"PR 已开、台账没记 pr 号"之间）。
    找到返回 (PR 号, 链接)，否则 None。"""
    for pr in await gitea.open_pulls(rec):
        if isinstance(pr, dict) and _pr_body_closes_issue(pr.get("body") or "", n):
            num = pr.get("number")
            if isinstance(num, int):
                return num, (pr.get("html_url") or "")
    return None


async def _reconcile_inflight():
    """启动对账在途登记簿：重启把内存里跑/排队的活蒸发了，用户端零提示。
    聊天任务无法自动续（prompt/上下文已丢）→ 有占位就把占位编辑成中断提示，没占位（排队中就
    死了）就补发一条催重发。工单交给 _reconcile_issues 自动重派，这里不催重发。
    房间可能已退（不在 client.rooms）→ 跳过别报错。处理完清空整簿。"""
    entries = inflight.active()
    if entries:
        log.info("启动对账：在途登记簿有 %d 条残留任务待收尾", len(entries))
    rooms = getattr(state.client, "rooms", {}) or {}
    for e in entries:
        if e.get("kind") == inflight.KIND_ISSUE:
            continue   # 工单自动重派（_reconcile_issues），不叫用户重发
        room = e.get("room") or ""
        if room not in rooms:
            log.info("启动对账：房间 %s 已退出，跳过在途条目", room)
            continue
        summary = (e.get("summary") or "").strip()
        tip = "⚠️ 我刚重启过，上一个任务被中断了，请把它重新发一遍。"
        if summary:
            tip += f"\n（中断的任务：{summary[:80]}）"
        eid = e.get("eid") or ""
        try:
            if eid and await _edit_message(room, eid, tip):
                log.info("启动对账：占位 %s 收尾成中断提示", eid)
            else:
                await send(room, tip)   # 无占位（排队中就死）或编辑失败 → 补发一条
        except Exception:
            log.exception("启动对账：向房间 %s 发送中断提示失败", room)
    inflight.clear()


async def _reconcile_issues():
    """启动对账工单：issue_ledger 里 pr==0（接了单没开出 PR）= 崩在执行中途，会被台账挡着
    永不重接、也不被 _sweep_closed 清（它只清已关闭的）→ 在这里重派。
    防重复开 PR：崩溃可能发生在「PR 已开、台账还没记 pr 号」之间——重派前先查该仓库 open PR 里
    有没有正文 Closes #N 的，有就只补记台账继续跟进，没有才真的重派执行。"""
    for entry in list(issue_ledger.active()):
        if entry.get("pr"):
            continue   # 已开过 PR：由 PR 跟进循环盯，不用重派
        pid, n, room = entry["pid"], entry["number"], entry.get("room") or ""
        rec = projects.get_project(pid)
        if not rec:
            continue   # 项目已不在册：交给 _sweep_closed 销账
        pr = await _find_open_pr_for_issue(rec, n)
        if pr:
            issue_ledger.update(pid, n, pr=pr[0])
            if pr_ledger.record(pid, pr[0], pr[1], room):
                log.info("[%s] 启动对账：工单 #%d 已有 PR #%d（崩在记账前），补记台账继续跟进",
                         pid, n, pr[0])
            continue
        info = await gitea.issue_info(rec, n)
        if info is None:
            log.info("[%s] 启动对账：工单 #%d 查不到（网络抖动？），留待轮询处理", pid, n)
            continue
        if info.get("state") == "closed":
            issue_ledger.remove(pid, n)   # 已被关（提问类答完关单）：无需重派
            log.info("[%s] 启动对账：工单 #%d 已关闭，销账", pid, n)
            continue
        log.info("[%s] 启动对账：工单 #%d 接了单没开 PR，重新派执行", pid, n)
        try:
            if room and room in (getattr(state.client, "rooms", {}) or {}):
                await send(room, f"↻ 我刚重启过，工单 #{n} 没做完，重新接手处理——", track=True)
        except Exception:
            log.exception("启动对账：工单 #%d 重启通知发送失败", n)
        state._spawn(_issue_execute(rec, room, info))


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
    if state.E2E:   # 开了 E2E 才有 olm 能跑 SAS：自动完成对方发起的设备验证，别让 bot 设备一直转圈
        state.client.add_to_device_callback(
            on_key_verification,
            (KeyVerificationStart, KeyVerificationKey, KeyVerificationMac, KeyVerificationCancel))

    # 初始同步消化积压（此时 _synced 仍 False，被 on_message 挡掉），之后才处理新消息
    await state.client.sync(timeout=30000, full_state=True)
    state._synced = True
    for rid in list(state.client.rooms):   # 上次运行以来人散了的房间，启动时一并清掉
        await _leave_if_alone(rid)
    # 重启对账：上次跑/排队中的活只在内存里，重启即蒸发、用户端零提示。把断掉的活收尾/催重发，
    # pr==0 的工单重派（先查是否已开过 PR，防重复）。发送失败只记日志，绝不中断启动。
    try:
        await _reconcile_inflight()
        await _reconcile_issues()
    except Exception:
        log.exception("启动对账失败（不影响继续运行）")
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
