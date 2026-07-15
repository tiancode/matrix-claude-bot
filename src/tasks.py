"""在某项目上跑 Claude 任务并回发；以及元命令 /reset /summarize /cancel /backfill /bind。"""
import logging
import os
import re
import time

from nio import MatrixRoom, RoomMessageText

from config import settings
import state
from state import (_context, _sess_key, _mark_dispatched, _clear_dispatched,
                   _drop_pending, _steered_dispatched, _unmark_dispatched,
                   _last_project_by_room, _save_last_projects, _project_last_active,
                   _room_model, _save_room_models)
from matrix_io import (send, _typing, _is_dm, _LiveReply, _emit_files, _redact,
                       _thread_of, _thread_root_of, _ack, _react, _resolve_reply,
                       _FILE_SEND_HINT)
from fmt import _format_context, _safe_name, _human_gap
from addressing import _strip_reply_fallback
from dispatch import _dispatch, _general_rec
from projects import projects, trusted_repo_info, _valid_name
from claude_runner import runner, ClaudeCancelled, _looks_transient
import memory
import issue_ledger
import pr_ledger
import transcript
import digest
import inflight
import gitea
import gitea_health

log = logging.getLogger("matrix-claude.tasks")


RESET_CMDS = {"/reset", "/new"}



HELP_CMDS = {"/help", "/?"}



SUMMARY_CMDS = {"/summarize", "/catchup"}   # 也认 "/summarize N" 前缀



CANCEL_CMDS = {"/cancel", "/stop"}          # 也认 "/cancel"/"/stop" 前缀



STATUS_CMDS = {"/status"}                   # 也认 "/status" 前缀



UNBIND_CMDS = {"/unbind"}                   # 解除本房间/私聊的仓库绑定



# /model：查看/设置本房间用的模型，按 "/model" 前缀路由（见 bot.py）。
# 模型名的合法形态（opus / fable / claude-opus-4-8 这类）：字母数字开头，之后允许 . _ : -。
_MODEL_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")

# 恢复默认（清掉本房间覆盖）的关键词
_MODEL_RESET_WORDS = {"reset", "clear", "default"}



_HELP_TEXT = (
    "**我能干嘛**\n"
    "我是接到 Matrix 的 Claude Code 工程师：群里 @我 或私聊我就能派活——写代码、查问题、做方案，"
    "改代码会自动开 PR 并跟到合并。群里没点名时，遇到求助或明显错误我也可能主动插一句。\n\n"
    "**怎么用**\n"
    "• 群里 @我 就能聊天/问问题；要派仓库的活先绑定：发 Gitea 仓库地址，或 `/bind <仓库URL>`\n"
    "• 然后 @我 派活；刚 @过我的几分钟内不必每句都 @（对话延续窗口）\n"
    "• 私聊发个仓库地址或 `/bind <URL>` 把它定住再派活；不绑也能闲聊/答疑，`/unbind` 解绑\n"
    "• 也可以不进 Matrix：在 Gitea 上把 issue **指派给我**，我会接单、开 PR（合并自动关单）并回报进展\n"
    "• 长任务我会边干边把进度更新到同一条消息；要文件我用附件发回来\n\n"
    "**命令**（群里不必 @ 也认）\n"
    "• `/help` 看这个——注意 `/help` 可能被 Matrix 客户端当自带命令吞掉，发不到我这，\n"
    "  这时用 `//help`（双斜杠发字面文本）、或 @我 带上命令\n"
    "• `/bind <URL>` 把本群 / 本私聊定到某仓库（私聊也能绑）；`/unbind` 解绑\n"
    "• `/new-project <仓库名>` 不想先手动建仓库？我在 Gitea 上新建一个（默认公开）再自动绑定本房间\n"
    "• `/status` 看我当前状态（项目 / 正在跑的任务 / 在跟的 PR）\n"
    "• `/model` 看本房间用的模型；`/model <名字>`（如 fable / opus / sonnet）单独给本房间换模型，\n"
    "  `/model reset` 恢复默认——改动对新会话生效，进行中的对话要 /reset 后才切换\n"
    "• `/summarize [N]` 小结最近 N 条对话（catch me up）\n"
    "• `/cancel` 停掉我正在跑的任务\n"
    "• `/reset` 开启新对话（清空多轮上下文）\n"
    "• `/backfill [天]` 回灌更早的聊天历史以便回溯"
)



_WELCOME = (
    "👋 我是 Claude Code 工程师 bot。@我（群里）或直接私聊就能派活：写代码、查问题、做方案，"
    "改代码会自动开 PR；闲聊、问一般问题也行。要派仓库的活，先发个 Gitea 仓库地址或 "
    "`/bind <URL>` 绑定（群和私聊都行）。发 `/help` 看完整用法"
    "（若被 Matrix 客户端当自带命令吞掉，用 `//help` 双斜杠发字面文本）。"
)



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
        f"5) 群里不全是派活——也有闲聊、与仓库无关的问题、对你上一条回复的追问：像同事一样自然接话，"
        f"别硬把话题扯回仓库，更别为此动代码。\n"
        f"用简洁中文回复，内容会直接发到群里。"
        + (_FILE_SEND_HINT if settings.send_files_back else "")
    )



async def do_bind(room: MatrixRoom, repo: dict,
                  event: RoomMessageText | None = None, task_text: str = "",
                  skip_body: str | None = None, mention_note: str = ""):
    rid = room.room_id
    try:
        prev = projects.get_room(rid)   # 换绑判断：还是同一个仓库就别重置会话（重发 URL 不该清掉多轮上下文）
        await send(rid, f"⏳ 正在绑定并 clone {repo['owner']}/{repo['repo']} …")
        rec = await projects.bind_room(rid, repo)
        if not prev or prev["id"] != rec["id"]:   # 真的换了仓库才重置本房间在该项目上的会话
            runner.reset(_sess_key(rec, rid))
            _clear_dispatched(rid)   # 会话换了 → 旧 dispatched 标记指向的是没了的会话，作废它们，
                                     # 否则新会话续接轮会误把这些跨上下文的旧消息从背景剔掉（它们不在新会话里）
            where = ("之后这条私聊都按它来（换仓库再发 /bind，/unbind 回到不绑闲聊）"
                     if _is_dm(room) else "直接在群里派活就行")
            await send(rid, f"✅ 已绑定 {rec['owner']}/{rec['repo']}（base: {rec['base']}）。{where}。")
        else:
            await send(rid, f"✅ 还是绑在 {rec['owner']}/{rec['repo']}（base: {rec['base']}），直接派活就行。")
    except Exception as e:
        log.exception("绑定失败")
        await send(rid, f"绑定失败：{e}")
        return
    if task_text and event is not None:   # 绑定后若还跟了任务，接着派下去
        await handle_task(room, event, task_text, skip_body=skip_body, mention_note=mention_note)



async def handle_new_project(room: MatrixRoom, event: RoomMessageText, body: str):
    """/new-project <仓库名> [接着派的任务]：在 GITEA_TOKEN 对应账号下新建一个仓库（默认公开），
    然后走 do_bind 同一条路（clone + 绑定本房间），免得非要先手动去 Gitea 建好库才能派活。
    命令词、仓库名、任务这三段一次 split(None, 2) 拆开——与 /bind <url> [任务] 的用法对齐。"""
    rid = room.room_id
    parts = body.split(None, 2)
    name = parts[1].strip() if len(parts) > 1 else ""
    task_text = parts[2].strip() if len(parts) > 2 else ""
    # GITEA_HOST 没配的报错交给 gitea.create_repo 自己判（它才是真正要用这个配置发请求的地方，
    # 这里只提前挡 GITEA_TOKEN——create_repo 不检查它，没有会直接匿名请求被 Gitea 拒）。
    if not settings.gitea_token:
        await send(rid, "还没配置 GITEA_TOKEN，没法建仓库。")
        return
    if not name or not _valid_name(name):
        await send(rid, "用法：`/new-project <仓库名> [接着派的任务]`（仓库名仅限字母/数字/`.` `_` `-`）。")
        return
    await send(rid, f"⏳ 正在 Gitea 上新建仓库 {name} …")
    created, err = await gitea.create_repo(name, private=settings.gitea_new_repo_private)
    if not created:
        await send(rid, f"建仓库失败：{err}")
        return
    owner = ((created.get("owner") or {}).get("login") or "").strip()
    repo = trusted_repo_info(owner, created.get("name") or name)
    if not repo:   # 理论上不会发生（owner/repo 名字来自我们自己刚建好的仓库），兜底别让用户卡住
        await send(rid, f"仓库建好了：{created.get('html_url', '')}，但没能自动绑定，请手动 `/bind <URL>`。")
        return
    await do_bind(room, repo, event, task_text)



async def handle_unbind(room: MatrixRoom):
    """/unbind：解除本房间/私聊的仓库绑定，回到「未绑定＝通用助手陪聊」。
    只清房间→项目映射，项目记录 / 本地 clone 保留（别的房间可能还在用）。"""
    rid = room.room_id
    bound = projects.get_room(rid)
    if not bound:
        await send(rid, "这条私聊本来就没绑定，直接说要干什么就行。" if _is_dm(room)
                        else "这个群还没绑定仓库。")
        return
    await projects.unbind(rid)
    # 项目登记簿也一并清掉，否则自驱心跳 / Gitea 健康度还会把这个房间当"在弄该项目"继续推消息
    if _last_project_by_room.pop(rid, None) is not None:
        _save_last_projects()
    if _is_dm(room):
        await send(rid, f"已解绑 {bound['owner']}/{bound['repo']} ✅ 现在不绑任何仓库，当通用助手陪聊；"
                        f"要再定住某个仓库发 /bind <URL>。")
    else:
        await send(rid, f"已解绑 {bound['owner']}/{bound['repo']} ✅ 群回到未绑定状态"
                        f"（可继续闲聊，派仓库活再发地址或 /bind <URL>）。")



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
        n = await transcript.backfill(state.client, rid, days)
        await send(rid, f"✅ 回灌完成，新增 {n} 条历史。之后问我“前天/上次聊了什么”就能回溯了。"
                        if n else "✅ 没有可回灌的更早历史（可能已灌过、或服务器/加密取不到更早的）。")
    except Exception as e:
        log.exception("回灌失败")
        await send(rid, f"回灌出错：{e}")



async def _auto_backfill(room_id: str):
    """开启记录后首次启动时，对还没灌过的房间静默回灌一次历史（不在房间里刷消息）。"""
    try:
        n = await transcript.backfill(state.client, room_id)
        if n:
            log.info("[%s] 历史回灌 %d 条", room_id, n)
    except Exception:
        log.exception("历史回灌失败 %s", room_id)



async def _thread_origin_line(room: MatrixRoom, thread_root: str) -> str:
    """取线程根消息作为线程会话的起点背景。线程会话与房间近况隔离后，唯一必须补的上下文是
    "这个线程在聊什么"——根消息可能从未进过房间会话（没人 @bot 时只进内存缓冲）。失败返回空串。"""
    try:
        resp = await state.client.room_get_event(room.room_id, thread_root)
        ev = getattr(resp, "event", None)
        body = (getattr(ev, "body", "") or "").strip()
        sender = getattr(ev, "sender", "") or ""
        if body:
            name = room.user_name(sender) or sender
            return f"{name}: {body[:500]}"
    except Exception:
        log.warning("取线程根消息失败 %s/%s", room.room_id, thread_root)
    return ""



async def _run_on_project(room: MatrixRoom, event: RoomMessageText, text: str, rec: dict,
                          skip_body: str | None = None, thread_root: str | None = None,
                          reply_to=None, sess_thread: str | None = None,
                          mention_note: str = ""):
    """在某项目上跑任务并回发。"正在输入"由调用方 handle_task 的 _typing 统一负责。
    skip_body：当前这条消息在背景上下文里的原文（连发合并任务是原文列表），用于从背景里
    剔除它们免得重复喂。媒体走的是 "[文件]…" 那行，和 event.body 不一样，必须由调用方显式传。
    mention_note：这条消息「@了 谁」的附注，由接收方（on_message/media）算好一次传进来——
    这里不自己重算：附注含显示名解析，接收和派活隔着排队/clone 等长间隙，两次算可能不同，
    而 skip/mark_after 靠与落背景时的原文【逐字节】匹配，重算一漂移去重就失效。
    thread_root：用户在线程里说话（或旧式 REPLY_IN_THREAD=1）时把答复挂进该线程。
    reply_to：零参可调用，发送/占位那一刻求值——群里已插进别的消息就返回触发消息 event_id
    （改用引用回复指明在回哪条），房间安静就返回 None（顶层直答）。
    sess_thread：用户自己开的线程的根 event_id → 会话细分到线程：首次派活从房间会话 fork
    （继承分叉点前的记忆，之后与房间/其它线程互相隔离），并且不再注入房间近况背景（那正是
    要隔离的串台源），只补一行线程起点。"""
    rid = room.room_id
    sender = room.user_name(event.sender) or event.sender
    trigger_eid = getattr(event, "event_id", "") or ""   # 触发本任务的消息 event_id：被删时据此精撤排队中的它
    text += mention_note   # 拼进任务正文让 Claude 看到点名对象（@pill 纯文本里只剩显示名）
    mark_after = None   # 顶层任务：等派活【成功】后再把这条标 dispatched（见下方 log「完成」处），
                        # 别在 ask 之前抢标——否则取消/报错、根本没进会话的消息也被标，下轮反被剔出背景
    if sess_thread:   # 线程任务：背景只带线程起点；fork 前的房间历史已在父会话里，fork 后要的就是隔离
        origin = await _thread_origin_line(room, sess_thread)
        ctx = f"—— 本线程起点 ——\n{origin}" if origin else ""
    else:
        cur_body = (skip_body if skip_body is not None
                    else _strip_reply_fallback(event.body or "",
                                               (event.source or {}).get("content", {})) + mention_note)
        # 已有可续接的房间会话（--resume 会带上历次派过的消息）→ 背景里就别再喂「以前派过的用户消息」；
        # 没有会话（首轮/reset/过期）→ 背景是唯一来源，drop_dispatched=False 照常全带。
        resuming = runner.session_ts(_sess_key(rec, rid)) is not None
        ctx = _format_context(rid, skip=(sender, cur_body), drop_sender=state.MY_NAME or None,
                              drop_dispatched=resuming)
        mark_after = (sender, cur_body)   # 成功派活后再标 dispatched（见下方），进了会话才算数
    if ctx:
        prompt = (
            "【所在会话最近的对话，仅供背景参考；带时间，可能跨较长时间，自行判断哪些与当前任务相关】\n"
            f"{ctx}\n\n"
            f"【当前要你处理的任务】来自 {sender}：{text}"
        )
    else:
        prompt = f"[来自 {sender}] {text}"
    log.info("[%s] 任务@%s%s: %s", rid, rec["id"], f" 线程{sess_thread[:12]}" if sess_thread else "",
             text[:80])
    if rec.get("general"):   # 通用助手：每房间独立 scratch 子目录（互不串文件）、无 employee/Gitea 指引、不碰 git
        sp = settings.claude_system_prompt + (_FILE_SEND_HINT if settings.send_files_back else "")
        if rec.get("unbound_room"):   # 未绑定仓库的房间（群或私聊）：闲聊/答疑照常，但派仓库活时要引导绑定
            sp += ("\n这里还没绑定仓库：闲聊、答疑照常自然回应；但若对方是想让你对某个仓库/项目干活"
                   "（改它的代码、查它的问题），引导他发一下 Gitea 仓库地址或 `/bind <仓库URL>`，"
                   "绑定后你才能拿到代码干活。")
        cwd = os.path.join(settings.claude_workdir, _safe_name(rid, "dm"))
        # 串行锁固定在房间维度（不带线程）：同一房间的 scratch 目录是共享的，
        # 各线程会话并行跑会在同一目录里互相踩文件。prepare 无。
        lock_key, prepare = _sess_key(rec, rid), None
    else:
        # 会话 key 带房间维度（互不串台）；lock_key 用 proj_id（同一 checkout 串行）；
        # 跑任务前先把工作树拉回干净 base，免得上个任务的脏树/残留分支污染这次。
        sp, cwd, lock_key = _employee_prompt(rec), rec["path"], rec["id"]
        sp = memory.augment_system_prompt(sp, rec["id"])   # 注入项目长期记忆（跨会话留存）
        prepare = lambda: projects.prepare_worktree(rec)
        _project_last_active[rec["id"]] = time.time()      # 标记活跃：自驱心跳会避让最近在弄的项目
    # 两层历史检索分工：transcript 给原始逐字日志的**指针**（逐字回溯的最终落点）；
    # digest 再叠一层**漏斗协议**——近 7 天主题索引已注入，引导先按天定位摘要、只在需要
    # 逐字原话时才回原始日志切片，别整篇读。
    sp = transcript.augment_system_prompt(sp, rid)
    sp = digest.augment_system_prompt(sp, rid)
    sess = _sess_key(rec, rid, sess_thread)
    # 线程首次派活：从房间会话分叉（--fork-session）。fork_from 只在线程任务时传——
    # 显式 kwargs 组装，普通任务不带此参数（测试里的假 runner 不必都认识它）。
    fork_kw = {"fork_from": _sess_key(rec, rid)} if sess_thread else {}
    if _room_model.get(rid):   # 房间 /model 覆盖：同样只在测试假 runner 认识它时才需要，条件组装
        fork_kw["model"] = _room_model[rid]
    # 在途登记：进执行/排队即记一笔，重启对账据此收尾占位/催重发（见 inflight）。摘除放 finally，
    # 覆盖成功/取消/报错所有退出路径。占位 eid 在占位创建后（首个 delta）再补录。
    inflight_key = inflight.record(rid, text, inflight.KIND_CHAT)

    def _reply_eid():   # 发送那一刻求值；未启用引用回复（DM/线程内）时恒 None
        return _resolve_reply(reply_to)

    async def _bg_notify(bg_text: str):
        # 常驻进程模式：回合结束后后台任务（子代理/后台命令）完成，Claude 续跑的产出从这里
        # 作为一条新消息投回房间（挂原线程）。与占位/inflight 无关——那套只管当轮问答。
        try:
            out = await _emit_files(room, bg_text, cwd, thread_root)
            await send(rid, out, track=True, thread_root=thread_root)
        except Exception:
            log.exception("[%s] 后台任务产出投递失败", rid)
    try:
        # 排队回执：串行锁被占（同项目已有任务在跑）或全局并发额度（MAX_CONCURRENCY）占满时
        # 立即知会，别让用户对着 typing 猜消息丢没丢。两层闸分开说：锁是同一 checkout 必须串行，
        # 额度是全局在跑的回合太多。/cancel 提示按"本房间有没有在途任务"分档——占着锁/额度的
        # 可能正是本房间自己排队中的任务（pending 连排队/准备中的都算，running 只算已起进程的），
        # 那种情况 /cancel 恰恰能停。预检尽力而为——与对方拿锁/占额度存在竞态；预检时额度还有
        # 空位、之后等锁/prepare 期间被占满的，由 _on_queued 在真正阻塞那一刻兜底补发（见下）。
        # capacity_full/pending 走 getattr 兜底：测试里的假 runner 不必都认识它们。
        own_inflight = runner.running(rid) or getattr(runner, "pending", lambda k: 0)(rid)
        cancel_hint = ("；等不及可发 /cancel 停掉本房间在途的任务。" if own_inflight
                       else "（正忙的是其它房间或自驱/工单任务，本房间 /cancel 停不了它）。")
        note = None
        if runner.busy(lock_key or sess):
            note = "⏳ 上一个任务还在跑，这条已排队，轮到会自动开始" + cancel_hint
        elif getattr(runner, "capacity_full", lambda: False)():
            note = ("⏳ 并发额度已满（MAX_CONCURRENCY，管理员可调大），这条已排队，"
                    "轮到会自动开始" + cancel_hint)
        queued_notified = {"v": note is not None}
        queued_eid = {"v": None}   # 「⏳ 已排队」回执的 event_id：静默撤单时连它一并 redact
        if note:
            queued_eid["v"] = await send(rid, note, thread_root=thread_root, reply_to=_reply_eid())

        async def _on_queued():
            # 真要在并发额度上阻塞的那一刻（预检时还有空位、等锁/prepare 期间被占满）：
            # 没知会过才补发一条；瞬时重试反复进出信号量也只发一次，不刷屏。
            if queued_notified["v"]:
                return
            queued_notified["v"] = True
            queued_eid["v"] = await send(
                rid, "⏳ 并发额度已满（MAX_CONCURRENCY），这条已排队，轮到会自动开始。",
                thread_root=thread_root, reply_to=_reply_eid())

        async def _cancelled_cleanup(silent: bool):
            # 回合被取消的收尾：steered 进来又没被处理的话撤回背景（别双头落空）。
            # 若是"因触发消息被删"而静默撤的排队任务，把之前发的「⏳ 已排队」回执也一并 redact——
            # 用户把消息删了，别单留个孤零零的排队提示。普通 /cancel（非静默）不动回执。
            for s_, b_ in _steered_dispatched.pop(rid, ()):
                _unmark_dispatched(rid, s_, b_)
            if silent:
                await _redact(rid, queued_eid["v"])
        # 两条投递路径（流式占位 / 整条直发）用完全相同的参数派活，只差 on_delta——
        # 公共 kwargs 收拢在一处，别让两个调用点各自漂移。
        ask_kwargs = dict(cwd=cwd, system_prompt=sp, lock_key=lock_key, prepare=prepare,
                          cancel_key=rid, on_reset=lambda: _clear_dispatched(rid),
                          on_notify=_bg_notify, steerable=True, on_queued=_on_queued,
                          trigger_eid=trigger_eid, **fork_kw)
        if settings.stream_replies:                      # 流式：边生成边编辑同一条占位消息
            live = _LiveReply(rid, thread_root=thread_root, reply_to=reply_to)
            _attached = {"v": False}

            async def _relay(t, tool):                   # 占位一建出来就把 eid 补进登记簿
                await live.on_delta(t, tool)
                if live.eid and not _attached["v"]:
                    inflight.attach_eid(inflight_key, live.eid)
                    _attached["v"] = True
            try:
                answer = await runner.ask(sess, prompt, on_delta=_relay, **ask_kwargs)
            except ClaudeCancelled as e:
                silent = getattr(e, "silent", False)
                await _cancelled_cleanup(silent)   # 静默撤单时连「⏳ 已排队」回执一并撤掉
                if not silent:                     # 因删消息撤的排队任务：静默收尾，别回"已停止"
                    await live.finalize("🛑 已停止。", track=False)
                elif live.eid:                     # 静默撤但占位已建（流已出过内容）→ 撤掉占位，别留残卡
                    await _redact(rid, live.eid)
                return
            except Exception as e:
                # 任务异常（超时 / 非零退出等）：先把占位收尾成报错，别让它永远停在"⏳ 正在干活…"。
                # 就地收尾即是给用户的唯一报错，故 return——不再往上抛给 handle_task 二次发一条"出错了"。
                log.exception("流式任务失败")
                try:
                    await live.finalize(_friendly_err(e, sess_key=sess), track=False)
                except Exception:
                    log.exception("占位收尾成报错也失败了")
                return
            answer = await _emit_files(room, answer, cwd, thread_root)
            await live.finalize(answer, track=True)
        else:
            try:
                answer = await runner.ask(sess, prompt, **ask_kwargs)
            except ClaudeCancelled as e:
                silent = getattr(e, "silent", False)
                await _cancelled_cleanup(silent)   # 静默撤单时连「⏳ 已排队」回执一并撤掉
                if not silent:
                    await send(rid, "🛑 已停止。", thread_root=thread_root, reply_to=_reply_eid())
                return
            answer = await _emit_files(room, answer, cwd, thread_root)
            await send(rid, answer, track=True, thread_root=thread_root, reply_to=_reply_eid())
        log.info("[%s] 完成 %d 字", rid, len(answer))
        if mark_after:   # 走到这里 = ask 成功、答复已发：这些消息确实进了会话，现在才标 dispatched，下轮背景剔掉
            m_sender, m_bodies = mark_after
            for b in (m_bodies if isinstance(m_bodies, (list, tuple)) else [m_bodies]):
                _mark_dispatched(rid, m_sender, b)
        _steered_dispatched.pop(rid, None)   # 回合善终：期间 steered 进来的话都被处理了，撤销记录不再需要
        if not rec.get("general"):   # 回复里若开了本项目的 PR，记进台账，由跟进循环盯到合并
            pr = _extract_pr(answer, rec)
            if pr and pr_ledger.record(rec["id"], pr[0], pr[1], rid):
                log.info("[%s] PR #%d 进台账，开始跟进", rid, pr[0])
    finally:
        inflight.remove(inflight_key)



def _transient_blurb(e: BaseException) -> str | None:
    """若是已知的上游瞬时故障/超时，返回一句人话定性（不含操作指引），否则 None。
    自驱/跟进等没有人类在场的路径直接用它（`_transient_blurb(e) or e`）——
    翻译瞬时性质但不给「发继续」这类没人能执行的指引。"""
    msg = str(e)
    if "响应超时" in msg:
        # 超时特意不进自动重试（回合可能已有副作用，重放不安全），只在文案层翻译
        return "⌛ Claude 响应超时（上游可能卡住了，这类不自动重试）"
    if _looks_transient(msg):
        return "🌊 上游模型服务过载/限流，自动重试几次仍未恢复"
    return None



def _friendly_err(e: BaseException, sess_key: str | None = None) -> str:
    """任务失败回报给用户的文案。已知瞬时故障/超时翻译成人话；操作指引按会话实况给：
    sess_key 查得到存活会话才承诺「发继续」（全新任务非零退出耗尽重试时从没存过 sid，
    喊继续只会开个对任务零记忆的空会话），否则引导重发任务。其它错误维持「出错了：…」原样。"""
    blurb = _transient_blurb(e)
    if blurb is None:
        return f"出错了：{e}"
    hint = ("会话没丢——稍等片刻发「继续」即可接着跑。"
            if sess_key and runner.session_ts(sess_key) is not None
            else "稍等片刻把任务重新发一遍即可。")
    return f"{blurb}。{hint}\n（{str(e)[:160]}）"



def _extract_pr(answer: str, rec: dict) -> tuple[int, str] | None:
    """从回复里抽出本项目刚开的 PR 编号 + 链接（匹配 <host>/<owner>/<repo>/pulls/<n>）。"""
    prefix = f"{(rec.get('host') or '').rstrip('/')}/{rec['owner']}/{rec['repo']}/pulls/"
    m = re.search(re.escape(prefix) + r"(\d+)", answer or "")
    return (int(m.group(1)), prefix + m.group(1)) if m else None



async def _quoted_subject(room: MatrixRoom, event: RoomMessageText) -> str:
    """引用回复里【被引用的那条消息】的正文（"发送者：内容"）。很多客户端发引用回复时只带
    m.relates_to.m.in_reply_to 这个 event_id 指针，并不把引文内联进 body / formatted_body
    （本条正文可能只剩一个 @）——于是被引用的内容对 bot 完全不可见。这里按需向服务器拉一次
    原消息补上。线程回退（rel_type=m.thread 或 is_falling_back）不是用户主动"引用某条"，
    不取；拉取失败 / 原消息无正文都返回空串。"""
    content = (getattr(event, "source", None) or {}).get("content", {})
    rel = content.get("m.relates_to") or {}
    if rel.get("rel_type") == "m.thread" or rel.get("is_falling_back"):
        return ""
    eid = (rel.get("m.in_reply_to") or {}).get("event_id")
    if not eid:
        return ""
    try:
        resp = await state.client.room_get_event(room.room_id, eid)
    except Exception:
        log.warning("取引用消息失败 %s/%s", room.room_id, eid)
        return ""
    ev = getattr(resp, "event", None)
    if ev is None:
        return ""
    src = getattr(ev, "source", None) or {}
    body = _strip_reply_fallback((getattr(ev, "body", "") or ""), src.get("content") or {}).strip()
    if not body:
        return ""
    sender = getattr(ev, "sender", "") or ""
    who = "你（bot）之前说" if sender == state.MY_ID else (room.user_name(sender) or sender)
    return f"{who}：{body[:800]}"



# 裸点名（@bot / 触发词，剥完啥也不剩、也没引用任何消息）时兜底派下去的任务正文。
# 不硬指定"就是上一条"——想让 bot 回应哪条（紧邻的上一条、还是楼上没人接的问题/报错）
# 由 Claude 结合背景对话自行判断；看不出来就反问，总比 @ 了毫无反应强。
_BARE_MENTION_TASK = (
    "我@了你，但没写具体内容。请结合最近的对话（上面的背景块，以及我们会话里的历史），"
    "判断我最可能想让你回应或处理的是哪条消息——通常是最近的消息，但也可能是更早"
    "没被接住的问题、报错或请求——直接针对它回应。实在判断不出就简短问我想让你做什么。")


async def handle_task(room: MatrixRoom, event: RoomMessageText, text: str,
                      skip_body: str | list[str] | None = None, mention_note: str = "",
                      ack_eid: str | None = None):
    rid = room.room_id
    # 回执：接手就给触发消息打 👀，处理完（含报错/取消）撤掉——用户不用盯 typing 猜有没有人接。
    # 包住整个函数体（引用回复拉取、线程解析等都在内）。ack_eid：连发合并层已在等待窗前打过 👀
    # 时传入（房间不能在合并窗里毫无反应），这里只接管撤销、不重复打。
    async with _ack(rid, getattr(event, "event_id", None), pre_eid=ack_eid):
        # 引用回复：客户端常只发 m.in_reply_to 指针、不内联引文，本条正文可能只剩一个 @。把被引用的
        # 消息拉进来——正文空时它就是要处理的主题；正文非空时作为"用户在指这条"的上文附带过去。
        # 重置类元命令不动，别把 /reset 揉进引文。
        if text.strip() not in RESET_CMDS:
            quoted = await _quoted_subject(room, event)
            if quoted:
                text = (f"我引用/回复了这条消息，请针对它回应：\n> {quoted}" if not text.strip()
                        else f"（我引用/回复了这条消息作为上文：\n> {quoted}\n）\n\n{text.strip()}")
            elif not text.strip():
                # 裸 @bot / 裸触发词（没写内容也没引用谁）：不再静默丢弃，派"结合背景自行判断
                # 要回应哪条"的兜底任务。背景为空（如刚重启）也照派——反问一句总比 @ 了没反应强。
                text = _BARE_MENTION_TASK
        # 线程策略：跟着用户走——他在线程里说话就把答复挂进那个线程，并且【会话也细分到线程】
        # （首次派活从房间会话 fork，记忆随视觉一起分叉，线程之间互不串台）；顶层消息不再强开新线程。
        # REPLY_IN_THREAD=1 保留旧式"每条顶层消息开线程"，供偏爱线程的群显式选回——但会话细分
        # 只认用户自己在线程里说话（sess_thr），旧式给顶层消息强开的线程不算。
        sess_thr = _thread_of(event)
        thr = sess_thr
        if thr is None and settings.reply_in_thread and not _is_dm(room):
            thr = _thread_root_of(event)
        # 引用回复决策器：发送/占位那一刻若群里已插进别的消息（长任务期间常有），改用引用回复
        # 指明在回哪条；房间安静就顶层直答。用尾条的 (ts,sender,body) 作稳定标识比对：别用对象身份——
        # _mark_dispatched 会把这条触发消息就地换成带 dispatched 标记的新元组，身份变了但并非新消息，
        # 用身份比会把每条顶层群回复都误判成「群里插了话」而强行引用回复。(ts,sender,body) 对同文本
        # 消息也不误判（各自 append 时的 ts 不同）。
        reply_to = None
        if thr is None and not _is_dm(room):
            dq = _context[rid]
            tail_at_start = tuple(dq[-1][:3]) if dq else None
            trigger_eid = getattr(event, "event_id", None)

            def reply_to():
                return trigger_eid if (dq and tuple(dq[-1][:3]) != tail_at_start) else None
        try:
            if text.strip() in RESET_CMDS:
                # 绑了重置该项目会话；没绑（群/私聊都一样）重置通用助手会话——总有东西可重置
                rec = projects.get_room(rid) or _general_rec()
                # 线程里发 /reset 只重置该线程的会话；顶层才重置房间会话+清背景缓冲
                runner.reset(_sess_key(rec, rid, sess_thr))
                if sess_thr:
                    await send(rid, "已重置本线程的对话 ✅（房间和其它线程不受影响）",
                               thread_root=thr)
                else:
                    _context[rid].clear()   # 连背景一起清，别让旧对话漏进新会话
                    await send(rid, "已开启新对话 ✅", thread_root=thr)
                return

            # steering：这个会话的常驻回合正在跑 → 不排队开新回合，把这条直接递进当前回合
            # （像 Claude Code 运行中打字）。产出由在跑的回合一并答复，或作为紧随的自发回合
            # 经 on_notify 投回。📎 回执（保留不撤）区别于排队；消息已进会话 → 标 dispatched，
            # 下轮背景不再重复喂。被 /cancel 杀回合时追加的话随之丢弃（与 Claude Code 一致）。
            if settings.steer_enabled:
                steer_rec = projects.get_room(rid) or _general_rec()
                sender0 = room.user_name(event.sender) or event.sender
                if await runner.try_steer(_sess_key(steer_rec, rid, sess_thr),
                                          f"[来自 {sender0}，任务进行中追加] {text}{mention_note}"):
                    eid0 = getattr(event, "event_id", None)
                    if eid0:
                        await _react(rid, eid0, "📎")
                    bodies = skip_body if isinstance(skip_body, (list, tuple)) else (
                        [skip_body] if skip_body else [])
                    for b in bodies:
                        _mark_dispatched(rid, sender0, b)
                        # 记下"这条是 steering 标的"：回合若被 /cancel 杀掉，消息随进程丢弃，
                        # 得撤标记让它回到背景（否则没被回答又被剔出背景，双头落空）
                        _steered_dispatched.setdefault(rid, []).append((sender0, b))
                    return

            # 解析/clone + 跑任务整段都开着"正在输入"，避免绑定首次 clone 时房间静默
            async with _typing(rid):
                rec = await _dispatch(room)   # 总返回一条 rec：绑定项目 or 通用助手（不再有 None）
                if not rec.get("general"):   # 通用助手不是项目，别记进项目登记簿
                    _last_project_by_room[rid] = rec["id"]
                    _save_last_projects()   # 落盘：记录"这个房间在弄哪个项目"，供自驱心跳/Gitea 健康度找汇报口
                await _run_on_project(room, event, text, rec, skip_body=skip_body,
                                      thread_root=thr, reply_to=reply_to, sess_thread=sess_thr,
                                      mention_note=mention_note)
        except ClaudeCancelled as e:
            # 静默撤（删消息）的取消绝不会外抛到这层（内层 handler 已 return），但保持一致、防御性判断：
            # 万一有 refactor 让它逃到这，也别对着已删的消息回"已停止"。
            if not getattr(e, "silent", False):
                try:
                    await send(rid, "🛑 已停止。", thread_root=thr,
                               reply_to=_resolve_reply(reply_to))
                except Exception:
                    pass
        except Exception as e:
            log.exception("处理失败")
            try:
                # 尽力算出这次任务本会用的会话 key（异常可能发生在 _dispatch 之前，rec 未必有），
                # 供文案层判断「发继续」是否真有会话可续
                try:
                    sess = _sess_key(projects.get_room(rid) or _general_rec(), rid, sess_thr)
                except Exception:
                    sess = None
                await send(rid, _friendly_err(e, sess_key=sess), thread_root=thr,
                           reply_to=_resolve_reply(reply_to))
            except Exception:
                pass



async def handle_summarize(room: MatrixRoom, event: RoomMessageText, body: str):
    """/summarize [N]：把最近 N 条对话让 Claude 做个 catch-up 小结（优先读逐字记录，否则用内存背景）。"""
    rid = room.room_id
    parts = body.split()
    n = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else settings.summary_lines
    recs = transcript.tail(rid, n) if settings.transcript_enabled else []
    if not recs:   # 没开/没历史记录就退回内存背景缓冲
        # -0 == 0，list[-0:] 是整个列表而非空表；max(1, n) 避免 /summarize 0 意外吐出全部背景
        # （transcript.tail 已有同样的 max(1, n) 兜底，这里的内存回退路径此前漏了）
        recs = [{"ts": ts, "sender": s, "body": b}
                for ts, s, b, *_ in list(_context[rid])[-max(1, n):]]

    def _is_summary_cmd(b) -> bool:   # 去掉 /summarize、/catchup（含带参数形式）命令本身
        b = (b or "").strip().lower()
        return b in SUMMARY_CMDS or b.startswith(("/summarize", "/catchup"))
    recs = [r for r in recs if not _is_summary_cmd(r.get("body"))]
    if not recs:
        await send(rid, "还没有可总结的对话。")
        return
    convo = "\n".join(
        f"[{time.strftime('%m-%d %H:%M', time.localtime(r.get('ts', 0)))}] "
        f"{r.get('sender', '?')}: {(r.get('body') or '').strip()[:500]}" for r in recs)
    prompt = (
        "下面是一个群聊最近的对话记录。请用简洁中文做一个 catch-up（追更）小结：\n"
        "- 讨论了哪些主题、有什么结论或决定\n"
        "- 还有哪些待办 / 未决问题、各自在等谁\n"
        "- 若提到具体任务或 bug，点出来\n"
        "控制在十几行内、分点写，别逐条复述。\n\n对话：\n" + convo)
    async with _ack(rid, getattr(event, "event_id", None)), _typing(rid):
        try:
            ans = (await runner.quick(prompt)).strip()
        except Exception as e:
            log.exception("总结失败")
            await send(rid, f"总结失败：{e}")
            return
    await send(rid, "📋 最近对话小结：\n" + ans)



async def handle_cancel(room: MatrixRoom):
    """/cancel：停掉本房间在途的任务——运行中的杀进程、排队/准备中的取消掉，
    连发合并窗里还没派出去的缓冲也一并作废（否则"已取消"几秒后任务照跑）。文案按实际区分。"""
    rid = room.room_id
    res = runner.cancel(rid)
    # runner.cancel 返回 (运行中被停, 排队中被取消)；兼容旧式只返回单个 int（测试里的假 runner）。
    running, queued = res if isinstance(res, tuple) else (res, 0)
    queued += _drop_pending(rid)   # 合并窗中的待派消息：runner 还不知道它们，这里直接作废
    if running:
        msg = "🛑 已停止正在运行的任务。"
        if queued:
            msg += f"排队中的 {queued} 个任务也一并取消了。"
    elif queued:
        msg = "🛑 已取消排队中的任务（还没轮到，不会再开跑了）。"
    else:
        msg = "现在没有正在运行或排队的任务。"
    await send(rid, msg)



def _sess_model_now(rid: str) -> str:
    """本房间当前可续接会话「创建时记录的模型」；无存活会话/旧版条目没记录返回 ""。
    /model 与 /status 用它提示"改了设置但对进行中的会话不生效"。getattr 兜底：
    测试里的假 runner 不必都认识 session_model。"""
    skey = _sess_key(projects.get_room(rid) or _general_rec(), rid)
    if runner.session_ts(skey) is None:
        return ""
    return getattr(runner, "session_model", lambda k: "")(skey)


async def handle_model(room: MatrixRoom, body: str):
    """/model：查看/设置本房间用的模型（如 fable / opus / sonnet）。
    · `/model`            看当前用的模型（本房间覆盖 > 全局 CLAUDE_MODEL > CLI 默认）
    · `/model <名字>`     给本房间单独设模型（存下来，重启不丢）
    · `/model reset`      清掉本房间覆盖，回到全局配置
    与 CLAUDE_MODEL 同语义：--resume 不回传 --model，改动只对【新开的会话】生效，
    进行中的会话要 /reset 翻篇后才切换——设置时如有存活会话会顺带提示。"""
    rid = room.room_id
    parts = body.split(None, 1)
    arg = (parts[1].strip() if len(parts) > 1 else "")
    override = _room_model.get(rid, "")
    eff = override or settings.claude_model
    sess_model = _sess_model_now(rid)
    stale = sess_model and sess_model != (eff or "")   # 存活会话还跑在别的模型上

    if not arg:   # 查看
        if override:
            lines = [f"🧠 当前模型：{override}（本房间通过 /model 设置；`/model reset` 恢复默认）"]
        elif settings.claude_model:
            lines = [f"🧠 当前模型：{settings.claude_model}（全局配置 CLAUDE_MODEL，本房间未单独设置）"]
        else:
            lines = ["🧠 当前模型：跟随 Claude CLI 的默认（未配置 CLAUDE_MODEL，本房间也未单独设置）"]
        if stale:
            lines.append(f"• 进行中的会话仍在用 {sess_model}（/reset 开新对话后按当前设置走）")
        lines.append("• 换模型：`/model <名字>`（如 fable / opus / sonnet / haiku）")
        await send(rid, "\n".join(lines))
        return

    if arg.lower() in _MODEL_RESET_WORDS:   # 恢复默认
        if _room_model.pop(rid, None) is None:
            await send(rid, "本房间没有单独设置过模型，现在用的就是默认"
                            + (f"（{settings.claude_model}）。" if settings.claude_model else "。"))
            return
        _save_room_models()
        msg = "✅ 已恢复默认模型" + (f"（{settings.claude_model}）" if settings.claude_model
                                     else "（跟随 Claude CLI 默认）")
        if sess_model and sess_model != (settings.claude_model or ""):
            msg += f"。进行中的会话仍在用 {sess_model}，/reset 后生效"
        await send(rid, msg + "。")
        return

    if not _MODEL_NAME_RE.match(arg):
        await send(rid, "模型名只能是字母/数字开头，含 `.` `_` `:` `-`，"
                        "比如 `/model fable`、`/model opus`；`/model reset` 恢复默认。")
        return
    if arg == override or (not override and arg == settings.claude_model):
        already = "本房间已经在用" if override else "全局配置就是"
        await send(rid, f"🧠 {already} {arg}，无需改动。"
                        + (f"（进行中的会话仍在用 {sess_model}，/reset 后切换）" if stale else ""))
        return
    _room_model[rid] = arg
    _save_room_models()
    msg = f"✅ 本房间模型已设为 {arg}"
    if sess_model and sess_model != arg:
        msg += f"。进行中的会话仍在用 {sess_model}，发 /reset 开新对话后生效"
    else:
        msg += "，下次派活生效"
    await send(rid, msg + "。")


async def handle_status(room: MatrixRoom):
    """/status：本房间视角的运行状态——项目、正在跑的任务、在跟的 PR、会话新鲜度、主动性开关。
    给用户一个"你现在在干嘛 / 盯着啥"的窗口，不用翻日志。"""
    rid = room.room_id
    lines = ["📊 **当前状态**"]
    rec = projects.get_room(rid)   # 群 / 私聊的绑定（同一套：绑了才有项目）
    if rec:
        # 私聊绑定点明可解绑；群沿用原 base 文案
        pinned = "（已绑定，消息都归它；/unbind 解绑回闲聊）" if _is_dm(room) else f"（base: {rec['base']}）"
        lines.append(f"• 项目：{rec['owner']}/{rec['repo']}{pinned}")
    else:
        lines.append("• 项目：未绑定——发仓库地址或 /bind <URL> 绑定；不绑我就当通用助手陪聊/答疑")
    if _room_model.get(rid):
        lines.append(f"• 模型：{_room_model[rid]}（本房间 /model 设置；`/model reset` 恢复默认）")
    n_run = runner.running(rid)
    # 排队/准备中 = 在途但还没起子进程的（pending 含全部在途，running 只算已起进程的）。
    # getattr 兜底同排队回执处：测试里的假 runner 不必都认识 pending/capacity_full。
    n_wait = max(0, getattr(runner, "pending", lambda k: 0)(rid) - n_run)
    lines.append(f"• 本房间正在跑的任务：{n_run} 个" if n_run else "• 本房间没有正在跑的任务")
    if n_wait:
        lines.append(f"• 本房间排队/准备中的任务：{n_wait} 个（/cancel 可取消）")
    if rec and not n_run and not n_wait and runner.busy(rec["id"]):
        # 本房间毫无在途任务、锁却被占 → 占用者才真是别处；本房间有排队任务时它就可能是
        # 占锁者本人，上面那行已经说清，这行的"其它房间在用"反而是误导，不再重复。
        lines.append("• 项目工作树正忙（其它房间/自驱任务在用，新任务会排队）")
    if getattr(runner, "capacity_full", lambda: False)():
        lines.append("• 全局并发额度已占满（MAX_CONCURRENCY），新任务会先排队等空位")
    prs = [e for e in pr_ledger.active()
           if e.get("room") == rid or (rec and e.get("pid") == rec["id"])]
    cap = settings.pr_autofix_max
    for e in prs[:10]:
        lines.append(f"• 在跟 PR #{e.get('number')}：{e.get('url', '')}"
                     f"（评审已自动处理 {e.get('review_fixes', 0)}/{cap} 次，"
                     f"CI 已自动修 {e.get('ci_fixes', 0)}/{cap} 次）")
    if not prs:
        lines.append("• 没有在跟的 PR")
    issues = [e for e in issue_ledger.active()
              if e.get("room") == rid or (rec and e.get("pid") == rec["id"])]
    for e in issues[:10]:
        lines.append(f"• 在办工单 #{e.get('number')}：{e.get('url', '')}"
                     + (f"（已开 PR #{e['pr']}）" if e.get("pr") else "（处理中）"))
    if rec:
        skey = _sess_key(rec, rid)
        ts = runner.session_ts(skey)
        lines.append(f"• 多轮会话：{_human_gap(max(0.0, time.time() - ts))}前活跃（/reset 可重开）"
                     if ts else "• 多轮会话：无（下次派活即新开）")
        # 模型设置漂移提示：续接的会话沿用创建时记录的模型（--resume 不回传 --model），改了
        # 房间 /model 或 CLAUDE_MODEL 对它不生效且不会自行过期——在这里暴露出来，别让用户以为已切换。
        # getattr 兜底同上：测试里的假 runner 不必都认识 session_model。
        sess_model = getattr(runner, "session_model", lambda k: "")(skey)
        eff_model = _room_model.get(rid) or settings.claude_model
        if ts and sess_model and eff_model and sess_model != eff_model:
            lines.append(f"• 会话模型：{sess_model}（设置已改为 {eff_model}，"
                         "对当前会话不生效，/reset 后按新值走）")
    if settings.proactive_heartbeat_enabled:
        import heartbeat   # 函数内延迟导入：heartbeat 模块顶层反过来 import 本模块，避免循环导入
        active = "" if heartbeat._in_heartbeat_window(time.time()) else "，当前不在巡检时段"
        hb = (f"开（每 {settings.proactive_heartbeat_interval // 60} 分钟，"
              f"autopilot={'开' if settings.proactive_autopilot else '关'}{active}）")
    else:
        hb = "关"
    lines.append(f"• 主动插话={'开' if settings.proactive else '关'} · 自驱心跳={hb}"
                 f" · 工单接活={'开' if settings.issue_intake_enabled else '关'}")
    gitea_line = gitea_health.status_line(gitea.health())   # Gitea 连不上/ token 失效时在这暴露出来
    if gitea_line:
        lines.append(gitea_line)
    await send(rid, "\n".join(lines))

