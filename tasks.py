"""在某项目上跑 Claude 任务并回发；以及元命令 /reset /summarize /cancel /backfill /bind。"""
import logging
import os
import re
import time

from nio import MatrixRoom, RoomMessageText

from config import settings
import state
from state import _context, _sess_key, _last_project_by_room, _save_last_projects, _project_last_active
from matrix_io import send, _typing, _is_dm, _LiveReply, _emit_files, _thread_root_of, _FILE_SEND_HINT
from fmt import _format_context, _safe_name, _human_gap
from addressing import _strip_reply_fallback
from dispatch import _dispatch
from projects import projects
from claude_runner import runner, ClaudeCancelled
import memory
import issue_ledger
import pr_ledger
import transcript
import inflight
import gitea
import gitea_health

log = logging.getLogger("matrix-claude.tasks")


RESET_CMDS = {"/reset", "/new", "重置", "新对话", "清空"}



HELP_CMDS = {"/help", "/?", "帮助", "用法"}



SUMMARY_CMDS = {"/summarize", "/catchup", "总结", "回顾", "小结"}   # 也认 "/summarize N" 前缀



CANCEL_CMDS = {"/cancel", "/stop", "停止", "取消", "停"}            # 也认 "/cancel"/"/stop" 前缀



STATUS_CMDS = {"/status", "状态"}                                   # 也认 "/status" 前缀



_HELP_TEXT = (
    "**我能干嘛**\n"
    "我是接到 Matrix 的 Claude Code 工程师：群里 @我 或私聊我就能派活——写代码、查问题、做方案，"
    "改代码会自动开 PR 并跟到合并。群里没点名时，遇到求助或明显错误我也可能主动插一句。\n\n"
    "**怎么用**\n"
    "• 群里先绑仓库：发 Gitea 仓库地址，或 `/bind <仓库URL>`\n"
    "• 然后 @我 派活；刚 @过我的几分钟内不必每句都 @（对话延续窗口）\n"
    "• 私聊直接说，我自动判断是哪个项目\n"
    "• 也可以不进 Matrix：在 Gitea 上把 issue **指派给我**，我会接单、开 PR（合并自动关单）并回报进展\n"
    "• 长任务我会边干边把进度更新到同一条消息；要文件我用附件发回来\n\n"
    "**命令**（群里不必 @ 也认）\n"
    "• `帮助` / `用法` 看这个——注意 `/help` 会被 Matrix 客户端当自带命令吞掉，发不到我这，\n"
    "  想发斜杠命令用中文词、或 `//help`（双斜杠发字面文本）、或 @我 带上命令\n"
    "• `/bind <URL>` 绑定本群到仓库\n"
    "• `/status` 看我当前状态（项目 / 正在跑的任务 / 在跟的 PR）\n"
    "• `/summarize [N]` 小结最近 N 条对话（catch me up）\n"
    "• `/cancel` 停掉我正在跑的任务\n"
    "• `/reset` 开启新对话（清空多轮上下文）\n"
    "• `/backfill [天]` 回灌更早的聊天历史以便回溯"
)



_WELCOME = (
    "👋 我是 Claude Code 工程师 bot。@我（群里）或直接私聊就能派活：写代码、查问题、做方案，"
    "改代码会自动开 PR。群里先发个 Gitea 仓库地址或 `/bind <URL>` 绑定本群。发 `帮助` 看完整用法"
    "（`/help` 会被 Matrix 客户端自己吞掉，用中文词 `帮助`）。"
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
        f"用简洁中文回复，内容会直接发到群里。"
        + (_FILE_SEND_HINT if settings.send_files_back else "")
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



async def _run_on_project(room: MatrixRoom, event: RoomMessageText, text: str, rec: dict,
                          skip_body: str | None = None, thread_root: str | None = None):
    """在某项目上跑任务并回发。"正在输入"由调用方 handle_task 的 _typing 统一负责。
    skip_body：当前这条消息在背景上下文里的原文，用于从背景里剔除它免得重复喂。
    媒体走的是 "[文件]…" 那行，和 event.body（文件名/caption）不一样，必须由调用方显式传。
    thread_root：群里把答复挂进该线程；流式时占位消息也挂这里。"""
    rid = room.room_id
    sender = room.user_name(event.sender) or event.sender
    cur_body = (skip_body if skip_body is not None
                else _strip_reply_fallback(event.body or "", (event.source or {}).get("content", {})))
    ctx = _format_context(rid, skip=(sender, cur_body), drop_sender=state.MY_NAME or None)
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
        sp = settings.claude_system_prompt + (_FILE_SEND_HINT if settings.send_files_back else "")
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
    sess = _sess_key(rec, rid)
    # 在途登记：进执行/排队即记一笔，重启对账据此收尾占位/催重发（见 inflight）。摘除放 finally，
    # 覆盖成功/取消/报错所有退出路径。占位 eid 在占位创建后（首个 delta）再补录。
    inflight_key = inflight.record(rid, text, inflight.KIND_CHAT)
    try:
        # 排队回执：串行锁被占（同项目已有任务在跑）时立即知会，别让用户对着 typing 猜消息丢没丢。
        # 尽力而为——与对方拿锁存在竞态，漏发只影响提示不影响排队本身。
        if runner.busy(lock_key or sess):
            note = "⏳ 上一个任务还在跑，这条已排队，轮到会自动开始"
            note += ("；等不及可发 /cancel 停掉当前任务。" if runner.running(rid)
                     else "（正忙的是其它房间或自驱/工单任务，本房间 /cancel 停不了它）。")
            await send(rid, note, thread_root=thread_root)
        if settings.stream_replies:                      # 流式：边生成边编辑同一条占位消息
            live = _LiveReply(rid, thread_root=thread_root)
            _attached = {"v": False}

            async def _relay(t, tool):                   # 占位一建出来就把 eid 补进登记簿
                await live.on_delta(t, tool)
                if live.eid and not _attached["v"]:
                    inflight.attach_eid(inflight_key, live.eid)
                    _attached["v"] = True
            try:
                answer = await runner.ask(sess, prompt, cwd=cwd, system_prompt=sp,
                                          lock_key=lock_key, prepare=prepare,
                                          on_delta=_relay, cancel_key=rid)
            except ClaudeCancelled:
                await live.finalize("🛑 已停止。", track=False)
                return
            except Exception as e:
                # 任务异常（超时 / 非零退出等）：先把占位收尾成报错，别让它永远停在"⏳ 正在干活…"。
                # 就地收尾即是给用户的唯一报错，故 return——不再往上抛给 handle_task 二次发一条"出错了"。
                log.exception("流式任务失败")
                try:
                    await live.finalize(f"出错了：{e}", track=False)
                except Exception:
                    log.exception("占位收尾成报错也失败了")
                return
            answer = await _emit_files(room, answer, cwd, thread_root)
            await live.finalize(answer, track=True)
        else:
            try:
                answer = await runner.ask(sess, prompt, cwd=cwd, system_prompt=sp,
                                          lock_key=lock_key, prepare=prepare, cancel_key=rid)
            except ClaudeCancelled:
                await send(rid, "🛑 已停止。", thread_root=thread_root)
                return
            answer = await _emit_files(room, answer, cwd, thread_root)
            await send(rid, answer, track=True, thread_root=thread_root)
        log.info("[%s] 完成 %d 字", rid, len(answer))
        if not rec.get("general"):   # 回复里若开了本项目的 PR，记进台账，由跟进循环盯到合并
            pr = _extract_pr(answer, rec)
            if pr and pr_ledger.record(rec["id"], pr[0], pr[1], rid):
                log.info("[%s] PR #%d 进台账，开始跟进", rid, pr[0])
    finally:
        inflight.remove(inflight_key)



def _extract_pr(answer: str, rec: dict) -> tuple[int, str] | None:
    """从回复里抽出本项目刚开的 PR 编号 + 链接（匹配 <host>/<owner>/<repo>/pulls/<n>）。"""
    prefix = f"{(rec.get('host') or '').rstrip('/')}/{rec['owner']}/{rec['repo']}/pulls/"
    m = re.search(re.escape(prefix) + r"(\d+)", answer or "")
    return (int(m.group(1)), prefix + m.group(1)) if m else None



async def handle_task(room: MatrixRoom, event: RoomMessageText, text: str,
                      skip_body: str | None = None):
    rid = room.room_id
    # 群里把答复挂进"提问那条"的线程；私聊不挂线程（1:1 无需分流，挂了反而碍眼）。
    thr = _thread_root_of(event) if (settings.reply_in_thread and not _is_dm(room)) else None
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
                await send(rid, "已开启新对话 ✅", thread_root=thr)
            else:
                await send(rid, "还没有可重置的会话；先发个仓库地址或派个活吧。", thread_root=thr)
            return

        # 分诊/clone + 跑任务整段都开着"正在输入"，避免 DM 首次路由时房间静默
        async with _typing(rid):
            rec = await _dispatch(room, event, text, skip_body=skip_body)
            if rec is None:
                return
            if not rec.get("general"):   # 通用助手不是项目，别记成"上次项目"
                _last_project_by_room[rid] = rec["id"]
                _save_last_projects()   # 落盘：重启后 DM 的 /reset 与多轮延续仍能定位项目
            await _run_on_project(room, event, text, rec, skip_body=skip_body, thread_root=thr)
    except ClaudeCancelled:
        try:
            await send(rid, "🛑 已停止。", thread_root=thr)
        except Exception:
            pass
    except Exception as e:
        log.exception("处理失败")
        try:
            await send(rid, f"出错了：{e}", thread_root=thr)
        except Exception:
            pass



async def handle_summarize(room: MatrixRoom, event: RoomMessageText, body: str):
    """/summarize [N]：把最近 N 条对话让 Claude 做个 catch-up 小结（优先读逐字记录，否则用内存背景）。"""
    rid = room.room_id
    parts = body.split()
    n = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else settings.summary_lines
    recs = transcript.tail(rid, n) if settings.transcript_enabled else []
    if not recs:   # 没开/没历史记录就退回内存背景缓冲
        recs = [{"ts": ts, "sender": s, "body": b} for ts, s, b in list(_context[rid])[-n:]]

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
    async with _typing(rid):
        try:
            ans = (await runner.quick(prompt)).strip()
        except Exception as e:
            log.exception("总结失败")
            await send(rid, f"总结失败：{e}")
            return
    await send(rid, "📋 最近对话小结：\n" + ans)



async def handle_cancel(room: MatrixRoom):
    """/cancel：停掉本房间在途的任务——运行中的杀进程、排队/准备中的取消掉。文案按实际区分三种情况。"""
    rid = room.room_id
    res = runner.cancel(rid)
    # runner.cancel 返回 (运行中被停, 排队中被取消)；兼容旧式只返回单个 int（测试里的假 runner）。
    running, queued = res if isinstance(res, tuple) else (res, 0)
    if running:
        msg = "🛑 已停止正在运行的任务。"
        if queued:
            msg += f"排队中的 {queued} 个任务也一并取消了。"
    elif queued:
        msg = "🛑 已取消排队中的任务（还没轮到，不会再开跑了）。"
    else:
        msg = "现在没有正在运行或排队的任务。"
    await send(rid, msg)



async def handle_status(room: MatrixRoom):
    """/status：本房间视角的运行状态——项目、正在跑的任务、在跟的 PR、会话新鲜度、主动性开关。
    给用户一个"你现在在干嘛 / 盯着啥"的窗口，不用翻日志。"""
    rid = room.room_id
    lines = ["📊 **当前状态**"]
    if not _is_dm(room):
        rec = projects.get_room(rid)
        lines.append(f"• 项目：{rec['owner']}/{rec['repo']}（base: {rec['base']}）" if rec
                     else "• 项目：未绑定（发仓库地址或 /bind <URL>）")
    else:
        pid = _last_project_by_room.get(rid)
        rec = projects.get_project(pid) if pid else None
        if rec:
            lines.append(f"• 项目：{rec['owner']}/{rec['repo']}（按对话自动路由，点名即可切换）")
        else:
            known = projects.list_projects()
            lines.append("• 项目：待定（我按消息内容自动分诊）"
                         + (f"；已知：{'、'.join(p['owner'] + '/' + p['repo'] for p in known[:8])}"
                            if known else ""))
    n_run = runner.running(rid)
    lines.append(f"• 本房间正在跑的任务：{n_run} 个" if n_run else "• 本房间没有正在跑的任务")
    if rec and not n_run and runner.busy(rec["id"]):
        lines.append("• 项目工作树正忙（其它房间/自驱任务在用，新任务会排队）")
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
        ts = runner.session_ts(_sess_key(rec, rid))
        lines.append(f"• 多轮会话：{_human_gap(max(0.0, time.time() - ts))}前活跃（/reset 可重开）"
                     if ts else "• 多轮会话：无（下次派活即新开）")
    hb = (f"开（每 {settings.proactive_heartbeat_interval // 60} 分钟，"
          f"autopilot={'开' if settings.proactive_autopilot else '关'}）"
          if settings.proactive_heartbeat_enabled else "关")
    lines.append(f"• 主动插话={'开' if settings.proactive else '关'} · 自驱心跳={hb}"
                 f" · 工单接活={'开' if settings.issue_intake_enabled else '关'}")
    gitea_line = gitea_health.status_line(gitea.health())   # Gitea 连不上/ token 失效时在这暴露出来
    if gitea_line:
        lines.append(gitea_line)
    await send(rid, "\n".join(lines))

