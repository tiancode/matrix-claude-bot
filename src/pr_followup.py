"""PR 台账跟进：开了 PR 就盯到合并——处理评审/CI、满足条件自动合并、回报原房间。"""
import asyncio
import logging
import time

from config import settings
import state
from state import _sess_key
from matrix_io import send, _typing
from tasks import _employee_prompt
from projects import projects
from claude_runner import runner, ClaudeCancelled
import gitea
import gitea_health
import pr_ledger
import memory

log = logging.getLogger("matrix-claude.pr")

# 连续这么多轮「确切 404」才把台账条目销账。为什么必须 >1：单次 404 常是 Gitea 抖动 /
# 反向代理瞬断，一次就销会误杀其实还活着的 PR / 工单；连续多轮都 404 才敢定性「真没了」。
_GONE_ROUNDS_LIMIT = 3


async def reconcile_gone(entry: dict, rec: dict, *, gone_check, ledger, noun: str,
                         reason: str, log: logging.Logger) -> None:
    """PR 跟进 / 工单接活共用的「连续 ≥N 轮确切 404 才销账」状态机（两边台账 API 同形，故收敛成一处）。

    gone_check: async (rec, number) -> bool 的确切-404 判定（gitea.pr_gone / gitea.issue_gone）；
    ledger: 有 remove/update(pid, number, **fields) 的台账模块（pr_ledger / issue_ledger）；
    noun / reason: 通知文案里的名词与括注（PR「被删 / 仓库改名」 vs 工单「被删 / 转移」）；
    log: 各自的 logger（保持 PR / 工单日志分流不变）。

    确切 404 才累加轮数，攒够 _GONE_ROUNDS_LIMIT 轮才销账并知会房间；非 404 的查不到（网络抖动）
    不计入、并把之前攒的轮数清零，免得一次断网就把台账条目误销。调用前须保证 rec 非 None。
    """
    pid, n = entry["pid"], entry["number"]
    if await gone_check(rec, n):
        gone = entry.get("gone_rounds", 0) + 1
        if gone >= _GONE_ROUNDS_LIMIT:
            ledger.remove(pid, n)
            room = entry.get("room") or ""
            if room:
                await send(room, f"⚠️ {noun} #{n} 在 Gitea 上已不存在（{reason}），停止跟进。")
            log.info("[%s] %s #%d 连续 %d 轮 404，销账", pid, noun, n, gone)
        else:
            ledger.update(pid, n, gone_rounds=gone)
    elif entry.get("gone_rounds"):
        ledger.update(pid, n, gone_rounds=0)   # 抖动而非真没了：清零重新计


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
            # cancel_key=原房间：让房间里的 /cancel 也能停掉跟进任务
            answer = await runner.ask(_sess_key(rec, room), prompt, cwd=rec["path"],
                                      system_prompt=sp, lock_key=rec["id"],
                                      prepare=lambda: projects.prepare_worktree(rec),
                                      cancel_key=room)
        await send(room, f"🔁 PR #{n} 跟进结果：\n{answer}", track=True)
    except ClaudeCancelled:
        await send(room, f"🛑 已停止 PR #{n} 的跟进任务。")
    except Exception as e:
        log.exception("PR #%s 跟进任务失败", n)
        await send(room, f"PR #{n} 跟进出错：{e}")



async def _handle_missing(entry: dict):
    """pr_info 查不到时定性：连续 ≥3 轮确切 404 才销账、网络抖动不计入并清零。状态机见 reconcile_gone。"""
    rec = projects.get_project(entry["pid"])
    if rec:   # 调用方 _followup_one 已保证 rec 非 None，这里冗余守一下
        await reconcile_gone(entry, rec, gone_check=gitea.pr_gone, ledger=pr_ledger,
                             noun="PR", reason="被删 / 仓库改名", log=log)


async def _followup_one(entry: dict):
    pid, n, room = entry["pid"], entry["number"], entry["room"]
    rec = projects.get_project(pid)
    if not rec:
        return
    info = await gitea.pr_info(rec, n)
    if info is None:
        await _handle_missing(entry)   # 查不到：分辨"网络抖动（下轮再试）"和"PR 真没了（连续 404 才销账）"
        return
    if entry.get("gone_rounds"):
        pr_ledger.update(pid, n, gone_rounds=0)   # 成功查到一次 → 之前攒的 404 轮数清零
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

    # 1) 新的评审意见（请求改动 / 评论）—— 评审优先于 CI。
    #    bot 自己（同 token）发的评论不算新意见：水位照常推进但不派活，免得它回应评审时自己触发自己。
    reviews = await gitea.pr_reviews(rec, n)
    own = await gitea.own_user_id()
    fresh_all = [r for r in reviews if isinstance(r.get("id"), int) and r["id"] > entry.get("seen_review", 0)
                 and r.get("state") in ("REQUEST_CHANGES", "COMMENT")]
    fresh = [r for r in fresh_all if own is None or (r.get("user") or {}).get("id") != own]
    if fresh_all:
        pr_ledger.update(pid, n, seen_review=max(r["id"] for r in fresh_all))
    if fresh:
        if entry.get("review_fixes", 0) < cap:
            pr_ledger.update(pid, n, review_fixes=entry.get("review_fixes", 0) + 1)
            bodies = "\n".join(f"- [{r.get('state')}] {(r.get('body') or '(见行内评论)').strip()[:300]}"
                               for r in fresh)
            await send(room, f"📝 PR #{n} 收到评审意见，我去处理…")
            state._spawn(_followup_dispatch(rec, entry, f"PR #{n} 收到评审意见：\n{bodies}"))
        else:
            await send(room, f"📝 PR #{n} 又有评审意见，但已到自动处理上限（{cap} 次），需要人看看：{entry.get('url', '')}")
        return

    # 2) CI 失败（ci 可能为 None=查询失败：None 不在 ("failure","error") 里，天然不会误报"CI 失败"）
    ci = await gitea.ci_state(rec, sha)
    if ci in ("failure", "error") and entry.get("ci_seen") != sha:
        pr_ledger.update(pid, n, ci_seen=sha)
        if entry.get("ci_fixes", 0) < cap:
            pr_ledger.update(pid, n, ci_fixes=entry.get("ci_fixes", 0) + 1)
            await send(room, f"❌ PR #{n} CI 失败，我去修…")
            state._spawn(_followup_dispatch(rec, entry, f"PR #{n} 的 CI（持续集成）检查失败，请定位并修复后推送。"))
        else:
            await send(room, f"❌ PR #{n} CI 还失败，已到自动处理上限（{cap} 次），需要人看看：{entry.get('url', '')}")
        return

    # 3) 没有待处理评审、CI 也不失败：满足条件就自动合并（PR_AUTOMERGE=1 才开）
    if settings.pr_automerge:
        await _maybe_automerge(rec, entry, info, ci, reviews)



async def _maybe_automerge(rec: dict, entry: dict, info: dict, ci: str | None, reviews: list):
    """followup 末尾的机械合并闸：PR 可合并 + CI 通过(或无 CI 配置) + 无未决"请求改动"评审 →
    直接按 Gitea API 合并、销账、回报。不经 Claude 评审（"只做合并"）；移除人工合并这道闸，
    安全性靠 CI（若配了）+ 快照环境。合并失败保持 PR 开启，下轮再试或等人工。
    "盯到合并"=遇冲突 / 合并失败要吭声，别每 180s 沉默重查——按 head sha 记水位，同一版本只告警一次。"""
    pid, n, room = entry["pid"], entry["number"], entry["room"]
    sha = (info.get("head") or {}).get("sha") or ""
    if not info.get("mergeable"):                # 有冲突 / 暂不可合并
        if entry.get("conflict_seen") != sha:    # 首见这个版本的冲突 → 告警一次（换 sha 会再报）
            pr_ledger.update(pid, n, conflict_seen=sha)
            await send(room, f"⚠️ PR #{n} 有冲突无法自动合并，需要人工处理或让我重做：{entry.get('url', '')}")
            log.info("[%s] PR #%d 有冲突，已知会", pid, n)
        return
    if entry.get("conflict_seen"):               # 变回可合并：清冲突水位，下次再冲突还能再报
        pr_ledger.update(pid, n, conflict_seen="")
    if ci is None:                               # CI 状态查询失败（网络抖动）：本轮别赌，跳过合并
        return
    if ci not in ("", "success"):                # pending：CI 没跑完就先等（failure 已在上一段处理）
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
    elif entry.get("merge_fail_seen") != sha:    # 同一 sha 的合并失败只告警一次，别刷屏
        pr_ledger.update(pid, n, merge_fail_seen=sha)
        await send(room, f"⚠️ PR #{n} 自动合并失败，需要人工处理或让我重做：{entry.get('url', '')}\n（原因：{detail}）")
        log.warning("[%s] PR #%d 自动合并失败（保持开启，待人工）：%s", pid, n, detail)
    else:
        log.warning("[%s] PR #%d 自动合并仍失败（同一 sha 不再刷屏）：%s", pid, n, detail)



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
            await gitea_health.check_and_alert()   # 搭车这轮巡检：Gitea 连不上/token 失效就告警一次
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("PR 跟进循环异常，继续")

