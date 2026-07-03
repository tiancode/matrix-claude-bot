"""Gitea 工单接活：把 issue 指派给 bot 账号 = 派活——不进 Matrix 也能给"员工"下任务。

轮询各已知项目里 assigned 给 bot 的 open issue：认领（issue 下评论）→ 像平常派活一样
干完开 PR（body 带 Closes #N，合并即自动关单）→ PR 进台账由跟进循环盯到合并 →
进展回报到项目的"汇报口"房间。台账见 issue_ledger（持久化，重启不重复接单）。
"""
import asyncio
import logging
import os
import time

from config import settings
import state
from state import _sess_key, _project_last_active
from matrix_io import send, _typing
from tasks import _employee_prompt, _extract_pr
from heartbeat import _project_home_room
from projects import projects
from claude_runner import runner, ClaudeCancelled
import gitea
import issue_ledger
import pr_ledger
import memory
import inflight

log = logging.getLogger("matrix-claude.issue")


def _issue_brief(issue: dict) -> str:
    n, title = issue.get("number"), (issue.get("title") or "").strip()
    body = (issue.get("body") or "").strip()
    return f"Issue #{n}: {title}" + (f"\n\n{body[:2000]}" if body else "")


async def _issue_execute(rec: dict, room: str, issue: dict):
    """把一条工单当正式派活做完：开 PR（带 Closes #N）→ 进 PR 台账 → 回报房间 + issue 下留言。"""
    n = issue["number"]
    # 在途登记：执行中途崩了，重启对账（issue_ledger pr==0 那条路）据此重派；摘除放 finally。
    inflight_key = inflight.record(room, _issue_brief(issue), inflight.KIND_ISSUE, issue=n)
    try:
        comments = await gitea.issue_comments(rec, n)
        ctx = "\n".join(f"- {(c.get('user') or {}).get('login') or '?'}: {(c.get('body') or '').strip()[:300]}"
                        for c in comments[-10:] if (c.get("body") or "").strip())
        prompt = (
            f"Gitea 上有人把这个 issue 指派给你，请把它当成正式派活完成：\n{_issue_brief(issue)}\n"
            + (f"\nissue 下的讨论（供参考）：\n{ctx}\n" if ctx else "")
            + f"\n要改代码就照常建分支、commit、push、开 PR——PR 描述里必须带一行 `Closes #{n}`"
            f"（合并后 Gitea 自动关单），最终回复附上 PR 链接；PR 链接我会自动贴回 issue，"
            f"你不用再去 issue 下留言。\n"
            f"若这个 issue 不需要改代码（提问 / 讨论类），直接给出结论，并用 Gitea API "
            f"在 issue #{n} 下回复结论后关闭它。\n用简洁中文回复。"
        )
        sp = memory.augment_system_prompt(_employee_prompt(rec), rec["id"])
        try:
            async with _typing(room):
                # cancel_key=汇报房间：房间里的 /cancel 也能停掉工单任务
                answer = await runner.ask(_sess_key(rec, room), prompt, cwd=rec["path"], system_prompt=sp,
                                          lock_key=rec["id"], prepare=lambda: projects.prepare_worktree(rec),
                                          cancel_key=room)
            _project_last_active[rec["id"]] = time.time()
            await send(room, f"📋 工单 #{n} 处理结果：\n{answer}", track=True)
            pr = _extract_pr(answer, rec)
            if pr:
                issue_ledger.update(rec["id"], n, pr=pr[0])
                if pr_ledger.record(rec["id"], pr[0], pr[1], room):
                    log.info("[%s] 工单 #%d 开了 PR #%d，进台账跟到合并", rec["id"], n, pr[0])
                await gitea.comment_issue(rec, n, f"已提交 PR：{pr[1]}（合并后本单自动关闭）")
        except ClaudeCancelled:
            await send(room, f"🛑 已停止工单 #{n} 的处理。")
        except Exception as e:
            log.exception("工单 #%s 处理失败", n)
            await send(room, f"工单 #{n} 处理出错：{e}")
    finally:
        inflight.remove(inflight_key)


async def _intake_one(rec: dict, room: str, login: str):
    """接下一个项目里所有新指派给 bot 的 issue：登记 → issue 下认领 → 派执行。"""
    for issue in await gitea.assigned_issues(rec, login):
        n = issue.get("number")
        if not isinstance(n, int) or issue_ledger.taken(rec["id"], n):
            continue
        url = issue.get("html_url") or ""
        issue_ledger.record(rec["id"], n, url, room)
        title = (issue.get("title") or "").strip()
        await gitea.comment_issue(rec, n, "已认领，开始处理；有 PR 会在这里贴链接。")
        await send(room, f"📥 [{rec['owner']}/{rec['repo']}] 接到指派的工单 #{n}：{title}\n"
                         f"{url}\n我来处理——", track=True)
        state._spawn(_issue_execute(rec, room, issue))


async def _sweep_closed():
    """在册工单里已被关闭的（PR 合并自动关 / 人工关）→ 销账；项目已不在册的也一并清掉。
    工单查不到时分辨"网络抖动（下轮再试）"和"工单真没了（被删 / 转移，连续 ≥3 轮 404 才销账并知会）"。"""
    for entry in issue_ledger.active():
        pid, n, room = entry["pid"], entry["number"], entry.get("room") or ""
        rec = projects.get_project(pid)
        if not rec:
            issue_ledger.remove(pid, n)
            continue
        info = await gitea.issue_info(rec, n)
        if info is None:
            if await gitea.issue_gone(rec, n):
                gone = entry.get("gone_rounds", 0) + 1
                if gone >= 3:
                    issue_ledger.remove(pid, n)
                    if room:
                        await send(room, f"⚠️ 工单 #{n} 在 Gitea 上已不存在（被删 / 转移），停止跟进。")
                    log.info("[%s] 工单 #%d 连续 %d 轮 404，销账", pid, n, gone)
                else:
                    issue_ledger.update(pid, n, gone_rounds=gone)
            elif entry.get("gone_rounds"):
                issue_ledger.update(pid, n, gone_rounds=0)   # 抖动而非真没了：清零重新计
            continue
        if entry.get("gone_rounds"):
            issue_ledger.update(pid, n, gone_rounds=0)        # 成功查到一次 → 清零
        if info.get("state") == "closed":
            issue_ledger.remove(pid, n)
            log.info("[%s] 工单 #%d 已关闭，销账", pid, n)


async def _issue_intake_loop():
    """周期轮询各已知项目指派给 bot 的 open issue：新单接下执行；已接的关单后销账。"""
    if not settings.issue_intake_enabled:
        return
    if not (settings.gitea_host and settings.gitea_token):
        return   # 没配 Gitea 就没有工单来源
    log.info("工单接活已启动（每 %ds 轮询一次 assigned issues）", settings.issue_poll_interval)
    while True:
        try:
            await asyncio.sleep(settings.issue_poll_interval)
            login = await gitea.own_user_login()
            if not login:
                continue   # 查不到 bot 的 Gitea 登录名（网络抖动）：这轮先跳过
            await _sweep_closed()
            for rec in projects.list_projects():
                room = _project_home_room(rec["id"])
                if not room:
                    continue   # 没有可回报的房间：先不接（进展没处说；绑群/私聊路由过即有）
                if not os.path.isdir(os.path.join(rec.get("path", ""), ".git")):
                    continue
                try:
                    await _intake_one(rec, room, login)
                except Exception:
                    log.exception("[%s] 工单接活失败", rec["id"])
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("工单接活循环异常，继续")
