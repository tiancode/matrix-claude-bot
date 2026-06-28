"""Gitea REST 小客户端：bot **自己**轮询 PR 状态 / 评审 / CI 用（Claude 干活时仍各自 curl）。

只读查询，走 GITEA_TOKEN；同步 urllib 包在 to_thread 里，避免给事件循环引第三方 http 依赖。
所有调用都吞异常返回安全默认值——轮询是后台尽力而为，网络抖动不该把循环带崩。
"""
import asyncio
import json
import urllib.error
import urllib.request

from config import settings


def _repo_api(rec: dict) -> str:
    return f"{rec['host'].rstrip('/')}/api/v1/repos/{rec['owner']}/{rec['repo']}"


def _get(url: str):
    req = urllib.request.Request(url)
    if settings.gitea_token:
        req.add_header("Authorization", "token " + settings.gitea_token)
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status, json.loads(r.read().decode() or "null")


async def _aget(url: str):
    try:
        return await asyncio.to_thread(_get, url)
    except (urllib.error.URLError, OSError, ValueError):
        return 0, None


async def pr_info(rec: dict, number: int) -> dict | None:
    """单个 PR 的状态：含 state(open/closed)、merged、mergeable、head.sha/ref。"""
    st, d = await _aget(f"{_repo_api(rec)}/pulls/{number}")
    return d if st == 200 and isinstance(d, dict) else None


async def pr_reviews(rec: dict, number: int) -> list:
    """PR 的评审列表：每条含 id、state(APPROVED/REQUEST_CHANGES/COMMENT/PENDING)、body、user、submitted_at。"""
    st, d = await _aget(f"{_repo_api(rec)}/pulls/{number}/reviews")
    return d if st == 200 and isinstance(d, list) else []


async def review_comments(rec: dict, number: int, review_id: int) -> list:
    """某条评审下的行级评论（body + 文件/行号），给 Claude 处理评审意见时当上下文。"""
    st, d = await _aget(f"{_repo_api(rec)}/pulls/{number}/reviews/{review_id}/comments")
    return d if st == 200 and isinstance(d, list) else []


async def ci_state(rec: dict, sha: str) -> str:
    """提交的合并 CI 状态：success / failure / error / pending / ""（无 CI）。"""
    if not sha:
        return ""
    st, d = await _aget(f"{_repo_api(rec)}/commits/{sha}/status")
    return (d or {}).get("state", "") if st == 200 and isinstance(d, dict) else ""
