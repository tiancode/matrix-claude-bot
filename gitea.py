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


def _post(url: str, payload: dict):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    if settings.gitea_token:
        req.add_header("Authorization", "token " + settings.gitea_token)
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status, (r.read().decode(errors="ignore") or "")


async def _aget(url: str):
    try:
        return await asyncio.to_thread(_get, url)
    except (urllib.error.URLError, OSError, ValueError):
        return 0, None


_own_user: dict = {}   # GITEA_TOKEN 对应用户的缓存（查到过才缓存，失败下次再试）


async def own_user_id() -> int | None:
    """GITEA_TOKEN 对应的 Gitea 用户 id（bot 自己）。查不到返回 None（调用方按"不过滤"处理）。
    用途：PR 跟进时把 bot 自己发的评审/评论从"新评审意见"里剔掉，免得自己触发自己。"""
    if "id" not in _own_user:
        if not settings.gitea_host:
            return None
        st, d = await _aget(f"{settings.gitea_host.rstrip('/')}/api/v1/user")
        if st == 200 and isinstance(d, dict) and isinstance(d.get("id"), int):
            _own_user["id"] = d["id"]
        else:
            return None
    return _own_user["id"]


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


async def merge(rec: dict, number: int, method: str = "merge",
                delete_branch: bool = False) -> tuple[bool, str]:
    """合并 PR（POST .../pulls/<n>/merge）。成功返回 (True, "")，失败返回 (False, 原因)。
    method: merge / squash / rebase（须为仓库允许的方式）。合并是写操作，不像只读查询那样
    把异常吞成默认值——把失败原因带回去好回报 / 记日志（不可合并、方法被禁、分支保护等）。"""
    url = f"{_repo_api(rec)}/pulls/{number}/merge"
    payload = {"Do": method, "delete_branch_after_merge": bool(delete_branch)}
    try:
        st, body = await asyncio.to_thread(_post, url, payload)
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode(errors="ignore")
        except Exception:
            detail = ""
        return False, f"HTTP {e.code} {detail[:200]}".strip()
    except (urllib.error.URLError, OSError, ValueError) as e:
        return False, str(e)[:200]
    ok = st in (200, 201, 204)
    return ok, ("" if ok else f"HTTP {st} {body[:200]}")
