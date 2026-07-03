"""Gitea REST 小客户端：bot **自己**轮询 PR 状态 / 评审 / CI 用（Claude 干活时仍各自 curl）。

只读查询，走 GITEA_TOKEN；同步 urllib 包在 to_thread 里，避免给事件循环引第三方 http 依赖。
所有调用都吞异常返回安全默认值——轮询是后台尽力而为，网络抖动不该把循环带崩。
"""
import asyncio
import json
import time
import urllib.error
import urllib.parse
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


# ---- Gitea 全局健康度 ----
# 在 _aget 的成功/失败路径**统一**埋点（别在每个业务函数里散着记）；下游用 health() 查询：
# /status 显示一条、后台循环跨阈值时告警。区分"token 失效(401/403)"与"网络连不上(0)"，
# 好点名说清是哪种。**404 是"对象不存在"的业务答案、不是故障**——与 2xx 一样算"Gitea 活着"。
# （写操作走 _post，不在此埋点：只读轮询已足够反映连通性，也不牵连合并/留言的失败语义。）
_health = {
    "consecutive_failures": 0,   # 连续失败次数（任一次"活着"即清零）
    "last_success_ts": 0.0,      # 最近一次"Gitea 活着"（2xx/404）的时间戳
    "last_failure_ts": 0.0,      # 最近一次失败的时间戳
    "last_code": 0,              # 最近一次失败的 HTTP 状态码（0=网络层没连上）
    "last_kind": "",             # 最近一次失败定性：auth(401/403,疑似 token 失效) / network / http
}


def _note_alive() -> None:
    _health["consecutive_failures"] = 0
    _health["last_success_ts"] = time.time()


def _note_failure(code: int, kind: str) -> None:
    _health["consecutive_failures"] += 1
    _health["last_failure_ts"] = time.time()
    _health["last_code"] = code
    _health["last_kind"] = kind


def health() -> dict:
    """Gitea 连通性健康快照（只读拷贝）。ok=当前没有连续失败；异常时看 last_kind/last_code 定性、
    last_success_ts 判断"最近成功多久前"。字段说明见 _health。"""
    h = dict(_health)
    h["ok"] = h["consecutive_failures"] == 0
    return h


async def _aget(url: str):
    # 返回 (status, data)。区分两类失败，好让上层分辨"对象真没了"和"网络抖动"：
    #   HTTPError（含 404）→ (e.code, None)：服务器答了、对象确实不在（被删/改名/无权）；
    #   URLError/OSError/解析失败 → (0, None)：连不上 / 抖动，语义是"下轮再试"。
    # HTTPError 是 URLError 子类，必须先捕。
    # 顺带在这里给全局健康度埋点（见上）——返回语义不变，只多记一笔连通性。
    try:
        st, data = await asyncio.to_thread(_get, url)
        _note_alive()                       # 2xx：Gitea 活着
        return st, data
    except urllib.error.HTTPError as e:
        if e.code == 404:
            _note_alive()                   # 404 = 对象不存在的业务答案，不是故障：Gitea 仍活着
        elif e.code in (401, 403):
            _note_failure(e.code, "auth")   # 鉴权失败：token 可能已失效
        else:
            _note_failure(e.code, "http")   # 5xx 等：连上了但服务器不正常
        return e.code, None
    except (urllib.error.URLError, OSError, ValueError):
        _note_failure(0, "network")         # 连不上 / 抖动
        return 0, None


_own_user: dict = {}   # GITEA_TOKEN 对应用户的缓存（查到过才缓存，失败下次再试）


async def _fetch_own_user() -> None:
    if not settings.gitea_host:
        return
    st, d = await _aget(f"{settings.gitea_host.rstrip('/')}/api/v1/user")
    if st == 200 and isinstance(d, dict):
        if isinstance(d.get("id"), int):
            _own_user["id"] = d["id"]
        if d.get("login"):
            _own_user["login"] = str(d["login"])


async def own_user_id() -> int | None:
    """GITEA_TOKEN 对应的 Gitea 用户 id（bot 自己）。查不到返回 None（调用方按"不过滤"处理）。
    用途：PR 跟进时把 bot 自己发的评审/评论从"新评审意见"里剔掉，免得自己触发自己。"""
    if "id" not in _own_user:
        await _fetch_own_user()
    return _own_user.get("id")


async def own_user_login() -> str:
    """GITEA_TOKEN 对应的 Gitea 登录名（bot 自己）。查不到返回 ""（调用方按"这轮先跳过"处理）。
    用途：工单接活按 assigned_by=<登录名> 过滤"指派给 bot 的 issue"。"""
    if "login" not in _own_user:
        await _fetch_own_user()
    return _own_user.get("login", "")


async def assigned_issues(rec: dict, assignee: str) -> list:
    """开着的、指派给 assignee 的 issue 列表（type=issues，不含 PR）。工单接活的轮询入口。"""
    if not assignee:
        return []
    q = urllib.parse.quote(assignee)
    st, d = await _aget(f"{_repo_api(rec)}/issues?state=open&type=issues&assigned_by={q}")
    return d if st == 200 and isinstance(d, list) else []


async def issue_info(rec: dict, number: int) -> dict | None:
    """单个 issue 的状态：含 state(open/closed)、title、assignees。查不到返回 None。"""
    st, d = await _aget(f"{_repo_api(rec)}/issues/{number}")
    return d if st == 200 and isinstance(d, dict) else None


async def issue_gone(rec: dict, number: int) -> bool:
    """确认某 issue 在 Gitea 上是否真的没了（HTTP 404）——与"网络抖动"区分，用于销账前定性。
    只有拿到确切 404 才返回 True；抖动 / 其它错误返回 False（宁可下轮再试也别误销账）。"""
    st, _ = await _aget(f"{_repo_api(rec)}/issues/{number}")
    return st == 404


async def issue_comments(rec: dict, number: int) -> list:
    """issue 下的评论列表（body + user），接单时给 Claude 当讨论上下文。"""
    st, d = await _aget(f"{_repo_api(rec)}/issues/{number}/comments")
    return d if st == 200 and isinstance(d, list) else []


async def comment_issue(rec: dict, number: int, body: str) -> bool:
    """在 issue 下留言（认领 / 回报 PR 链接）。写操作但尽力而为：失败只返回 False，
    不影响接单主流程——留言只是让 Gitea 侧的人看得到进展，Matrix 侧仍会回报。"""
    url = f"{_repo_api(rec)}/issues/{number}/comments"
    try:
        st, _ = await asyncio.to_thread(_post, url, {"body": body})
    except (urllib.error.URLError, OSError, ValueError):
        return False
    return st in (200, 201)


async def pr_info(rec: dict, number: int) -> dict | None:
    """单个 PR 的状态：含 state(open/closed)、merged、mergeable、head.sha/ref。"""
    st, d = await _aget(f"{_repo_api(rec)}/pulls/{number}")
    return d if st == 200 and isinstance(d, dict) else None


async def pr_gone(rec: dict, number: int) -> bool:
    """确认某 PR 在 Gitea 上是否真的没了（HTTP 404）——与"网络抖动"区分，用于销账前定性。
    只有拿到确切 404 才返回 True；抖动 / 其它错误返回 False（宁可下轮再试也别误销账）。"""
    st, _ = await _aget(f"{_repo_api(rec)}/pulls/{number}")
    return st == 404


async def pr_reviews(rec: dict, number: int) -> list:
    """PR 的评审列表：每条含 id、state(APPROVED/REQUEST_CHANGES/COMMENT/PENDING)、body、user、submitted_at。"""
    st, d = await _aget(f"{_repo_api(rec)}/pulls/{number}/reviews")
    return d if st == 200 and isinstance(d, list) else []


async def review_comments(rec: dict, number: int, review_id: int) -> list:
    """某条评审下的行级评论（body + 文件/行号），给 Claude 处理评审意见时当上下文。"""
    st, d = await _aget(f"{_repo_api(rec)}/pulls/{number}/reviews/{review_id}/comments")
    return d if st == 200 and isinstance(d, list) else []


async def ci_state(rec: dict, sha: str) -> str | None:
    """提交的合并 CI 状态：success / failure / error / pending / ""（真没配 CI）。
    查询失败（网络抖动 / 非 200）返回 None——务必与"没配 CI"的 "" 区分开：调用方遇 None 应保守跳过，
    别把一次查询抖动当成"CI 通过"放行自动合并（否则 CI 还红着就可能把 PR 合进 main）。"""
    if not sha:
        return ""
    st, d = await _aget(f"{_repo_api(rec)}/commits/{sha}/status")
    if st == 200 and isinstance(d, dict):
        return d.get("state", "")
    return None


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
