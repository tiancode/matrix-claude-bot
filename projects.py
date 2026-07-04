"""房间 ↔ Gitea 仓库的解析、绑定、clone 与路由（重启保留绑定）。"""
import asyncio
import json
import logging
import os
import re
import shutil
import signal
from urllib.parse import urlparse, urlsplit

from config import settings, redact
from storage import atomic_write_json

log = logging.getLogger("matrix-claude.projects")

_SSH_RE = re.compile(r"git@([^\s:]+):([^\s/]+)/([^\s]+?)(?:\.git)?(?=\s|$)", re.I)
_HTTP_RE = re.compile(r"https?://[^\s<>\"')]+", re.I)
_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")    # 合法 owner/repo 段，挡掉路径穿越

# Gitea 顶级保留路由，不可能是真实 owner（避免把 /explore、/user 等页面当仓库）
_RESERVED_OWNERS = frozenset({
    "explore", "issues", "pulls", "notifications", "user", "admin", "api",
    "org", "repo", "-", "assets", "avatars", "attachments", "login",
})


def _valid_name(name: str) -> bool:
    return bool(name) and name not in (".", "..") and bool(_NAME_RE.match(name))


_DEFAULT_PORT = {"https": "443", "http": "80"}


def _canon_host(url: str) -> str:
    """取可比较的 host[:port]：小写、去掉默认端口。带 userinfo(user@) 视为可疑，返回 ""。"""
    p = urlsplit(url)
    if "@" in (p.netloc or ""):
        return ""
    host = (p.hostname or "").lower()
    if not host:
        return ""
    try:
        port = p.port
    except ValueError:     # 非法端口(非数字/越界)：视为不可信，拒掉
        return ""
    if port is None or str(port) == _DEFAULT_PORT.get((p.scheme or "").lower(), ""):
        return host
    return f"{host}:{port}"


def _trusted_host() -> str:
    return _canon_host(settings.gitea_host) if settings.gitea_host else ""


def _host_trusted(url: str) -> bool:
    """URL 主机是否就是配置的受信 Gitea（决定能否注入 token）。按 host[:port] 规整后精确比较。"""
    allowed = _trusted_host()
    return bool(allowed) and _canon_host(url) == allowed


def _mk(scheme: str, netloc: str, owner: str, repo: str) -> dict:
    canon = _canon_host(f"{scheme}://{netloc}")
    netloc = canon or netloc.lower()   # 规整 host[:port]；非法的保留原值，受信校验会拒掉
    host = f"{scheme}://{netloc}"
    return {
        "host": host,
        "owner": owner,
        "repo": repo,
        "web_url": f"{host}/{owner}/{repo}",
        "clone_url": f"{host}/{owner}/{repo}.git",
    }


def _validate_repo(info: dict | None) -> dict | None:
    """对单个候选做受信主机 + 合法 owner/repo 校验；过则返回，否则 None。"""
    if not info:
        return None
    if not _host_trusted(info["host"]):
        return None
    if not (_valid_name(info["owner"]) and _valid_name(info["repo"])):
        return None
    if info["owner"].lower() in _RESERVED_OWNERS:
        return None
    return info


def parse_repo_url(text: str) -> dict | None:
    """找出文本里第一个【受信】git 仓库地址；找不到返回 None。

    fail-closed：只认 GITEA_HOST 的仓库，没配就一律不当仓库（防任意主机 clone + token 注入到第三方）。
    逐个候选校验、返回第一个通过的，避免前面的不相关链接抢先定下来。
    """
    text = text or ""
    for m in _SSH_RE.finditer(text):
        hit = _validate_repo(_mk("https", m.group(1), m.group(2), m.group(3)))
        if hit:
            return hit
    for m in _HTTP_RE.finditer(text):
        u = urlparse(m.group(0))
        parts = [p for p in u.path.split("/") if p]
        if len(parts) < 2:
            continue
        repo = parts[1][:-4] if parts[1].endswith(".git") else parts[1]
        hit = _validate_repo(_mk(u.scheme or "https", u.netloc, parts[0], repo))
        if hit:
            return hit
    return None


async def _git(*args: str, cwd: str | None = None) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        "git", *args, cwd=cwd,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True,   # 独立进程组，超时连同子进程一起杀
    )
    try:
        out, err = await asyncio.wait_for(
            proc.communicate(), timeout=settings.git_timeout
        )
    except asyncio.TimeoutError:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            proc.kill()
        await proc.wait()
        # 让上层当普通失败处理，而不是永远卡住占着绑定锁
        return 124, "", f"git {args[0] if args else ''} 超时（>{settings.git_timeout}s）"
    return proc.returncode, out.decode(errors="ignore"), err.decode(errors="ignore")


def _auth_url(clone_url: str) -> str:
    """把 token 注入 https clone 地址（仅受信 GITEA_HOST），用于私有库 clone/push。"""
    if not (settings.gitea_token and clone_url.startswith("http")):
        return clone_url
    if not _host_trusted(clone_url):
        log.warning("目标主机 %s 不在 GITEA_HOST 白名单内，跳过 token 注入（私有库可能 clone 失败；"
                    "如确为你的 Gitea，请设置 GITEA_HOST）", urlparse(clone_url).netloc)
        return clone_url
    return clone_url.replace("://", f"://{settings.gitea_token}@", 1)


def proj_id(info: dict) -> str:
    """项目稳定标识 host/owner/repo，同时作为 Claude worker 的会话 key。"""
    return f"{urlparse(info['host']).netloc}/{info['owner']}/{info['repo']}"


async def _repo_ok(path: str) -> bool:
    """是不是一个能用的 git 工作树。比"存在 .git 目录"严：半截 clone（被 SIGKILL）会留下残缺
    .git，目录在但 rev-parse 过不了；这种必须判为坏，否则会被当成健康 checkout 让 Claude 在里头干活。"""
    if not os.path.isdir(os.path.join(path, ".git")):
        return False
    rc, _, _ = await _git("rev-parse", "--is-inside-work-tree", cwd=path)
    return rc == 0


class Projects:
    """两张表：_projects(proj_id -> 记录，含 path/base) 和 _rooms(room_id -> proj_id)。
    一个项目一个 Claude worker；DM 不绑房间，靠内容分诊路由。
    """

    def __init__(self):
        self.bindings_path = os.path.abspath(settings.bindings_path)
        self.root = os.path.abspath(settings.projects_root)
        data = self._load()
        self._projects: dict[str, dict] = data.get("projects", {})
        self._rooms: dict[str, str] = data.get("rooms", {})
        self._save_lock = asyncio.Lock()                 # 护内存表 + 落盘
        self._pid_locks: dict[str, asyncio.Lock] = {}    # 每仓库一把：同仓库 clone 串行

    def _load(self) -> dict:
        if not os.path.exists(self.bindings_path):
            return {}
        try:
            with open(self.bindings_path) as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            # 损坏：备份成 .corrupt 再从空开始，保留人工恢复机会
            bak = self.bindings_path + ".corrupt"
            try:
                os.replace(self.bindings_path, bak)
                log.error("绑定文件 %s 损坏，已备份为 %s 并从空开始；请人工检查后恢复: %s",
                          self.bindings_path, bak, e)
            except OSError:
                log.error("绑定文件 %s 损坏且无法备份，从空开始: %s", self.bindings_path, e)
            return {}
        except OSError as e:
            # 一时不可读（权限/IO）：不动原文件，从空开始（下次 _save 会覆盖它）
            log.error("绑定文件 %s 暂不可读，从空开始: %s", self.bindings_path, e)
            return {}

    def _save(self):
        # 原子写 + fsync：绑定是重启后定位项目的底账，值得多一道刷盘
        atomic_write_json(self.bindings_path,
                          {"projects": self._projects, "rooms": self._rooms},
                          indent=2, fsync=True)

    # ---- 查询 ----
    def list_projects(self) -> list[dict]:
        return list(self._projects.values())

    def get_project(self, pid: str) -> dict | None:
        return self._projects.get(pid)

    def get_room(self, room_id: str) -> dict | None:
        pid = self._rooms.get(room_id)
        return self._projects.get(pid) if pid else None

    def first_room_for(self, pid: str) -> str | None:
        """绑定到该项目的第一个群房间（给自驱心跳找个"汇报口"）；没有返回 None。"""
        for room, p in self._rooms.items():
            if p == pid:
                return room
        return None

    def rooms_for(self, pid: str) -> list[str]:
        """绑定到该项目的所有房间 id（自驱心跳/健康度挑汇报口用，可据此在群/私聊间优选）。"""
        return [room for room, p in self._rooms.items() if p == pid]

    # ---- 登记 / 绑定 ----
    def _pid_lock(self, pid: str) -> asyncio.Lock:
        return self._pid_locks.setdefault(pid, asyncio.Lock())

    async def _ensure(self, info: dict) -> dict:
        """确保仓库已 clone 并登记，返回项目记录。按 pid 加锁：同仓库串行、异仓库并行。"""
        pid = proj_id(info)
        async with self._pid_lock(pid):
            rec = self._projects.get(pid)
            if rec and await _repo_ok(rec["path"]):
                return rec

            netloc = urlparse(info["host"]).netloc
            local = os.path.join(self.root, netloc, info["owner"], info["repo"])
            auth_url = _auth_url(info["clone_url"])
            if not await _repo_ok(local):
                # 目标目录有残留（半截 clone / 被删了 .git / 非空脏目录）会让 git clone 直接 fatal，
                # 且无法自愈。先清掉再 clone；clone 失败也清掉，免得残骸卡死后续每一次重试。
                if os.path.exists(local):
                    shutil.rmtree(local, ignore_errors=True)
                os.makedirs(os.path.dirname(local), exist_ok=True)
                rc, _, err = await _git("clone", auth_url, local)
                if rc != 0:
                    shutil.rmtree(local, ignore_errors=True)
                    raise RuntimeError(f"git clone 失败: {redact(err.strip())[:300]}")

            await _git("config", "user.name", settings.git_user_name, cwd=local)
            await _git("config", "user.email", settings.git_user_email, cwd=local)
            if settings.gitea_token:
                await _git("remote", "set-url", "origin", auth_url, cwd=local)

            base = await self._detect_base(local)
            rec = {**info, "id": pid, "path": local, "base": base}
            async with self._save_lock:
                self._projects[pid] = rec
                self._save()
            return rec

    async def ensure_project(self, info: dict) -> dict:
        """登记并 clone 一个项目（DM 分诊路由到新仓库时用）。"""
        return await self._ensure(info)

    async def bind_room(self, room_id: str, info: dict) -> dict:
        """把群房间绑定到项目（必要时 clone）。"""
        rec = await self._ensure(info)
        async with self._save_lock:
            self._rooms[room_id] = rec["id"]
            self._save()
        return rec

    async def unbind(self, room_id: str) -> bool:
        """解除某房间的绑定并落盘（退房清理用），返回是否真的解了。
        只清房间→项目这一条映射；项目记录 / 本地 clone 保留（别的房间或 DM 路由可能还在用）。"""
        async with self._save_lock:
            if room_id not in self._rooms:
                return False
            self._rooms.pop(room_id, None)
            self._save()
        return True

    async def prepare_worktree(self, rec: dict) -> None:
        """每次派活前把工作树拉回干净的 origin/base：fetch 一次，丢弃上个任务残留的脏改动 /
        半截分支 / 未跟踪文件，免得在脏的或过期的状态上接着开工、把改动串进下一个 PR。

        未提交的残留不直接删：先 auto-stash 寄存到 refs/stash（含未跟踪文件），清干净照常从
        干净 base 开工，但「忘了提交就永久丢」变成「git stash list 随时能捞回」。

        必须在 runner 的 checkout 串行锁内调用（由 ask(prepare=...) 保证），别和同仓库的别的任务并发。
        best-effort：fetch/checkout 失败只告警，仍按现状把活派下去，免得离线时彻底卡死。"""
        path = rec.get("path")
        base = rec.get("base") or "main"
        if not path or not os.path.isdir(os.path.join(path, ".git")):
            return
        await self._stash_dirty(path, rec)   # 先把脏树寄存住，别让下面的 checkout -f/reset/clean 把没提交的活删没
        await _git("fetch", "--prune", "origin", cwd=path)
        rc, _, _ = await _git("rev-parse", "--verify", "--quiet",
                              f"refs/remotes/origin/{base}", cwd=path)
        target = f"origin/{base}" if rc == 0 else base   # 远端没有就退回本地 base
        # -f 丢弃工作树/索引改动；-B 把 base 重建到 target 再切过去（无论当前停在哪个分支）
        rc2, _, err = await _git("checkout", "-f", "-B", base, target, cwd=path)
        if rc2 != 0:
            log.warning("prepare_worktree: checkout %s 失败，按现状派活：%s",
                        base, redact(err.strip())[:200])
            return
        await _git("reset", "--hard", target, cwd=path)
        await _git("clean", "-fd", cwd=path)

    @staticmethod
    async def _stash_dirty(path: str, rec: dict) -> None:
        """派活前若工作树有未提交改动/未跟踪文件，先 auto-stash（含 -u 未跟踪）到 refs/stash，
        免得随后的 checkout -f / reset --hard / clean -fd 把没 commit 的活永久删掉——
        stash 不随 reset/clean 丢，用 `git stash list` 可捞回。best-effort：失败只告警照常派活。"""
        rc, out, _ = await _git("status", "--porcelain", cwd=path)
        if rc != 0 or not out.strip():
            return                                     # 查不了或本就干净 → 无需寄存
        _, br, _ = await _git("rev-parse", "--abbrev-ref", "HEAD", cwd=path)
        label = str(rec.get("id") or rec.get("name") or "").strip()
        msg = f"auto-park before task {label}".rstrip() + f" (was on {br.strip() or '?'})"
        rc2, _, err = await _git("stash", "push", "--include-untracked", "-m", msg, cwd=path)
        if rc2 != 0:
            log.warning("prepare_worktree: auto-stash 失败，未提交的改动可能随 reset 丢失：%s",
                        redact(err.strip())[:200])
        else:
            log.info("prepare_worktree: 已寄存脏工作树到 stash「%s」，git stash list 可捞回", msg)

    @staticmethod
    async def _detect_base(local: str) -> str:
        rc, out, _ = await _git("symbolic-ref", "refs/remotes/origin/HEAD", cwd=local)
        if rc != 0:  # 某些 clone 不设 origin/HEAD，主动向远端探测一次
            await _git("remote", "set-head", "origin", "-a", cwd=local)
            rc, out, _ = await _git("symbolic-ref", "refs/remotes/origin/HEAD", cwd=local)
        if rc == 0 and out.strip():
            # 只剥 refs/remotes/origin/ 前缀；不能用 rsplit("/")，否则默认分支名带斜杠
            # （如 release/2.0）会被截成 2.0，之后所有 origin/<base> 操作都打空。
            ref = out.strip()
            prefix = "refs/remotes/origin/"
            return ref[len(prefix):] if ref.startswith(prefix) else ref.rsplit("/", 1)[-1]
        # 探测不到：取 main / master 里实际存在的那个
        for cand in ("main", "master"):
            rc2, _, _ = await _git("rev-parse", "--verify", "--quiet",
                                   f"refs/remotes/origin/{cand}", cwd=local)
            if rc2 == 0:
                log.warning("无法探测 %s 的 origin/HEAD，按实际存在的远端分支取 %s", local, cand)
                return cand
        log.warning("无法探测 %s 的默认分支，回退为 main", local)
        return "main"


projects = Projects()
