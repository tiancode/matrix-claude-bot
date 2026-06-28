"""调用本机 Claude Code (claude -p)。

- ask():  带工具、按会话维护多轮上下文，真正干活
- quick(): 一次性纯文本判断（主动插话、DM 分诊），不带危险权限、剔除密钥
"""
import asyncio
import json
import logging
import os
import shlex
import signal
import time

from config import settings, redact

log = logging.getLogger("matrix-claude.runner")


# quick() 处理不可信外来内容，给子进程剔除这些密钥（它用不到）。agentic 的 ask() 仍需 GITEA_TOKEN。
_QUICK_STRIP_ENV = ("GITEA_TOKEN", "MATRIX_PASSWORD")


def _quick_env() -> dict:
    env = os.environ.copy()
    for k in _QUICK_STRIP_ENV:
        env.pop(k, None)
    return env


def _kill_group(proc) -> None:
    """杀掉子进程所在的整个进程组（含它 fork 的 git/curl/bash）。"""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
        except ProcessLookupError:
            pass


class ClaudeRunner:
    def __init__(self):
        self._sessions: dict[str, tuple[str, float]] = self._load_sessions()  # key -> (session_id, ts)
        self._locks: dict[str, asyncio.Lock] = {}
        self._epoch: dict[str, int] = {}                   # key -> 重置代数，防 in-flight 任务复活已重置会话
        self._sema = asyncio.Semaphore(max(1, settings.max_concurrency))
        self._quick_sema = asyncio.Semaphore(max(1, settings.max_concurrency))  # quick 独立并发池

    # ---- 会话持久化：重启后恢复 session_id，多轮上下文不至于每次重启全断 ----
    def _sessions_file(self) -> str:
        return os.path.join(settings.store_path, "sessions.json")

    def _load_sessions(self) -> dict:
        try:
            with open(self._sessions_file()) as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}
        out: dict[str, tuple[str, float]] = {}
        now = time.time()
        if isinstance(raw, dict):
            for k, v in raw.items():
                try:
                    sid, ts = v[0], float(v[1])
                except (TypeError, ValueError, IndexError):
                    continue
                if sid and now - ts <= settings.session_ttl:   # 早就过期的不必恢复
                    out[k] = (sid, ts)
        return out

    def _save_sessions(self) -> None:
        # 原子写；调用点都在事件循环里且无 await，对其它协程是原子的，不会丢更新
        path = self._sessions_file()
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                json.dump({k: [sid, ts] for k, (sid, ts) in self._sessions.items()}, f)
            os.replace(tmp, path)
        except OSError as e:
            log.warning("会话持久化失败 %s: %s", path, e)

    # ---- 会话管理（按 key：项目+房间，见 bot._sess_key）----
    def _lock(self, key: str) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    def reset(self, key: str):
        self._sessions.pop(key, None)
        self._epoch[key] = self._epoch.get(key, 0) + 1
        self._save_sessions()

    def _sid(self, key: str) -> tuple[str | None, bool]:
        """返回 (有效 session_id 或 None, 是否因空闲超时刚被清掉)。"""
        item = self._sessions.get(key)
        if not item:
            return None, False
        sid, ts = item
        if time.time() - ts > settings.session_ttl:
            self._sessions.pop(key, None)
            return None, True
        return sid, False

    # ---- 进程调用 ----
    async def _run(self, cmd: list[str], cwd: str | None = None,
                   sema: asyncio.Semaphore | None = None,
                   env: dict | None = None,
                   timeout: float | None = None):
        workdir = cwd or settings.claude_workdir
        os.makedirs(workdir, exist_ok=True)
        async with (sema or self._sema):
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=workdir,
                env=env,   # None=继承父进程全部环境；quick 传裁剪过的
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,   # 独立进程组，超时能连子进程一起杀
            )
            # 用显式 None 判定而非 `or`：传进来的小超时（如 quick 的 60s）不该被 0 假值化吞掉；
            # 真传了 <=0 才回落到默认，免得 wait_for(0) 直接立刻超时。
            eff = timeout if timeout is not None else settings.claude_timeout
            if not eff or eff <= 0:
                eff = settings.claude_timeout or 600
            try:
                out, err = await asyncio.wait_for(
                    proc.communicate(), timeout=eff
                )
            except asyncio.TimeoutError:
                _kill_group(proc)
                await proc.wait()
                raise RuntimeError("Claude 响应超时")
        return proc.returncode, out or b"", err or b""

    @staticmethod
    def _parse(out: bytes) -> tuple[str, str | None, bool]:
        try:
            data = json.loads(out.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise RuntimeError(f"无法解析 claude 输出: {redact(out.decode(errors='ignore'))[:300]}")
        return (data.get("result") or "").strip(), data.get("session_id"), bool(data.get("is_error"))

    def _cmd(self, prompt: str, sid: str | None, agentic: bool,
             system_prompt: str | None = None) -> list[str]:
        cmd = [settings.claude_bin, "-p", prompt, "--output-format", "json"]
        if settings.claude_model:
            cmd += ["--model", settings.claude_model]
        if agentic:
            if settings.claude_dangerous:
                cmd += ["--dangerously-skip-permissions"]
            elif settings.claude_permission_mode:
                cmd += ["--permission-mode", settings.claude_permission_mode]
        sp = system_prompt if system_prompt is not None else settings.claude_system_prompt
        if sp and not sid:  # 系统提示只在开新会话时设一次
            cmd += ["--append-system-prompt", sp]
        if sid:
            cmd += ["--resume", sid]
        if agentic and settings.claude_extra_args:
            cmd += shlex.split(settings.claude_extra_args)
        return cmd

    @staticmethod
    def _looks_like_session_error(out: bytes, err: bytes) -> bool:
        """非零退出是否确像 --resume 找不到会话。只有这种才安全重试：
        其它错误时任务可能已 push/开 PR，重跑会产生重复分支/PR。

        只认"找不到/失效"字样紧贴 session/conversation 的报错；不做"出现 session + 出现 not found
        就算"的宽松匹配——curl/git 普通报错里凑巧两词都有并不罕见，会把已 push 的任务误判成可重跑。"""
        blob = (err.decode(errors="ignore") + "\n" + out.decode(errors="ignore")).lower()
        return any(p in blob for p in (
            "no conversation found",
            "session not found", "no such session",
            "could not find session", "couldn't find session",
            "session does not exist", "no session with",
            "session expired", "session has expired",
            "invalid session", "unknown session",
        ))

    # ---- agentic：真正干活 ----
    async def ask(self, key: str, prompt: str, cwd: str | None = None,
                  system_prompt: str | None = None, lock_key: str | None = None,
                  prepare=None) -> str:
        """带工具、带会话上下文地干活；cwd=该项目的仓库目录。

        key:      会话维度（项目+房间），不同房间/私聊用户互不串台。
        lock_key: 串行维度（默认同 key）。同一份 checkout 必须串行，否则两个房间并发在同一
                  工作树里 checkout/改文件会互相踩烂——所以这里传 proj_id 按 checkout 串行。
        prepare:  进锁后、跑任务前执行的准备协程（如把工作树拉回干净 base）。
        """
        lock_key = lock_key or key
        async with self._lock(lock_key):
            if prepare is not None:
                try:
                    await prepare()
                except Exception:
                    log.exception("任务前置准备失败（继续按现状跑）")
            epoch = self._epoch.get(key, 0)
            sid, expired = self._sid(key)
            rc, out, err = await self._run(self._cmd(prompt, sid, True, system_prompt), cwd)
            if rc != 0 and sid and self._looks_like_session_error(out, err):
                self.reset(key)
                epoch = self._epoch.get(key, 0)
                rc, out, err = await self._run(self._cmd(prompt, None, True, system_prompt), cwd)
            if rc != 0:
                # 先 redact 再截断，免得 token 跨在截断边界被切成半截
                detail = (redact(err.decode(errors="ignore").strip())
                          or redact(out.decode(errors="ignore").strip()))
                raise RuntimeError(f"claude 退出码 {rc}: {detail[:400]}")
            result, new_sid, is_err = self._parse(out)
            if new_sid and self._epoch.get(key, 0) == epoch:  # 运行期间被 /reset 过就别写回旧会话
                self._sessions[key] = (new_sid, time.time())
                self._save_sessions()
            if is_err:
                raise RuntimeError(f"claude: {redact(result)}")
            answer = result or "(空回复)"
            if expired:  # 上次对话隔太久被清，提示用户已开新会话
                answer = "（距上次较久，已开启新对话）\n\n" + answer
            return answer

    async def quick(self, prompt: str) -> str:
        """一次性纯文本判断，不带危险权限、不复用会话。"""
        rc, out, err = await self._run(self._cmd(prompt, None, agentic=False),
                                       sema=self._quick_sema, env=_quick_env(),
                                       timeout=settings.quick_timeout)
        if rc != 0:
            raise RuntimeError(f"claude 退出码 {rc}: {redact(err.decode(errors='ignore'))[:300]}")
        result, _, is_err = self._parse(out)
        if is_err:
            raise RuntimeError(f"claude: {redact(result)}")
        return result

    def _cmd_ro(self, prompt: str, system_prompt: str | None = None) -> list[str]:
        """只读 agentic 命令：plan 模式能读代码但不会改/提交，用于主动插话在仓库里查证。"""
        cmd = [settings.claude_bin, "-p", prompt, "--output-format", "json",
               "--permission-mode", "plan"]
        if settings.claude_model:
            cmd += ["--model", settings.claude_model]
        sp = system_prompt if system_prompt is not None else settings.claude_system_prompt
        if sp:
            cmd += ["--append-system-prompt", sp]
        return cmd

    async def consult(self, prompt: str, cwd: str | None = None,
                      system_prompt: str | None = None) -> str:
        """在仓库里【只读】地判断/回答（主动插话用）：plan 模式不会改动代码，不复用会话、剔密钥。
        让主动插话在绑了仓库的群里也能看着真实代码作答，而不是凭空瞎猜。"""
        rc, out, err = await self._run(self._cmd_ro(prompt, system_prompt), cwd=cwd,
                                       sema=self._quick_sema, env=_quick_env(),
                                       timeout=settings.quick_timeout)
        if rc != 0:
            raise RuntimeError(f"claude 退出码 {rc}: {redact(err.decode(errors='ignore'))[:300]}")
        result, _, is_err = self._parse(out)
        if is_err:
            raise RuntimeError(f"claude: {redact(result)}")
        return result


runner = ClaudeRunner()
