"""调用本机 Claude Code (claude -p)。

- ask():     带工具、按会话维护多轮上下文，真正干活
- quick():   一次性纯文本判断（DM 分诊、未绑库时的主动插话兜底），不带危险权限、剔除密钥
- consult(): plan 模式【只读】在仓库里查证作答（绑了库的主动插话 / 自驱巡检），不改代码、不复用会话
"""
import asyncio
import json
import logging
import os
import shlex
import signal
import time

from config import settings, redact
from storage import atomic_write_json

log = logging.getLogger("matrix-claude.runner")


class ClaudeCancelled(Exception):
    """用户 /cancel 主动停止任务——与运行出错区分开，让调用方回报"已停止"而非"出错了"。"""


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
        self._active: dict[str, list] = {}   # cancel_key -> 正在跑的子进程列表（供 /cancel 杀）
        self._cancel_req: set[str] = set()   # 已请求取消的 cancel_key

    # ---- 取消：杀掉某 cancel_key 下在跑的子进程；ask() 据 _cancel_req 抛 ClaudeCancelled ----
    def cancel(self, cancel_key: str) -> int:
        """停掉该维度下正在运行的任务，返回杀掉的进程数（0=本来就没在跑）。"""
        live = [p for p in self._active.get(cancel_key, []) if p.returncode is None]
        if live:
            self._cancel_req.add(cancel_key)
        for p in live:
            _kill_group(p)
        return len(live)

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
            atomic_write_json(
                path, {k: [sid, ts] for k, (sid, ts) in self._sessions.items()})
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
                   timeout: float | None = None,
                   on_proc=None):
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
            if on_proc:
                on_proc(proc)   # 登记进程，供 /cancel 杀
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

    async def _run_stream(self, cmd: list[str], cwd: str | None, on_proc, on_line,
                          timeout: float | None = None):
        """流式跑：边读 stdout 的 NDJSON 边回调 on_line(obj)。返回 (returncode, stderr 字节)。

        自己按 \\n 切行（不用 StreamReader.readline，避免 tool_result 里的大块输出撑爆行缓冲），
        on_line 是协程：在里面节流编辑 Matrix 消息。被 cancel() 杀掉时 stdout 直接 EOF，正常收尾。
        """
        workdir = cwd or settings.claude_workdir
        os.makedirs(workdir, exist_ok=True)
        eff = timeout if (timeout and timeout > 0) else (settings.claude_timeout or 600)
        async with self._sema:
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=workdir,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                start_new_session=True, limit=1024 * 1024,
            )
            if on_proc:
                on_proc(proc)
            err_buf = bytearray()

            async def _drain_err():
                try:
                    while True:
                        b = await proc.stderr.read(65536)
                        if not b:
                            break
                        err_buf.extend(b)
                except Exception:
                    pass

            async def _read_stdout():
                buf = bytearray()
                while True:
                    chunk = await proc.stdout.read(65536)
                    if not chunk:
                        break
                    buf.extend(chunk)
                    while True:
                        nl = buf.find(b"\n")
                        if nl < 0:
                            break
                        line = bytes(buf[:nl]); del buf[:nl + 1]
                        if not line.strip():
                            continue
                        try:
                            obj = json.loads(line.decode(errors="ignore"))
                        except json.JSONDecodeError:
                            continue
                        try:
                            await on_line(obj)
                        except Exception:
                            log.exception("流式 on_line 回调异常（忽略，继续读）")
                    if len(buf) > 32 * 1024 * 1024:   # 防单行失控吃内存
                        del buf[:]
                await proc.wait()

            err_task = asyncio.create_task(_drain_err())
            try:
                await asyncio.wait_for(_read_stdout(), timeout=eff)
            except asyncio.TimeoutError:
                _kill_group(proc)
                await proc.wait()
                raise RuntimeError("Claude 响应超时")
            finally:
                err_task.cancel()
                try:
                    await err_task
                except (asyncio.CancelledError, Exception):
                    pass
        return proc.returncode, bytes(err_buf)

    @staticmethod
    def _parse(out: bytes) -> tuple[str, str | None, bool]:
        try:
            data = json.loads(out.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise RuntimeError(f"无法解析 claude 输出: {redact(out.decode(errors='ignore'))[:300]}")
        return (data.get("result") or "").strip(), data.get("session_id"), bool(data.get("is_error"))

    def _cmd(self, prompt: str, sid: str | None, agentic: bool,
             system_prompt: str | None = None, stream: bool = False) -> list[str]:
        cmd = [settings.claude_bin, "-p", prompt]
        cmd += (["--output-format", "stream-json", "--verbose"] if stream
                else ["--output-format", "json"])
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
                  prepare=None, on_delta=None, cancel_key: str | None = None) -> str:
        """带工具、带会话上下文地干活；cwd=该项目的仓库目录。

        key:        会话维度（项目+房间），不同房间/私聊用户互不串台。
        lock_key:   串行维度（默认同 key）。同一份 checkout 必须串行，否则两个房间并发在同一
                    工作树里 checkout/改文件会互相踩烂——所以这里传 proj_id 按 checkout 串行。
        prepare:    进锁后、跑任务前执行的准备协程（如把工作树拉回干净 base）。
        on_delta:   传入则走 stream-json 流式；await on_delta(已产出文本, 工具名或None) 边跑边回报。
        cancel_key: /cancel 的取消维度（默认 lock_key）；被 cancel() 杀掉时抛 ClaudeCancelled。
        """
        lock_key = lock_key or key
        ckey = cancel_key or lock_key
        my_procs: list = []

        def _reg(proc):
            my_procs.append(proc)
            self._active.setdefault(ckey, []).append(proc)

        async def _once(sid):
            """跑一次。返回 (rc, payload, err, meta)：
            rc==0 → payload=结果字符串, meta=(session_id, is_error)；rc!=0 → payload=原始 stdout 字节, meta=None。"""
            if on_delta is None:
                rc, out, err = await self._run(self._cmd(prompt, sid, True, system_prompt), cwd, on_proc=_reg)
                if rc != 0:
                    return rc, out, err, None
                result, new_sid, is_err = self._parse(out)
                return rc, result, err, (new_sid, is_err)
            st = {"sid": None, "result": None, "text": "", "is_err": False}

            async def _on_line(obj):
                if obj.get("session_id"):
                    st["sid"] = obj["session_id"]
                t = obj.get("type")
                if t == "assistant":
                    for blk in (obj.get("message") or {}).get("content") or []:
                        if blk.get("type") == "text" and blk.get("text"):
                            st["text"] += (("\n\n" if st["text"] else "") + blk["text"])
                            await on_delta(st["text"], None)
                        elif blk.get("type") == "tool_use":
                            await on_delta(st["text"], blk.get("name"))
                elif t == "result":
                    if isinstance(obj.get("result"), str):
                        st["result"] = obj["result"]
                    st["is_err"] = bool(obj.get("is_error"))
            rc, err = await self._run_stream(
                self._cmd(prompt, sid, True, system_prompt, stream=True), cwd, _reg, _on_line)
            if rc != 0:
                return rc, b"", err, None
            result = st["result"] if st["result"] is not None else st["text"]
            return rc, (result or "").strip(), err, (st["sid"], st["is_err"])

        async with self._lock(lock_key):
            if prepare is not None:
                try:
                    await prepare()
                except Exception:
                    log.exception("任务前置准备失败（继续按现状跑）")
            try:
                epoch = self._epoch.get(key, 0)
                sid, expired = self._sid(key)
                rc, payload, err, meta = await _once(sid)
                if ckey in self._cancel_req:
                    raise ClaudeCancelled()
                if rc != 0 and sid and self._looks_like_session_error(
                        payload if isinstance(payload, bytes) else b"", err):
                    self.reset(key)
                    epoch = self._epoch.get(key, 0)
                    rc, payload, err, meta = await _once(None)
                    if ckey in self._cancel_req:
                        raise ClaudeCancelled()
                if rc != 0:
                    # 先 redact 再截断，免得 token 跨在截断边界被切成半截
                    detail = redact(err.decode(errors="ignore").strip())
                    if not detail and isinstance(payload, bytes):
                        detail = redact(payload.decode(errors="ignore").strip())
                    raise RuntimeError(f"claude 退出码 {rc}: {detail[:400]}")
                new_sid, is_err = meta
                if new_sid and self._epoch.get(key, 0) == epoch:  # 运行期间被 /reset 过就别写回旧会话
                    self._sessions[key] = (new_sid, time.time())
                    self._save_sessions()
                if is_err:
                    raise RuntimeError(f"claude: {redact(payload)}")
                answer = (payload if isinstance(payload, str) else "") or "(空回复)"
                if expired:  # 上次对话隔太久被清，提示用户已开新会话
                    answer = "（距上次较久，已开启新对话）\n\n" + answer
                return answer
            finally:
                lst = self._active.get(ckey)
                if lst:
                    for p in my_procs:
                        try:
                            lst.remove(p)
                        except ValueError:
                            pass
                    if not lst:
                        self._active.pop(ckey, None)
                self._cancel_req.discard(ckey)

    async def _oneshot(self, cmd: list[str], cwd: str | None = None) -> str:
        """跑一次性纯文本判断（quick / consult 共用）：短超时、独立并发池、剔密钥环境，不复用会话。"""
        rc, out, err = await self._run(cmd, cwd=cwd, sema=self._quick_sema,
                                       env=_quick_env(), timeout=settings.quick_timeout)
        if rc != 0:
            raise RuntimeError(f"claude 退出码 {rc}: {redact(err.decode(errors='ignore'))[:300]}")
        result, _, is_err = self._parse(out)
        if is_err:
            raise RuntimeError(f"claude: {redact(result)}")
        return result

    async def quick(self, prompt: str) -> str:
        """一次性纯文本判断，不带危险权限、不复用会话。"""
        return await self._oneshot(self._cmd(prompt, None, agentic=False))

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
        return await self._oneshot(self._cmd_ro(prompt, system_prompt), cwd=cwd)


runner = ClaudeRunner()
