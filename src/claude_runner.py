"""调用本机 Claude Code (claude -p)。

- ask():     带工具、按会话维护多轮上下文，真正干活
- quick():   一次性纯文本判断（DM 分诊、未绑库时的主动插话兜底），不带危险权限、剔除密钥
- consult(): plan 模式【只读】在仓库里查证作答（绑了库的主动插话 / 自驱巡检），不改代码、不复用会话
"""
import asyncio
import json
import logging
import os
import re
import shlex
import signal
import time

from config import settings, redact
from storage import atomic_write_json

log = logging.getLogger("matrix-claude.runner")


# 上游瞬时故障（过载/限流/5xx/网络抖动）的报错特征：这类几乎都会在几十秒内自愈，值得自动重试。
# 只用来匹配 CLI 的报错文本（stderr / is_error 的 result），不会碰正常回答。
# 两档：特征明确的短语单独出现即认；泛化 token（裸状态码、rate limit、connection 词）太容易
# 撞上普通报错里的行号（foo.js:503:10）、计数（processed 500 files）、被测代码自己的输出
# （做限流功能的任务回显 rate limit）——必须紧邻 API/HTTP 错误上下文才认，否则确定性失败
# 会被误判成瞬时而重试（rc!=0 分支是整任务重放，误判代价是重复副作用）。
_TRANSIENT_RE = re.compile(
    r"overloaded|too many requests|internal server error|service unavailable|"
    r"socket hang up|fetch failed|econnreset|etimedout"
    r"|(?:api.?error|http|status|upstream)[^\n]{0,40}"
    r"(?:\b(?:429|500|502|503|504|529)\b|rate.?limit|connection\s+(?:error|reset|refused))",
    re.I)

_TRANSIENT_BASE_DELAY = 5.0   # 退避基数（秒）：5 → 15 → 45。单测置 0 免真等。

# 结果型瞬时错误（CLI 正常退出、result 是 "API Error: Overloaded" 这类）重试时的接续提示：
# 会话已存在且可能带着半截工作，--resume 它续跑，别把原任务再注入一遍导致重做。
_RESUME_NUDGE = ("（刚才的回合被上游服务过载/限流打断了，这是自动重试：请接着完成上面的任务；"
                 "若任务其实已经完成，直接给出最终答复即可。）")


def _looks_transient(text: str) -> bool:
    """报错是否像上游瞬时故障（过载/限流/5xx/网络抖动）——只有这类才安全自动重试。"""
    return bool(_TRANSIENT_RE.search(text or ""))


class ClaudeCancelled(Exception):
    """用户 /cancel 主动停止任务——与运行出错区分开，让调用方回报"已停止"而非"出错了"。"""


class _CancelToken:
    """一次 ask() 的在途取消令牌：/cancel 把当下在途的每个令牌 cancelled 置真；
    每个任务只认自己的令牌、并在结束时把自己摘掉——所以取消只作用于"当时在途"的任务，
    不会毒杀之后新派的活（新活拿的是干净的新令牌）。started：是否已起子进程（供 /cancel 区分运行中/排队中）。"""
    __slots__ = ("cancelled", "started")

    def __init__(self):
        self.cancelled = False
        self.started = False


# quick() 处理不可信外来内容，给子进程剔除这些密钥（它用不到）。agentic 的 ask() 仍需 GITEA_TOKEN。
_QUICK_STRIP_ENV = ("GITEA_TOKEN", "MATRIX_PASSWORD")

# quick/consult 是近乎每条消息一次的轻判断，绝不需要 MCP 连接器（Gmail/日历等）。但 claude 每次启动
# 都会去 spawn/健康检查用户级配的远端 MCP server——在本机这类网络受限环境里每次要多耗 ~70s（远端连接器
# 还「需要鉴权」根本用不了），直接把 60s 的 quick 超时撑爆。用空 --mcp-config + --strict-mcp-config 关掉
# 所有 MCP，冷启动从 ~75s 降到 ~5s。agentic 的 ask() 不加：那是真干活的路径，留着 MCP 的可能性。
_NO_MCP = ["--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}']


def _user_msg(text: str) -> bytes:
    """stream-json 输入的一条 user 消息帧（编码好、带换行）。try_steer 与常驻回合共用——
    CLI 的信封若有变动只改这一处，别让两处手拼各自漂移（漂了 steering 会静默失效）。"""
    return (json.dumps({"type": "user", "message": {
        "role": "user", "content": [{"type": "text", "text": text}]}},
        ensure_ascii=False) + "\n").encode()


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


class _Persist:
    """一个会话 key 的常驻 claude 进程（--input-format stream-json）。

    回合结束（result 事件）后进程不退出，留在原地等 stdin 的下一条用户消息——于是
    Claude 在回合里启动的后台任务（子代理/后台命令）跨轮次存活；任务完成时 CLI 会自动
    续跑再吐一轮 assistant/result，这类"回合外自发产出"经 on_notify 回投给房间。
    """
    __slots__ = ("key", "proc", "reader", "turn", "on_notify", "sid",
                 "last_activity", "err_tail", "spont_text", "cwd")

    def __init__(self, key: str, proc, cwd: str):
        self.key = key
        self.proc = proc
        self.cwd = cwd
        self.reader = None        # stdout 读取任务（进程同寿命）
        self.turn = None          # 活动回合状态 dict；None=回合外
        self.on_notify = None     # async fn(text)：回合外自发产出的投递回调（每轮 ask 可刷新）
        self.sid = None           # 事件流里报的 session_id（进程死后靠它 --resume 续命）
        self.last_activity = time.time()
        self.err_tail = bytearray()   # stderr 尾部（诊断/判 resume 失效用）
        self.spont_text = ""      # 回合外累计的 assistant 文本

    def alive(self) -> bool:
        return self.proc.returncode is None


class ClaudeRunner:
    def __init__(self):
        self._sessions: dict[str, tuple[str, float]] = self._load_sessions()  # key -> (session_id, ts)
        self._locks: dict[str, asyncio.Lock] = {}
        self._epoch: dict[str, int] = {}                   # key -> 重置代数，防 in-flight 任务复活已重置会话
        self._sema = asyncio.Semaphore(max(1, settings.max_concurrency))
        self._quick_sema = asyncio.Semaphore(max(1, settings.max_concurrency))  # quick 独立并发池
        self._active: dict[str, list] = {}   # cancel_key -> 正在跑的子进程列表（供 /cancel 杀）
        self._tokens: dict[str, set] = {}    # cancel_key -> 在途任务的取消令牌集合（含排队等锁、prepare 中、运行中的）
        self._persist: dict[str, _Persist] = {}   # key -> 常驻进程（CLAUDE_PERSISTENT=1）
        self._reaper: asyncio.Task | None = None  # 空闲常驻进程回收循环

    # ---- 取消：杀在跑的子进程 + 给在途任务（含还没起进程的排队/准备中）置取消令牌 ----
    def cancel(self, cancel_key: str) -> tuple[int, int]:
        """停掉该维度下的在途任务。返回 (运行中被停的任务数, 排队/准备中被取消的任务数)。

        关键点（防毒丸）：只对"此刻确有在途任务"的令牌置位；空场（该 key 下没有任何在途任务）
        绝不留下任何标记，否则几小时后新派的活会被这条陈旧标记莫名秒杀。令牌随任务结束即被摘除。
        运行中的任务不仅置令牌、还立刻杀掉其子进程组；排队/准备中的任务没有进程可杀，靠令牌在
        它拿到锁后自我了断（见 ask()）。"""
        live = [p for p in self._active.get(cancel_key, []) if p.returncode is None]
        running = queued = 0
        for t in self._tokens.get(cancel_key) or ():
            t.cancelled = True
            if t.started:
                running += 1
            else:
                queued += 1
        for p in live:
            _kill_group(p)
        return running, queued

    # ---- steering：任务进行中把追加消息递进当前回合（像 Claude Code 运行中打字）----
    async def try_steer(self, key: str, text: str) -> bool:
        """该 key 的常驻进程【聊天回合在途】时，把 text 作为新的 user 事件写进 stdin 并返回 True；
        否则返回 False（调用方走正常排队）。只认 turn["steerable"] 的回合——自驱/PR 跟进/工单
        与聊天共用同一个会话 key，但那些回合正带着危险权限写代码/推 PR，且没设 on_notify 投递口，
        把用户的话塞进去要么污染自驱产出、要么答案没处送；非聊天回合一律回落排队。
        CLI 对 mid-turn 消息的两种处理都兼容：
        · 真 steering——在当前回合的工具间隙注入给模型，最终一并答复；
        · 排成紧接着的下一回合——多出的 result 走"自发产出"通道（_persist_event 的
          spont/on_notify 路径）投回房间（聊天回合必设 on_notify，投递有保障）。
        写入前后回合恰好结束的竞态同理落入第二种，无害。写失败（进程刚死）返回 False。"""
        if not settings.claude_persistent:
            return False
        ps = self._persist.get(key)
        if (ps is None or not ps.alive() or ps.turn is None
                or not ps.turn.get("steerable")):
            return False
        try:
            ps.proc.stdin.write(_user_msg(text))
            await ps.proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError, RuntimeError, OSError):
            return False
        ps.last_activity = time.time()
        log.info("[%s] steering：追加消息已递进运行中的回合（%d 字）", key, len(text))
        return True

    # ---- 只读查询（/status 用）----
    def running(self, cancel_key: str) -> int:
        """该取消维度（房间）下正在跑的子进程数。"""
        return len([p for p in self._active.get(cancel_key, []) if p.returncode is None])

    def busy(self, lock_key: str) -> bool:
        """该串行维度（项目 checkout）是否被占用（有任务在跑或在排队）。"""
        lock = self._locks.get(lock_key)
        return bool(lock and lock.locked())

    def capacity_full(self) -> bool:
        """全局 agentic 并发额度（MAX_CONCURRENCY）是否已占满——此刻新派的回合起跑前得先等空位。
        只查不占；与真实获取存在竞态，同 busy 一样只供派活层发"已排队"的尽力知会。"""
        return self._sema.locked()

    def pending(self, cancel_key: str) -> int:
        """该取消维度（房间）下在途任务数——排队等锁、prepare 中、运行中的都算，
        即 /cancel 此刻能停掉的任务数。与 running 的区别：running 只数已起子进程的。"""
        return len(self._tokens.get(cancel_key) or ())

    def session_ts(self, key: str) -> float | None:
        """该会话最近一次活跃的时刻；无有效会话（不存在/已过 TTL）返回 None。不产生副作用。"""
        item = self._sessions.get(key)
        if not item or time.time() - item[1] > settings.session_ttl:
            return None
        return item[1]

    def session_model(self, key: str) -> str:
        """该会话创建时记录的 CLAUDE_MODEL（仅观测：/status 据此提示"配置改了但对本会话
        不生效"，绝不回传给 --resume）。无记录（旧版条目/会话不存在）返回 ""。不产生副作用。"""
        item = self._sessions.get(key)
        return item[2] if item and len(item) > 2 and isinstance(item[2], str) else ""

    # ---- 会话持久化：重启后恢复 session_id，多轮上下文不至于每次重启全断 ----
    def _sessions_file(self) -> str:
        return os.path.join(settings.store_path, "sessions.json")

    def _load_sessions(self) -> dict:
        try:
            with open(self._sessions_file()) as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}
        out: dict[str, tuple] = {}
        now = time.time()
        if isinstance(raw, dict):
            for k, v in raw.items():
                try:
                    sid, ts = v[0], float(v[1])
                except (TypeError, ValueError, IndexError):
                    continue
                # 第三元=会话创建时的 CLAUDE_MODEL（仅观测，见 session_model）；旧版条目没有，补 ""
                model = v[2] if len(v) > 2 and isinstance(v[2], str) else ""
                if sid and now - ts <= settings.session_ttl:   # 早就过期的不必恢复
                    out[k] = (sid, ts, model)
        return out

    def _save_sessions(self) -> None:
        # 原子写；调用点都在事件循环里且无 await，对其它协程是原子的，不会丢更新。
        # 按下标取值：测试/旧代码可能注入无模型元的二元组，别让落盘炸在解包上。
        path = self._sessions_file()
        try:
            atomic_write_json(
                path, {k: [v[0], v[1], v[2] if len(v) > 2 else ""]
                       for k, v in self._sessions.items()})
        except OSError as e:
            log.warning("会话持久化失败 %s: %s", path, e)

    # ---- 会话管理（按 key：项目+房间，见 bot._sess_key）----
    def _lock(self, key: str) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    def reset(self, key: str):
        self._sessions.pop(key, None)
        self._epoch[key] = self._epoch.get(key, 0) + 1
        self._save_sessions()
        ps = self._persist.pop(key, None)   # 常驻进程与会话同寿命：/reset 一并杀掉
        if ps is not None:
            _kill_group(ps.proc)

    def _sid(self, key: str) -> tuple[str | None, bool]:
        """返回 (有效 session_id 或 None, 是否因空闲超时刚被清掉)。"""
        item = self._sessions.get(key)
        if not item:
            return None, False
        sid, ts = item[0], item[1]
        if time.time() - ts > settings.session_ttl:
            self._sessions.pop(key, None)
            return None, True
        return sid, False

    # ---- 进程调用 ----
    @staticmethod
    async def _notify_queued(sema: asyncio.Semaphore, on_queued) -> None:
        """即将在并发额度上真正阻塞（信号量已满）时通知调用方。派发时刻的预检
        （capacity_full）有竞态窗口——等锁/prepare 期间额度可能被占满——这里才是
        排队确实发生的时刻，兜底补发知会。回调失败只记日志，不影响排队本身。"""
        if on_queued is not None and sema.locked():
            try:
                await on_queued()
            except Exception:
                log.exception("on_queued 回调失败（忽略，继续排队）")

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
            except asyncio.CancelledError:
                # 手动前台跑时 Ctrl-C：协程被取消，若不主动收尾，子进程（独立进程组）会变孤儿
                # 继续在后台 push / 开 PR。杀掉整组再把取消抛上去。
                _kill_group(proc)
                raise
        return proc.returncode, out or b"", err or b""

    async def _run_stream(self, cmd: list[str], cwd: str | None, on_proc, on_line,
                          timeout: float | None = None):
        """流式跑：边读 stdout 的 NDJSON 边回调 on_line(obj)。返回 (returncode, stderr 字节)。

        自己按 \\n 切行（不用 StreamReader.readline，避免 tool_result 里的大块输出撑爆行缓冲），
        on_line 是协程：在里面节流编辑 Matrix 消息。被 cancel() 杀掉时 stdout 直接 EOF，正常收尾。
        超时是【空闲】超时（按单次 read 计）：持续产出就不限总时长，仅长时间静默才判卡死。
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
                    # eff 是【空闲】超时而非整体墙钟：只要还在持续产出（token / 工具事件）就一直读，
                    # 仅当 eff 秒内一个字节都没来（真卡死）才超时。整体跑多久不设限——长任务
                    # （clone→改→测→push→开 PR）边流式输出边干，常超 10 分钟，不该被误杀。
                    chunk = await asyncio.wait_for(proc.stdout.read(65536), timeout=eff)
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
                # stdout 已 EOF，进程应即将退出；给个上限别在僵死进程上无限等
                try:
                    await asyncio.wait_for(proc.wait(), timeout=30)
                except asyncio.TimeoutError:
                    _kill_group(proc)
                    await proc.wait()

            err_task = asyncio.create_task(_drain_err())
            try:
                await _read_stdout()
            except asyncio.TimeoutError:
                _kill_group(proc)
                await proc.wait()
                raise RuntimeError("Claude 响应超时")
            except asyncio.CancelledError:
                # 同 _run：Ctrl-C 取消时别把子进程组丢成孤儿，杀掉再抛。
                _kill_group(proc)
                raise
            finally:
                err_task.cancel()
                try:
                    await err_task
                except (asyncio.CancelledError, Exception):
                    pass
        return proc.returncode, bytes(err_buf)

    # ---- 常驻进程模式（CLAUDE_PERSISTENT）：进程跨轮次保活，后台任务不随回合死 ----
    def _cmd_persistent(self, sid: str | None, system_prompt: str | None,
                        fork: bool) -> list[str]:
        """常驻 agentic 进程的命令行：无位置 prompt（消息走 stdin NDJSON），其余同 _cmd(agentic)。"""
        cmd = [settings.claude_bin, "-p",
               "--input-format", "stream-json",
               "--output-format", "stream-json", "--verbose"]
        if settings.claude_model and not sid:   # 只在开新会话时传（同 _cmd，理由见彼处）
            cmd += ["--model", settings.claude_model]
        if settings.claude_dangerous:
            cmd += ["--dangerously-skip-permissions"]
        elif settings.claude_permission_mode:
            cmd += ["--permission-mode", settings.claude_permission_mode]
        sp = system_prompt if system_prompt is not None else settings.claude_system_prompt
        if sp and not sid:   # 系统提示只在开新会话时设一次（同 _cmd）
            cmd += ["--append-system-prompt", sp]
        if sid:
            cmd += ["--resume", sid]
            if fork:
                cmd += ["--fork-session"]
        if settings.claude_extra_args:
            cmd += shlex.split(settings.claude_extra_args)
        return cmd

    async def _persist_spawn(self, key: str, sid: str | None, fork: bool,
                             system_prompt: str | None, cwd: str | None) -> _Persist:
        workdir = cwd or settings.claude_workdir
        os.makedirs(workdir, exist_ok=True)
        proc = await asyncio.create_subprocess_exec(
            *self._cmd_persistent(sid, system_prompt, fork),
            cwd=workdir,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True, limit=1024 * 1024,
        )
        ps = _Persist(key, proc, workdir)
        ps.sid = None if fork else sid   # fork 会派生新 sid，事件流里会报真值
        loop = asyncio.get_running_loop()
        ps.reader = loop.create_task(self._persist_reader(ps))
        loop.create_task(self._persist_drain_err(ps))
        self._persist[key] = ps
        self._ensure_reaper()
        return ps

    async def _persist_drain_err(self, ps: _Persist):
        """持续吸 stderr（防管道堵死），只留尾部 64KB 供诊断/判 resume 失效。"""
        try:
            while True:
                b = await ps.proc.stderr.read(65536)
                if not b:
                    break
                ps.err_tail.extend(b)
                if len(ps.err_tail) > 65536:
                    del ps.err_tail[:len(ps.err_tail) - 65536]
        except Exception:
            pass

    async def _persist_reader(self, ps: _Persist):
        """常驻进程的 stdout 读取循环（进程同寿命）。回合内事件喂给活动回合；回合外的
        自发产出（后台任务完成后 CLI 自动续跑）经 on_notify 投递。进程一死就收尾摘除。"""
        proc = ps.proc
        buf = bytearray()
        try:
            while True:
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    break
                ps.last_activity = time.time()
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
                        await self._persist_event(ps, obj)
                    except Exception:
                        log.exception("常驻进程事件处理异常（忽略，继续读）")
                if len(buf) > 32 * 1024 * 1024:   # 防单行失控吃内存
                    del buf[:]
        finally:
            try:
                await proc.wait()
            except Exception:
                pass
            if self._persist.get(ps.key) is ps:
                self._persist.pop(ps.key, None)
            t, ps.turn = ps.turn, None
            if t and not t["fut"].done():   # 回合进行中进程死了（被杀/崩溃/resume 失败秒退）
                t["fut"].set_exception(RuntimeError("claude 常驻进程退出"))

    async def _persist_event(self, ps: _Persist, obj: dict):
        if obj.get("session_id"):
            ps.sid = obj["session_id"]
        t = obj.get("type")
        turn = ps.turn
        if turn is not None:                     # 回合内：行为等同 _run_stream 的 _on_line
            turn["last_io"] = time.time()
            if t == "assistant":
                for blk in (obj.get("message") or {}).get("content") or []:
                    if blk.get("type") == "text" and blk.get("text"):
                        turn["text"] += (("\n\n" if turn["text"] else "") + blk["text"])
                        if turn["on_delta"]:
                            await turn["on_delta"](turn["text"], None)
                    elif blk.get("type") == "tool_use" and turn["on_delta"]:
                        await turn["on_delta"](turn["text"], blk.get("name"))
            elif t == "result":
                if isinstance(obj.get("result"), str):
                    turn["result"] = obj["result"]
                turn["is_err"] = bool(obj.get("is_error"))
                if not turn["fut"].done():
                    turn["fut"].set_result(None)
            return
        # 回合外：后台任务完成 → CLI 自动续跑的自发产出，攒文本、result 时整体投递
        if t == "assistant":
            for blk in (obj.get("message") or {}).get("content") or []:
                if blk.get("type") == "text" and blk.get("text"):
                    ps.spont_text += (("\n\n" if ps.spont_text else "") + blk["text"])
        elif t == "result":
            text = obj["result"] if isinstance(obj.get("result"), str) else ps.spont_text
            ps.spont_text = ""
            cb = ps.on_notify
            if cb is not None and (text or "").strip():
                asyncio.get_running_loop().create_task(self._safe_notify(cb, text.strip()))

    @staticmethod
    async def _safe_notify(cb, text: str):
        try:
            await cb(text)
        except Exception:
            log.exception("后台产出投递回调失败（丢弃这条产出）")

    def _ensure_reaper(self):
        if self._reaper is None or self._reaper.done():
            self._reaper = asyncio.get_running_loop().create_task(self._reap_loop())

    async def _reap_loop(self):
        """周期回收空闲常驻进程：释放内存。会话 sid 已落盘，下条消息照常 --resume，上下文不丢。"""
        while True:
            await asyncio.sleep(600)
            now = time.time()
            for key, ps in list(self._persist.items()):
                if ps.turn is None and now - ps.last_activity > settings.persistent_idle:
                    self._persist.pop(key, None)
                    _kill_group(ps.proc)
                    log.info("回收空闲常驻 claude 进程 %s（闲置 %.0f 分钟）",
                             key, (now - ps.last_activity) / 60)
            if not self._persist:
                self._reaper = None
                return

    async def _persist_once(self, key: str, sid: str | None, fork: bool, prompt: str,
                            system_prompt: str | None, cwd: str | None,
                            on_delta, on_notify, on_proc, fail_info: dict | None = None,
                            steerable: bool = False):
        """常驻进程跑一个回合。返回值形状与 ask._once 相同：
        rc==0 → (0, 结果字符串, b"", (session_id, is_error))；失败 → (rc, b"", stderr尾, None)。
        fail_info：失败返回前把进程已报过的 session_id 记进去（供瞬时重试改走 resume 而非重放）。
        steerable：这是不是聊天回合（try_steer 只允许把追加消息递进聊天回合）。"""
        async with self._sema:   # 并发额度只在回合期间占用；空闲常驻进程不占
            ps = self._persist.get(key)
            if ps is not None and not ps.alive():
                self._persist.pop(key, None)
                ps = None
            if ps is None:
                ps = await self._persist_spawn(key, sid, fork, system_prompt, cwd)
            if on_proc:
                on_proc(ps.proc)   # 登记给 /cancel（杀常驻进程=停任务；下轮凭 sid 重生）
            if on_notify is not None:
                ps.on_notify = on_notify
            fut = asyncio.get_running_loop().create_future()
            turn = {"fut": fut, "on_delta": on_delta, "text": "", "result": None,
                    "is_err": False, "last_io": time.time(), "steerable": steerable}
            ps.turn = turn
            try:
                ps.proc.stdin.write(_user_msg(prompt))
                await ps.proc.stdin.drain()
            except (BrokenPipeError, ConnectionResetError, RuntimeError, OSError):
                ps.turn = None
                _kill_group(ps.proc)
                if fail_info is not None and ps.sid:
                    fail_info["sid"] = ps.sid
                return 1, b"", bytes(ps.err_tail), None
            eff = settings.claude_timeout or 600
            # 空闲超时语义同 _run_stream：持续产出不限总时长，静默 eff 秒才判卡死
            while not fut.done():
                try:
                    await asyncio.wait_for(asyncio.shield(fut), timeout=10)
                except asyncio.TimeoutError:
                    if time.time() - turn["last_io"] > eff:
                        ps.turn = None
                        _kill_group(ps.proc)
                        raise RuntimeError("Claude 响应超时")
                except asyncio.CancelledError:
                    # 同 _run：协程被取消（Ctrl-C）别把子进程组丢成孤儿
                    ps.turn = None
                    _kill_group(ps.proc)
                    raise
                except Exception:
                    # fut 以异常收尾（进程被 /cancel 杀、崩溃）：await 会把异常直接抛出来，
                    # 这里吞掉跳出循环，交给下面 fut.result() 统一分流成非零 rc——
                    # 让 ask() 判 token.cancelled 报"已停止"，而不是把异常一路穿成"出错了"。
                    break
            try:
                fut.result()
            except RuntimeError:
                # 回合中进程退出（被 /cancel 杀、崩溃、--resume 失败秒退）：
                # 交回非零 rc + stderr 尾，让 ask() 现有的"会话失效重试"逻辑接手判断
                ps.turn = None
                if fail_info is not None and ps.sid:
                    fail_info["sid"] = ps.sid
                return (ps.proc.returncode if ps.proc.returncode is not None else 1), \
                    b"", bytes(ps.err_tail), None
            ps.turn = None
            ps.last_activity = time.time()
            result = turn["result"] if turn["result"] is not None else turn["text"]
            return 0, (result or "").strip(), b"", (ps.sid, turn["is_err"])

    @staticmethod
    def _parse(out: bytes) -> tuple[str, str | None, bool]:
        try:
            data = json.loads(out.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise RuntimeError(f"无法解析 claude 输出: {redact(out.decode(errors='ignore'))[:300]}")
        return (data.get("result") or "").strip(), data.get("session_id"), bool(data.get("is_error"))

    def _cmd(self, prompt: str, sid: str | None, agentic: bool,
             system_prompt: str | None = None, stream: bool = False,
             fork: bool = False) -> list[str]:
        cmd = [settings.claude_bin, "-p", prompt]
        cmd += (["--output-format", "stream-json", "--verbose"] if stream
                else ["--output-format", "json"])
        # 干活用 CLAUDE_MODEL；轻判断（非 agentic 的 quick）优先 CLAUDE_QUICK_MODEL（小模型省钱提速）。
        # 只在开新会话时传（同 --append-system-prompt）：--resume 时再带 --model 会把恢复出的
        # 会话【连同被恢复的子代理】整体强制成该模型，覆盖掉子代理各自的模型路由——被中断的
        # opus 子代理续跑时会被打回主模型，烧错额度。不传则会话沿用中断前各自记录的模型。
        # 代价：改 CLAUDE_MODEL 对续接中的会话不生效——空闲 TTL 每轮都被刷新（见 ask 的写回），
        # 活跃房间的会话不会自行过期，要对【房间】/reset 才切换（线程会话从房间会话分叉，线程内
        # /reset 后仍会从房间会话重新 fork 出旧模型）。/status 会拿记录的会话模型对比配置来提示。
        model = settings.claude_model if agentic else (settings.claude_quick_model or settings.claude_model)
        if model and not sid:
            cmd += ["--model", model]
        if agentic:
            if settings.claude_dangerous:
                cmd += ["--dangerously-skip-permissions"]
            elif settings.claude_permission_mode:
                cmd += ["--permission-mode", settings.claude_permission_mode]
        else:
            cmd += _NO_MCP   # 轻判断不碰 MCP，省掉远端 server 的启动/健康检查延迟
        sp = system_prompt if system_prompt is not None else settings.claude_system_prompt
        if sp and not sid:  # 系统提示只在开新会话时设一次
            cmd += ["--append-system-prompt", sp]
        if sid:
            cmd += ["--resume", sid]
            if fork:   # 从 sid 分叉出新会话（继承历史、各走各的），而不是续写原会话
                cmd += ["--fork-session"]
        if agentic and settings.claude_extra_args:
            cmd += shlex.split(settings.claude_extra_args)
        return cmd

    @staticmethod
    async def _transient_wait(attempt: int, token: "_CancelToken | None", what: str) -> None:
        """瞬时故障重试前的退避等待（5s→15s→45s），小步睡以便 /cancel 能即时打断。
        注意这段睡眠发生在项目串行锁内（resume 续跑必须守住同一 checkout，锁不能放）：
        调大 CLAUDE_TRANSIENT_RETRIES 会按 5·3^n 拉长同项目排队任务的空等。"""
        delay = _TRANSIENT_BASE_DELAY * (3 ** attempt)
        log.warning("上游瞬时故障，%.0fs 后自动重试（第 %d 次）：%s", delay, attempt + 1, what[:200])
        end = time.monotonic() + delay
        while time.monotonic() < end:
            if token is not None and token.cancelled:
                raise ClaudeCancelled()
            await asyncio.sleep(min(0.5, max(0.0, end - time.monotonic())))
        # 落在最后一个睡片里的 /cancel 会躲过循环顶部的检查：这里必须再验一次，
        # 否则 continue 会起新子进程把整个回合跑完（cancel 已执行过，杀不到后起的进程）。
        if token is not None and token.cancelled:
            raise ClaudeCancelled()

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
                  prepare=None, on_delta=None, cancel_key: str | None = None,
                  fork_from: str | None = None, on_reset=None, on_notify=None,
                  steerable: bool = False, on_queued=None) -> str:
        """带工具、带会话上下文地干活；cwd=该项目的仓库目录。

        key:        会话维度（项目+房间[+线程]），不同房间/私聊用户互不串台。
        lock_key:   串行维度（默认同 key）。同一份 checkout 必须串行，否则两个房间并发在同一
                    工作树里 checkout/改文件会互相踩烂——所以这里传 proj_id 按 checkout 串行。
        prepare:    进锁后、跑任务前执行的准备协程（如把工作树拉回干净 base）。
        on_delta:   传入则走 stream-json 流式；await on_delta(已产出文本, 工具名或None) 边跑边回报。
        cancel_key: /cancel 的取消维度（默认 lock_key）；被 cancel() 杀掉时抛 ClaudeCancelled。
        fork_from:  key 还没有会话时，从这个父会话 key 分叉（--resume 父 + --fork-session）：
                    新会话继承分叉点之前的全部历史、之后与父互相隔离（线程会话从房间会话分叉用）。
                    公共前缀逐字节相同，缓存 TTL 内还能命中 prompt cache。父会话也不存在就全新开。
        on_reset:   续接的 sid 被 claude 判失效、就地清掉会话改全新开时回调一次（无参）。调用方据此
                    作废「本以为在这条会话里、其实已丢」的派生状态（如背景缓冲的 dispatched 标记）。
        on_notify:  常驻进程模式下"回合外自发产出"（后台任务完成后 CLI 自动续跑）的投递回调
                    async fn(text)。不传则沿用该会话上次设置的回调（心跳/工单等复用聊天会话时
                    别把聊天设好的投递口冲掉）。
        steerable:  这是不是聊天回合——只有聊天回合允许 try_steer 把追加消息递进来。
                    自驱/PR 跟进/工单不传（False），它们与聊天共用会话 key，但用户的话
                    绝不能被注入进那些带危险权限的自动化回合。
        on_queued:  即将在全局并发额度上真正阻塞（信号量已满）时回调 async fn()。
                    派活层的派发时刻预检（capacity_full）有竞态窗口——等锁/prepare
                    （git fetch 可数十秒）期间额度可能被别人占满——这里兜底补发"已排队"
                    知会。瞬时重试会再次经过信号量，回调可能多次触发，去重由调用方负责。
        """
        lock_key = lock_key or key
        ckey = cancel_key or lock_key
        my_procs: list = []
        token = _CancelToken()
        # 一进 ask 就登记令牌（此刻可能还在排队等锁、还没起子进程）：这样 /cancel 也能标记到
        # "已派但没轮到"的任务，而不是只逮住已在跑的那个。
        self._tokens.setdefault(ckey, set()).add(token)

        def _reg(proc):
            token.started = True   # 起了子进程：/cancel 据此把本任务算作"运行中"而非"排队中"
            my_procs.append(proc)
            self._active.setdefault(ckey, []).append(proc)

        # 上次失败回合的残留信号（供瞬时重试决策）：sid=进程死前已捕获的会话、tool=是否已跑过工具。
        # rc!=0 时 meta 是 None，这些信号只能从这里拿。
        last_fail: dict = {}

        async def _once(sid, fork=False, prompt_override=None):
            """跑一次。返回 (rc, payload, err, meta)：
            rc==0 → payload=结果字符串, meta=(session_id, is_error)；rc!=0 → payload=原始 stdout 字节, meta=None。
            prompt_override：瞬时故障重试续跑时用接续提示替代原 prompt（原任务已在会话里，别再注入一遍）。"""
            p = prompt_override or prompt
            last_fail.clear()
            # 三条执行路径进门第一件事都是抢全局并发额度（_sema）：额度已满就在这里统一
            # 发"真要排队了"的兜底通知——不把 on_queued 下传进内部签名，测试里 monkeypatch
            # 的假 _run/_persist_once 就不必跟着认识它。
            await self._notify_queued(self._sema, on_queued)
            if settings.claude_persistent:
                return await self._persist_once(key, sid, fork, p, system_prompt,
                                                cwd, on_delta, on_notify, _reg,
                                                fail_info=last_fail, steerable=steerable)
            if on_delta is None:
                rc, out, err = await self._run(self._cmd(p, sid, True, system_prompt, fork=fork),
                                               cwd, on_proc=_reg)
                if rc != 0:
                    return rc, out, err, None
                result, new_sid, is_err = self._parse(out)
                return rc, result, err, (new_sid, is_err)
            st = {"sid": None, "result": None, "text": "", "is_err": False, "tool": False}

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
                            st["tool"] = True   # 已有工具落地（可能含 push 等副作用）：重试决策要知道
                            await on_delta(st["text"], blk.get("name"))
                elif t == "result":
                    if isinstance(obj.get("result"), str):
                        st["result"] = obj["result"]
                    st["is_err"] = bool(obj.get("is_error"))
            rc, err = await self._run_stream(
                self._cmd(p, sid, True, system_prompt, stream=True, fork=fork),
                cwd, _reg, _on_line)
            if rc != 0:
                # 进程死了也别把流里已捕获的会话/工具信号丢掉：有 sid 重试就能 resume 续跑
                # 而不是整任务重放；跑过工具则重放有重复副作用（重复 push/PR）的风险。
                last_fail.update(sid=st["sid"], tool=st["tool"])
                return rc, b"", err, None
            result = st["result"] if st["result"] is not None else st["text"]
            return rc, (result or "").strip(), err, (st["sid"], st["is_err"])

        try:
            async with self._lock(lock_key):
                # 拿到锁后、起子进程前先验一次：排队等锁期间若被 /cancel，这里即刻了断，
                # 连 prepare（git fetch 可数十秒）和子进程都不再起——正是"派 A 又派 B、B 排队时 /cancel"
                # 那条 B 该走的路：不能等它默默开跑。
                if token.cancelled:
                    raise ClaudeCancelled()
                epoch = self._epoch.get(key, 0)
                sid, expired = self._sid(key)
                if sid is None:
                    # 会话已过期/不存在：残留的常驻进程（若有）承载的是旧上下文，一并作废，
                    # 别让"新任务"接在一条按理该翻篇的进程里续写。
                    stale = self._persist.pop(key, None)
                    if stale is not None:
                        _kill_group(stale.proc)
                fork = False
                if sid is None and fork_from:      # 本 key 还没会话：从父会话分叉（父也没有就全新开）
                    parent_sid, _ = self._sid(fork_from)
                    if parent_sid:
                        sid, fork = parent_sid, True
                # 「新任务」= 拿不到任何可续接的会话（自己的没有、也没从父会话分叉到）。fork 出的新线程
                # 是接着父对话往下走（--resume 父 sid），跟续接轮同理：算续接、不算新任务，故这里判 fresh
                # 必须在上面的 fork 解析【之后】——否则 fork 时 sid 还是 None 会被误判成新任务而 reset。
                fresh = sid is None
                # 本轮会话对应的「创建时模型」（随 new_sid 记进 sessions.json 第三元，仅观测用）：
                # 全新开=当前配置；续接/fork=沿用原会话记录的（fork 子承父）。--resume 不带 --model，
                # 会话实际跑的就是这个记录值——/status 拿它对比当前配置，提示"改了没生效"。
                sess_model = (settings.claude_model or "") if fresh \
                    else self.session_model(fork_from if fork else key)
                if expired and fresh and on_reset is not None:
                    # 拿到锁时才发现本 key 会话已过 TTL → 本轮要全新开；但拼 prompt 那刻会话还在，调用方
                    # 可能已按「续接」裁掉派过的消息（drop_dispatched）。这里 sid 从一开始就是 None，走不到
                    # 下面「resume 被拒」的回调，必须在此单独通知，别让被裁的消息两头落空（背景没、新会话也没）。
                    # 必须再判一次 fresh：若 fork 从父会话接上了（expired 但 fork 成功→sid 非 None），
                    # 那就不是「全新开」，父会话历史还在，dispatched 标记仍然有效，不该被清掉。
                    try:
                        on_reset()
                    except Exception:
                        log.exception("on_reset 回调失败（继续全新开）")
                # prepare（把工作树拉回干净 base）只在「新任务」跑一次：续接同一场对话（含 fork 分叉）时
                # 跳过，否则上一轮还没提交的活会被这一轮 reset 冲掉。reset 跟会话生命周期走、不跟每条
                # 消息走（全新任务的首条→会 reset；续接轮和 fork 出的新线程都不会）。
                if prepare is not None and fresh:
                    try:
                        await prepare()
                    except Exception:
                        log.exception("任务前置准备失败（继续按现状跑）")
                    if token.cancelled:   # prepare（git fetch 可数十秒）跑完再验一次，别白起子进程
                        raise ClaudeCancelled()
                # 上游瞬时故障（Overloaded/限流/5xx）自动退避重试。重试姿势按错误形态区分：
                # · CLI 正常退出但结果 is_error（如 "API Error: Overloaded"）→ session_id 已拿到，
                #   --resume 它并用接续提示续跑：已完成的工作（含已 push 的）都在会话里，不会重做；
                # · CLI 非零退出 → 拿不到 session_id，只能原样重跑同一调用（resume 的还 resume）。
                # 两种都仅在报错明确匹配瞬时特征时才重试——其它错误维持原状不敢碰：
                # 任务可能已 push/开 PR，盲目重跑会产生重复分支/PR（同 _looks_like_session_error 的顾虑）。
                retries = max(0, settings.claude_transient_retries)
                run_sid, run_fork, nudge = sid, fork, None
                for attempt in range(retries + 1):
                    rc, payload, err, meta = await _once(run_sid, run_fork, prompt_override=nudge)
                    if token.cancelled:
                        raise ClaudeCancelled()
                    if rc != 0 and run_sid and self._looks_like_session_error(
                            payload if isinstance(payload, bytes) else b"", err):
                        # fork 场景失效的是【父】会话（--resume 的是它）：清父，别让房间任务再撞尸体；
                        # 普通 resume 失效清自己。然后全新开一条。
                        self.reset(fork_from if run_fork else key)
                        if on_reset is not None:
                            # 会话没了 → 那条 prompt 是照「续接」裁过的（可能已剔掉派过的消息），如今要全新
                            # 开：通知调用方作废相关派生状态，别让被剔的消息永久两头落空（背景+新会话都没）。
                            try:
                                on_reset()
                            except Exception:
                                log.exception("on_reset 回调失败（继续全新开）")
                        epoch = self._epoch.get(key, 0)
                        run_sid, run_fork, nudge = None, False, None   # 会话没了，接续提示也无从谈起
                        sess_model = settings.claude_model or ""       # 全新开：记当前配置的模型
                        rc, payload, err, meta = await _once(None)
                        if token.cancelled:
                            raise ClaudeCancelled()
                    if rc != 0:
                        # 先 redact 再截断，免得 token 跨在截断边界被切成半截
                        detail = redact(err.decode(errors="ignore").strip())
                        if not detail and isinstance(payload, bytes):
                            detail = redact(payload.decode(errors="ignore").strip())
                        if attempt < retries and _looks_transient(detail):
                            fail_sid, ran_tool = last_fail.get("sid"), last_fail.get("tool")
                            if fail_sid is None and run_sid is None and ran_tool:
                                # 全新任务已跑过工具（可能已 push/开 PR）却没捕获到会话：
                                # 整任务重放会重做副作用，这一注不赌——按失败收场。
                                raise RuntimeError(f"claude 退出码 {rc}: {detail[:400]}")
                            await self._transient_wait(attempt, token, detail)
                            if self._epoch.get(key, 0) != epoch:
                                # 退避期间被 /reset：会话已翻篇，这次重试作废，按原错误收场
                                raise RuntimeError(f"claude 退出码 {rc}: {detail[:400]}")
                            if fail_sid:   # 进程死前已捕获会话 → 改走 resume+接续提示，别整任务重放
                                run_sid, run_fork, nudge = fail_sid, False, _RESUME_NUDGE
                            continue   # 否则原样重跑（run_sid/nudge 不变）
                        raise RuntimeError(f"claude 退出码 {rc}: {detail[:400]}")
                    new_sid, is_err = meta
                    if new_sid and self._epoch.get(key, 0) == epoch:  # 运行期间被 /reset 过就别写回旧会话
                        self._sessions[key] = (new_sid, time.time(), sess_model)
                        self._save_sessions()
                    if is_err:
                        detail = redact(payload)
                        if attempt < retries and _looks_transient(detail):
                            await self._transient_wait(attempt, token, detail)
                            if self._epoch.get(key, 0) != epoch:
                                # 退避期间被 /reset：别 resume 用户刚丢弃的会话把旧任务跑完
                                raise RuntimeError(f"claude: {detail}")
                            if new_sid:   # 本次回合的会话在：续跑 + 接续提示
                                run_sid, run_fork, nudge = new_sid, False, _RESUME_NUDGE
                            # 没拿到本回合 sid（三条路径都会报 sid，几乎不可能）：保持原参数重跑。
                            # 特意不回退到 run_sid——fork 场景那是【父】会话，nudge 打进去会让
                            # 从未收到任务的房间共享会话去"接着完成"一个不存在的上文。
                            continue
                        raise RuntimeError(f"claude: {detail}")
                    answer = (payload if isinstance(payload, str) else "") or "(空回复)"
                    if expired and fresh:  # 真·全新开（fork 接上父会话时历史还在，不算，别误导用户）
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
            # 摘掉自己的令牌：任务一结束（正常/取消/异常）它就不再"在途"，之后的 /cancel 也不该再动它。
            toks = self._tokens.get(ckey)
            if toks is not None:
                toks.discard(token)
                if not toks:
                    self._tokens.pop(ckey, None)

    async def _oneshot(self, cmd: list[str], cwd: str | None = None) -> str:
        """跑一次性纯文本判断（quick / consult 共用）：短超时、独立并发池、剔密钥环境，不复用会话。
        无状态无副作用，上游瞬时故障（过载/限流）原样重跑一次即可（固定最多一次，不跟
        CLAUDE_TRANSIENT_RETRIES 的次数走、只认它的开关）；这类轻判断多在消息处理链上，
        退避取短（基数的 0.6 倍，默认 3s），别把语义闸/插话判断拖太久。"""
        for attempt in range(2):
            rc, out, err = await self._run(cmd, cwd=cwd, sema=self._quick_sema,
                                           env=_quick_env(), timeout=settings.quick_timeout)
            if rc != 0:   # 瞬时判定用未截断原文（别让长前缀把特征挤出截断窗），截断只在报错时做
                detail = redact(err.decode(errors="ignore"))
                msg = f"claude 退出码 {rc}: {detail[:300]}"
            else:
                result, _, is_err = self._parse(out)
                if not is_err:
                    return result
                detail = redact(result)
                msg = f"claude: {detail}"
            if (attempt == 0 and settings.claude_transient_retries > 0
                    and _looks_transient(detail)):
                log.warning("上游瞬时故障，%.0fs 后重跑轻判断：%s",
                            _TRANSIENT_BASE_DELAY * 0.6, detail[:200])
                await asyncio.sleep(_TRANSIENT_BASE_DELAY * 0.6)
                continue
            raise RuntimeError(msg)
        raise AssertionError("unreachable")   # range(2) 的末轮必 return/raise，兜底防悄悄返回 None

    async def quick(self, prompt: str) -> str:
        """一次性纯文本判断，不带危险权限、不复用会话。"""
        return await self._oneshot(self._cmd(prompt, None, agentic=False))

    def _cmd_ro(self, prompt: str, system_prompt: str | None = None) -> list[str]:
        """只读 agentic 命令：plan 模式能读代码但不会改/提交，用于主动插话在仓库里查证。"""
        cmd = [settings.claude_bin, "-p", prompt, "--output-format", "json",
               "--permission-mode", "plan"] + _NO_MCP   # 只读查证同样不需要 MCP，别付启动税
        model = settings.claude_quick_model or settings.claude_model   # 只读查证也算轻判断
        if model:
            cmd += ["--model", model]
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
