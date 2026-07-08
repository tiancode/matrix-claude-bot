"""集中配置：全部从环境变量 / .env 读取（各项说明见 .env.example）。"""
import os
from datetime import timedelta, timezone

from dotenv import load_dotenv

load_dotenv()


def fixed_tz(hours: int) -> timezone:
    """固定 UTC 偏移的 tzinfo，不依赖服务器本地时区，跨机器/跨部署一致
    （日期分桶、巡检时段等"按部署时区判日夜/周几"的场景统一走这个）。"""
    return timezone(timedelta(hours=hours))


def _s(key, default=""):
    return (os.environ.get(key, default) or "").strip()


def _i(key, default):
    try:
        return int(_s(key, str(default)))
    except ValueError:
        return default


def _f(key, default):
    try:
        return float(_s(key, str(default)))
    except ValueError:
        return default


def _b(key, default=False):
    v = _s(key)
    if not v:                # 未设置 / 空值 / 纯空白 → 用默认
        return default
    return v.lower() in ("1", "true", "yes", "on")


def _csv(key):
    """逗号分隔列表：去空白、去空项。未设置 → 空元组。"""
    return tuple(x.strip() for x in _s(key).split(",") if x.strip())


def _parse_known_bots(entries):
    """KNOWN_BOTS 条目 → (完整 MXID 集, localpart 集)。带 : 的当完整 MXID，漏写 @ 自动补——
    localpart 简写不用 @，操作者很容易在完整形式上也漏掉它，而 sender 恒以 @ 开头，
    不补的话这条名单项会静默失效（bot 照旧互答，正是这配置要防的事故）。"""
    full = frozenset((b if b.startswith("@") else "@" + b) for b in entries if ":" in b)
    local = frozenset(b.lstrip("@") for b in entries if ":" not in b)
    return full, local


class Settings:
    # Matrix 账号
    homeserver   = _s("MATRIX_HOMESERVER", "https://matrix.org")
    user_id      = _s("MATRIX_USER_ID")
    password     = _s("MATRIX_PASSWORD")
    device_name  = _s("MATRIX_DEVICE_NAME", "claude-bot")
    store_path   = _s("MATRIX_STORE_PATH", "./store")
    creds_path   = _s("MATRIX_CREDS_PATH", "./store/credentials.json")
    enable_e2e   = _b("MATRIX_ENABLE_E2E", False)
    # 自建 / 自签名证书 homeserver 的 TLS：默认走系统 CA 校验（公网 homeserver 不用动）。
    # 自签名证书两种接法：① MATRIX_CA_CERT 指向服务器证书/CA（推荐：仍做校验、仍防 MITM，
    # 前提是证书 SAN 覆盖你连的主机名/IP）；② MATRIX_SSL_VERIFY=0 直接关校验（最省事但失去
    # MITM 防护，仅用于可信内网/自有机器）。两者都设时以 CA 校验为准。
    matrix_ssl_verify = _b("MATRIX_SSL_VERIFY", True)
    matrix_ca_cert    = _s("MATRIX_CA_CERT")

    # 行为
    reply_in_dm    = _b("REPLY_IN_DM_ALWAYS", True)
    proactive      = _b("PROACTIVE", True)
    proactive_cooldown = _i("PROACTIVE_COOLDOWN", 120)
    # 判定"不插话"后占用的短冷却（秒）：太小会让活跃群里几乎每条疑问都起一次 Claude 判断
    proactive_pass_cooldown = _i("PROACTIVE_PASS_COOLDOWN", 60)
    # 主动插话是否仍需"像求助"的关键词预筛。True=只评估含求助/报错词的消息（省判断调用）；
    # False=对群里每条消息都让 Claude 判断该不该插话——能抓到"没人求助但话里有错"的情形
    # （同事聊错了你纠正），代价是判断调用更多，靠冷却 + 强 __PASS__ 倾向兜底防刷屏。默认 False。
    proactive_require_hint = _b("PROACTIVE_REQUIRE_HINT", False)
    # 群里"对话延续窗口"（秒）：你 @ 过 bot 后，这段时间内你的后续消息**免重复 @** 也当成续话
    # 直接接着干（多轮里不用每句都点名）。@了别人、或窗口内有第三人插话则不算续话。0=关。默认 180。
    group_followup_window = _i("GROUP_FOLLOWUP_WINDOW", 180)
    # 续话"软窗口"（秒）：超过上面的硬窗口后，你（近期和 bot 聊过的人）再发没点名的消息，在这段更长
    # 的时限内**仍逐条过语义闸**——结合最近对话（含中途旁人的话、时间间隔）判断"是不是还在跟我说"，
    # 而不是被 180s 硬时间一刀切掉真的续话（如"讲个故事"后隔几十分钟又说"再讲一个"）。硬窗口靠"有没有
    # 旁人插话"廉价预筛省判断；软窗口不预筛、全交给语义闸定夺，故只在 followup_semantic_gate=True 时生效。
    # 0=关（只保留硬窗口的老行为）。默认 1800（30 分钟）。
    followup_semantic_window = _i("GROUP_FOLLOWUP_SEMANTIC_WINDOW", 1800)
    # 续话窗口命中属"弱信号"（没点名、只是刚聊过又接着说）：再过一道轻量语义闸，确认这条确实在
    # 接着跟 bot 说、而不是转头跟旁人说/自言自语，堵"没话找话"的误触发。用 CLAUDE_QUICK_MODEL
    # 小模型判、拿不准仍当在跟你说（宁可接着聊）。0=关（回到纯规则续话）。默认开。
    followup_semantic_gate = _b("GROUP_FOLLOWUP_SEMANTIC_GATE", True)
    # 群答复的组织方式。默认 0：答复直接发在主时间线（bot 的记忆按房间拍平，视觉也不 fork），
    # 发送那一刻群里已插进别的消息时自动改用「引用回复」指明在回哪条；用户自己在线程里说话则跟进该线程。
    # 1=旧式：群里每条顶层消息的答复都挂进它的线程(m.thread)——多话题强分流，但多轮会碎成一堆小线程。
    reply_in_thread = _b("REPLY_IN_THREAD", False)
    # 流式：发占位消息后随 Claude 产出**边生成边编辑**同一条消息，长任务不再全程静默。默认开。
    stream_replies = _b("STREAM_REPLIES", True)
    # 允许把【工作目录内】的文件作为附件回传（Claude 在回复里写 [[send-file: 路径]] 标记触发）。默认开。
    send_files_back = _b("SEND_FILES_BACK", True)
    # /summarize 不带参数时默认回看多少条对话。
    summary_lines = _i("SUMMARY_LINES", 60)
    # 同一个人连发的消息在这个窗口（秒）内合并成一个任务再派——一句话分几条打是聊天里的常态，
    # 排两个任务不如并成一个。每来一条窗口顺延，最多总等 6s；代价是每个任务开跑晚这么点。
    # 0=关（逐条即派）。默认 1.5。
    message_debounce = _f("MESSAGE_DEBOUNCE", 1.5)
    # 任务进行中收到的新点名不再排队，而是把消息直接递进正在跑的回合（像 Claude Code 运行中
    # 打字），打 📎 回执。仅常驻进程模式（CLAUDE_PERSISTENT=1）生效。默认开。
    steer_enabled = _b("STEER_WHILE_RUNNING", True)
    trigger_phrase = _s("TRIGGER_PHRASE")
    context_lines  = _i("CONTEXT_LINES", 20)
    process_backlog = _b("PROCESS_BACKLOG", False)
    # 已知机器人名单：逗号分隔。这些 sender 的消息照常进上下文/逐字记录（真人接手时背景完整），
    # 但 bot 对它们【绝不应答】——点名/回复/元命令/续话/主动插话一概不认，防两个 bot 互相
    # 应答陷入死循环。带 : 的条目按完整 MXID 精确匹配（@weather:example.com，漏写 @ 会自动补）；
    # 不带 : 的按 localpart 匹配、不限 homeserver（weather 命中 @weather:任何服务器）。
    # 留空=没有已知机器人。
    known_bots_full, known_bots_local = _parse_known_bots(_csv("KNOWN_BOTS"))

    # Gitea / 项目路由
    gitea_token    = _s("GITEA_TOKEN")
    gitea_host     = _s("GITEA_HOST")              # 受信主机：token 只注入到这里
    projects_root  = _s("PROJECTS_ROOT", "./projects")
    bindings_path  = _s("BINDINGS_PATH", "./store/bindings.json")
    git_user_name  = _s("GIT_USER_NAME", "claude-bot")
    git_user_email = _s("GIT_USER_EMAIL", "claude-bot@localhost")
    git_timeout    = _i("GIT_TIMEOUT", 300)
    # /new-project：新建仓库默认可见性（GITEA_TOKEN 对应账号下）。默认公开；要默认私有可设为 1。
    gitea_new_repo_private = _b("GITEA_NEW_REPO_PRIVATE", False)

    # Claude Code
    claude_bin     = _s("CLAUDE_BIN", "claude")
    claude_model   = _s("CLAUDE_MODEL")
    # 轻判断（quick 分诊/插话判定 + consult 只读查证）单独的模型；留空=跟随 CLAUDE_MODEL。
    # 这类调用群里几乎每条消息一次，用小模型（如 haiku）省钱且更快；干活的 ask() 不受影响。
    claude_quick_model = _s("CLAUDE_QUICK_MODEL")
    # 未绑定仓库时的兜底工作目录：与源码/凭证隔离的空目录
    claude_workdir = _s("CLAUDE_WORKDIR") or os.path.join(os.path.abspath(projects_root), "_scratch")
    # agentic 干活的超时（秒）。流式(默认)下是【空闲】超时：只要 Claude 还在持续产出就不限总时长，
    # 仅连续 CLAUDE_TIMEOUT 秒无任何输出才判为卡死；非流式回退路径下退化为整体超时。
    claude_timeout = _i("CLAUDE_TIMEOUT", 600)
    quick_timeout  = _i("CLAUDE_QUICK_TIMEOUT", 300)  # quick()/consult()/摘要 等一次性判断的超时，别和 claude_timeout 混用
    # 上游瞬时故障（API Error: Overloaded / 限流 / 5xx）自动重试次数（退避 5s/15s/45s）。
    # 结果型错误会 --resume 原会话续跑、不重做已完成的工作；轻判断（quick/consult）固定最多重试
    # 1 次、短退避，不受此值影响（0 时也一并关掉）。注意退避睡眠发生在项目串行锁内（续跑必须
    # 守住同一 checkout），调大次数会按 5·3^n 秒拉长同项目排队任务的空等。0=关。默认 2。
    claude_transient_retries = _i("CLAUDE_TRANSIENT_RETRIES", 2)
    claude_system_prompt = _s(
        "CLAUDE_SYSTEM_PROMPT",
        "你是通过 Matrix 接入的助手，会被派来干活（写代码、查问题、做方案等）。用简洁中文回复。",
    )
    claude_permission_mode = _s("CLAUDE_PERMISSION_MODE", "acceptEdits")
    claude_dangerous = _b("CLAUDE_DANGEROUS", True)
    claude_extra_args = _s("CLAUDE_EXTRA_ARGS")
    # agentic 会话用常驻进程（--input-format stream-json）：回合结束进程不退出，等下一条消息。
    # 这样 Claude 在回合里启动的后台任务（子代理/后台命令）跨轮次存活，完成后 CLI 自动续跑，
    # 产出经 on_notify 回投房间。关掉则回到每消息一次性进程（后台任务随回合死）。
    claude_persistent = _b("CLAUDE_PERSISTENT", True)
    # 常驻进程空闲回收（秒）：超过此时长没有任何活动（无回合、无后台产出）就杀掉释放内存；
    # 下一条消息会用落盘的 session_id 照常 --resume，对话上下文不丢，只是后台任务不再跨过回收点。
    persistent_idle = _i("CLAUDE_PERSISTENT_IDLE", 7200)
    session_ttl    = _i("SESSION_TTL", 86400)   # 多轮上下文空闲过期：默认 24 小时
    max_concurrency = _i("MAX_CONCURRENCY", 2)

    # 聊天逐字记录：按房间把对话明文落盘（store/transcripts/<房间>.jsonl），让 Claude 能回溯
    # 更早的对话（"前天我们聊了什么"）。补会话 TTL / 背景缓冲都够不着的"远期对话"短板。
    # 默认开（会持久留存对话明文，仅授权用户、store/ 0700、受保留天数约束）。
    transcript_enabled       = _b("TRANSCRIPT_ENABLED", True)
    transcript_keep_days     = _i("TRANSCRIPT_KEEP_DAYS", 30)     # 保留天数，超期滚动删旧
    transcript_max_lines     = _i("TRANSCRIPT_MAX_LINES", 5000)   # 每房间行数硬上限，兜底防膨胀
    transcript_backfill_days = _i("TRANSCRIPT_BACKFILL_DAYS", 30) # 回灌（从 Matrix 拉历史）默认往回多少天

    # 聊天日摘要 + 主题索引：在原始逐字日志之上再叠一层可 KB 级检索的漏斗（store/transcripts/
    # digests/<房间>/ 下），被问"昨天中午聊过什么 / 以前讨论过某某"时先按天定位摘要、按需回原文，
    # 不必整段读原始日志。攒够量 / 跨天残留就后台用 CLAUDE_QUICK_MODEL 把当天对话压成话题摘要。
    digest_enabled      = _b("DIGEST_ENABLED", True)
    digest_min_lines    = _i("DIGEST_MIN_LINES", 30)      # 水位线后攒够这么多新行才触发（路径 a）
    digest_min_interval = _i("DIGEST_MIN_INTERVAL", 7200) # 同房间两次摘要的最小间隔（秒），别太密
    digest_keep_days    = _i("DIGEST_KEEP_DAYS", 180)     # 摘要保留天数，超期删日文件 + 同步删索引行
    digest_tz_hours     = _i("DIGEST_TZ_HOURS", 8)        # 日期分桶 / HH:MM 显示用的固定时区偏移（东八区）

    # 项目长期记忆（跨会话 / 跨重启留存，补会话 TTL 之外的"长程记忆"短板）
    memory_enabled       = _b("MEMORY_ENABLED", True)
    memory_recall_budget = _i("MEMORY_RECALL_BUDGET", 6000)   # 注入系统提示的事实正文字符预算

    # PR 台账：bot 开了 PR 就跟到合并（轮询评审/CI，自动处理并回报到原房间）
    pr_followup_enabled  = _b("PR_FOLLOWUP_ENABLED", True)
    pr_followup_interval = _i("PR_FOLLOWUP_INTERVAL", 180)   # 轮询间隔（秒）
    pr_autofix_max       = _i("PR_AUTOFIX_MAX", 3)           # 每个 PR 自动改评审/CI 的次数上限，防反复失败空转

    # PR 自动合并：followup 巡检到 PR 可合并 + CI 通过（或无 CI）+ 无未决"请求改动" → 直接调 Gitea API 合并。
    # 这是机械闸（不经 Claude 评审）：移除最后一道人工合并闸，安全性改由 CI（若配了）+ 快照环境兜底。
    pr_automerge               = _b("PR_AUTOMERGE", True)
    pr_merge_method            = _s("PR_MERGE_METHOD", "merge")   # merge / squash / rebase（须为仓库允许的方式）
    pr_automerge_delete_branch = _b("PR_AUTOMERGE_DELETE_BRANCH", True)  # 合并后删源分支 claude/xxx

    # Gitea 工单接活：把 issue 指派给 bot 的 Gitea 账号 = 派活，不进 Matrix 也能下任务。
    # 轮询各已知项目 assigned 给 bot 的 open issue：认领（issue 下评论）→ 干活开 PR
    # （body 带 Closes #N，合并即自动关单）→ PR 进台账盯到合并 → 进展回报到项目房间。
    issue_intake_enabled = _b("ISSUE_INTAKE", True)
    issue_poll_interval  = _i("ISSUE_POLL_INTERVAL", 300)   # 轮询间隔（秒）

    # 主动性·自驱心跳：没人派活时也巡检各项目、主动找值得做的事
    proactive_heartbeat_enabled  = _b("PROACTIVE_HEARTBEAT_ENABLED", True)
    proactive_heartbeat_interval = _i("PROACTIVE_HEARTBEAT_INTERVAL", 3600)  # 巡检间隔（秒），别太密以免烧钱/打扰
    # 0=只读巡检 + 提议（安全）；1=autopilot：直接认领、开 PR（无人值守自主行动）。默认开
    proactive_autopilot          = _b("PROACTIVE_AUTOPILOT", True)
    # 自驱心跳的巡检时段：只在工作日 + 白天巡检，别半夜/周末打扰人、也省着烧钱
    proactive_heartbeat_weekdays_only = _b("PROACTIVE_HEARTBEAT_WEEKDAYS_ONLY", True)  # 只周一~周五
    proactive_heartbeat_start_hour    = _i("PROACTIVE_HEARTBEAT_START_HOUR", 9)        # 巡检时段起（含，当地时）
    proactive_heartbeat_end_hour      = _i("PROACTIVE_HEARTBEAT_END_HOUR", 19)         # 巡检时段止（不含）
    proactive_heartbeat_tz_hours      = _i("PROACTIVE_HEARTBEAT_TZ_HOURS", 8)          # 时段判断用的时区偏移（默认东八区）

    # 媒体（图片 / 文件 / 音视频）
    media_enabled  = _b("MEDIA_ENABLED", True)
    media_root     = _s("MEDIA_ROOT") or os.path.join(os.path.abspath(projects_root), "_media")
    media_max_mb   = _i("MEDIA_MAX_MB", 25)        # 单文件下载上限
    media_keep     = _i("MEDIA_KEEP", 50)          # 每房间最多保留几个文件，超出删旧


settings = Settings()


# 运行期才拿得到的凭证（如 Matrix access_token），登录后登记进来一并 redact。
_extra_secrets: set[str] = set()


def register_secret(value: str | None) -> None:
    if value and len(value) >= 6:   # 太短的串当 secret 易误伤正常文本
        _extra_secrets.add(value)


def redact(text: str) -> str:
    """抹掉外发文本里的凭证（精确子串替换，编码/拆分后仍可能漏）。"""
    if not text:
        return text
    for sec in (settings.gitea_token, settings.password, *_extra_secrets):
        if sec:
            text = text.replace(sec, "***")
    return text
