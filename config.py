"""集中配置：全部从环境变量 / .env 读取（各项说明见 .env.example）。"""
import os
from dotenv import load_dotenv

load_dotenv()


def _s(key, default=""):
    return (os.environ.get(key, default) or "").strip()


def _i(key, default):
    try:
        return int(_s(key, str(default)))
    except ValueError:
        return default


def _b(key, default=False):
    v = _s(key)
    if not v:                # 未设置 / 空值 / 纯空白 → 用默认
        return default
    return v.lower() in ("1", "true", "yes", "on")


def _list(key):
    return [x.strip() for x in _s(key).replace("，", ",").split(",") if x.strip()]


class Settings:
    # Matrix 账号
    homeserver   = _s("MATRIX_HOMESERVER", "https://matrix.org")
    user_id      = _s("MATRIX_USER_ID")
    password     = _s("MATRIX_PASSWORD")
    device_name  = _s("MATRIX_DEVICE_NAME", "claude-bot")
    store_path   = _s("MATRIX_STORE_PATH", "./store")
    creds_path   = _s("MATRIX_CREDS_PATH", "./store/credentials.json")
    enable_e2e   = _b("MATRIX_ENABLE_E2E", False)

    # 行为
    room_allowlist = _list("ROOM_ALLOWLIST")       # 只在这些房间工作；空=全部
    allow_users    = _list("ALLOW_USERS")          # 只响应这些人；空=所有人
    reply_in_dm    = _b("REPLY_IN_DM_ALWAYS", True)
    proactive      = _b("PROACTIVE", False)
    proactive_cooldown = _i("PROACTIVE_COOLDOWN", 120)
    # 判定"不插话"后占用的短冷却（秒）：太小会让活跃群里几乎每条疑问都起一次 Claude 判断
    proactive_pass_cooldown = _i("PROACTIVE_PASS_COOLDOWN", 60)
    trigger_phrase = _s("TRIGGER_PHRASE")
    context_lines  = _i("CONTEXT_LINES", 20)
    process_backlog = _b("PROCESS_BACKLOG", False)

    # Gitea / 项目路由
    gitea_token    = _s("GITEA_TOKEN")
    gitea_host     = _s("GITEA_HOST")              # 受信主机：token 只注入到这里
    projects_root  = _s("PROJECTS_ROOT", "./projects")
    bindings_path  = _s("BINDINGS_PATH", "./store/bindings.json")
    git_user_name  = _s("GIT_USER_NAME", "claude-bot")
    git_user_email = _s("GIT_USER_EMAIL", "claude-bot@localhost")
    git_timeout    = _i("GIT_TIMEOUT", 300)

    # Claude Code
    claude_bin     = _s("CLAUDE_BIN", "claude")
    claude_model   = _s("CLAUDE_MODEL")
    # 未绑定仓库时的兜底工作目录：与源码/凭证隔离的空目录
    claude_workdir = _s("CLAUDE_WORKDIR") or os.path.join(os.path.abspath(projects_root), "_scratch")
    claude_timeout = _i("CLAUDE_TIMEOUT", 600)
    quick_timeout  = _i("CLAUDE_QUICK_TIMEOUT", 60)   # quick() 轻量判断专用的短超时，别和 claude_timeout 混用
    claude_system_prompt = _s(
        "CLAUDE_SYSTEM_PROMPT",
        "你是通过 Matrix 接入的助手，会被派来干活（写代码、查问题、做方案等）。用简洁中文回复。",
    )
    claude_permission_mode = _s("CLAUDE_PERMISSION_MODE", "acceptEdits")
    claude_dangerous = _b("CLAUDE_DANGEROUS", True)
    claude_extra_args = _s("CLAUDE_EXTRA_ARGS")
    session_ttl    = _i("SESSION_TTL", 7200)
    max_concurrency = _i("MAX_CONCURRENCY", 2)

    # 项目长期记忆（跨会话 / 跨重启留存，补会话 TTL 之外的"长程记忆"短板）
    memory_enabled       = _b("MEMORY_ENABLED", True)
    memory_recall_budget = _i("MEMORY_RECALL_BUDGET", 6000)   # 注入系统提示的事实正文字符预算

    # PR 台账：bot 开了 PR 就跟到合并（轮询评审/CI，自动处理并回报到原房间）
    pr_followup_enabled  = _b("PR_FOLLOWUP_ENABLED", True)
    pr_followup_interval = _i("PR_FOLLOWUP_INTERVAL", 180)   # 轮询间隔（秒）
    pr_autofix_max       = _i("PR_AUTOFIX_MAX", 3)           # 每个 PR 自动改评审/CI 的次数上限，防反复失败空转

    # 主动性·自驱心跳：没人派活时也巡检各项目、主动找值得做的事
    proactive_heartbeat_enabled  = _b("PROACTIVE_HEARTBEAT_ENABLED", True)
    proactive_heartbeat_interval = _i("PROACTIVE_HEARTBEAT_INTERVAL", 3600)  # 巡检间隔（秒），别太密以免烧钱/打扰
    # 0=只读巡检 + 提议（默认，安全）；1=autopilot：直接认领、开 PR（无人值守自主行动，慎开）
    proactive_autopilot          = _b("PROACTIVE_AUTOPILOT", False)

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
