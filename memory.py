"""项目长期记忆：跨会话、跨重启留存的「每项目」事实库。

会话有 TTL（默认 2h），超时 / 重启后多轮上下文会断；但"当初为什么这么设计、
项目里的约定、长期目标、踩过的坑"这类**值得跨周记住**的事实不该随会话蒸发——
这正是「被动单步、无长程记忆」这一短板的根。

本模块把这些事实以「一事一文件 + 一份索引」的形式落到
`store/memory/<项目>/`（在仓库工作树之外，不会被误 commit）。开新会话时把相关
内容重新注入系统提示，让远程工程师"记得住事"，即便会话早被 TTL 清掉。

设计刻意对齐 Claude Code 自身的文件式记忆：Claude 在 agentic 模式下能直接往这个
目录追加 / 订正 markdown，无需再造一套工具接口——augment_system_prompt() 注入的
说明就是在引导它这么做。后续的「主动性」「对结果负责」两块也都以这份持久状态为底座。
"""
import logging
import os
import re
import time

from config import settings

log = logging.getLogger("matrix-claude.memory")

# 只挡路径分隔符 / 控制字符（保留中文等 Unicode，文件名才能见名知意）
_BAD = re.compile(r"[/\\\x00-\x1f]+")
_WS = re.compile(r"\s+")
_DEFAULT_RECALL_BUDGET = 6000   # 注入系统提示的事实正文字符预算（MEMORY.md 索引始终全量注入）
_INDEX = "MEMORY.md"


def _root() -> str:
    # 跟随 settings.store_path：自检会把它指到临时目录，真实 store 不被污染
    return os.path.join(settings.store_path, "memory")


def _safe(seg: str, fallback: str = "_") -> str:
    """压成安全的扁平目录 / 文件名段：挡 ../ 穿越与路径分隔符、空白归一、去首尾点，
    但保留中文等 Unicode，让记忆文件名见名知意。"""
    seg = (seg or "").replace("..", "_")          # 先拆掉 .. 再清分隔符，挡路径穿越
    seg = _BAD.sub("_", seg)
    seg = _WS.sub("_", seg).strip("._")
    return (seg or fallback)[:120]


def proj_dir(project_id: str) -> str:
    """某项目的记忆目录：store/memory/<安全化的项目 id>/。"""
    return os.path.join(_root(), _safe(project_id, "_project"))


def ensure(project_id: str) -> str:
    """建好该项目的记忆目录与索引文件，返回目录路径。"""
    d = proj_dir(project_id)
    os.makedirs(d, exist_ok=True)
    idx = os.path.join(d, _INDEX)
    if not os.path.exists(idx):
        with open(idx, "w") as f:
            f.write(f"# 项目长期记忆：{project_id}\n\n"
                    "> 一行一条，指向同目录下的事实文件。新事实在文末追加。\n\n")
    return d


def _append_index(d: str, slug: str, desc: str) -> None:
    idx = os.path.join(d, _INDEX)
    try:
        existing = open(idx).read() if os.path.exists(idx) else ""
    except OSError:
        existing = ""
    if f"({slug}.md)" in existing:   # 已登记过：正文订正即可，索引不重复加
        return
    with open(idx, "a") as f:
        if not existing:
            f.write("# 项目长期记忆\n\n")
        f.write(f"- [{slug}]({slug}.md) — {desc}\n")


def remember(project_id: str, name: str, body: str,
             description: str = "", mtype: str = "project") -> str | None:
    """写入 / 覆盖一条事实（一文件）并在 MEMORY.md 登记索引。返回文件路径，失败返回 None。

    主要给程序化写入用（后续「对结果负责」「主动性」会用它落 PR 台账 / 待办）；
    日常 agentic 任务里 Claude 多半直接自己写文件，不必走这里。
    """
    try:
        d = ensure(project_id)
        slug = _safe(name, "fact")
        path = os.path.join(d, f"{slug}.md")
        fm = (f"---\nname: {slug}\ndescription: {description or name}\n"
              f"type: {mtype}\ncreated: {time.strftime('%Y-%m-%d')}\n---\n\n")
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            f.write(fm + body.rstrip() + "\n")
        os.replace(tmp, path)
        _append_index(d, slug, description or name)
        return path
    except OSError as e:
        log.warning("写记忆失败 %s/%s: %s", project_id, name, e)
        return None


def recall(project_id: str, budget: int | None = None) -> str:
    """把该项目的记忆汇成可注入文本；无记忆返回 ""。

    索引（MEMORY.md）始终全量带上（小、是地图）；事实正文按"最近修改优先"在
    字符预算内尽量带，超预算的剩下条数会标注出来，提示 Claude 按需去目录里读。
    """
    if budget is None:
        budget = getattr(settings, "memory_recall_budget", _DEFAULT_RECALL_BUDGET)
    d = proj_dir(project_id)
    if not os.path.isdir(d):
        return ""

    index = ""
    try:
        idx_path = os.path.join(d, _INDEX)
        if os.path.exists(idx_path):
            index = open(idx_path).read().strip()
    except OSError:
        pass

    try:
        names = [n for n in os.listdir(d) if n.endswith(".md") and n != _INDEX]
    except OSError:
        names = []
    names.sort(key=lambda n: os.path.getmtime(os.path.join(d, n)), reverse=True)

    facts, used, omitted = [], 0, 0
    for n in names:
        try:
            body = open(os.path.join(d, n)).read().strip()
        except OSError:
            continue
        if used + len(body) > budget:
            omitted += 1
            continue
        facts.append(body)
        used += len(body)

    if not index and not facts:
        return ""
    parts = []
    if index:
        parts.append(index)
    if facts:
        parts.append("\n\n".join(facts))
    if omitted:
        parts.append(f"（另有 {omitted} 条记忆未全文载入，需要时直接读上面目录里的文件。）")
    return "\n\n".join(parts)


def augment_system_prompt(system_prompt: str, project_id: str) -> str:
    """把项目长期记忆拼到系统提示之后（开新会话时注入一次）。任何异常都不影响主流程。"""
    if not getattr(settings, "memory_enabled", True):
        return system_prompt
    try:
        d = ensure(project_id)
        recalled = recall(project_id) or "（这个项目暂无长期记忆，按需开始积累。）"
    except OSError as e:
        log.warning("注入记忆失败 %s: %s", project_id, e)
        return system_prompt
    block = (
        "\n\n【项目长期记忆（跨会话 / 跨重启留存，不随会话 TTL 清空）】\n"
        f"你为这个项目维护的持久记忆在目录：{d}\n"
        "下面是开新会话时载入的相关记忆；要更多细节就直接读该目录里的文件。\n"
        "当你形成**值得跨周记住**的事实（关键决策 + 原因、项目约定 / 规范、长期目标、"
        "踩过的坑），就往该目录追加一条 markdown（frontmatter 至少含 name / description / "
        "type；正文写清「是什么 + 为什么」），并在 MEMORY.md 加一行索引；与已有记忆有出入"
        "就就地订正。**别把它 commit 进仓库**——这是工作树之外的工作记忆。\n"
        "————\n"
        f"{recalled}"
    )
    return system_prompt + block
