# Matrix × Claude Code 机器人

在 Matrix 房间里监听消息，被 @ / 私聊 / 命中触发词时（可选：不被 @ 也主动）调用**本机 Claude Code** 干活，并把结果发回房间。

为什么用 Matrix：能看到所在房间的**全部消息**、能**随时主动发消息**、可以**直接用你自己的账号**登录 —— 这三点企业微信官方 API 做不到。

## 能力

- **被 @ / 单聊 / 触发词** → 调用 Claude Code 干活后回复
- **PROACTIVE 模式**（默认开）→ 不被 @ 时也对群消息先让 Claude 判断该不该插话——既包括**有人求助**，也包括**对话里出现明显错误/有风险的做法值得纠正**（同事聊错了你能开口指正），带冷却 + 强 `__PASS__` 倾向防刷屏；群已绑仓库时用**只读**方式看着真实代码作答（不会改代码/开 PR）。默认 `PROACTIVE_REQUIRE_HINT=0`：对每条群消息都判断（抓"没人求助但话里有错"）；设 =1 则只看含求助/报错词的消息（更省判断调用）。要收敛打扰：调大 `PROACTIVE_COOLDOWN`，或 `PROACTIVE=0` 关掉
- **图片 / 文件 / 多媒体** → 自动下载（加密房间会解密）存到本地并在上下文标注；被 @ 时把文件路径交给 Claude 读取/分析
- 每个**房间 × 项目**维护**多轮上下文**（Claude Code 会话，不同房间/私聊互不串台），发 `重置` / `/reset` 清空
- **项目长期记忆**（跨会话/跨重启留存）→ 会话有 TTL，但「当初为什么这么设计、项目约定、长期目标、踩过的坑」这类**值得跨周记住**的事实落在 `store/memory/<项目>/`（一事一文件 + 索引，工作树之外不会被误 commit）；开新会话时按预算注入系统提示，会话被 TTL 清掉也"记得住事"。补「无长程记忆」短板的第一步（详见 `memory.py`）
- **PR 台账（对结果负责）**→ bot 开了 PR 不撒手：后台轮询该 PR，收到**评审意见**或 **CI 失败**就自动在原分支上处理并推送、把进展回报到当初派活的房间，**合并/关闭才销账**（带自动处理次数上限防空转）。**PR 自动合并默认开**（`PR_AUTOMERGE=1`）：PR 可合并 + CI 通过（或没配 CI）+ 无未决"请求改动" → 直接调 Gitea API 合并、销账、回报，无需人工点合并（机械闸、不经 Claude 评审，安全性靠 CI＋快照环境）。补「fire-and-forget、不跟进」短板（详见 `pr_ledger.py` / `gitea.py`）
- **主动性·自驱心跳**→ 没人派活时也按 `PROACTIVE_HEARTBEAT_INTERVAL` 巡检各项目（只读），挑一件值得主动推进的事（陈旧 PR、TODO/FIXME、缺测试、小 bug）。**默认 autopilot**（`PROACTIVE_AUTOPILOT=1`）：自己认领、开 PR（开的 PR 自动进台账→被 PR 跟进盯到合并）；设 `PROACTIVE_AUTOPILOT=0` 则只提议到项目房间等你点头（更安全、不自动改）。补「永远被动、无自驱」短板
- **聊天历史回溯**（`TRANSCRIPT_ENABLED`，默认开）→ 会话有 24h 滑动 TTL、背景缓冲只有最近十几条且重启即清，都答不了「前天我们聊了什么」。开启后按房间把对话**明文**落到 `store/transcripts/<房间>.jsonl`（一行一条 + 保留天数/行数上限滚动删旧），派活时把日志**路径**注入系统提示，让 Claude 被问到更早对话时**自己去读/grep**（与项目记忆同一套"告诉它存哪、按需取"的玩法，不把整段历史塞进每次 prompt）。`/backfill [天数]` 可从 Matrix 时间线回灌开启前的历史；首次启用还会对各房间自动回灌一次。E2EE 下落盘的是收到时的明文，回溯可靠
- **多轮·对话延续**→ 单聊每条必回；群里被 @ / 回复 bot / 命中触发词触发，且 `GROUP_FOLLOWUP_WINDOW` 秒内你的后续消息**免重复 @**也接着处理（@ 别人则不算，避免插话到你和别人的对话）
- 登录态与 E2EE 密钥**持久化**；Claude 会话 id（`store/sessions.json`）与房间→项目路由（`store/last_projects.json`）也落盘，重启后多轮上下文与 `/reset` 仍可用（群聊背景对话、"回复了 bot"识别、对话延续窗口不持久，重启后清空）

## 任务分配：一个项目 = 一个 Claude Code worker

**一个仓库对应一个"远程工程师"** —— 群和私聊都只是入口，同一仓库的活共用同一份本地 checkout（按仓库串行，避免两个房间并发改同一工作树）。但**多轮会话按「房间 × 项目」隔离**：不同群、不同私聊用户即便都在聊同一个仓库，各自的对话上下文互不串台。每次派活前会把工作树拉回干净的 `origin/base`（丢弃上个任务的脏改动/残留分支），不让改动串进下一个 PR。

- **群 ↔ 项目（固定绑定）**：未绑定的群里发 Gitea 仓库地址（纯链接，或 `/bind <url>`，或 `@bot <url> 顺带派个活`）→ 用 `GITEA_TOKEN` clone 到 `PROJECTS_ROOT/<host>/<owner>/<repo>`，记录到 `store/bindings.json`（重启保留）。之后该群的活都归这个项目；已绑后再发裸地址不会自动换绑，需用 `/bind` 显式换。
- **私聊（DM）↔ 自动分诊**：DM 不绑死仓库，按内容判断归属：①带仓库链接→直接路由；②文本含 `owner/repo` 全名→路由；③用轻量 `claude`（带最近对话）对照已知项目清单分诊；④仅裸仓库名命中→兜底路由；⑤都没命中→沿用这个 DM 上次的项目（多轮对话不必每条都点名）；⑥仍不确定→反问；无任何已知项目→让对方发地址。命中后都会按需登记并校验/修复本地 checkout。
- **派活后**：改代码 → 从 `base` 建分支 `claude/xxx` → commit → push → 调 Gitea API 开 PR → **PR 链接**发回；纯问答直接回答；信息不足直接反问（接同一会话）。
- **并发**：不同项目并行（受 `MAX_CONCURRENCY`），同一项目任务串行。

"已知项目清单"零配置 —— 由被绑定 / 被路由过的仓库自动汇总。需要 `GITEA_TOKEN`（clone 私有库 + 开 PR）；本地 `origin` 内嵌该 token 以便 push（明文存于 `.git/config`，自托管可接受）。

## 前置条件

1. 本机已安装并登录 **Claude Code**（`claude` 命令可用）。机器人以**运行它的系统用户**身份调用。
2. 一个 Matrix 账号：用**你自己的账号**（以"你本人"收发）或**专用 bot 账号**（机器人身份）。
3. （可选，建议）E2EE：装系统 `libolm`。Arch/CachyOS：`sudo pacman -S libolm`

## 安装

```bash
cd matrix-claude-bot
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# E2EE 额外：sudo pacman -S libolm && .venv/bin/pip install "matrix-nio[e2e]"
cp .env.example .env        # 然后填好账号
```

## 配置（.env 关键项）

| 项 | 说明 |
|---|---|
| `MATRIX_HOMESERVER` / `MATRIX_USER_ID` / `MATRIX_PASSWORD` | 账号；首次登录后可删掉密码 |
| `MATRIX_ENABLE_E2E` | 加密房间需要；需先装 libolm + `matrix-nio[e2e]` |
| `GITEA_HOST` | 受信任的 Gitea 根地址；**token 只注入到该主机**，建议必填 |
| `CLAUDE_DANGEROUS` | `1`=无人值守全自动（跳过工具授权）；`0` 更安全 |
| `PROACTIVE` | 主动插话：群里没 @ 也会判断该不该插话/纠错（**默认开**）；`0` 关 |
| `GROUP_FOLLOWUP_WINDOW` | 群里 @ 过 bot 后，这段秒数内的后续消息免重复 @ 也接着处理；`0` 关 |

## 运行

```bash
./run.sh           # 或： .venv/bin/python bot.py
```

首次用密码登录并把 token 存到 `store/credentials.json`（权限 600），之后用 token 复用同一设备。

### 开机自启（systemd user，按你自己的用户跑，保证能用到 claude 的登录态）

```ini
# ~/.config/systemd/user/matrix-claude.service
[Unit]
Description=Matrix x Claude Code bot
After=network-online.target

[Service]
WorkingDirectory=%h/Projects/matrix-claude-bot
ExecStart=%h/Projects/matrix-claude-bot/.venv/bin/python bot.py
Restart=on-failure

[Install]
WantedBy=default.target
```
```bash
systemctl --user enable --now matrix-claude.service
journalctl --user -u matrix-claude -f
```

## 自检（冒烟测试）

不连真实 Matrix，用假 client 跑通「启动 → 收消息 → 派活 → 回复」整条链路，校验上下文/回复识别/会话/安全等行为：

```bash
.venv/bin/python tests/smoke.py
```

## E2EE 说明

- 开启后机器人是一个**未验证的新设备**。要解密别人发的加密消息，通常需在 Element 里给它**共享密钥/验证**一次；发消息用了 `ignore_unverified_devices=True`，不会因对方设备未验证而卡住。
- 没装 libolm 时即使 `MATRIX_ENABLE_E2E=1` 也会降级为明文模式，日志会提示。

## 安全提醒

机器人会在你的服务器上**运行命令、读写文件**（`CLAUDE_DANGEROUS=1` 时无需逐次授权）。要点：

- **无访问控制（有意为之）**：本 bot 不做用户/房间白名单——任何能给它发消息的人都能驱动 Claude（跑命令、读写文件、用 `GITEA_TOKEN` clone/读私有库），谁邀请都进房。只适用于"所有可达用户都可信"的环境：私有 / 仅 LAN 可达 / 关了注册的 homeserver。**若你的 homeserver 开放注册或可被外部/联邦访问，等于把服务器和 token 能读到的私有库交给任何能连上的人**——这种情况别这么部署。
- **token 隔离**：`GITEA_TOKEN` 只注入到 `GITEA_HOST` 的 clone/push 地址，其他主机一律不带。**fail-closed**：不设 `GITEA_HOST` 就完全不识别任何仓库（连 clone 都不做）。
- **工作目录隔离**：兜底目录是 `PROJECTS_ROOT/_scratch`，别把 `CLAUDE_WORKDIR` 指到含 `.env` / `store/` 的目录。
- **出口脱敏**：外发文本自动抹掉 `GITEA_TOKEN`、登录密码与 Matrix access token（兜底，非主防线）。
- **媒体**：下载收到的文件（不再按用户/房间过滤），单文件受 `MEDIA_MAX_MB` 限制、每房间按 `MEDIA_KEEP` 滚动删旧；文件名经消毒（挡 `../` 穿越）。存到 `PROJECTS_ROOT/_media`（已 gitignore），不放进仓库工作树以免被误 commit。Claude 用绝对路径读取，`CLAUDE_DANGEROUS=0` 时可能读不到 cwd 之外的文件。
- **默认偏"积极自主"**：开箱默认 `PROACTIVE=1`（群里没 @ 也会判断要不要插话/纠错）、`PROACTIVE_AUTOPILOT=1`（巡检到事自己开 PR）、`PR_AUTOMERGE=1`（PR 满足条件自动合并）、`TRANSCRIPT_ENABLED=1`（对话明文落盘）。**合起来就是"巡检→开 PR→自动合并到 main"的全自动闭环**，且无访问控制、无审批。想要保守：把对应项设 `0`。

> **把"谁能用"放到 homeserver 这层**：bot 自己不再挡人，所以谁能驱动它 = 谁能在你的 Synapse 上给它发消息。请在 homeserver 侧收紧（**关闭开放注册、别开放联邦**），而不是指望 bot 拦。
>
> `--dangerously-skip-permissions` 是有意保留的（让 Claude 真正干活），风险靠"快照可回滚的环境 + 可信用户 + 凭证隔离"兜底。

## 文件

- `bot.py` — Matrix 客户端：登录/持久化、监听消息、触发判断、调 Claude、回发；PR 跟进与自驱心跳两个后台循环
- `claude_runner.py` — 调 `claude -p`：`ask()` 带工具干活+多轮会话，`quick()` 一次性判断，`consult()` 只读查证
- `memory.py` — 项目长期记忆：跨会话/跨重启留存的事实库，开新会话时注入系统提示
- `transcript.py` — 按房间的聊天逐字记录：落盘 + 回溯更早对话 + 从 Matrix 回灌历史
- `pr_ledger.py` — PR 台账：bot 开过的 PR 记账，跟到合并/关闭才销账（持久化到 `store/pr_ledger.json`）
- `gitea.py` — Gitea REST 只读小客户端：bot 自己轮询 PR 状态 / 评审 / CI
- `projects.py` — 房间↔Gitea 仓库的解析/绑定/clone 与路由
- `config.py` — 读取 `.env` 的全部开关
- `tests/smoke.py` — 离线冒烟自检
