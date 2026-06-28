# Matrix × Claude Code 机器人

在 Matrix 房间里监听消息，被 @ / 私聊 / 命中触发词时（可选：不被 @ 也主动）调用**本机 Claude Code** 干活，并把结果发回房间。

为什么用 Matrix：能看到所在房间的**全部消息**、能**随时主动发消息**、可以**直接用你自己的账号**登录 —— 这三点企业微信官方 API 做不到。

## 能力

- **被 @ / 单聊 / 触发词** → 调用 Claude Code 干活后回复
- **PROACTIVE 模式**（默认关）→ 不被 @ 时，对"像在求助"的消息先让 Claude 判断该不该插话，带冷却防刷屏；群已绑仓库时用**只读**方式看着真实代码作答（不会改代码/开 PR）
- **图片 / 文件 / 多媒体** → 自动下载（加密房间会解密）存到本地并在上下文标注；被 @ 时把文件路径交给 Claude 读取/分析
- 每个**房间 × 项目**维护**多轮上下文**（Claude Code 会话，不同房间/私聊互不串台），发 `重置` / `/reset` 清空
- **项目长期记忆**（跨会话/跨重启留存）→ 会话有 TTL，但「当初为什么这么设计、项目约定、长期目标、踩过的坑」这类**值得跨周记住**的事实落在 `store/memory/<项目>/`（一事一文件 + 索引，工作树之外不会被误 commit）；开新会话时按预算注入系统提示，会话被 TTL 清掉也"记得住事"。补「无长程记忆」短板的第一步（详见 `memory.py`）
- **PR 台账（对结果负责）**→ bot 开了 PR 不撒手：后台轮询该 PR，收到**评审意见**或 **CI 失败**就自动在原分支上处理并推送、把进展回报到当初派活的房间，**合并/关闭才销账**（带自动处理次数上限防空转）。补「fire-and-forget、不跟进」短板（详见 `pr_ledger.py` / `gitea.py`）
- **白名单**（房间 / 用户）、登录态与 E2EE 密钥**持久化**；Claude 会话 id（`store/sessions.json`）与房间→项目路由（`store/last_projects.json`）也落盘，重启后多轮上下文与 `/reset` 仍可用（群聊背景对话与"回复了 bot"识别不持久，重启后清空）

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
| `ALLOW_USERS` | 只让这些人能派活（会跑命令/改文件，**强烈建议设**） |
| `ROOM_ALLOWLIST` | 只在指定房间工作 |
| `PROACTIVE` | `1` 开启主动插话（实验性） |

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

- **访问控制**：不设 `ALLOW_USERS` 时默认任何人都能驱动 Claude；要收紧就设白名单（同时也限制谁能邀请它进房）。设了白名单后，非授权用户的消息不进上下文，避免被借"最近对话"做间接 prompt 注入。
- **token 隔离**：`GITEA_TOKEN` 只注入到 `GITEA_HOST` 的 clone/push 地址，其他主机一律不带。**fail-closed**：不设 `GITEA_HOST` 就完全不识别任何仓库（连 clone 都不做）。
- **工作目录隔离**：兜底目录是 `PROJECTS_ROOT/_scratch`，别把 `CLAUDE_WORKDIR` 指到含 `.env` / `store/` 的目录。
- **出口脱敏**：外发文本自动抹掉 `GITEA_TOKEN`、登录密码与 Matrix access token（兜底，非主防线）。
- **媒体**：只下载授权用户、白名单房间里的文件，单文件受 `MEDIA_MAX_MB` 限制、每房间按 `MEDIA_KEEP` 滚动删旧；文件名经消毒（挡 `../` 穿越）。存到 `PROJECTS_ROOT/_media`（已 gitignore），不放进仓库工作树以免被误 commit。Claude 用绝对路径读取，`CLAUDE_DANGEROUS=0` 时可能读不到 cwd 之外的文件。

> **DM 默认开放（有意为之）**：不设 `ALLOW_USERS` 时陌生人也能私聊驱动 Claude。因 bot 持有 `GITEA_TOKEN`，等于让它能 clone 并读走 token 可访问的任意私有库。多人 / 公开 / 联邦可达的部署务必设 `ALLOW_USERS`。
>
> `--dangerously-skip-permissions` 是有意保留的（让 Claude 真正干活），风险靠"快照可回滚的环境 + 上述访问控制 + 凭证隔离"兜底。

## 文件

- `bot.py` — Matrix 客户端：登录/持久化、监听消息、触发判断、调 Claude、回发
- `claude_runner.py` — 调 `claude -p`：`ask()` 带工具干活+多轮会话，`quick()` 一次性判断
- `memory.py` — 项目长期记忆：跨会话/跨重启留存的事实库，开新会话时注入系统提示
- `projects.py` — 房间↔Gitea 仓库的解析/绑定/clone 与路由
- `config.py` — 读取 `.env` 的全部开关
- `tests/smoke.py` — 离线冒烟自检
