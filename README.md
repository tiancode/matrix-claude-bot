# Matrix × Claude Code：给自己雇一名不下班的工程师

**在 Matrix 群里 @它派活，或在 Gitea 上把 issue 指派给它——它自己写代码、开 PR、盯 CI、改评审意见、自动合并、随时汇报。整条链路无人值守，跑在你自己的机器上。**

这不是又一个「聊天机器人接了个 LLM API」。它是一名**对结果负责的远程员工**：

> **你**：@claude 用户反馈登录 token 过期后不会自动刷新，修一下
> **bot**：（流式更新同一条消息，边干边给你看进度）
> **bot**：搞定 ✅ 改了 token 刷新逻辑并补了测试，PR：`…/pulls/42`
> **bot**（几分钟后）：✅ PR #42 CI 通过，已自动合并。

不在电脑前？在 Gitea 上建个 issue、指派给它的账号，回来时活已干完、PR 已合并、工单已自动关闭。

**这个仓库自己就是它维护的**——`docs/CONTRIBUTING.md` 就是给它提了个 issue 后，它自己接单、写完、开 PR、CI 绿了自动合并进来的（见 issue #2 / PR #3）。

## 它凭什么不一样

- 🎯 **对结果负责，不是 fire-and-forget。** 开了 PR 就盯到合并：收到评审意见自动改、CI 挂了自动修（各带次数上限防空转）、满足条件自动合并，全程回报进展。合并/关闭才销账。
- 📥 **五个派活入口。** 群里 @ / 私聊 / 触发词 / **Gitea issue 指派**（不进聊天软件也能下任务）/ 没人理它时**自己巡检找活**（autopilot 自己认领开 PR）。
- 🧠 **有记忆，跨周不失忆。** 多轮会话按「房间×项目」隔离并持久化（重启不断）；超过会话 TTL 后，靠**项目长期记忆**（关键决策+原因、约定、踩过的坑，它自己沉淀）和**聊天逐字记录**（问它"上周聊到哪了"它自己去翻）接续。
- 🗣️ **像同事，不像命令行。** 线程化回复不串台、长任务流式输出不静默、排队了立刻回执、能收发图片/文件/附件（E2EE 房间照常）、群里有人说错了它还会主动插话纠正（可关）。
- 🏠 **一切都在你自己的机器上。** 本机 Claude Code + 自托管 Matrix + 自托管 Gitea：代码、凭证、聊天记录不出门。

## 架构一图流

```
   Matrix（你和同事）              Gitea（issue / PR / CI）
     │ @派活·私聊·发文件              │ issue 指派 = 派活
     ▼                              ▼
  ┌────────────────────────────────────────────┐
  │ bot（单进程异步守护，职责单一的小模块）       │
  │  寻址 → 路由 → 派活 → 流式回报               │
  │  ├─ PR 台账：盯评审/CI → 自动改 → 自动合并    │
  │  ├─ 工单接活：认领 → 干活 → PR(Closes #N)    │
  │  └─ 自驱心跳：闲时巡检 → 自己找活开 PR        │
  └───────────────────┬────────────────────────┘
                      ▼
          本机 Claude Code（claude -p）
     每仓库一份 checkout：建分支 → 改码 → 测试 → push → 开 PR
```

**一个仓库 = 一名工程师**：群和私聊只是入口，同一仓库共用一份本地 checkout（按仓库串行，排队有回执）；但会话按「房间×项目」隔离，不同群/不同人互不串台。每次派活前工作树先拉回干净的 `origin/base`，上个任务的残留不会污染下个 PR。不同仓库并行（`MAX_CONCURRENCY`）。

## 五分钟跑起来

前置：本机装好并登录 [Claude Code](https://claude.com/claude-code)（`claude` 命令可用）；一个 Matrix 账号（自己的或专用 bot 号）；（可选）E2EE 需要系统 `libolm`。

```bash
git clone <this-repo> && cd matrix-claude-bot
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# E2EE（可选）：sudo pacman -S libolm && .venv/bin/pip install "matrix-nio[e2e]"
cp .env.example .env   # 填 Matrix 账号 + GITEA_HOST / GITEA_TOKEN
./run.sh
```

然后：
1. 把 bot 拉进群（谁邀请都进），群里发一个 Gitea 仓库地址（或 `/bind <URL>`）→ 自动 clone 绑定；
2. @它派活。改代码的活它会开 PR 并盯到合并；纯问答直接回。私聊不用绑定，它按内容自动分诊到项目；
3. 想不打字：Gitea 上把 issue 指派给它的账号，它每 5 分钟收一次单。

常驻运行（systemd user，用你自己的用户跑，才能用到 `claude` 的登录态）：

```ini
# ~/.config/systemd/user/matrix-claude.service
[Unit]
Description=Matrix x Claude Code bot
After=network-online.target

[Service]
WorkingDirectory=%h/Projects/matrix-claude-bot
ExecStart=%h/Projects/matrix-claude-bot/.venv/bin/python bot.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
```
```bash
systemctl --user enable --now matrix-claude.service && loginctl enable-linger $USER
journalctl --user -u matrix-claude -f
```

## 能力清单

**派活与对话**
- 群 @ / 私聊 / 触发词派活；群里 @ 过后的续话窗口内免重复 @（`GROUP_FOLLOWUP_WINDOW`）
- 线程化回复（`m.thread`，多话题并行不串台）；流式渐进输出（占位消息边生成边编辑，长任务不静默）
- 排队回执：同项目已有任务在跑时立即告知「已排队」，并说明能否 `/cancel` 让路
- 收文件：图片/文件/音视频自动下载落盘（加密房解密），路径交给 Claude 读取分析；发文件：回复里写 `[[send-file: 路径]]` 即作为附件发回（仅限工作目录内，挡越权外泄）
- 主动插话（`PROACTIVE`，默认开）：没被 @ 也判断该不该开口——有人求助，或**对话里有明显错误值得纠正**；绑了仓库的群里用只读模式看着真实代码作答；冷却 + 强 PASS 倾向防刷屏
- 人走光自动退房：房间里除自己外没人了就自动退房 + forget，并连带清掉它的仓库绑定、路由记忆、聊天记录、媒体目录、停掉房内在跑的任务——不再对着死房间发心跳/工单/PR 跟进
- 重启对账：跑/排队中的任务落在途登记簿（`store/inflight.json`），重启后把孤儿占位收尾成「已中断请重发」、接了单没开出 PR 的工单自动重派（先查有没有已开的 `Closes #N` PR，防重复）——重启不再留「⏳ 永远在干活」的幽灵消息和工单黑洞
- 加密消息解不开时不再沉默：自动向对方要会话密钥 + 回明文提示（每房间限流），至少知道该重发还是该验证设备

**Gitea 闭环**
- PR 台账：bot 开的 PR 全部记账跟进——新评审意见→自动在原分支处理并推送；CI 失败→自动修（各 `PR_AUTOFIX_MAX` 次上限）；可合并+CI 绿+无未决改动→**自动合并**（`PR_AUTOMERGE`，默认开）
- 工单接活（`ISSUE_INTAKE`，默认开）：issue 指派给 bot 账号 = 派活。接单即评论认领，干完开 PR（描述带 `Closes #N`，合并自动关单）、链接贴回 issue，进展同步到项目房间；台账持久化，重启不重复接单；重派 = 关单再重开
- 自驱心跳（`PROACTIVE_HEARTBEAT_ENABLED`，默认开）：没人派活时按小时巡检各项目找值得做的事（小 bug、缺测试、陈旧 TODO）；autopilot 模式自己认领开 PR，关掉则只提议等你点头
- Gitea 健康度：连接失败/token 失效不再静默降级——`/status` 显示连通性，连续失败跨阈值向相关房间告警一次、恢复再报一次

**记忆与上下文**
- 会话按「房间×项目」隔离，session id 落盘，重启多轮不断；空闲 24h（`SESSION_TTL`）后自动开新会话
- 项目长期记忆：值得跨周记住的事实（决策+原因、约定、坑）由它自己写进 `store/memory/<项目>/`，开新会话时按预算注入——会话过期也"记得住事"
- 聊天逐字记录：按房间落盘（保留 30 天），系统提示里只给**路径**，被问到更早对话时它自己去读/grep——不把历史塞爆每次 prompt；`/backfill` 可回灌开启前的历史

**元命令**（群里不必 @ 也认）：`/help` `/bind <URL>` `/status`（项目/在跑任务/在跟 PR/在办工单一屏可见）`/summarize [N]` `/cancel` `/reset` `/backfill [天]`

## 安全模型（必读）

这个 bot 的默认姿态是**全自动**：`CLAUDE_DANGEROUS=1`（跳过工具授权）+ 主动插话 + autopilot 自驱 + PR 自动合并 + 工单接活全默认开。合起来就是「巡检→开 PR→自动合并到 main」的闭环，**而且它不做用户白名单**——任何能给它发消息的人都能驱动它。

这套取舍只适用于：**私有/仅内网可达/关闭注册的 homeserver + 可信用户 + 有快照可回滚的环境**。要点：

- **把"谁能用"放到 homeserver 层**：关闭开放注册、不开联邦。bot 自己不挡人。
- **token 隔离**：`GITEA_TOKEN` 只注入 `GITEA_HOST` 一台主机的 clone/push 地址；不设 `GITEA_HOST` 则 fail-closed（连仓库都不识别）。
- **出口脱敏**：外发文本自动抹掉 token/密码（兜底，非主防线）。
- **想保守**：`PROACTIVE=0` `PROACTIVE_AUTOPILOT=0` `PR_AUTOMERGE=0` `ISSUE_INTAKE=0` `CLAUDE_DANGEROUS=0`，逐项关回。

若你的 homeserver 对公网/联邦开放，**等于把服务器和私有库交给任何能连上的人**——别这么部署。

## 自检

不连真实 Matrix，93 项离线冒烟跑通「启动→收消息→派活→回复」全链路（含上下文隔离、PR 跟进、工单接活、安全位）：

```bash
.venv/bin/python tests/smoke.py
```

仓库自带 Gitea Actions CI（冒烟 + pyflakes）；配合 `PR_AUTOMERGE`，绿了 PR 才会被自动合并。

## 常见问题

**为什么是 Matrix，不是 Slack/企业微信？** 三件事官方 API 给不了：看到房间**全部消息**（主动插话的前提）、**随时主动发消息**（自驱汇报的前提）、可以直接用**你自己的账号**跑。Matrix 自托管还意味着聊天记录不出门。

**为什么不怕它乱来？** CI 是闸（挂了不合并）、环境有快照可回滚、token 只到一台内网 Gitea。信任是配置出来的，不是祈祷出来的——上面每个默认开的开关都能关。

**它和在终端里用 Claude Code 有什么区别？** 终端是「你在场才干活」；这是「你不在场它也在干」——接工单、盯 PR、修 CI、找活，以及一个团队都能 @ 它。

## 模块导览（贡献者向）

依赖方向单向无环：`state → fmt → matrix_io → addressing → dispatch → tasks → {pr_followup, heartbeat → issue_intake, proactive, media} → bot`

| 模块 | 职责 |
|---|---|
| `bot.py` | 入口：登录/持久化、事件回调、起后台循环，只做编排 |
| `state.py` | 进程级共享运行态（client、上下文缓冲、路由记忆） |
| `fmt.py` | 纯文本/富文本格式化（分块、代码围栏自洽、消毒 HTML） |
| `matrix_io.py` | 收发：分块、线程化、流式占位与编辑、附件上传 |
| `addressing.py` | 该不该应答：点名/回复/触发词/续话窗口 |
| `dispatch.py` | 消息归哪个项目：群按绑定、DM 按内容分诊 |
| `tasks.py` | 在项目上跑任务并回发 + 全部元命令 |
| `pr_followup.py` / `pr_ledger.py` | PR 跟进循环 / PR 台账（对结果负责的状态底座） |
| `issue_intake.py` / `issue_ledger.py` | 工单接活循环 / 工单台账（防重复接单） |
| `heartbeat.py` | 自驱心跳：闲时巡检找活 |
| `proactive.py` | 主动插话：求助/纠错判定 |
| `media.py` | 图片/文件收发落盘 |
| `claude_runner.py` | 调 `claude -p`：多轮会话/流式/取消/只读查证 |
| `memory.py` / `transcript.py` | 项目长期记忆 / 聊天逐字记录 |
| `projects.py` / `gitea.py` | 仓库解析绑定 clone / Gitea REST 小客户端 |
| `config.py` / `storage.py` | `.env` 开关 / 原子落盘小工具 |

全部配置项及说明见 [.env.example](.env.example)。
