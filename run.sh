#!/usr/bin/env bash
# 启动 Matrix × Claude Code 机器人
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python3 -m venv .venv
  .venv/bin/pip install -q -r requirements.txt
fi
if [ ! -f .env ]; then
  echo "没有 .env，请先: cp .env.example .env 并填好账号" >&2
  exit 1
fi
chmod 600 .env 2>/dev/null || true   # .env 含 GITEA_TOKEN / 登录密码，收紧权限别让同机其他用户读到
exec .venv/bin/python src/bot.py
