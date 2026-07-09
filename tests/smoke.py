#!/usr/bin/env python3
"""冒烟自检 runner：不连真实 Matrix，用假 client 跑通全链路并校验关键行为。

用法（项目根目录）：
    .venv/bin/python tests/smoke.py

各主题用例拆在 tests/test_*.py，共享夹具在 tests/_helpers.py。本文件只按
「拆分前 TESTS 的原有顺序」把各模块的 TESTS 子列表拼起来顺序跑——这些用例共享
可变模块态、靠每-用例重置而非隔离，顺序不能乱。全部通过退出码 0；任一失败退出码 1。
    --list  只打印 序号/中文名/函数名 清单（供核对执行顺序），不跑用例。
"""
import sys

from _helpers import settings

import test_dispatch_flow, test_addressing_safety, test_followup_gates, test_bind_dispatch, test_media_proactive, test_context_resume, test_projects_worktree, test_dm_binding, test_ledgers_reconcile, test_gitea_health, test_ops_digest, test_cancel_misc, test_persistent, test_redaction

# 拼接顺序 == 拆分前 TESTS 的原有顺序（各段在原列表里连续）
TESTS = (
    test_dispatch_flow.TESTS +
    test_addressing_safety.TESTS +
    test_followup_gates.TESTS +
    test_bind_dispatch.TESTS +
    test_media_proactive.TESTS +
    test_context_resume.TESTS +
    test_projects_worktree.TESTS +
    test_dm_binding.TESTS +
    test_ledgers_reconcile.TESTS +
    test_gitea_health.TESTS +
    test_ops_digest.TESTS +
    test_cancel_misc.TESTS +
    test_persistent.TESTS +
    test_redaction.TESTS
)


def main():
    import traceback
    import tempfile
    import gitea
    if "--list" in sys.argv:
        for i, (name, fn) in enumerate(TESTS, 1):
            print(f"{i:3d}\t{name}\t{fn.__name__}")
        return
    # 把状态目录指到临时目录，别让自检把 sessions.json / last_projects.json 写进真实 store
    settings.store_path = tempfile.mkdtemp(prefix="mxbot-smoke-store-")
    # 预置"bot 自己的 Gitea 用户 id/登录名"缓存：自检必须离线，不许真去查 /api/v1/user
    gitea._own_user["id"] = -1
    gitea._own_user["login"] = "claudebot"
    failed = 0
    for name, fn in TESTS:
        try:
            fn()
            print(f"  \u2705 {name}")
        except Exception as e:
            failed += 1
            print(f"  \u274c {name}: {e}")
            traceback.print_exc()
    print()
    if failed:
        print(f"FAILED: {failed}/{len(TESTS)}")
        sys.exit(1)
    print(f"ALL {len(TESTS)} SMOKE TESTS PASSED \u2705")


if __name__ == "__main__":
    main()
