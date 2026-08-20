#!/bin/bash
# =============================================================================
# notify_headless_example.sh — command 渠道示例: headless 一次性调用 agent
# =============================================================================
# 场景: 无常驻 agent 进程, 批次终态时起一个无头实例处理通知。
#
# 用法 (config.json):
#   "notify": {"command": ["bash", "sched/scripts/notify_headless_example.sh"]}
#
# 自动检测 harness CLI (按优先级):
#   1. claude -p "..."  (Claude Code)
#   2. kimi -p "..."    (Kimi Code)
#   3. pi -p "..."      (Pi Coding Agent)
#   4. 均不可用时打印 prompt 并 exit 1 (提醒用户安装)
#
# 事件 JSON 从 stdin 读入; 完整事件在 notify_inbox, prompt 只需点到为止。
# =============================================================================
set -u

EVENT=$(cat)   # stdin: 事件 JSON

PROMPT=$(echo "$EVENT" | python3 -c "
import json, sys
ev = json.load(sys.stdin)
status = 'done' if ev['event'] == 'batch_done' else 'blocked'
print(f'sched 批次 {ev[\"batch\"]} {status}。请运行 sched notify-inbox 查看事件, '
      f'blocked 则按事件里的日志绝对路径排雷, 处理完 sched notify-ack 确认。')
")

# --- 自动检测 harness CLI ---
if command -v claude &>/dev/null; then
    echo "notify: 使用 claude 处理通知" >&2
    claude -p "$PROMPT"
    exit $?
elif command -v kimi &>/dev/null; then
    echo "notify: 使用 kimi 处理通知" >&2
    kimi -p "$PROMPT"
    exit $?
elif command -v pi &>/dev/null; then
    echo "notify: 使用 pi 处理通知" >&2
    pi -p "$PROMPT"
    exit $?
else
    echo "notify_headless_example: 未检测到 harness CLI (claude/kimi/pi)" >&2
    echo "安装后取消 scripts/notify_headless_example.sh 中对应行注释" >&2
    echo "prompt 为:" >&2
    echo "$PROMPT" >&2
    exit 1   # rc!=0 会让 notify.send 记 FAIL, 提醒用户完成配置
fi
