#!/bin/bash
# =============================================================================
# notify_headless_example.sh — command 渠道示例: headless 一次性调用 agent
# =============================================================================
# 场景: 无常驻 agent 进程, 批次终态时起一个无头实例处理通知。
#
# 用法 (config.json):
#   "notify": {"command": ["bash", "sched/scripts/notify_headless_example.sh"]}
#
# 按你用的 harness 取消对应一行注释 (或换成别的 CLI):
#   Claude Code: claude -p "..." (https://docs.anthropic.com/en/docs/claude-code)
#   Kimi Code:   kimi -p "..."
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

# --- 按 harness 二选一 (或自行替换) ---
# claude -p "$PROMPT"
# kimi -p "$PROMPT"

echo "notify_headless_example: 未配置 harness CLI, prompt 为:" >&2
echo "$PROMPT" >&2
exit 1   # rc!=0 会让 notify.send 记 FAIL, 提醒用户完成配置
