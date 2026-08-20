#!/bin/bash
# =============================================================================
# notify_tmux_example.sh — command 渠道示例: tmux send-keys 唤醒交互式 agent
# =============================================================================
# 场景: LLM agent (Claude Code / Kimi Code / Pi) 跑在 tmux 会话里,
#       批次终态时往 agent 输入注入一条 prompt, 让它自己查 inbox 开工。
#
# 用法 (config.json):
#   "notify": {"command": ["bash", "sched/scripts/notify_tmux_example.sh", "agent:0"]}
#   参数 $1 = 目标 tmux session:window (默认 agent:0)
#   注意: tmux 配了 base-index 1 时窗口号从 1 开始, 目标写 agent:1
#
# 事件 JSON 从 stdin 读入 (与 email 正文/inbox 文件同源)。
# =============================================================================
set -u
TARGET=${1:-agent:0}

EVENT=$(cat)   # stdin: 事件 JSON

# 提取 batch/event 摘要 (python 一定在, 不依赖 jq)
SUMMARY=$(echo "$EVENT" | python3 -c "
import json, sys
ev = json.load(sys.stdin)
status = 'done' if ev['event'] == 'batch_done' else 'blocked'
print(f\"批次 {ev['batch']} {status} (counts={ev['counts']})\")
")

MSG="$SUMMARY, 请 sched notify-inbox 查事件并处理 (blocked 则读日志排雷)"

# 往 agent 的 tmux 窗格注入 prompt (C-u 清行防残留输入, 再发文本+回车)
tmux send-keys -t "$TARGET" C-u
tmux send-keys -t "$TARGET" "$MSG" Enter
