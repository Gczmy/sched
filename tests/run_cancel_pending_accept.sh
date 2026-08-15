#!/bin/bash
# =============================================================================
# run_cancel_pending_accept.sh — cancel 排队任务验收 (fake-gpu 快速回归)
# =============================================================================
# 用途: cmd_cancel 改动 (支持取消 pending 排队任务) 的回归验证.
#       不烧 GPU (SCHED_FAKE_GPUS), 不依赖真实训练.
#
# 覆盖场景:
#   1. 批次级 cancel: running (先写 kill_reason 再 killpg, N2) + pending
#      (排队中未启动, 直接标 cancelled) 全部收敛为 cancelled, 批次 blocked
#   2. --yes 确认门禁: 不带 --yes 拒绝执行
#   3. 任务级 cancel <batch>:<task>: 只取消单个任务, 不影响同批其他任务
#   4. Q4 下游依赖告警: cancel 上游后提示依赖批次将挂起
#
# 用法: bash sched/tests/run_cancel_pending_accept.sh
# 退出码: 0 = 全过, 1 = 有失败 (输出 FAIL 行)
# =============================================================================
set -u
cd "$(dirname "$0")/../.."   # 仓库根
PY=${PY:-/Users/zzc/miniconda3/envs/vnpy_env/bin/python}
ROOT=$(pwd)

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  pkill -f "gsched.dispatcher_main" 2>/dev/null
  sleep 1
}

# 任务状态计数: $1=state_dir $2=batch_name $3=status -> 数量
count_status() {
  local st=$1 bn=$2 want=$3
  export SCHED_STATE=$st SCHED_CONFIG=$st/config.json
  $PY -m gsched.cli status --json 2>/dev/null | \
    $PY -c "
import json, sys
d = json.load(sys.stdin)
bn = '$bn'; want = '$want'
n = 0
for j in d['jobs']:
    if j['batch'].split('-')[0] == bn and j['status'] == want:
        n += 1
print(n)
"
}

# 轮询等待任务状态达到预期 (daemon reap 有 poll 间隔, kill 后需等下一轮 tick)
wait_status() { # $1=state_dir $2=batch_name $3=status $4=期望数 $5=超时秒(默认15)
  local st=$1 bn=$2 want=$3 exp=$4 timeout=${5:-15}
  for _ in $(seq 1 $timeout); do
    [ "$(count_status $st $bn $want)" = "$exp" ] && return 0
    sleep 1
  done
  return 1
}

mk_config() { # $1=state_dir
  cat > $1/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$1", "gpus": [0],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "/Users/zzc/miniconda3/envs/vnpy_env/bin/python"}
}
EOF
}

echo "=== cancel 排队任务验收 (fake-gpu) ==="

# ---------- 场景 1+4: 批次级 cancel (running + pending) + Q4 下游告警 ----------
echo "--- 场景 1+4: 批次 cancel 收敛 running + pending, 提示下游 ---"
S1=/tmp/sched_acc_c1; rm -rf $S1; mkdir -p $S1
mk_config $S1
cat > $S1/batch.json << EOF
{
  "name": "c1", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(60)"], "duration_min": 1},
    {"id": "t2", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(60)"], "duration_min": 1},
    {"id": "t3", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(60)"], "duration_min": 1}
  ]
}
EOF
cat > $S1/down.json << EOF
{
  "name": "c1_down", "mode": "mix", "depends_on": ["c1"],
  "tasks": [
    {"id": "d1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(60)"], "duration_min": 1}
  ]
}
EOF
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
$PY -m gsched.cli submit $S1/batch.json >/dev/null 2>&1 || { bad "c1 submit 失败"; exit 1; }
$PY -m gsched.cli submit $S1/down.json >/dev/null 2>&1 || { bad "c1_down submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 3  # 等 t1 running (单卡), t2/t3 pending
[ "$(count_status $S1 c1 running)" = "1" ] && ok "t1 进入 running" || bad "t1 未 running (got $(count_status $S1 c1 running))"
[ "$(count_status $S1 c1 pending)" = "2" ] && ok "t2/t3 排队 pending" || bad "t2/t3 非 2 个 pending (got $(count_status $S1 c1 pending))"
# --yes 门禁
if $PY -m gsched.cli cancel c1 >/dev/null 2>&1; then
  bad "不带 --yes 竟执行了 cancel"
else
  ok "--yes 确认门禁生效 (不带 --yes 拒绝)"
fi
# 批次级 cancel
$PY -m gsched.cli cancel c1 --yes > $S1/cancel_out.txt 2>&1
wait_status $S1 c1 cancelled 3 15 && ok "3 任务全部 cancelled (1 running kill + 2 pending 直标)" \
  || bad "cancelled 数 != 3 (got $(count_status $S1 c1 cancelled))"
[ "$(count_status $S1 c1 running)" = "0" ] && [ "$(count_status $S1 c1 pending)" = "0" ] \
  && ok "无 running/pending 残留" || bad "有 running/pending 残留"
grep -q "c1_down" $S1/cancel_out.txt && ok "Q4 下游依赖告警输出 (c1_down 挂起提示)" \
  || bad "Q4 告警缺失 (输出: $(cat $S1/cancel_out.txt))"
# 批次收敛 blocked (cancelled 是终态)
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
$PY -m gsched.cli status --json 2>/dev/null | grep -A2 '"name": "c1"' | grep -q '"status": "blocked"' \
  && ok "批次 c1 收敛 blocked" || bad "批次 c1 未收敛 blocked"
stop_daemon $S1

# ---------- 场景 2+3: 任务级 cancel 只取消单个 ----------
echo "--- 场景 2+3: 任务级 cancel <batch>:<task> 只取消单个 ---"
S2=/tmp/sched_acc_c2; rm -rf $S2; mkdir -p $S2
mk_config $S2
cat > $S2/batch.json << EOF
{
  "name": "c2", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(60)"], "duration_min": 1},
    {"id": "t2", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(60)"], "duration_min": 1},
    {"id": "t3", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(60)"], "duration_min": 1}
  ]
}
EOF
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
$PY -m gsched.cli submit $S2/batch.json >/dev/null 2>&1 || { bad "c2 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 3  # 等 t1 running, t2/t3 pending
$PY -m gsched.cli cancel c2:t2 --yes > $S2/cancel2_out.txt 2>&1
sleep 1
[ "$(count_status $S2 c2 cancelled)" = "1" ] && ok "任务级 cancel 只取消 t2 (1 个 cancelled)" \
  || bad "cancelled 数 != 1 (got $(count_status $S2 c2 cancelled))"
[ "$(count_status $S2 c2 running)" = "1" ] && ok "t1 继续 running (不受影响)" \
  || bad "t1 受影响 (running=$(count_status $S2 c2 running))"
[ "$(count_status $S2 c2 pending)" = "1" ] && ok "t3 仍 pending (不受影响)" \
  || bad "t3 受影响 (pending=$(count_status $S2 c2 pending))"grep -q "t2-v1" $S2/cancel2_out.txt && ok "取消输出指明 c2:t2" \
  || bad "取消输出未指明任务 (输出: $(cat $S2/cancel2_out.txt))"
# 收尾: 批次 cancel 全杀 (t1 running + t3 pending)
$PY -m gsched.cli cancel c2 --yes >/dev/null 2>&1
wait_status $S2 c2 cancelled 3 15 && ok "批次 cancel 收尾: 3 任务全 cancelled" \
  || bad "收尾 cancelled 数 != 3 (got $(count_status $S2 c2 cancelled))"

stop_daemon $S2

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" = "0" ]
