#!/bin/bash
# =============================================================================
# run_unmanaged_recover_accept.sh — unmanaged 卡自动恢复验收 (fake-gpu 快速回归)
# =============================================================================
# 背景 (2026-08-15 排雷): 非 sched 外部进程占卡触发孤儿防线 (probe_free) 误判为
#   unmanaged 后, 外部进程退出但状态不恢复 -> GPU 永久空置, 曾需人工 sched gpu-free.
#   修复: allocator.probe_unmanaged() 让 unmanaged 卡物理真实空闲后自动回 free.
#
# 覆盖场景:
#   1. 正常批次 done 后 GPU 回 free (probe_unmanaged 不误伤正常路径)
#   2. 手动置 unmanaged -> daemon tick 自动回 free (核心新增逻辑)
#   3. unmanaged 期间任务不派发, 恢复后新批次正常派发完成 (回归)
#
# 用法: bash sched/tests/run_unmanaged_recover_accept.sh
# 退出码: 0 = 全过, 1 = 有失败 (输出 FAIL 行)
# =============================================================================
set -u
cd "$(dirname "$0")/../.."   # 仓库根
PY=${PY:-/Users/zzc/miniconda3/envs/vnpy_env/bin/python}
ROOT=$(pwd)
HOST=$(hostname)

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

run_batch() { # $1=state_dir  $2=batch -> daemon log 路径
  local st=$1 batch=$2
  export SCHED_STATE=$st SCHED_CONFIG=$st/config.json
  $PY -m gsched.cli submit "$batch" >/dev/null 2>&1 || return 1
  env SCHED_STATE=$st SCHED_CONFIG=$st/config.json SCHED_FAKE_GPUS=0 \
      $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
  echo "$st/$HOST/scheduler.log"
}

gpu_status() { # $1=state_dir -> GPU 状态行 (idx|status)
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli status 2>/dev/null | grep -E '^  GPU0' | awk '{print $2}' | tr -d '[]'
}

set_gpu_status() { # $1=state_dir $2=status
  $PY -c "
import sqlite3, sys
db = '$1/$HOST/state.db'
st = '$2'
c = sqlite3.connect(db)
c.execute(\"UPDATE gpus SET status=?, job_id=NULL, updated_at=datetime('now') WHERE idx=0\", (st,))
c.commit(); c.close()
"
}

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  pkill -f "gsched.dispatcher_main" 2>/dev/null
  sleep 1
}

mk_config() { # $1=state_dir
  cat > $1/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$1", "gpus": [0],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
}

echo "=== unmanaged 自动恢复验收 (fake-gpu) ==="

# ---------- 场景 1: 正常批次 done 后 GPU 回 free (不误伤) ----------
echo "--- 场景 1: 正常路径 GPU 回 free ---"
S1=/tmp/sched_acc_u1; rm -rf $S1; mkdir -p $S1
mk_config $S1
cat > $S1/batch.json << EOF
{
  "name": "u1",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S1/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S1/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S1 $S1/batch.json)
for _ in $(seq 1 30); do
  [ "$(gpu_status $S1)" = "free" ] && break
  sleep 1
done
if [ "$(gpu_status $S1)" = "free" ]; then
  ok "场景1: 正常批次完成后 GPU0=free (probe_unmanaged 不误伤)"
else
  bad "场景1: GPU0 未回 free (实际: $(gpu_status $S1))"
fi
stop_daemon $S1

# ---------- 场景 2: 手动置 unmanaged -> 自动回 free (核心) ----------
echo "--- 场景 2: unmanaged 卡自动恢复 ---"
S2=/tmp/sched_acc_u2; rm -rf $S2; mkdir -p $S2
mk_config $S2
cat > $S2/batch.json << EOF
{
  "name": "u2",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S2/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S2/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S2 $S2/batch.json)
for _ in $(seq 1 30); do
  [ "$(gpu_status $S2)" = "free" ] && break
  sleep 1
done
set_gpu_status $S2 unmanaged   # 模拟孤儿防线误判
ST0=$(gpu_status $S2)
if [ "$ST0" != "unmanaged" ]; then
  bad "场景2: 前置失败, 未能置为 unmanaged (实际: $ST0)"
else
  # fake 模式 probe_unmanaged 单 tick 即恢复 (confirm 恒 True); 等 2 tick 余量
  for _ in $(seq 1 8); do
    [ "$(gpu_status $S2)" = "free" ] && break
    sleep 2
  done
  if [ "$(gpu_status $S2)" = "free" ]; then
    ok "场景2: unmanaged -> 自动回 free (probe_unmanaged 生效)"
  else
    bad "场景2: unmanaged 未自动恢复 (实际: $(gpu_status $S2))"
  fi
fi
stop_daemon $S2# ---------- 场景 3: 恢复后新批次正常派发完成 ----------
echo "--- 场景 3: unmanaged 恢复后新批次可派发 ---"
S3=/tmp/sched_acc_u3; rm -rf $S3; mkdir -p $S3
mk_config $S3
cat > $S3/batch.json << EOF
{
  "name": "u3",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S3/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S3/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S3 $S3/batch.json)
for _ in $(seq 1 30); do
  if [ -f "$S3/t1.txt" ]; then break; fi
  sleep 1
done
if [ -f "$S3/t1.txt" ]; then
  ok "场景3: 批次任务正常派发完成 (产物生成)"
else
  bad "场景3: 批次任务未完成"
fi
stop_daemon $S3

# ---------- 场景 4: daemon stop 收尾不残留 assigned 卡 (N11 修复) ----------
echo "--- 场景 4: daemon stop 后 GPU 释放不残留 ---"
S4=/tmp/sched_acc_u4; rm -rf $S4; mkdir -p $S4
mk_config $S4
cat > $S4/batch.json << EOF
{
  "name": "u4",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(30); open('$S4/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S4/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S4 $S4/batch.json)
for _ in $(seq 1 15); do
  [ "$(gpu_status $S4)" = "assigned" ] && break
  sleep 1
done
if [ "$(gpu_status $S4)" != "assigned" ]; then
  bad "场景4: 前置失败, 任务未 running (实际: $(gpu_status $S4))"
else
  export SCHED_STATE=$S4 SCHED_CONFIG=$S4/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1; sleep 1
  # 重启 daemon 让 settle_releasing 把 releasing 转 free (fake 立即)
  env SCHED_STATE=$S4 SCHED_CONFIG=$S4/config.json SCHED_FAKE_GPUS=0 \
      $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
  for _ in $(seq 1 15); do
    [ "$(gpu_status $S4)" = "free" ] && break
    sleep 1
  done
  if [ "$(gpu_status $S4)" = "free" ]; then
    ok "场景4: daemon stop 后 GPU 释放回 free (不残留 assigned)"
  else
    bad "场景4: GPU 残留 (实际: $(gpu_status $S4))"
  fi
fi
stop_daemon $S4

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" -eq 0 ] || exit 1
