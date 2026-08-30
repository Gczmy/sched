#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_ux_accept.sh — 使用体验优化验收 (fake-gpu 快速回归)
# =============================================================================
# 覆盖场景 (P1-P4):
#   1. P2 批次级 retry: `sched retry <batch>` (无 :task) 一次性解锁全部失败终态
#   2. P1 cmd_diag: 一站式诊断 (失败任务 + 实际命令 + git 对比 + 日志尾部)
#   3. P3 git rev 警告: retry 时检测任务 git_rev 与当前仓库 rev 不一致 -> 警告
#   4. P4 status 进度列: running 任务显示 epoch/trial 进度
#
# 用法: bash sched/tests/run_ux_accept.sh
# 退出码: 0 = 全过, 1 = 有失败 (输出 FAIL 行)
# =============================================================================
set -u
cd "$(dirname "$0")/.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install

source tests/acceptance_cleanup.sh
RUN_TAG=$$
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  sleep 1
}

count_status() { # $1=state_dir $2=batch_name $3=status -> 数量
  local st=$1 bn=$2 want=$3
  export SCHED_STATE=$st SCHED_CONFIG=$st/config.json
  $PY -m gsched.cli status --json 2>/dev/null | \
    $PY -c "
import json, sys
try:
    d = json.loads(sys.stdin.read())
except (json.JSONDecodeError, RecursionError):
    print(-1)
    raise SystemExit(0)
if not isinstance(d, dict) or not isinstance(d.get('jobs'), list):
    print(-1)
    raise SystemExit(0)
bn = '$bn'; want = '$want'
n = 0
for j in d['jobs']:
    if j['batch_name'] == bn and j['status'] == want:
        n += 1
print(n)
"
}

wait_status() { # $1=state_dir $2=batch_name $3=status $4=期望数 $5=超时秒
  local st=$1 bn=$2 want=$3 exp=$4 timeout=${5:-30}
  for _ in $(seq 1 $timeout); do
    [ "$(count_status $st $bn $want)" = "$exp" ] && return 0
    sleep 1
  done
  return 1
}

mk_config() { # $1=state_dir $2=git(true/false)
  cat > $1/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$1", "gpus": [0],
  "projects": {"default": {"root": "$ROOT", "git": $2}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
}

echo "=== 使用体验优化验收 (fake-gpu) ==="

# ---------- 场景 1+2: 批次级 retry + diag (P2/P1) ----------
# 设计: 任务用 flag 文件实现"首跑失败、retry 后成功" (验证 retry 真实恢复价值).
#   t2: max_retry 0 -> 首跑失败直接 failed, 批次 blocked (t3 pending 冻结)
#   t3: max_retry 0 -> retry 批次解锁后才跑, 首跑失败 -> 需第二次 retry
echo "--- 场景 1+2: retry 批次级解锁 + diag 一站式诊断 ---"
sched_accept_make_root S1 "sched-ux-retry"
mk_config $S1 true
cat > $S1/batch.json << EOF
{
  "name": "u1",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "print('ok')"], "duration_min": 1},
    {"id": "t2", "max_retry": 0,
     "cmd": ["{VENV:k}", "-c", "import os,sys; p='$S1/f2'; print('boom'); os.path.exists(p) or (open(p,'w').close(), sys.exit(1))"], "duration_min": 1},
    {"id": "t3", "max_retry": 0,
     "cmd": ["{VENV:k}", "-c", "import os,sys; p='$S1/f3'; print('boom2'); os.path.exists(p) or (open(p,'w').close(), sys.exit(1))"], "duration_min": 1}
  ]
}
EOF
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
$PY -m gsched.cli submit $S1/batch.json >/dev/null 2>&1 || { bad "u1 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
wait_status $S1 u1 done 1 30 && ok "t1 完成 done" || bad "t1 未 done"
wait_status $S1 u1 blocked 1 30 && ok "t2 失败 -> blocked (批次 blocked, t3 pending 冻结)" \
  || bad "t2 未 blocked (got $(count_status $S1 u1 blocked))"
[ "$(count_status $S1 u1 pending)" = "1" ] && ok "t3 保持 pending 冻结" \
  || bad "t3 非 1 个 pending (got $(count_status $S1 u1 pending))"
# P1: diag 批次级 (列出非 done/skip: t2 failed + t3 pending)
$PY -m gsched.cli diag u1 > $S1/diag_out.txt 2>&1
grep -qE "u1-[0-9]+:t2" $S1/diag_out.txt && ok "diag 列出 failed 任务" \
  || bad "diag 缺 failed 任务 (输出: $(tail -15 $S1/diag_out.txt))"
grep -q "boom" $S1/diag_out.txt && ok "diag 含日志尾部内容" || bad "diag 无日志尾部"
grep -q "cmd:" $S1/diag_out.txt && ok "diag 含实际命令" || bad "diag 无命令展示"
# P2: 批次级 retry (无 :task) -> 解锁 t2, 批次回 active, t3 继续
$PY -m gsched.cli retry u1 > $S1/retry_out.txt 2>&1
grep -q "已解锁重跑" $S1/retry_out.txt && ok "批次级 retry 解锁了失败任务" \
  || bad "retry 未解锁 (输出: $(cat $S1/retry_out.txt))"
wait_status $S1 u1 done 2 40 && ok "retry 后 t1+t2 done (t2 第二次成功)" \
  || bad "t1+t2 未 done (running=$(count_status $S1 u1 running) failed=$(count_status $S1 u1 failed))"
wait_status $S1 u1 blocked 1 30 && ok "t3 首跑失败 -> blocked (t2 已 done)" \
  || bad "t3 未 blocked (blocked=$(count_status $S1 u1 blocked))"
# 场景 3 合并: 第二次 retry 前把 t3 的 git_rev 改成旧值 -> 验证 P3 警告
$PY -c "
import sys; sys.path.insert(0, 'sched')
from gsched import state
with state.connect() as conn:
    conn.execute(\"UPDATE jobs SET git_rev='deadbeefdeadbeef' WHERE task_id='t3'\")
print('t3 git_rev 已改为 deadbeef')
" >/dev/null 2>&1
$PY -m gsched.cli retry u1 > $S1/retry2_out.txt 2>&1
grep -q "代码已更新" $S1/retry2_out.txt && ok "P3 git rev 警告输出 (deadbeef vs 当前 rev)" \
  || bad "P3 警告缺失 (输出: $(cat $S1/retry2_out.txt))"
wait_status $S1 u1 done 3 40 && ok "第二次 retry 后全部 done" \
  || bad "未全 done (running=$(count_status $S1 u1 running) failed=$(count_status $S1 u1 failed) pending=$(count_status $S1 u1 pending))"

# ---------- 场景 5: status --detail (P5) + 终态 marker (P7) ----------
echo "--- 场景 5: status --detail + sched markers ---"
sched_accept_make_root S3 "sched-ux-detail"
mk_config $S3 false
cat > $S3/batch.json << EOF
{
  "name": "u5",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; print('Epoch 1/3'); time.sleep(2)"], "duration_min": 1},
    {"id": "t2", "cmd": ["{VENV:k}", "-c", "print('ok2')"], "duration_min": 1}
  ]
}
EOF
export SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json
$PY -m gsched.cli submit $S3/batch.json >/dev/null 2>&1 || { bad "u5 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
# P5: running 任务 --detail 显示时间字段
wait_status $S3 u5 running 1 15 && ok "u5:t1 进入 running" || bad "u5:t1 未 running"
sleep 2
$PY -m gsched.cli status u5 --detail > $S3/detail_out.txt 2>&1
grep -q "start=" $S3/detail_out.txt && grep -q "耗时=" $S3/detail_out.txt \
  && ok "P5 status --detail 含起止时间/耗时" \
  || bad "P5 --detail 缺时间字段 (输出: $(head -8 $S3/detail_out.txt))"
# P7: 批次 done 后 marker 文件 + sched markers
wait_status $S3 u5 done 2 30 && ok "u5 全部 done" || bad "u5 未全 done"
[ -f $S3/testnode/markers/u5.done ] && ok "P7 marker 文件 u5.done 已写" || bad "marker u5.done 缺失"
$PY -m gsched.cli markers > $S3/markers_out.txt 2>&1
grep -q "u5.done" $S3/markers_out.txt && ok "sched markers 列出 u5.done" \
  || bad "sched markers 缺 u5.done (输出: $(cat $S3/markers_out.txt))"
# blocked marker: 失败任务也应有 marker
cat > $S3/bad.json << EOF
{
  "name": "u5bad",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "x1", "max_retry": 0,
     "cmd": ["{VENV:k}", "-c", "import sys; print('boomx'); sys.exit(1)"], "duration_min": 1}
  ]
}
EOF
$PY -m gsched.cli submit $S3/bad.json >/dev/null 2>&1
wait_status $S3 u5bad blocked 1 20 && ok "u5bad 失败 -> blocked" || bad "u5bad 未 blocked"
sleep 2
[ -f $S3/testnode/markers/u5bad.blocked ] && grep -q "x1" $S3/testnode/markers/u5bad.blocked \
  && ok "P7 blocked marker 含失败任务列表" || bad "blocked marker 缺失/无失败列表"
$PY -m gsched.cli markers > $S3/markers2_out.txt 2>&1
grep -q "u5bad.blocked" $S3/markers2_out.txt && ok "sched markers 列出 blocked" \
  || bad "sched markers 缺 blocked (输出: $(cat $S3/markers2_out.txt))"

# ---------- 场景 4: status 进度列 (P4) ----------
echo "--- 场景 4: running 任务 status 显示进度列 ---"
sched_accept_make_root S2 "sched-ux-progress"
mk_config $S2 false
cat > $S2/batch.json << EOF
{
  "name": "u4",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "print('Epoch 3/30'); import time; time.sleep(60)"], "duration_min": 2}
  ]
}
EOF
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
$PY -m gsched.cli submit $S2/batch.json >/dev/null 2>&1 || { bad "u4 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
wait_status $S2 u4 running 1 15 && ok "u4:t1 进入 running" || bad "u4:t1 未 running"
sleep 3  # 等日志 flush (PYTHONUNBUFFERED)
$PY -m gsched.cli status > $S2/status_out.txt 2>&1
grep -q "3/30" $S2/status_out.txt && ok "status 显示进度 3/30" \
  || bad "status 无进度列 (输出: $(grep 'u4' $S2/status_out.txt))"
$PY -m gsched.cli cancel u4 --yes >/dev/null 2>&1
wait_status $S2 u4 cancelled 1 30 && ok "清理: u4 已 cancel" || bad "u4 cancel 失败"

stop_daemon $S1; stop_daemon $S2; stop_daemon $S3

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ $FAIL -eq 0 ]
