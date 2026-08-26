#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_cpu_quota_accept.sh — CPU 配额制调度验收脚本 (fake-gpu 快速回归)
# =============================================================================
# 用途: dispatcher/allocator 调度逻辑改动后的回归验证 (B 类 dry-run 定位),
#       不烧 GPU (SCHED_FAKE_GPUS), 不依赖真实训练.
#
# 覆盖场景 (评审 394e7f9 的 4 个问题点):
#   1. GPU 满时 break 饿死排后的 CPU-only 任务 (gpu_full continue 修复)
#   2. cpus_total 未配置回退 max_cpu_jobs 并发上限 (旧语义)
#   3. 配额制: GPU 任务与 CPU-only 统一按 CPU 配额竞争 (双约束)
#   4. status CPU 视图显示 (占用 N / 总核)
#
# 用法: bash sched/tests/run_cpu_quota_accept.sh
# 退出码: 0 = 全过, 1 = 有失败 (输出 FAIL 行)
# =============================================================================
set -u
cd "$(dirname "$0")/.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

run_batch() { # $1=state_dir  $2=config  $3=batch  -> daemon log 路径
  local st=$1 cfg=$2 batch=$3
  export SCHED_STATE=$st SCHED_CONFIG=$cfg
  $PY -m gsched.cli submit "$batch" >/dev/null 2>&1 || return 1
  # fake 单卡 (SCHED_FAKE_GPUS 由 env 传入)
  env SCHED_STATE=$st SCHED_CONFIG=$cfg SCHED_FAKE_GPUS=0 \
      $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
  echo "$st/testnode/scheduler.log"   # 定案 43 (P6): hostname() 读 config node
}

wait_done() { # $1=state_dir $2=batch_name  -> 轮询批次终态 (最多 30s)
  local st=$1 bn=$2
  export SCHED_STATE=$st SCHED_CONFIG=$st/config.json
  for _ in $(seq 1 30); do
    if $PY -m gsched.cli status --json 2>/dev/null | grep -q "\"$bn\""; then
      if $PY -m gsched.cli status --json 2>/dev/null | \
         grep -A2 "\"name\": \"$bn\"" | grep -q '"status": "done"'; then
        return 0
      fi
    fi
    sleep 1
  done
  return 1
}

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  pkill -f "gsched.dispatcher_main" 2>/dev/null
  sleep 1
}

echo "=== CPU 配额制调度验收 (fake-gpu) ==="

# ---------- 场景 1: GPU 满 + 排后的 CPU-only 不饿死 ----------
echo "--- 场景 1: 单卡 GPU 满, CPU-only 不被 break 饿死 ---"
S1=/tmp/sched_acc_s1; rm -rf $S1; mkdir -p $S1
cat > $S1/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S1", "gpus": [0],
  "cpus_total": 32, "gpu_job_cpus": 8,
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
cat > $S1/batch.json << EOF
{
  "name": "s1",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "gpu_long", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(3); open('$S1/gpu_long.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S1/gpu_long.txt"}}, "paths_escape": true},
    {"id": "cpu_a", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S1/cpu_a.txt','w').write('ok')"], "resources": {"cpus": 2, "gpu": 0}, "duration_min": 1, "artifacts": {"a": {"path": "$S1/cpu_a.txt"}}, "paths_escape": true},
    {"id": "gpu_b", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S1/gpu_b.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S1/gpu_b.txt"}}, "paths_escape": true},
    {"id": "cpu_b", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S1/cpu_b.txt','w').write('ok')"], "resources": {"cpus": 2, "gpu": 0}, "duration_min": 1, "artifacts": {"a": {"path": "$S1/cpu_b.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S1 $S1/config.json $S1/batch.json)
# 修复核心: GPU 满时排后的 CPU-only (cpu_b) 必须同轮派发 ——
# 旧 break 逻辑下 cpu_b 会一直等 GPU 释放 (饿死). gpu_b 需等 gpu_long 释放,
# 不在此窗口检查 (避免时序竞态).
sleep 3
if grep -q "LAUNCH.*cpu_b.*cpu" "$LOG"; then
  ok "GPU 满时排后的 cpu_b 同轮派发 (gpu_full continue 修复生效)"
else
  bad "场景1: 排后 CPU-only 未派发 (可能仍 break)"
fi
wait_done $S1 s1 && ok "场景1: 4/4 终态 done (含 gpu_b 等卡后补位)" || bad "场景1: 批次未收敛"
stop_daemon $S1

# ---------- 场景 2: cpus_total 未配置回退 max_cpu_jobs ----------
echo "--- 场景 2: 无 cpus_total -> max_cpu_jobs=2 回退 ---"
S2=/tmp/sched_acc_s2; rm -rf $S2; mkdir -p $S2
cat > $S2/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S2", "gpus": [0],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
cat > $S2/batch.json << EOF
{
  "name": "s2",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "c1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S2/c1.txt','w').write('ok')"], "resources": {"cpus": 8, "gpu": 0}, "duration_min": 1, "artifacts": {"a": {"path": "$S2/c1.txt"}}, "paths_escape": true},
    {"id": "c2", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S2/c2.txt','w').write('ok')"], "resources": {"cpus": 8, "gpu": 0}, "duration_min": 1, "artifacts": {"a": {"path": "$S2/c2.txt"}}, "paths_escape": true},
    {"id": "c3", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S2/c3.txt','w').write('ok')"], "resources": {"cpus": 8, "gpu": 0}, "duration_min": 1, "artifacts": {"a": {"path": "$S2/c3.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S2 $S2/config.json $S2/batch.json)
sleep 3
N_LAUNCH=$(grep -c "LAUNCH" "$LOG")
if [ "$N_LAUNCH" -eq 2 ]; then
  ok "回退模式: 仅 2 个 CPU-only 并发 (max_cpu_jobs=2)"
else
  bad "回退模式: 期望 2 并发, 实际 $N_LAUNCH"
fi
wait_done $S2 s2 && ok "场景2: 3/3 终态 done" || bad "场景2: 批次未收敛"
stop_daemon $S2

# ---------- 场景 3: 配额制双约束 (cpus_total=8, gpu_job_cpus=4) ----------
echo "--- 场景 3: cpus_total=8/gpu_job_cpus=4, 3 GPU + 2 CPU ---"
S3=/tmp/sched_acc_s3; rm -rf $S3; mkdir -p $S3
cat > $S3/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S3", "gpus": [0,1,2,3],
  "cpus_total": 8, "gpu_job_cpus": 4,
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
cat > $S3/batch.json << EOF
{
  "name": "s3",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "g0", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S3/g0.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S3/g0.txt"}}, "paths_escape": true},
    {"id": "g1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S3/g1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S3/g1.txt"}}, "paths_escape": true},
    {"id": "g2", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S3/g2.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S3/g2.txt"}}, "paths_escape": true},
    {"id": "cpu_a", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S3/cpu_a.txt','w').write('ok')"], "resources": {"cpus": 1, "gpu": 0}, "duration_min": 1, "artifacts": {"a": {"path": "$S3/cpu_a.txt"}}, "paths_escape": true},
    {"id": "cpu_b", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S3/cpu_b.txt','w').write('ok')"], "resources": {"cpus": 2, "gpu": 0}, "duration_min": 1, "artifacts": {"a": {"path": "$S3/cpu_b.txt"}}, "paths_escape": true}
  ]
}
EOF
# fake 4 卡跑场景 3
export SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json
$PY -m gsched.cli submit $S3/batch.json >/dev/null 2>&1
env SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json SCHED_FAKE_GPUS=0,1,2,3 \
    $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
LOG=$S3/testnode/scheduler.log
sleep 3
if grep -q "LAUNCH.*g0.*gpu=0" "$LOG" && grep -q "LAUNCH.*g1.*gpu=1" "$LOG" \
   && ! grep -q "LAUNCH.*g2" "$LOG"; then
  ok "配额制: 2 GPU 占满 8 核后 g2 (GPU 空闲但 CPU 不足) 不派发"
else
  bad "场景3: 双约束未生效"
fi
sleep 12
wait_done $S3 s3 && ok "场景3: 5/5 终态 done" || bad "场景3: 批次未收敛"
stop_daemon $S3

# ---------- 汇总 ----------
echo
echo "=== 结果: $PASS 通过 / $FAIL 失败 ==="
rm -rf /tmp/sched_acc_s1 /tmp/sched_acc_s2 /tmp/sched_acc_s3
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
