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
source tests/acceptance_cleanup.sh
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

latest_batch_id() { # $1=state_dir $2=batch_name
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli status "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
print(batches[0].get("batch_id", "") if batches else "")'
}

start_batch() { # $1=state_dir $2=config $3=batch $4=batch_name $5=fake_gpu_spec
  local st=$1 cfg=$2 batch=$3 name=$4 fake_gpus=$5 bid
  # submit 会调 ensure_running；因此首个可能启动 daemon 的命令前
  # 就必须带上 fake GPU，不能等后续 `daemon start --fake` 补救。
  export SCHED_STATE=$st SCHED_CONFIG=$cfg SCHED_FAKE_GPUS=$fake_gpus
  "$PY" -m gsched.cli submit "$batch" >/dev/null 2>&1 || return 1
  bid=$(latest_batch_id "$st" "$name")
  [ -n "$bid" ] || return 1
  "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1 || return 1
  printf '%s\n' "$bid"
}

task_status() { # $1=state_dir $2=<batch id>:<task>
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli task "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    jobs = []
print(jobs[-1].get("status", "") if jobs else "")'
}

task_gpu() { # $1=state_dir $2=<batch id>:<task>
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli task "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    jobs = []
gpu = jobs[-1].get("gpu") if jobs else None
print("none" if gpu is None else gpu)'
}

pending_like() { # $1=status
  case "$1" in
    pending|waiting_quota|waiting_dep) return 0 ;;
  esac
  return 1
}

wait_first_tick() { # $1=state_dir $2=timeout
  local st=$1
  for _ in $(seq 1 ${2:-120}); do
    if SCHED_STATE=$st SCHED_CONFIG=$st/config.json \
      "$PY" -m gsched.cli status --json 2>/dev/null | \
      "$PY" -c '
import json, sys
try:
    health = json.load(sys.stdin).get("daemon_health", {})
except Exception:
    health = {}
raise SystemExit(0 if health.get("tick_ok_age_s") is not None else 1)'; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_done() { # $1=state_dir $2=batch_id $3=timeout
  local st=$1 bid=$2
  for _ in $(seq 1 ${3:-120}); do
    if SCHED_STATE=$st SCHED_CONFIG=$st/config.json \
      "$PY" -m gsched.cli status "$bid" --json 2>/dev/null | \
      "$PY" -c '
import json, sys
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
raise SystemExit(0 if batches and batches[0].get("status") == "done" else 1)'; then
      return 0
    fi
    sleep 1
  done
  return 1
}

release_barrier() { # $1=release path
  "$PY" -c 'from pathlib import Path; import sys; Path(sys.argv[1]).touch()' "$1"
}

stop_daemon() { # $1=state_dir
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli daemon stop >/dev/null 2>&1
}

echo "=== CPU 配额制调度验收 (fake-gpu) ==="

# ---------- 场景 1: GPU 满 + 排后的 CPU-only 不饿死 ----------
echo "--- 场景 1: 单卡 GPU 满, CPU-only 不被 break 饿死 ---"
sched_accept_make_root S1 "sched-cpu-quota-1"
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
    {"id": "gpu_long", "cmd": ["{VENV:k}", "-c", "import os,time\nwhile not os.path.exists('$S1/release'):\n    time.sleep(0.1)\nopen('$S1/gpu_long.txt','w').write('ok')"], "duration_min": 5, "artifacts": {"a": {"path": "$S1/gpu_long.txt"}}, "paths_escape": true},
    {"id": "cpu_a", "cmd": ["{VENV:k}", "-c", "open('$S1/cpu_a.txt','w').write('ok')"], "resources": {"cpus": 2, "gpu": 0}, "duration_min": 5, "artifacts": {"a": {"path": "$S1/cpu_a.txt"}}, "paths_escape": true},
    {"id": "gpu_b", "cmd": ["{VENV:k}", "-c", "open('$S1/gpu_b.txt','w').write('ok')"], "duration_min": 5, "artifacts": {"a": {"path": "$S1/gpu_b.txt"}}, "paths_escape": true},
    {"id": "cpu_b", "cmd": ["{VENV:k}", "-c", "import os,time\nwhile not os.path.exists('$S1/release'):\n    time.sleep(0.1)\nopen('$S1/cpu_b.txt','w').write('ok')"], "resources": {"cpus": 2, "gpu": 0}, "duration_min": 5, "artifacts": {"a": {"path": "$S1/cpu_b.txt"}}, "paths_escape": true}
  ]
}
EOF
S1_BID=$(start_batch "$S1" "$S1/config.json" "$S1/batch.json" s1 "0:24")
# 修复核心: 首轮 tick 结束时 gpu_long 仍持有唯一 GPU，gpu_b
# 必须继续等卡，而排在它后面的 cpu_b 已派发。释放文件使这个
# 首轮快照不依赖任务时长或主机速度。
if [ -n "$S1_BID" ] && wait_first_tick "$S1" 120; then
  S1_GPU_LONG_STATUS=$(task_status "$S1" "$S1_BID:gpu_long")
  S1_GPU_B_STATUS=$(task_status "$S1" "$S1_BID:gpu_b")
  S1_CPU_B_STATUS=$(task_status "$S1" "$S1_BID:cpu_b")
else
  S1_GPU_LONG_STATUS=missing
  S1_GPU_B_STATUS=missing
  S1_CPU_B_STATUS=missing
fi
if [ "$S1_GPU_LONG_STATUS" = "running" ] \
  && [ "$S1_CPU_B_STATUS" = "running" ] \
  && pending_like "$S1_GPU_B_STATUS"; then
  ok "GPU 满时排后的 cpu_b 同轮派发 (gpu_full continue 修复生效)"
else
  bad "场景1: 首轮快照异常 (gpu_long=$S1_GPU_LONG_STATUS gpu_b=$S1_GPU_B_STATUS cpu_b=$S1_CPU_B_STATUS)"
fi
release_barrier "$S1/release"
wait_done "$S1" "$S1_BID" 120 && ok "场景1: 4/4 终态 done (含 gpu_b 等卡后补位)" || bad "场景1: 批次未收敛"
stop_daemon "$S1" || bad "场景1: daemon 未正常停止"

# ---------- 场景 2: cpus_total 未配置回退 max_cpu_jobs ----------
echo "--- 场景 2: 无 cpus_total -> max_cpu_jobs=2 回退 ---"
sched_accept_make_root S2 "sched-cpu-quota-2"
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
    {"id": "c1", "cmd": ["{VENV:k}", "-c", "import os,time\nwhile not os.path.exists('$S2/release'):\n    time.sleep(0.1)\nopen('$S2/c1.txt','w').write('ok')"], "resources": {"cpus": 8, "gpu": 0}, "duration_min": 5, "artifacts": {"a": {"path": "$S2/c1.txt"}}, "paths_escape": true},
    {"id": "c2", "cmd": ["{VENV:k}", "-c", "import os,time\nwhile not os.path.exists('$S2/release'):\n    time.sleep(0.1)\nopen('$S2/c2.txt','w').write('ok')"], "resources": {"cpus": 8, "gpu": 0}, "duration_min": 5, "artifacts": {"a": {"path": "$S2/c2.txt"}}, "paths_escape": true},
    {"id": "c3", "cmd": ["{VENV:k}", "-c", "import os,time\nwhile not os.path.exists('$S2/release'):\n    time.sleep(0.1)\nopen('$S2/c3.txt','w').write('ok')"], "resources": {"cpus": 8, "gpu": 0}, "duration_min": 5, "artifacts": {"a": {"path": "$S2/c3.txt"}}, "paths_escape": true}
  ]
}
EOF
S2_BID=$(start_batch "$S2" "$S2/config.json" "$S2/batch.json" s2 "0:24")
if [ -n "$S2_BID" ] && wait_first_tick "$S2" 120; then
  S2_C1_STATUS=$(task_status "$S2" "$S2_BID:c1")
  S2_C2_STATUS=$(task_status "$S2" "$S2_BID:c2")
  S2_C3_STATUS=$(task_status "$S2" "$S2_BID:c3")
else
  S2_C1_STATUS=missing
  S2_C2_STATUS=missing
  S2_C3_STATUS=missing
fi
if [ "$S2_C1_STATUS" = "running" ] \
  && [ "$S2_C2_STATUS" = "running" ] \
  && pending_like "$S2_C3_STATUS"; then
  ok "回退模式: 仅 2 个 CPU-only 并发 (max_cpu_jobs=2)"
else
  bad "回退模式: 首轮快照异常 (c1=$S2_C1_STATUS c2=$S2_C2_STATUS c3=$S2_C3_STATUS)"
fi
release_barrier "$S2/release"
wait_done "$S2" "$S2_BID" 120 && ok "场景2: 3/3 终态 done" || bad "场景2: 批次未收敛"
stop_daemon "$S2" || bad "场景2: daemon 未正常停止"

# ---------- 场景 3: 配额制双约束 (cpus_total=8, gpu_job_cpus=4) ----------
echo "--- 场景 3: cpus_total=8/gpu_job_cpus=4, 3 GPU + 2 CPU ---"
sched_accept_make_root S3 "sched-cpu-quota-3"
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
    {"id": "g0", "cmd": ["{VENV:k}", "-c", "import os,time\nwhile not os.path.exists('$S3/release'):\n    time.sleep(0.1)\nopen('$S3/g0.txt','w').write('ok')"], "duration_min": 5, "artifacts": {"a": {"path": "$S3/g0.txt"}}, "paths_escape": true},
    {"id": "g1", "cmd": ["{VENV:k}", "-c", "import os,time\nwhile not os.path.exists('$S3/release'):\n    time.sleep(0.1)\nopen('$S3/g1.txt','w').write('ok')"], "duration_min": 5, "artifacts": {"a": {"path": "$S3/g1.txt"}}, "paths_escape": true},
    {"id": "g2", "cmd": ["{VENV:k}", "-c", "import os,time\nwhile not os.path.exists('$S3/release'):\n    time.sleep(0.1)\nopen('$S3/g2.txt','w').write('ok')"], "duration_min": 5, "artifacts": {"a": {"path": "$S3/g2.txt"}}, "paths_escape": true},
    {"id": "cpu_a", "cmd": ["{VENV:k}", "-c", "import os,time\nwhile not os.path.exists('$S3/release'):\n    time.sleep(0.1)\nopen('$S3/cpu_a.txt','w').write('ok')"], "resources": {"cpus": 1, "gpu": 0}, "duration_min": 5, "artifacts": {"a": {"path": "$S3/cpu_a.txt"}}, "paths_escape": true},
    {"id": "cpu_b", "cmd": ["{VENV:k}", "-c", "import os,time\nwhile not os.path.exists('$S3/release'):\n    time.sleep(0.1)\nopen('$S3/cpu_b.txt','w').write('ok')"], "resources": {"cpus": 2, "gpu": 0}, "duration_min": 5, "artifacts": {"a": {"path": "$S3/cpu_b.txt"}}, "paths_escape": true}
  ]
}
EOF
S3_BID=$(start_batch "$S3" "$S3/config.json" "$S3/batch.json" s3 \
  "0:24,1:24,2:24,3:24")
if [ -n "$S3_BID" ] && wait_first_tick "$S3" 120; then
  S3_G0_STATUS=$(task_status "$S3" "$S3_BID:g0")
  S3_G1_STATUS=$(task_status "$S3" "$S3_BID:g1")
  S3_G2_STATUS=$(task_status "$S3" "$S3_BID:g2")
  S3_CPU_A_STATUS=$(task_status "$S3" "$S3_BID:cpu_a")
  S3_CPU_B_STATUS=$(task_status "$S3" "$S3_BID:cpu_b")
  S3_G0_GPU=$(task_gpu "$S3" "$S3_BID:g0")
  S3_G1_GPU=$(task_gpu "$S3" "$S3_BID:g1")
else
  S3_G0_STATUS=missing
  S3_G1_STATUS=missing
  S3_G2_STATUS=missing
  S3_CPU_A_STATUS=missing
  S3_CPU_B_STATUS=missing
  S3_G0_GPU=missing
  S3_G1_GPU=missing
fi
if [ "$S3_G0_STATUS" = "running" ] && [ "$S3_G0_GPU" = "0" ] \
  && [ "$S3_G1_STATUS" = "running" ] && [ "$S3_G1_GPU" = "1" ] \
  && pending_like "$S3_G2_STATUS" \
  && pending_like "$S3_CPU_A_STATUS" \
  && pending_like "$S3_CPU_B_STATUS"; then
  ok "配额制: 2 GPU 占满 8 核后 g2 (GPU 空闲但 CPU 不足) 不派发"
else
  bad "场景3: 首轮双约束快照异常 (g0=$S3_G0_STATUS/gpu$S3_G0_GPU g1=$S3_G1_STATUS/gpu$S3_G1_GPU g2=$S3_G2_STATUS cpu_a=$S3_CPU_A_STATUS cpu_b=$S3_CPU_B_STATUS)"
fi
release_barrier "$S3/release"
wait_done "$S3" "$S3_BID" 120 && ok "场景3: 5/5 终态 done" || bad "场景3: 批次未收敛"
stop_daemon "$S3" || bad "场景3: daemon 未正常停止"

# ---------- 汇总 ----------
echo
echo "=== 结果: $PASS 通过 / $FAIL 失败 ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
