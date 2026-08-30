#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_probes_accept.sh — L6 probes 日志门控验收 (fake-gpu 快速回归)
# =============================================================================
# 用途: dispatcher._check_probes (fail_on_log / ready_on_log 消费) 的验证.
#       不烧 GPU (SCHED_FAKE_GPUS), 不依赖真实训练.
#
# 覆盖场景:
#   1. fail_on_log 命中: 组级 kill -> job blocked (failure=probe), 不 retry
#   2. ready_on_log 命中 + 产物存在: 组级 kill -> done (产物校验通过)
#   3. ready_on_log 命中但产物缺失: 降级 failed (failure=artifact)
#   4. probes 未声明: 正常退出码/产物校验路径不受扰
#
# 用法: bash sched/tests/run_probes_accept.sh
# 退出码: 0 = 全过, 1 = 有失败 (输出 FAIL 行)
# =============================================================================
set -u
cd "$(dirname "$0")/.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
source tests/acceptance_cleanup.sh
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install
# submit 会自动 ensure_running；必须在第一次 submit 前固定 fake 模式，避免
# 独立运行本验收时先拉起 real-mode daemon。
export SCHED_FAKE_GPUS="${SCHED_FAKE_GPUS:-0:24}"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() { # $1=state_dir
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli daemon stop >/dev/null 2>&1
}

start_fake_daemon() { # $1=state_dir
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    SCHED_FAKE_GPUS="$SCHED_FAKE_GPUS" \
    "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1
}

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

task_field() { # $1=state_dir $2=<batch id>:<task> $3=field
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli task "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
field = sys.argv[1]
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    jobs = []
job = max(jobs, key=lambda row: row.get("version", -1)) if jobs else {}
value = job.get(field, "")
print("" if value is None else value)' "$3"
}

task_status() { # $1=state_dir $2=<batch id>:<task>
  task_field "$1" "$2" status
}

batch_status() { # $1=state_dir $2=batch_id
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli status "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
wanted = sys.argv[1]
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
row = next((item for item in batches if item.get("batch_id") == wanted), {})
print(row.get("status", ""))' "$2"
}

wait_task_status() { # $1=state_dir $2=<batch id>:<task> $3=status $4=timeout
  for _ in $(seq 1 ${4:-120}); do
    [ "$(task_status "$1" "$2")" = "$3" ] && return 0
    sleep 1
  done
  return 1
}

wait_batch_status() { # $1=state_dir $2=batch_id $3=status $4=timeout
  for _ in $(seq 1 ${4:-120}); do
    [ "$(batch_status "$1" "$2")" = "$3" ] && return 0
    sleep 1
  done
  return 1
}

wait_file() { # $1=path $2=timeout
  for _ in $(seq 1 ${2:-120}); do
    [ -s "$1" ] && return 0
    sleep 1
  done
  return 1
}

wait_task_at_gate() { # $1=state_dir $2=<batch id>:<task> $3=ready file
  wait_task_status "$1" "$2" running 120 \
    && wait_file "$3" 120 \
    && [ "$(task_status "$1" "$2")" = "running" ]
}

wait_process_dead() { # $1=pid $2=timeout
  local observed=""
  for _ in $(seq 1 ${2:-120}); do
    observed=$(ps -o stat= -p "$1" 2>/dev/null | tr -d ' ')
    if [ -z "$observed" ] || [ "${observed#Z}" != "$observed" ]; then
      return 0
    fi
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
  "venvs": {"k": "$PY"}
}
EOF
}

echo "=== probes 日志门控验收 (fake-gpu) ==="

# ---------- 场景 1: fail_on_log 命中 -> blocked (不 retry) ----------
echo "--- 场景 1: fail_on_log 命中 -> blocked ---"
sched_accept_make_root S1 "sched-probes-1"
mk_config "$S1"
cat > "$S1/task.py" << EOF
from pathlib import Path
import time

Path("$S1/ready").write_text("ready", encoding="utf-8")
while not Path("$S1/release").exists():
    time.sleep(0.05)
print("FATAL Traceback boom", flush=True)
time.sleep(300)
EOF
cat > "$S1/batch.json" << EOF
{
  "name": "p1",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "$S1/task.py"],
     "duration_min": 5, "max_retry": 3,
     "probes": {"fail_on_log": "Traceback"}}
  ]
}
EOF
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
$PY -m gsched.cli submit "$S1/batch.json" >/dev/null 2>&1 || { bad "p1 submit 失败"; exit 1; }
P1_BID=$(latest_batch_id "$S1" p1)
[ -n "$P1_BID" ] || { bad "p1 批次 ID 不可见"; stop_daemon "$S1"; exit 1; }
start_fake_daemon "$S1" || { bad "p1 fake daemon 启动失败"; exit 1; }
wait_task_at_gate "$S1" "$P1_BID:t1" "$S1/ready" \
  && ok "t1 进入 running 并等待 gate" \
  || { bad "t1 未在 120s 内进入 gate"; stop_daemon "$S1"; exit 1; }
touch "$S1/release"
wait_task_status "$S1" "$P1_BID:t1" blocked 120 \
  && ok "fail_on_log 命中 -> blocked" \
  || { bad "未 blocked (实际=$(task_status "$S1" "$P1_BID:t1"))"; stop_daemon "$S1"; exit 1; }
# 不 retry: retries 应为 0 (probe 命中视为确定失败)
RETRIES=$(task_field "$S1" "$P1_BID:t1" retries)
FAILURE=$(task_field "$S1" "$P1_BID:t1" failure)
[ "$RETRIES" = "0" ] && [ "$FAILURE" = "probe" ] \
  && ok "probe 命中不 retry (failure=$FAILURE retries=$RETRIES)" \
  || bad "probe 终态字段异常 (failure=$FAILURE retries=$RETRIES)"
wait_batch_status "$S1" "$P1_BID" blocked 120 \
  || { bad "p1 批次未收敛 blocked"; stop_daemon "$S1"; exit 1; }
stop_daemon "$S1" || { bad "p1 daemon 停止失败"; exit 1; }

# ---------- 场景 2: ready_on_log 命中 + 产物存在 -> done ----------
echo "--- 场景 2: ready_on_log 命中 + 产物存在 -> done ---"
sched_accept_make_root S2 "sched-probes-2"
mk_config "$S2"
cat > "$S2/task.py" << EOF
from pathlib import Path
import time

Path("$S2/ready").write_text("ready", encoding="utf-8")
while not Path("$S2/release").exists():
    time.sleep(0.05)
Path("$S2/out.txt").write_text("ok", encoding="utf-8")
print("ALL_DONE", flush=True)
time.sleep(300)
EOF
cat > "$S2/batch.json" << EOF
{
  "name": "p2",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "$S2/task.py"],
     "duration_min": 5,
     "probes": {"ready_on_log": "ALL_DONE"},
     "artifacts": {"out": {"path": "$S2/out.txt"}}, "paths_escape": true}
  ]
}
EOF
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
$PY -m gsched.cli submit "$S2/batch.json" >/dev/null 2>&1 || { bad "p2 submit 失败"; exit 1; }
P2_BID=$(latest_batch_id "$S2" p2)
[ -n "$P2_BID" ] || { bad "p2 批次 ID 不可见"; stop_daemon "$S2"; exit 1; }
start_fake_daemon "$S2" || { bad "p2 fake daemon 启动失败"; exit 1; }
wait_task_at_gate "$S2" "$P2_BID:t1" "$S2/ready" \
  && ok "t1 进入 running 并等待 gate" \
  || { bad "t1 未在 120s 内进入 gate"; stop_daemon "$S2"; exit 1; }
touch "$S2/release"
wait_task_status "$S2" "$P2_BID:t1" done 120 \
  && ok "ready_on_log 命中 + 产物存在 -> done" \
  || { bad "未 done (实际=$(task_status "$S2" "$P2_BID:t1"))"; stop_daemon "$S2"; exit 1; }
[ -f "$S2/out.txt" ] && ok "产物 out.txt 已生成" || bad "产物缺失"
wait_batch_status "$S2" "$P2_BID" done 120 \
  || { bad "p2 批次未收敛 done"; stop_daemon "$S2"; exit 1; }
stop_daemon "$S2" || { bad "p2 daemon 停止失败"; exit 1; }

# ---------- 场景 3: ready_on_log 命中但产物缺失 -> 降级 failed ----------
echo "--- 场景 3: ready_on_log 命中但产物缺失 -> failed ---"
sched_accept_make_root S3 "sched-probes-3"
mk_config "$S3"
cat > "$S3/task.py" << EOF
from pathlib import Path
import time

Path("$S3/ready").write_text("ready", encoding="utf-8")
while not Path("$S3/release").exists():
    time.sleep(0.05)
print("ALL_DONE", flush=True)
time.sleep(300)
EOF
cat > "$S3/batch.json" << EOF
{
  "name": "p3",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "$S3/task.py"],
     "duration_min": 5, "max_retry": 0,
     "probes": {"ready_on_log": "ALL_DONE"},
     "artifacts": {"out": {"path": "$S3/out.txt"}}, "paths_escape": true}
  ]
}
EOF
export SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json
$PY -m gsched.cli submit "$S3/batch.json" >/dev/null 2>&1 || { bad "p3 submit 失败"; exit 1; }
P3_BID=$(latest_batch_id "$S3" p3)
[ -n "$P3_BID" ] || { bad "p3 批次 ID 不可见"; stop_daemon "$S3"; exit 1; }
start_fake_daemon "$S3" || { bad "p3 fake daemon 启动失败"; exit 1; }
wait_task_at_gate "$S3" "$P3_BID:t1" "$S3/ready" \
  && ok "t1 进入 running 并等待 gate" \
  || { bad "t1 未在 120s 内进入 gate"; stop_daemon "$S3"; exit 1; }
touch "$S3/release"
wait_task_status "$S3" "$P3_BID:t1" failed 120 \
  && [ "$(task_field "$S3" "$P3_BID:t1" failure)" = "artifact" ] \
  && ok "ready 命中但产物缺失 -> 降级 failed/artifact" \
  || { bad "未收敛 failed/artifact"; stop_daemon "$S3"; exit 1; }
wait_batch_status "$S3" "$P3_BID" blocked 120 \
  || { bad "p3 批次未收敛 blocked"; stop_daemon "$S3"; exit 1; }
stop_daemon "$S3" || { bad "p3 daemon 停止失败"; exit 1; }

# ---------- 场景 4: 无 probes 任务不受扰 (正常退出码路径) ----------
echo "--- 场景 4: 无 probes 任务走正常 rc/产物路径 ---"
sched_accept_make_root S4 "sched-probes-4"
mk_config "$S4"
cat > "$S4/task.py" << EOF
from pathlib import Path
import time

Path("$S4/ready").write_text("ready", encoding="utf-8")
while not Path("$S4/release").exists():
    time.sleep(0.05)
print("Traceback unrelated")
print("OK")
EOF
cat > "$S4/batch.json" << EOF
{
  "name": "p4",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "$S4/task.py"],
     "duration_min": 5, "max_retry": 0,
     "artifacts": {"out": {"path": "$S4/out.txt"}}, "paths_escape": true}
  ]
}
EOF
export SCHED_STATE=$S4 SCHED_CONFIG=$S4/config.json
$PY -m gsched.cli submit "$S4/batch.json" >/dev/null 2>&1 || { bad "p4 submit 失败"; exit 1; }
P4_BID=$(latest_batch_id "$S4" p4)
[ -n "$P4_BID" ] || { bad "p4 批次 ID 不可见"; stop_daemon "$S4"; exit 1; }
start_fake_daemon "$S4" || { bad "p4 fake daemon 启动失败"; exit 1; }
wait_task_at_gate "$S4" "$P4_BID:t1" "$S4/ready" \
  || { bad "p4:t1 未在 120s 内进入 gate"; stop_daemon "$S4"; exit 1; }
touch "$S4/release"
# 无 probes -> 任务自行退出 rc=0; 产物缺失 -> failed(artifact) -> 重试耗尽
# (max_retry=0, 复核注记) -> blocked 终态。断言 blocked + failure=artifact,
# 而非 failed (failed 是瞬态, 立即被 _maybe_retry 转 blocked)。
wait_task_status "$S4" "$P4_BID:t1" blocked 120 \
  && [ "$(task_field "$S4" "$P4_BID:t1" failure)" = "artifact" ] \
  && ok "无 probes 任务正常收敛 (rc 路径, 产物缺失 -> blocked/artifact)" \
  || { bad "p4:t1 未收敛 blocked/artifact"; stop_daemon "$S4"; exit 1; }
wait_batch_status "$S4" "$P4_BID" blocked 120 \
  || { bad "p4 批次未收敛 blocked"; stop_daemon "$S4"; exit 1; }
stop_daemon "$S4" || { bad "p4 daemon 停止失败"; exit 1; }

# ---------- 场景 5: SIGKILL 升级 (L3) ----------
# 任务忽略 SIGTERM: probe kill 发 SIGTERM 无效 -> daemon 逐轮 SIGKILL 升级
# -> 进程真实死亡 (不与 cancel 脱节, 防占卡直至 releasing 超时)
echo "--- 场景 5: probe SIGKILL 升级 (进程忽略 SIGTERM) ---"
sched_accept_make_root S5 "sched-probes-5"
mk_config "$S5"
cat > "$S5/task.py" << EOF
from pathlib import Path
import os
import signal
import time

signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path("$S5/pid.txt").write_text(str(os.getpid()), encoding="utf-8")
Path("$S5/ready").write_text("ready", encoding="utf-8")
while not Path("$S5/release").exists():
    time.sleep(0.05)
print("FATAL Traceback", flush=True)
time.sleep(300)
EOF
cat > "$S5/batch.json" << EOF
{
  "name": "p5",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "$S5/task.py"],
     "duration_min": 5, "max_retry": 0,
     "probes": {"fail_on_log": "Traceback"}}
  ]
}
EOF
export SCHED_STATE=$S5 SCHED_CONFIG=$S5/config.json
$PY -m gsched.cli submit "$S5/batch.json" >/dev/null 2>&1 || { bad "p5 submit 失败"; exit 1; }
P5_BID=$(latest_batch_id "$S5" p5)
[ -n "$P5_BID" ] || { bad "p5 批次 ID 不可见"; stop_daemon "$S5"; exit 1; }
start_fake_daemon "$S5" || { bad "p5 fake daemon 启动失败"; exit 1; }
wait_task_at_gate "$S5" "$P5_BID:t1" "$S5/ready" \
  || { bad "p5:t1 未在 120s 内进入 gate"; stop_daemon "$S5"; exit 1; }
PID=$(cat "$S5/pid.txt" 2>/dev/null)
[ -n "$PID" ] || { bad "p5 进程 PID 不可见"; stop_daemon "$S5"; exit 1; }
touch "$S5/release"
wait_task_status "$S5" "$P5_BID:t1" blocked 120 \
  && [ "$(task_field "$S5" "$P5_BID:t1" failure)" = "probe" ] \
  && ok "fail_on_log 命中 -> blocked (忽略 SIGTERM 的任务)" \
  || { bad "p5:t1 未收敛 blocked/probe"; stop_daemon "$S5"; exit 1; }
# 用 ps stat 而非 kill -0: SIGKILL 后进程可能成 zombie，kill -0
# 对 zombie 仍返回 0。zombie 不再执行/占卡，视为已杀。
wait_process_dead "$PID" 120 \
  && ok "SIGKILL 升级生效: 忽略 SIGTERM 的进程最终被杀 (pid=$PID)" \
  || { bad "进程未被 SIGKILL 升级杀死 (pid=$PID)"; stop_daemon "$S5"; exit 1; }
wait_batch_status "$S5" "$P5_BID" blocked 120 \
  || { bad "p5 批次未收敛 blocked"; stop_daemon "$S5"; exit 1; }
stop_daemon "$S5" || { bad "p5 daemon 停止失败"; exit 1; }

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" = "0" ]
