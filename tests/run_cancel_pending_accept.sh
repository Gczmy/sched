#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
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
cd "$(dirname "$0")/.."   # 仓库根
source tests/acceptance_cleanup.sh
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install
# submit 会自动 ensure_running；必须在第一次 submit 前固定 fake 模式，避免
# 独立运行本验收时先拉起 real-mode daemon。
export SCHED_FAKE_GPUS="${SCHED_FAKE_GPUS:-0:24}"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  "$PY" -m gsched.cli daemon stop >/dev/null 2>&1
}

# 任务状态计数: $1=state_dir $2=batch_name $3=status -> 数量
count_status() {
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

# 轮询等待任务状态达到预期 (daemon reap 有 poll 间隔, kill 后需等下一轮 tick)
wait_status() { # $1=state_dir $2=batch_name $3=status $4=期望数 $5=超时秒(默认15)
  local st=$1 bn=$2 want=$3 exp=$4 timeout=${5:-15}
  for _ in $(seq 1 $timeout); do
    [ "$(count_status $st $bn $want)" = "$exp" ] && return 0
    sleep 1
  done
  return 1
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

wait_task_status() { # $1=state_dir $2=<batch id>:<task> $3=status $4=timeout
  for _ in $(seq 1 ${4:-120}); do
    [ "$(task_status "$1" "$2")" = "$3" ] && return 0
    sleep 1
  done
  return 1
}

task_pending_like() { # $1=state_dir $2=<batch id>:<task>
  case "$(task_status "$1" "$2")" in
    pending|waiting_quota|waiting_dep) return 0 ;;
  esac
  return 1
}

wait_task_pending_like() { # $1=state_dir $2=<batch id>:<task> $3=timeout
  for _ in $(seq 1 ${3:-120}); do
    task_pending_like "$1" "$2" && return 0
    sleep 1
  done
  return 1
}

wait_batch_status() { # $1=state_dir $2=batch id $3=status $4=timeout
  local actual
  for _ in $(seq 1 ${4:-120}); do
    actual=$(SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
      "$PY" -m gsched.cli status "$2" --json 2>/dev/null | \
      "$PY" -c '
import json, sys
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
print(batches[0].get("status", "") if batches else "")')
    [ "$actual" = "$3" ] && return 0
    sleep 1
  done
  return 1
}

batch_task_states() { # $1=state_dir $2=batch id; one coherent status snapshot
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli status "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    jobs = []
parts = []
for job in sorted(jobs, key=lambda item: item.get("task", "")):
    wait_reason = job.get("wait_reason") or "-"
    parts.append("{}={}:{}".format(
        job.get("task", ""), job.get("status", ""), wait_reason
    ))
print(",".join(parts))'
}

wait_batch_task_states() { # $1=state_dir $2=batch id $3=exact snapshot $4=timeout
  for _ in $(seq 1 ${4:-120}); do
    [ "$(batch_task_states "$1" "$2")" = "$3" ] && return 0
    sleep 1
  done
  return 1
}

wait_marker() { # $1=marker path $2=timeout
  for _ in $(seq 1 ${2:-120}); do
    [ -f "$1" ] && return 0
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

echo "=== cancel 排队任务验收 (fake-gpu) ==="

# ---------- 场景 1+4: 批次级 cancel (running + pending) + Q4 下游告警 ----------
echo "--- 场景 1+4: 批次 cancel 收敛 running + pending, 提示下游 ---"
sched_accept_make_root S1 "sched-cancel-pending-1"
mk_config $S1
cat > $S1/batch.json << EOF
{
  "name": "c1",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(300)"], "duration_min": 10},
    {"id": "t2", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(300)"], "duration_min": 10},
    {"id": "t3", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(300)"], "duration_min": 10}
  ]
}
EOF
cat > $S1/down.json << EOF
{
  "name": "c1_down",
  "project": "default", "mode": "mix", "depends_on": ["c1"],
  "tasks": [
    {"id": "d1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(300)"], "duration_min": 10}
  ]
}
EOF
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
$PY -m gsched.cli submit $S1/batch.json >/dev/null 2>&1 || { bad "c1 submit 失败"; exit 1; }
C1_BID=$(latest_batch_id "$S1" c1)
[ -n "$C1_BID" ] || { bad "c1 批次 ID 不可见"; exit 1; }
$PY -m gsched.cli submit $S1/down.json >/dev/null 2>&1 || { bad "c1_down submit 失败"; exit 1; }
DOWN_BID=$(latest_batch_id "$S1" c1_down)
[ -n "$DOWN_BID" ] || { bad "c1_down 批次 ID 不可见"; exit 1; }
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
if wait_batch_task_states "$S1" "$C1_BID" \
  "t1=running:-,t2=pending:-,t3=pending:-" 120; then
  ok "同一快照中 t1 running、t2/t3 pending"
else
  bad "c1 未在 120s 内进入预期单卡调度状态 (got $(batch_task_states "$S1" "$C1_BID"))"
  stop_daemon "$S1"
  exit 1
fi
[ "$(task_status "$S1" "$DOWN_BID:d1")" = "pending" ] \
  && wait_batch_status "$S1" "$DOWN_BID" queued 1 \
  && ok "精确下游实例保持 queued/pending" \
  || { bad "下游实例未保持依赖挂起"; stop_daemon "$S1"; exit 1; }
# --yes 门禁
BEFORE_GATE=$(batch_task_states "$S1" "$C1_BID")
if $PY -m gsched.cli cancel "$C1_BID" >/dev/null 2>&1; then
  bad "不带 --yes 竟执行了 cancel"
else
  ok "--yes 确认门禁生效 (不带 --yes 拒绝)"
fi
[ "$(batch_task_states "$S1" "$C1_BID")" = "$BEFORE_GATE" ] \
  && ok "--yes 门禁无任务状态副作用" \
  || { bad "--yes 门禁调用改变了任务状态"; stop_daemon "$S1"; exit 1; }
# 批次级 cancel
if ! $PY -m gsched.cli cancel "$C1_BID" --yes > $S1/cancel_out.txt 2>&1; then
  bad "c1 批次 cancel 命令失败"
  stop_daemon "$S1"
  exit 1
fi
if wait_task_status "$S1" "$C1_BID:t1" cancelled 120 \
  && wait_task_status "$S1" "$C1_BID:t2" cancelled 120 \
  && wait_task_status "$S1" "$C1_BID:t3" cancelled 120; then
  ok "3 任务全部 cancelled (1 running kill + 2 pending 直标)"
  ok "无 running/pending 残留"
else
  bad "c1 任务未全部收敛 cancelled"
  stop_daemon "$S1"
  exit 1
fi
grep -q "c1_down" $S1/cancel_out.txt && ok "Q4 下游依赖告警输出 (c1_down 挂起提示)" \
  || bad "Q4 告警缺失 (输出: $(cat $S1/cancel_out.txt))"
# 批次收敛 blocked (cancelled 是终态)
if wait_batch_status "$S1" "$C1_BID" blocked 120 \
  && wait_marker "$S1/testnode/markers/c1.blocked" 120; then
  ok "批次 c1 收敛 blocked"
else
  bad "批次 c1 未收敛 blocked"
  stop_daemon "$S1"
  exit 1
fi
[ "$(task_status "$S1" "$DOWN_BID:d1")" = "pending" ] \
  && wait_batch_status "$S1" "$DOWN_BID" queued 1 \
  && ok "上游取消后精确下游实例仍未误派发" \
  || { bad "上游取消后下游实例状态异常"; stop_daemon "$S1"; exit 1; }
stop_daemon "$S1" || { bad "S1 daemon stop 失败"; exit 1; }

# ---------- 场景 2+3: 任务级 cancel 只取消单个 ----------
echo "--- 场景 2+3: 任务级 cancel <batch>:<task> 只取消单个 ---"
sched_accept_make_root S2 "sched-cancel-pending-2"
mk_config $S2
cat > $S2/batch.json << EOF
{
  "name": "c2",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(300)"], "duration_min": 10},
    {"id": "t2", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(300)"], "duration_min": 10},
    {"id": "t3", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(300)"], "duration_min": 10}
  ]
}
EOF
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
$PY -m gsched.cli submit $S2/batch.json >/dev/null 2>&1 || { bad "c2 submit 失败"; exit 1; }
C2_BID=$(latest_batch_id "$S2" c2)
[ -n "$C2_BID" ] || { bad "c2 批次 ID 不可见"; exit 1; }
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
wait_batch_task_states "$S2" "$C2_BID" \
  "t1=running:-,t2=pending:-,t3=pending:-" 120 \
  || { bad "c2 未在 120s 内进入预期单卡调度状态"; stop_daemon "$S2"; exit 1; }
if ! $PY -m gsched.cli cancel "$C2_BID:t2" --yes > $S2/cancel2_out.txt 2>&1; then
  bad "c2:t2 cancel 命令失败"
  stop_daemon "$S2"
  exit 1
fi
wait_task_status "$S2" "$C2_BID:t2" cancelled 30 \
  && ok "任务级 cancel 只取消 t2 (1 个 cancelled)" \
  || { bad "c2:t2 未收敛 cancelled"; stop_daemon "$S2"; exit 1; }
[ "$(task_status "$S2" "$C2_BID:t1")" = "running" ] \
  && ok "t1 继续 running (不受影响)" \
  || { bad "t1 受影响"; stop_daemon "$S2"; exit 1; }
task_pending_like "$S2" "$C2_BID:t3" \
  && ok "t3 仍为排队态 (不受影响)" \
  || { bad "t3 受影响"; stop_daemon "$S2"; exit 1; }
grep -q "t2-v1" $S2/cancel2_out.txt && ok "取消输出指明 c2:t2" \
  || bad "取消输出未指明任务 (输出: $(cat $S2/cancel2_out.txt))"
# 收尾: 批次 cancel 全杀 (t1 running + t3 pending)
if ! $PY -m gsched.cli cancel "$C2_BID" --yes >/dev/null 2>&1; then
  bad "c2 批次 cancel 命令失败"
  stop_daemon "$S2"
  exit 1
fi
if wait_task_status "$S2" "$C2_BID:t1" cancelled 120 \
  && wait_task_status "$S2" "$C2_BID:t2" cancelled 120 \
  && wait_task_status "$S2" "$C2_BID:t3" cancelled 120; then
  ok "批次 cancel 收尾: 3 任务全 cancelled"
else
  bad "c2 收尾未全部收敛 cancelled"
  stop_daemon "$S2"
  exit 1
fi
wait_batch_status "$S2" "$C2_BID" blocked 120 || {
  bad "c2 批次未收敛 blocked"
  stop_daemon "$S2"
  exit 1
}

stop_daemon "$S2" || { bad "S2 daemon stop 失败"; exit 1; }

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" = "0" ]
