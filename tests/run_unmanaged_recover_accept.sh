#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
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
cd "$(dirname "$0")/.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
source tests/acceptance_cleanup.sh
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install
# submit 会自动 ensure_running；必须在第一次 submit 前固定 fake 模式，避免
# 独立运行本验收时先拉起 real-mode daemon。
export SCHED_FAKE_GPUS="${SCHED_FAKE_GPUS:-0:24}"
HOST=testnode   # 定案 43 (P6): hostname() 读 config node 字段, 测试 config 统一 node=testnode

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

latest_batch_id() { # $1=state_dir $2=batch_name
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli status "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
wanted = sys.argv[1]
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
row = next((item for item in batches if item.get("batch_name") == wanted), {})
print(row.get("batch_id", ""))' "$2"
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
job = max(jobs, key=lambda row: row.get("version", -1)) if jobs else {}
print(job.get("status", ""))'
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

gpu_status() { # $1=state_dir -> GPU0 exact JSON status
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli status --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    gpus = json.load(sys.stdin).get("gpus", [])
except Exception:
    gpus = []
row = next((item for item in gpus if item.get("idx") == 0), {})
print(row.get("status", ""))'
}

daemon_alive() { # $1=state_dir -> 1 alive / 0 dead
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -c "from gsched import daemon; import sys; sys.exit(0 if daemon.is_running() else 1)" \
    2>/dev/null && echo 1 || echo 0
}

wait_task_status() { # $1=state_dir $2=<batch id>:<task> $3=status $4=timeout
  for _ in $(seq 1 ${4:-120}); do
    [ "$(task_status "$1" "$2")" = "$3" ] && return 0
    sleep 1
  done
  return 1
}

wait_task_batch_gpu() { # $1=state $2=task ref $3=task status $4=batch id $5=batch status $6=gpu status $7=timeout
  for _ in $(seq 1 ${7:-120}); do
    if [ "$(task_status "$1" "$2")" = "$3" ] \
      && [ "$(batch_status "$1" "$4")" = "$5" ] \
      && [ "$(gpu_status "$1")" = "$6" ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_gpu_status() { # $1=state_dir $2=status $3=timeout
  for _ in $(seq 1 ${3:-120}); do
    [ "$(gpu_status "$1")" = "$2" ] && return 0
    sleep 1
  done
  return 1
}

gpu_jobs_count() { # $1=state_dir
  "$PY" -c "
import sqlite3
c = sqlite3.connect('$1/$HOST/state.db')
print(c.execute('SELECT COUNT(*) FROM gpu_jobs').fetchone()[0])
c.close()
" 2>/dev/null
}

wait_gpu_jobs_count() { # $1=state_dir $2=count $3=timeout
  for _ in $(seq 1 ${3:-120}); do
    [ "$(gpu_jobs_count "$1")" = "$2" ] && return 0
    sleep 1
  done
  return 1
}

wait_daemon_state() { # $1=state_dir $2=1 alive/0 dead $3=timeout
  for _ in $(seq 1 ${3:-120}); do
    [ "$(daemon_alive "$1")" = "$2" ] && return 0
    sleep 1
  done
  return 1
}

run_batch() { # $1=state_dir $2=batch path $3=batch name -> exact batch id
  local st=$1 batch=$2 name=$3 output bid
  output=$(env SCHED_STATE="$st" SCHED_CONFIG="$st/config.json" \
    SCHED_FAKE_GPUS="$SCHED_FAKE_GPUS" \
    "$PY" -m gsched.cli submit "$batch" 2>&1) || {
      echo "submit failed: $output" >&2
      return 1
    }
  bid=$(latest_batch_id "$st" "$name")
  if [ -z "$bid" ]; then
    echo "submit succeeded but exact batch id was not visible: $output" >&2
    return 1
  fi
  env SCHED_STATE="$st" SCHED_CONFIG="$st/config.json" \
    SCHED_FAKE_GPUS="$SCHED_FAKE_GPUS" \
    "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1 || return 1
  printf '%s\n' "$bid"
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
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli daemon stop >/dev/null 2>&1 || return 1
  wait_daemon_state "$1" 0 120
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
sched_accept_make_root S1 "sched-unmanaged-1"
mk_config $S1
cat > $S1/batch.json << EOF
{
  "name": "u1",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(10); open('$S1/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S1/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
S1_BID=$(run_batch "$S1" "$S1/batch.json" u1) \
  || { bad "场景1: 提交或 fake daemon 启动失败"; exit 1; }
if wait_task_status "$S1" "$S1_BID:t1" running 120; then
  ok "场景1: 精确任务先进入 running"
else
  bad "场景1: 精确任务未在 120s 内进入 running (实际: $(task_status "$S1" "$S1_BID:t1"))"
fi
if wait_task_batch_gpu "$S1" "$S1_BID:t1" done "$S1_BID" done free 120; then
  ok "场景1: 正常批次完成后 GPU0=free (probe_unmanaged 不误伤)"
else
  bad "场景1: 未收敛为 task=done/batch=done/GPU0=free (实际: task=$(task_status "$S1" "$S1_BID:t1"), batch=$(batch_status "$S1" "$S1_BID"), gpu=$(gpu_status "$S1"))"
fi
stop_daemon "$S1" || bad "场景1: daemon 未在 120s 内停止"

# ---------- 场景 2: 手动置 unmanaged -> 自动回 free (核心) ----------
echo "--- 场景 2: unmanaged 卡自动恢复 ---"
sched_accept_make_root S2 "sched-unmanaged-2"
mk_config $S2
cat > $S2/batch.json << EOF
{
  "name": "u2",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(10); open('$S2/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S2/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
S2_BID=$(run_batch "$S2" "$S2/batch.json" u2) \
  || { bad "场景2: 提交或 fake daemon 启动失败"; exit 1; }
S2_READY=1
if ! wait_task_status "$S2" "$S2_BID:t1" running 120; then
  bad "场景2: 精确任务未在 120s 内进入 running (实际: $(task_status "$S2" "$S2_BID:t1"))"
fi
if ! wait_task_batch_gpu "$S2" "$S2_BID:t1" done "$S2_BID" done free 120; then
  bad "场景2: 前置任务未收敛为 task=done/batch=done/GPU0=free (实际: task=$(task_status "$S2" "$S2_BID:t1"), batch=$(batch_status "$S2" "$S2_BID"), gpu=$(gpu_status "$S2"))"
  S2_READY=0
fi
if [ "$S2_READY" = "1" ]; then
  set_gpu_status "$S2" unmanaged   # 模拟孤儿防线误判
  ST0=$(gpu_status "$S2")
  if [ "$ST0" != "unmanaged" ]; then
    bad "场景2: 前置失败, 未能置为 unmanaged (实际: $ST0)"
  else
    if wait_gpu_status "$S2" free 120; then
      ok "场景2: unmanaged -> 自动回 free (probe_unmanaged 生效)"
    else
      bad "场景2: unmanaged 未在 120s 内自动恢复 (实际: $(gpu_status "$S2"))"
    fi
  fi
fi
stop_daemon "$S2" || bad "场景2: daemon 未在 120s 内停止"
# ---------- 场景 3: 恢复后新批次正常派发完成 ----------
echo "--- 场景 3: unmanaged 恢复后新批次可派发 ---"
sched_accept_make_root S3 "sched-unmanaged-3"
mk_config $S3
cat > $S3/batch.json << EOF
{
  "name": "u3",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(10); open('$S3/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S3/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
S3_BID=$(run_batch "$S3" "$S3/batch.json" u3) \
  || { bad "场景3: 提交或 fake daemon 启动失败"; exit 1; }
if ! wait_task_status "$S3" "$S3_BID:t1" running 120; then
  bad "场景3: 精确任务未在 120s 内进入 running (实际: $(task_status "$S3" "$S3_BID:t1"))"
fi
if wait_task_batch_gpu "$S3" "$S3_BID:t1" done "$S3_BID" done free 120 \
  && [ -f "$S3/t1.txt" ]; then
  ok "场景3: 批次任务正常派发完成 (产物生成)"
else
  bad "场景3: 精确任务/批次/GPU/产物未收敛 (task=$(task_status "$S3" "$S3_BID:t1"), batch=$(batch_status "$S3" "$S3_BID"), gpu=$(gpu_status "$S3"), artifact=$( [ -f "$S3/t1.txt" ] && echo yes || echo no ))"
fi
stop_daemon "$S3" || bad "场景3: daemon 未在 120s 内停止"

# ---------- 场景 5: blocked 批次 retry 后自动回 active (定案 37) ----------
echo "--- 场景 5: blocked 批次 retry 后自动回 active ---"
sched_accept_make_root S5 "sched-unmanaged-5"
mk_config $S5
cat > $S5/batch.json << EOF
{
  "name": "u5",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(30); open('$S5/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S5/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
S5_BID=$(run_batch "$S5" "$S5/batch.json" u5) \
  || { bad "场景5: 提交或 fake daemon 启动失败"; exit 1; }
if ! wait_task_batch_gpu "$S5" "$S5_BID:t1" running "$S5_BID" active assigned 120; then
  bad "场景5: 前置失败, 精确任务未 running (task=$(task_status "$S5" "$S5_BID:t1"), batch=$(batch_status "$S5" "$S5_BID"), gpu=$(gpu_status "$S5"))"
else
  # stop 杀任务 -> 批次 blocked; 重启 daemon
  if ! stop_daemon "$S5"; then
    bad "场景5: daemon 未在 120s 内停止"
  elif ! env SCHED_STATE="$S5" SCHED_CONFIG="$S5/config.json" \
    SCHED_FAKE_GPUS="$SCHED_FAKE_GPUS" \
    "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1; then
    bad "场景5: fake daemon 重启失败"
  elif ! wait_task_batch_gpu "$S5" "$S5_BID:t1" cancelled "$S5_BID" blocked free 120; then
    bad "场景5: stop 后未精确收敛为 task=cancelled/batch=blocked/GPU0=free (实际: task=$(task_status "$S5" "$S5_BID:t1"), batch=$(batch_status "$S5" "$S5_BID"), gpu=$(gpu_status "$S5"))"
  else
    # 人工 retry -> 任务 pending -> daemon 下一轮自动回 active 并重跑
    if ! SCHED_STATE="$S5" SCHED_CONFIG="$S5/config.json" \
      "$PY" -m gsched.cli retry "$S5_BID:t1" >/dev/null 2>&1; then
      bad "场景5: 精确任务 retry 失败"
    elif ! wait_task_status "$S5" "$S5_BID:t1" running 120; then
      bad "场景5: retry 后精确任务未在 120s 内重新 running (实际: $(task_status "$S5" "$S5_BID:t1"))"
    fi
    if wait_task_batch_gpu "$S5" "$S5_BID:t1" done "$S5_BID" done free 120 \
      && [ -f "$S5/t1.txt" ]; then
      ok "场景5: retry 后批次自动回 active 并重跑完成 (无手工 UPDATE)"
    else
      bad "场景5: retry 后未精确收敛 (task=$(task_status "$S5" "$S5_BID:t1"), batch=$(batch_status "$S5" "$S5_BID"), gpu=$(gpu_status "$S5"), artifact=$( [ -f "$S5/t1.txt" ] && echo yes || echo no ))"
    fi
  fi
fi
stop_daemon "$S5" || bad "场景5: daemon 未在 120s 内停止"

# ---------- 场景 6: 空转自动退出 + submit 自动拉起 (定案 38) ----------
echo "--- 场景 6: idle 自动退出 + submit 自动拉起 daemon ---"
sched_accept_make_root S6 "sched-unmanaged-6"
cat > $S6/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S6", "gpus": [0], "idle_timeout_min": 1,
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
# u6a: 提交即自动拉起 daemon (ensure_running, SCHED_FAKE_GPUS 驱动 fake)
cat > $S6/batch_a.json << EOF
{
  "name": "u6a",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S6/a.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S6/a.txt"}}, "paths_escape": true}
  ]
}
EOF
env SCHED_STATE="$S6" SCHED_CONFIG="$S6/config.json" \
    SCHED_FAKE_GPUS="$SCHED_FAKE_GPUS" \
    "$PY" -m gsched.cli submit "$S6/batch_a.json" >/dev/null 2>&1 \
  || { bad "场景6: u6a submit 失败"; exit 1; }
S6A_BID=$(latest_batch_id "$S6" u6a)
if [ -z "$S6A_BID" ]; then
  bad "场景6: u6a 精确批次 ID 不可见"
elif ! wait_daemon_state "$S6" 1 120; then
  bad "场景6: u6a submit 后 daemon 未在 120s 内自动拉起"
elif ! wait_task_batch_gpu "$S6" "$S6A_BID:t1" done "$S6A_BID" done free 120 \
  || [ ! -f "$S6/a.txt" ]; then
  bad "场景6: u6a 未精确完成 (task=$(task_status "$S6" "$S6A_BID:t1"), batch=$(batch_status "$S6" "$S6A_BID"), gpu=$(gpu_status "$S6"), artifact=$( [ -f "$S6/a.txt" ] && echo yes || echo no ))"
else
  # 等 idle 超时 (idle_timeout_min=1 -> 60s + tick 边界)
  if wait_daemon_state "$S6" 0 120; then
    ok "场景6a: 连续 idle 1min 后 daemon 自动退出"
  else
    bad "场景6a: daemon 未在 120s 内自动退出 (idle_timeout 未生效)"
  fi
fi
# u6b: 提交新批次 -> ensure_running 自动拉起 daemon -> 完成
cat > $S6/batch_b.json << EOF
{
  "name": "u6b",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S6/b.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S6/b.txt"}}, "paths_escape": true}
  ]
}
EOF
env SCHED_STATE="$S6" SCHED_CONFIG="$S6/config.json" \
    SCHED_FAKE_GPUS="$SCHED_FAKE_GPUS" \
    "$PY" -m gsched.cli submit "$S6/batch_b.json" >/dev/null 2>&1 \
  || { bad "场景6: u6b submit 失败"; exit 1; }
S6B_BID=$(latest_batch_id "$S6" u6b)
if [ -n "$S6B_BID" ] \
  && wait_daemon_state "$S6" 1 120 \
  && wait_task_batch_gpu "$S6" "$S6B_BID:t1" done "$S6B_BID" done free 120 \
  && [ -f "$S6/b.txt" ] \
  && [ "$(daemon_alive "$S6")" = "1" ]; then
  ok "场景6b: submit 自动拉起 daemon 并完成新批次 (idle 退出后自愈)"
else
  bad "场景6b: 自动拉起未精确收敛 (bid=${S6B_BID:-missing}, task=$( [ -n "$S6B_BID" ] && task_status "$S6" "$S6B_BID:t1" || echo missing ), batch=$( [ -n "$S6B_BID" ] && batch_status "$S6" "$S6B_BID" || echo missing ), gpu=$(gpu_status "$S6"), artifact=$( [ -f "$S6/b.txt" ] && echo yes || echo no ), daemon=$(daemon_alive "$S6"))"
fi
stop_daemon "$S6" || bad "场景6: daemon 未在 120s 内停止"

# ---------- 场景 4: daemon stop 收尾不残留 assigned 卡 (N11 修复) ----------
echo "--- 场景 4: daemon stop 后 GPU 释放不残留 ---"
sched_accept_make_root S4 "sched-unmanaged-4"
mk_config $S4
cat > $S4/batch.json << EOF
{
  "name": "u4",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(30); open('$S4/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S4/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
S4_BID=$(run_batch "$S4" "$S4/batch.json" u4) \
  || { bad "场景4: 提交或 fake daemon 启动失败"; exit 1; }
if ! wait_task_batch_gpu "$S4" "$S4_BID:t1" running "$S4_BID" active assigned 120; then
  bad "场景4: 前置失败, 精确任务未 running (task=$(task_status "$S4" "$S4_BID:t1"), batch=$(batch_status "$S4" "$S4_BID"), gpu=$(gpu_status "$S4"))"
else
  if ! stop_daemon "$S4"; then
    bad "场景4: daemon 未在 120s 内停止"
  # 重启 daemon 让 settle_releasing 把 releasing 转 free (fake 立即)
  elif ! env SCHED_STATE="$S4" SCHED_CONFIG="$S4/config.json" \
    SCHED_FAKE_GPUS="$SCHED_FAKE_GPUS" \
    "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1; then
    bad "场景4: fake daemon 重启失败"
  elif wait_task_batch_gpu "$S4" "$S4_BID:t1" cancelled "$S4_BID" blocked free 120; then
    ok "场景4: daemon stop 后 GPU 释放回 free (不残留 assigned)"
  else
    bad "场景4: stop 后未精确收敛 (task=$(task_status "$S4" "$S4_BID:t1"), batch=$(batch_status "$S4" "$S4_BID"), gpu=$(gpu_status "$S4"))"
  fi
fi
stop_daemon "$S4" || bad "场景4: 收尾 daemon 未在 120s 内停止"

# ---------- 场景 7: gpu_jobs 迁移 + 独占生命周期 (§3.2e A2/B, 定案 39) ----------
echo "--- 场景 7: gpu_jobs 迁移 + 独占模式零行为变化 ---"
# 7a 迁移验证: 独立目录, 手工造旧库 (gpus.job_id 非空, 无 gpu_jobs 表)
sched_accept_make_root S7 "sched-unmanaged-migration"
mk_config "$S7"
SCHED_STATE="$S7" SCHED_CONFIG="$S7/config.json" "$PY" - <<EOF
import os, sqlite3, sys
sys.path.insert(0, '$ROOT')
st = __import__('gsched.state', fromlist=['x'])
db = '$S7/$HOST/state.db'
os.makedirs(os.path.dirname(db), exist_ok=True)
c = sqlite3.connect(db)
c.executescript("""
CREATE TABLE batches (id TEXT PRIMARY KEY, name TEXT NOT NULL, mode TEXT NOT NULL DEFAULT 'mix',
 depends_on TEXT NOT NULL DEFAULT '[]', gpus TEXT, cwd TEXT, env TEXT, status TEXT NOT NULL DEFAULT 'queued', created_at TEXT NOT NULL);
CREATE TABLE tasks (batch_id TEXT NOT NULL, id TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1,
 spec TEXT NOT NULL, order_idx INTEGER NOT NULL, PRIMARY KEY (batch_id, id, version));
CREATE TABLE jobs (id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, task_id TEXT NOT NULL, version INTEGER NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending', gpu INTEGER, pgid INTEGER, kill_reason TEXT, rc INTEGER, failure TEXT,
 retries INTEGER NOT NULL DEFAULT 0, fingerprint TEXT, stage_fingerprints TEXT, git_rev TEXT,
 submitted_at TEXT, started_at TEXT, finished_at TEXT, UNIQUE (batch_id, task_id, version));
CREATE TABLE gpus (idx INTEGER PRIMARY KEY, status TEXT NOT NULL, job_id TEXT,
 quarantined INTEGER NOT NULL DEFAULT 0, ignore_until TEXT, updated_at TEXT);
INSERT INTO gpus VALUES (0,'assigned','job_A',0,NULL,'2026-08-15 12:00:00');
INSERT INTO gpus VALUES (1,'free',NULL,0,NULL,'2026-08-15 12:00:00');
""")
c.commit(); c.close()
st.init_db()
c = sqlite3.connect(db); c.row_factory = sqlite3.Row
rows = [(r['gpu_id'], r['job_id']) for r in c.execute("SELECT * FROM gpu_jobs").fetchall()]
c.close()
assert rows == [(0, 'job_A')], rows
print('MIGRATE_OK')
EOF
if [ $? -eq 0 ]; then ok "场景7a: gpus.job_id 存量行迁移到 gpu_jobs (每卡 1 行)"; else bad "场景7a: 迁移失败"; fi
# 7b 独占生命周期: 干净目录, 正常批次 -> assigned 有 gpu_jobs 行 -> 完成后行清空 + 回 free
sched_accept_make_root S7B "sched-unmanaged-exclusive"
mk_config $S7B
cat > $S7B/batch.json << EOF
{
  "name": "u7b",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(10); open('$S7B/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S7B/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
S7B_BID=$(run_batch "$S7B" "$S7B/batch.json" u7b) \
  || { bad "场景7b: 提交或 fake daemon 启动失败"; exit 1; }
if wait_task_batch_gpu "$S7B" "$S7B_BID:t1" running "$S7B_BID" active assigned 120 \
  && wait_gpu_jobs_count "$S7B" 1 120; then
  ok "场景7b: running/assigned 时 gpu_jobs 精确为 1 行"
else
  bad "场景7b: 运行态记账不精确 (task=$(task_status "$S7B" "$S7B_BID:t1"), batch=$(batch_status "$S7B" "$S7B_BID"), gpu=$(gpu_status "$S7B"), rows=$(gpu_jobs_count "$S7B"))"
fi
if wait_task_batch_gpu "$S7B" "$S7B_BID:t1" done "$S7B_BID" done free 120 \
  && wait_gpu_jobs_count "$S7B" 0 120; then
  ok "场景7b: 独占任务完成后 gpu_jobs 计数归零 + GPU 回 free (零行为变化)"
else
  bad "场景7b: 终态未精确收敛 (task=$(task_status "$S7B" "$S7B_BID:t1"), batch=$(batch_status "$S7B" "$S7B_BID"), gpu=$(gpu_status "$S7B"), rows=$(gpu_jobs_count "$S7B"))"
fi
stop_daemon "$S7B" || bad "场景7b: daemon 未在 120s 内停止"

# ---------- 场景 8: profile 消费 (定案 39, daemon 侧) ----------
echo "--- 场景 8: SCHED_PROFILE_OUT 注入 + upsert + 删临时 ---"
sched_accept_make_root S8 "sched-unmanaged-profile"
mk_config $S8
# 任务: 训练侧模拟 - 写 SCHED_PROFILE_OUT 指向的文件 (peak_gib), 声明 profile_key
cat > $S8/batch.json << EOF
{
  "name": "u8",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import json,os; json.dump({'peak_gib': 3.25}, open(os.environ['SCHED_PROFILE_OUT'],'w')); open('$S8/t1.txt','w').write('ok')"], "duration_min": 1, "resources": {"profile_key": "raft/b158/bs4096"}, "artifacts": {"a": {"path": "$S8/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
S8_BID=$(run_batch "$S8" "$S8/batch.json" u8) \
  || { bad "场景8: 提交或 fake daemon 启动失败"; exit 1; }
if ! wait_task_batch_gpu "$S8" "$S8_BID:t1" done "$S8_BID" done free 120; then
  bad "场景8: 精确任务未收敛为 task=done/batch=done/GPU0=free (实际: task=$(task_status "$S8" "$S8_BID:t1"), batch=$(batch_status "$S8" "$S8_BID"), gpu=$(gpu_status "$S8"))"
fi
R=$($PY -c "
import sqlite3
from gsched.dispatcher import _profile_cache_key
db='$S8/$HOST/state.db'
c=sqlite3.connect(db)
key=_profile_cache_key('default', 'raft/b158/bs4096')
r=c.execute(\"SELECT peak_gib, git_rev FROM profile_cache WHERE profile_key=?\", (key,)).fetchone()
c.close()
print(r[0] if r else 'NONE')" 2>/dev/null)
if [ "$R" != "NONE" ]; then
  ok "场景8a: rc=0 后 upsert profile_cache (peak=$R GiB)"
else
  bad "场景8a: profile_cache 未 upsert (peak=$R)"
fi
# 临时文件应已删除
if [ ! -f "$S8/$HOST/profiles/"*u8*t1*.json ] 2>/dev/null; then
  ok "场景8b: 临时 profile 文件已删除"
else
  bad "场景8b: 临时文件残留: $(ls $S8/$HOST/profiles/ 2>/dev/null)"
fi
stop_daemon "$S8" || bad "场景8: daemon 未在 120s 内停止"

# ---------- 场景 8c: 失败任务只删不 upsert ----------
echo "--- 场景 8c: 失败任务 profile 不入库 ---"
sched_accept_make_root S8C "sched-unmanaged-profile-fail"
mk_config $S8C
cat > $S8C/batch.json << EOF
{
  "name": "u8c",
  "project": "default",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import json,os; json.dump({'peak_gib': 9.9}, open(os.environ['SCHED_PROFILE_OUT'],'w')); exit(3)"], "duration_min": 1, "resources": {"profile_key": "raft/b158/bs4096_fail"}, "artifacts": {"a": {"path": "$S8C/none.txt"}}, "paths_escape": true}
  ]
}
EOF
S8C_BID=$(run_batch "$S8C" "$S8C/batch.json" u8c) \
  || { bad "场景8c: 提交或 fake daemon 启动失败"; exit 1; }
if ! wait_task_batch_gpu "$S8C" "$S8C_BID:t1" blocked "$S8C_BID" blocked free 120; then
  bad "场景8c: 精确失败任务未收敛为 task=blocked/batch=blocked/GPU0=free (实际: task=$(task_status "$S8C" "$S8C_BID:t1"), batch=$(batch_status "$S8C" "$S8C_BID"), gpu=$(gpu_status "$S8C"))"
fi
R=$($PY -c "
import sqlite3
from gsched.dispatcher import _profile_cache_key
db='$S8C/$HOST/state.db'
c=sqlite3.connect(db)
key=_profile_cache_key('default', 'raft/b158/bs4096_fail')
r=c.execute(\"SELECT COUNT(*) FROM profile_cache WHERE profile_key=?\", (key,)).fetchone()[0]
c.close()
print(r)" 2>/dev/null)
if [ "$R" = "0" ] \
  && [ "$(gpu_status "$S8C")" = "free" ] \
  && [ ! -f "$S8C/$HOST/profiles/"*u8c*t1*.json ] 2>/dev/null; then
  ok "场景8c: 失败任务 profile 未入库 + 临时已清 + GPU 回 free"
else
  bad "场景8c: 失败任务 profile 异常入库或未清理 (rows=$R, gpu=$(gpu_status "$S8C"))"
fi
stop_daemon "$S8C" || bad "场景8c: daemon 未在 120s 内停止"

# ---------- 场景 9: H08 releasing 安全判据 (任意 compute pid 均按占用处理) ----------
echo "--- 场景 9: H08 compute pid 占用判据 (框架残留 / 外部进程均 fail-closed) ---"
sched_accept_make_root S9 "sched-unmanaged-external"
SCHED_STATE=$S9 SCHED_FAKE_GPUS=0 $PY - <<'EOF'
import os, sys
import gsched.state as st
st.init_db()
with st.connect() as conn:
    st.init_gpus(conn, [0])
    # 造一个已知 job (pgid=111), 验证已知 sched 残留进程仍按占用处理.
    # _known_job_pgids 只认 running + 近 10min 终态,
    # 故 finished_at 必须是当前时间, 以保留已知残留进程覆盖.
    conn.execute("INSERT INTO jobs (id,batch_id,task_id,version,status,pgid,submitted_at,finished_at) "
                 "VALUES ('known','b','t',1,'done',111,"
                 " datetime('now','localtime'), datetime('now','localtime'))")
from gsched.allocator import Allocator
al = Allocator([0], fake=True)

def set_releasing():
    with st.connect() as conn:
        # 注意: 必须用本地时间 (datetime('now','localtime')) —— 框架 state.now() 是本地,
        # settle_releasing 用 time.mktime 解析 (假定本地); 用 UTC 会算出 ~8h  elapsed
        # 误触发 5min -> unmanaged 分支
        conn.execute("UPDATE gpus SET status='releasing', job_id=NULL, updated_at=datetime('now','localtime') WHERE idx=0")

def gpu_status():
    with st.connect() as conn:
        return conn.execute("SELECT status FROM gpus WHERE idx=0").fetchone()['status']

# 9a: 已知 sched 残留进程 (pid 111 的 pgid 属已知 job pgid=111) -> 占用, 不转 free
set_releasing()
os.environ['SCHED_FAKE_COMPUTE_APPS'] = '0:111'
assert al._card_has_compute(0) is True, al._card_has_compute(0)  # 已知 sched 残留进程占用
assert al.settle_releasing() == ([], [])
assert gpu_status() == 'releasing', gpu_status()
print('9a OK')
# 9b: 外部/非 sched 进程直接隔离为 unmanaged，绝不进入可派发 free 窗口
os.environ['SCHED_FAKE_COMPUTE_APPS'] = '0:999'
assert al._card_has_compute(0) is True, al._card_has_compute(0)  # 外部 compute 进程占用
assert al.settle_releasing() == ([], [])
assert gpu_status() == 'unmanaged', gpu_status()
print('9b OK')
# 9c: 无进程 -> free (常规路径不受影响)
set_releasing()
os.environ['SCHED_FAKE_COMPUTE_APPS'] = ''
assert al._card_has_compute(0) is False
assert al.settle_releasing() == ([0], [])
assert gpu_status() == 'free', gpu_status()
print('9c OK')
print('H08_OK')
EOF
if [ $? -eq 0 ]; then ok "场景9: H08 占用判据 (框架残留 / 外部进程均占用 / 无进程即 free)"; else bad "场景9: H08 判据失败"; fi

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" -eq 0 ] || exit 1
