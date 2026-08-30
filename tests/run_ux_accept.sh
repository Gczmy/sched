#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_ux_accept.sh — 使用体验优化验收 (fake-gpu 快速回归)
# =============================================================================
# 覆盖场景 (P1-P5/P7):
#   1. P2 批次级 retry: `sched retry <batch>` (无 :task) 一次性解锁全部失败终态
#   2. P1 cmd_diag: 一站式诊断 (失败任务 + 实际命令 + git 对比 + 日志尾部)
#   3. P3 git rev 警告: retry 时检测任务 git_rev 与当前仓库 rev 不一致 -> 警告
#   4. P4 status 进度列: running 任务显示 epoch/trial 进度
#   5. P5 status --detail 时间字段 + P7 终态 marker
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
# submit 会自动 ensure_running；必须在第一次 submit 前固定 fake 模式，避免
# 独立运行本验收时先拉起 real-mode daemon。显存声明也固定，避免远端默认值差异。
export SCHED_FAKE_GPUS=0:24

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

latest_batch_id() { # $1=state_dir $2=唯一 batch_name
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli status "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
wanted = sys.argv[1]
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
rows = [row for row in batches if row.get("batch_name") == wanted]
print(rows[0].get("batch_id", "") if len(rows) == 1 else "")' "$2"
}

submit_batch_id() { # $1=state_dir $2=spec path $3=唯一 batch_name
  local output bid
  output=$(SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    SCHED_FAKE_GPUS="$SCHED_FAKE_GPUS" \
    "$PY" -m gsched.cli submit "$2" 2>&1) || {
      echo "submit failed: $output" >&2
      return 1
    }
  for _ in $(seq 1 120); do
    bid=$(latest_batch_id "$1" "$3")
    if [ -n "$bid" ]; then
      printf '%s\n' "$bid"
      return 0
    fi
    sleep 1
  done
  echo "submit succeeded but exact batch id was not visible: $output" >&2
  return 1
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

batch_status() { # $1=state_dir $2=batch id
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

batch_snapshot() { # $1=state_dir $2=batch id; coherent status --json snapshot
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli status "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
wanted = sys.argv[1]
try:
    payload = json.load(sys.stdin)
except Exception:
    payload = {}
batches = payload.get("batches", [])
batch = next((row for row in batches if row.get("batch_id") == wanted), {})
jobs = [row for row in payload.get("jobs", []) if row.get("batch_id") == wanted]
parts = ["{}={}".format(row.get("task", ""), row.get("status", ""))
         for row in sorted(jobs, key=lambda row: row.get("task", ""))]
print("batch={};{}".format(batch.get("status", ""), ",".join(parts)))' "$2"
}

job_progress() { # $1=state_dir $2=batch id $3=task
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli status "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
wanted_batch, wanted_task = sys.argv[1:]
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    jobs = []
row = next((item for item in jobs
            if item.get("batch_id") == wanted_batch
            and item.get("task") == wanted_task), {})
print(row.get("progress") or "")' "$2" "$3"
}

wait_task_status() { # $1=state_dir $2=<batch id>:<task> $3=status $4=timeout
  for _ in $(seq 1 ${4:-120}); do
    [ "$(task_status "$1" "$2")" = "$3" ] && return 0
    sleep 1
  done
  return 1
}

wait_batch_status() { # $1=state_dir $2=batch id $3=status $4=timeout
  for _ in $(seq 1 ${4:-120}); do
    [ "$(batch_status "$1" "$2")" = "$3" ] && return 0
    sleep 1
  done
  return 1
}

wait_batch_snapshot() { # $1=state_dir $2=batch id $3=exact snapshot $4=timeout
  for _ in $(seq 1 ${4:-120}); do
    [ "$(batch_snapshot "$1" "$2")" = "$3" ] && return 0
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

wait_marker() { # $1=path $2=timeout
  for _ in $(seq 1 ${2:-120}); do
    [ -f "$1" ] && return 0
    sleep 1
  done
  return 1
}

wait_task_at_gate() { # $1=state_dir $2=<batch id>:<task> $3=ready path
  wait_task_status "$1" "$2" running 120 \
    && wait_file "$3" 120 \
    && [ "$(task_status "$1" "$2")" = "running" ]
}

wait_job_progress() { # $1=state_dir $2=batch id $3=task $4=expected $5=timeout
  for _ in $(seq 1 ${5:-120}); do
    [ "$(job_progress "$1" "$2" "$3")" = "$4" ] && return 0
    sleep 1
  done
  return 1
}

wait_detail_fields() { # $1=state_dir $2=batch id $3=output path $4=timeout
  for _ in $(seq 1 ${4:-120}); do
    SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
      "$PY" -m gsched.cli status "$2" --detail > "$3" 2>&1
    if grep -q "start=" "$3" && grep -q "耗时=" "$3"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_fake_daemon() { # $1=state_dir
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json SCHED_FAKE_GPUS="$SCHED_FAKE_GPUS" \
    "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1
}

daemon_status_text() { # $1=state_dir; daemon status 当前无 JSON 形式
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli daemon status 2>/dev/null
}

stop_daemon() { # $1=state_dir; 仅通过公开 CLI 停止并确认
  local output
  output=$(SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli daemon stop 2>&1) || {
      echo "daemon stop failed: $output" >&2
      return 1
    }
  for _ in $(seq 1 120); do
    [ "$(daemon_status_text "$1")" = "未运行" ] && return 0
    sleep 1
  done
  echo "daemon did not stop: $(daemon_status_text "$1")" >&2
  return 1
}

mk_config() { # $1=state_dir $2=project_root $3=git(true/false)
  cat > "$1/config.json" << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$1", "gpus": [0],
  "projects": {"default": {"root": "$2", "git": $3}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
}

echo "=== 使用体验优化验收 (fake-gpu) ==="

# ---------- 场景 1+2+3: 批次级 retry + diag + 真实 git rev 漂移 ----------
# 设计: 任务用 flag 文件实现“首跑失败、retry 后成功”。项目在临时 git 仓库中
# 提交 rev A；t3 首跑失败后再真实提交 rev B，验证 retry 的 P3 警告。
echo "--- 场景 1+2+3: retry 解锁 + diag + git rev 警告 ---"
sched_accept_make_root S1 "sched-ux-retry" || exit 1
mkdir -p "$S1/project"
git init -q "$S1/project" \
  && git -C "$S1/project" config user.name "sched acceptance" \
  && git -C "$S1/project" config user.email "sched-accept@example.invalid" \
  && git -C "$S1/project" config commit.gpgsign false \
  || { bad "临时 git 项目初始化失败"; exit 1; }
printf 'rev-a\n' > "$S1/project/tracked.txt"
git -C "$S1/project" add tracked.txt \
  && git -C "$S1/project" commit --no-gpg-sign -qm "rev A" \
  || { bad "临时 git rev A 提交失败"; exit 1; }
REV_A=$(git -C "$S1/project" rev-parse HEAD) || exit 1
mk_config "$S1" "$S1/project" true
cat > "$S1/batch.json" << EOF
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
U1_BID=$(submit_batch_id "$S1" "$S1/batch.json" u1) \
  || { bad "u1 submit/批次 ID 获取失败"; exit 1; }
start_fake_daemon "$S1" || { bad "u1 fake daemon 启动失败"; exit 1; }
if wait_batch_snapshot "$S1" "$U1_BID" \
  "batch=blocked;t1=done,t2=blocked,t3=pending" 120; then
  ok "同一 JSON 快照确认 t1 done / t2 blocked / t3 pending"
else
  bad "u1 首轮未收敛到精确状态 (got $(batch_snapshot "$S1" "$U1_BID"))"
  stop_daemon "$S1"
  exit 1
fi

# P1: diag 精确批次 (列出非 done/skip: t2 blocked + t3 pending)
SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json \
  "$PY" -m gsched.cli diag "$U1_BID" > "$S1/diag_out.txt" 2>&1
grep -Fq "$U1_BID:t2" "$S1/diag_out.txt" && ok "diag 列出精确 blocked 任务" \
  || bad "diag 缺 blocked 任务 (输出: $(tail -15 "$S1/diag_out.txt"))"
grep -q "boom" "$S1/diag_out.txt" && ok "diag 含日志尾部内容" || bad "diag 无日志尾部"
grep -q "cmd:" "$S1/diag_out.txt" && ok "diag 含实际命令" || bad "diag 无命令展示"

# P2: 批次级 retry (无 :task) -> 解锁 t2, 批次回 active, t3 继续。
SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json \
  "$PY" -m gsched.cli retry "$U1_BID" > "$S1/retry_out.txt" 2>&1
grep -q "已解锁重跑" "$S1/retry_out.txt" && ok "批次级 retry 解锁了失败任务" \
  || bad "retry 未解锁 (输出: $(cat "$S1/retry_out.txt"))"
if wait_batch_snapshot "$S1" "$U1_BID" \
  "batch=blocked;t1=done,t2=done,t3=blocked" 120; then
  ok "retry 后精确确认 t2 done / t3 blocked"
else
  bad "首次 retry 未收敛到精确状态 (got $(batch_snapshot "$S1" "$U1_BID"))"
  stop_daemon "$S1"
  exit 1
fi

CAPTURED_REV=$(task_field "$S1" "$U1_BID:t3" git_rev)
[ "$CAPTURED_REV" = "$REV_A" ] \
  && ok "t3 记录真实提交 rev A" \
  || { bad "t3 git_rev 非 rev A (got $CAPTURED_REV)"; stop_daemon "$S1"; exit 1; }
printf 'rev-b\n' > "$S1/project/tracked.txt"
git -C "$S1/project" add tracked.txt \
  && git -C "$S1/project" commit --no-gpg-sign -qm "rev B" \
  || { bad "临时 git rev B 提交失败"; stop_daemon "$S1"; exit 1; }
REV_B=$(git -C "$S1/project" rev-parse HEAD) || exit 1
[ "$REV_A" != "$REV_B" ] \
  || { bad "临时 git revision 未变化"; stop_daemon "$S1"; exit 1; }

SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json \
  "$PY" -m gsched.cli retry "$U1_BID" > "$S1/retry2_out.txt" 2>&1
REV_A_SHORT=${REV_A:0:12}
REV_B_SHORT=${REV_B:0:12}
if grep -q "代码已更新" "$S1/retry2_out.txt" \
  && grep -Fq "$REV_A_SHORT" "$S1/retry2_out.txt" \
  && grep -Fq "$REV_B_SHORT" "$S1/retry2_out.txt"; then
  ok "P3 真实 git rev A -> rev B 警告输出"
else
  bad "P3 警告缺失/版本不符 (输出: $(cat "$S1/retry2_out.txt"))"
fi
if wait_batch_snapshot "$S1" "$U1_BID" \
  "batch=done;t1=done,t2=done,t3=done" 120 \
  && wait_marker "$S1/testnode/markers/u1.done" 120; then
  ok "第二次 retry 后精确批次全部 done 且 marker 已写"
else
  bad "第二次 retry 未收敛 (got $(batch_snapshot "$S1" "$U1_BID"))"
  stop_daemon "$S1"
  exit 1
fi
stop_daemon "$S1" && ok "u1 daemon 经公开 CLI 停止并确认" \
  || { bad "u1 daemon 停止确认失败"; exit 1; }

# ---------- 场景 5: status --detail (P5) + 终态 marker (P7) ----------
echo "--- 场景 5: status --detail + sched markers ---"
sched_accept_make_root S3 "sched-ux-detail" || exit 1
mk_config "$S3" "$ROOT" false
cat > "$S3/detail_task.py" << EOF
from pathlib import Path
import time

print("Epoch 1/3", flush=True)
Path("$S3/ready").write_text("ready", encoding="utf-8")
while not Path("$S3/release").exists():
    time.sleep(0.05)
EOF
cat > "$S3/batch.json" << EOF
{
  "name": "u5",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "$S3/detail_task.py"], "duration_min": 5},
    {"id": "t2", "cmd": ["{VENV:k}", "-c", "print('ok2')"], "duration_min": 1}
  ]
}
EOF
U5_BID=$(submit_batch_id "$S3" "$S3/batch.json" u5) \
  || { bad "u5 submit/批次 ID 获取失败"; exit 1; }
start_fake_daemon "$S3" || { bad "u5 fake daemon 启动失败"; exit 1; }
if wait_task_at_gate "$S3" "$U5_BID:t1" "$S3/ready" \
  && wait_batch_status "$S3" "$U5_BID" active 120; then
  ok "u5:t1 在 gate 中稳定保持 running"
else
  bad "u5:t1 未进入稳定 running gate"
  stop_daemon "$S3"
  exit 1
fi
if wait_detail_fields "$S3" "$U5_BID" "$S3/detail_out.txt" 120; then
  ok "P5 精确批次 status --detail 含起止时间/耗时"
else
  bad "P5 --detail 缺时间字段 (输出: $(head -8 "$S3/detail_out.txt"))"
fi
touch "$S3/release"
if wait_batch_snapshot "$S3" "$U5_BID" \
  "batch=done;t1=done,t2=done" 120 \
  && wait_marker "$S3/testnode/markers/u5.done" 120; then
  ok "u5 精确批次 done 且 marker 已写"
else
  bad "u5 未收敛 (got $(batch_snapshot "$S3" "$U5_BID"))"
  stop_daemon "$S3"
  exit 1
fi
SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json \
  "$PY" -m gsched.cli markers > "$S3/markers_out.txt" 2>&1
grep -q "u5.done" "$S3/markers_out.txt" && ok "sched markers 列出 u5.done" \
  || bad "sched markers 缺 u5.done (输出: $(cat "$S3/markers_out.txt"))"

# blocked marker: 失败任务也应有 marker。
cat > "$S3/bad.json" << EOF
{
  "name": "u5bad",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "x1", "max_retry": 0,
     "cmd": ["{VENV:k}", "-c", "import sys; print('boomx'); sys.exit(1)"], "duration_min": 1}
  ]
}
EOF
U5_BAD_BID=$(submit_batch_id "$S3" "$S3/bad.json" u5bad) \
  || { bad "u5bad submit/批次 ID 获取失败"; stop_daemon "$S3"; exit 1; }
if wait_batch_snapshot "$S3" "$U5_BAD_BID" \
  "batch=blocked;x1=blocked" 120 \
  && wait_marker "$S3/testnode/markers/u5bad.blocked" 120; then
  ok "u5bad 精确批次 blocked 且 marker 已写"
else
  bad "u5bad 未收敛 (got $(batch_snapshot "$S3" "$U5_BAD_BID"))"
  stop_daemon "$S3"
  exit 1
fi
grep -q "x1" "$S3/testnode/markers/u5bad.blocked" \
  && ok "P7 blocked marker 含失败任务列表" \
  || bad "blocked marker 无失败任务列表"
SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json \
  "$PY" -m gsched.cli markers > "$S3/markers2_out.txt" 2>&1
grep -q "u5bad.blocked" "$S3/markers2_out.txt" && ok "sched markers 列出 blocked" \
  || bad "sched markers 缺 blocked (输出: $(cat "$S3/markers2_out.txt"))"
stop_daemon "$S3" && ok "u5/u5bad daemon 经公开 CLI 停止并确认" \
  || { bad "u5 daemon 停止确认失败"; exit 1; }

# ---------- 场景 4: status 进度列 (P4) ----------
echo "--- 场景 4: running 任务 status 显示进度列 ---"
sched_accept_make_root S2 "sched-ux-progress" || exit 1
mk_config "$S2" "$ROOT" false
cat > "$S2/progress_task.py" << EOF
from pathlib import Path
import time

print("Epoch 3/30", flush=True)
Path("$S2/ready").write_text("ready", encoding="utf-8")
while not Path("$S2/release").exists():
    time.sleep(0.05)
EOF
cat > "$S2/batch.json" << EOF
{
  "name": "u4",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "$S2/progress_task.py"],
     "progress_regex": "Epoch [0-9]+/[0-9]+", "duration_min": 5}
  ]
}
EOF
U4_BID=$(submit_batch_id "$S2" "$S2/batch.json" u4) \
  || { bad "u4 submit/批次 ID 获取失败"; exit 1; }
start_fake_daemon "$S2" || { bad "u4 fake daemon 启动失败"; exit 1; }
if wait_task_at_gate "$S2" "$U4_BID:t1" "$S2/ready" \
  && wait_batch_status "$S2" "$U4_BID" active 120; then
  ok "u4:t1 在 gate 中稳定保持 running"
else
  bad "u4:t1 未进入稳定 running gate"
  stop_daemon "$S2"
  exit 1
fi
if wait_job_progress "$S2" "$U4_BID" t1 "Epoch 3/30" 120; then
  ok "精确 status --json 快照记录进度 Epoch 3/30"
else
  bad "进度未在 120s 内入库 (got $(job_progress "$S2" "$U4_BID" t1))"
  stop_daemon "$S2"
  exit 1
fi
SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json \
  "$PY" -m gsched.cli status "$U4_BID" > "$S2/status_out.txt" 2>&1
grep -q "3/30" "$S2/status_out.txt" && ok "status 显示进度 3/30" \
  || bad "status 无进度列 (输出: $(grep 'u4' "$S2/status_out.txt"))"
touch "$S2/release"
if wait_batch_snapshot "$S2" "$U4_BID" "batch=done;t1=done" 120 \
  && wait_marker "$S2/testnode/markers/u4.done" 120; then
  ok "u4 精确批次 done 且 marker 已写"
else
  bad "u4 未收敛 (got $(batch_snapshot "$S2" "$U4_BID"))"
  stop_daemon "$S2"
  exit 1
fi
stop_daemon "$S2" && ok "u4 daemon 经公开 CLI 停止并确认" \
  || { bad "u4 daemon 停止确认失败"; exit 1; }

# 主动执行共享清理并验证 root/claim anchor 均无残留；EXIT trap 因 DONE=1 将 no-op。
CLEANUP_PATHS=("$S1" "$S3" "$S2" "${SCHED_ACCEPT_CLEANUP_ANCHORS[@]}")
sched_accept_cleanup
CLEANUP_RESIDUE=0
for path in "${CLEANUP_PATHS[@]}"; do
  if [ -e "$path" ] || [ -L "$path" ]; then
    echo "  cleanup residue: $path" >&2
    CLEANUP_RESIDUE=1
  fi
done
[ "$CLEANUP_RESIDUE" = "0" ] && ok "共享 cleanup 无 root/claim 残留" \
  || bad "共享 cleanup 保留了验收目录或 claim"

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" -eq 0 ]
