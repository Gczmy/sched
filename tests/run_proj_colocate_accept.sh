#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_proj_colocate_accept.sh — B12-b 项目级 colocate 开关验收 (fake-gpu)
# =============================================================================
# 覆盖场景:
#   S1a 项目缺省(中立) + 全局开     -> 两个 gpu_share 任务同卡并发装箱
#   S1b 项目 colocate=false         -> 任务降级独占, 串行执行; 提交期打印告警
#   S2  热更新翻转项目开关          -> 不重启 daemon, 装箱行为随之改变
#
# 用法: bash sched/tests/run_proj_colocate_accept.sh
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
export SCHED_FAKE_GPUS=0:24
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
source tests/acceptance_cleanup.sh
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() {
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1 \
    || { bad "daemon stop 失败 ($1)"; return 1; }
  sleep 1
}

state_counts() { # $1=state_dir $2=batch_name -> canonical status counts
  local st=$1 bn=$2 payload
  export SCHED_STATE=$st SCHED_CONFIG=$st/config.json
  payload=$($PY -m gsched.cli status --json) || return 2
  printf '%s\n' "$payload" | $PY -c '
import json, sys
d = json.load(sys.stdin)
bn = sys.argv[1]
statuses = (
    "running", "pending", "done", "skip", "failed", "blocked",
    "cancelled", "timed_out", "interrupted",
)
jobs = [j for j in d["jobs"] if j["batch_name"] == bn]
print(" ".join(str(sum(j["status"] == status for j in jobs)) for status in statuses))
' "$bn" || return 2
}

LAST_COUNTS=""
wait_counts() { # $1=dir $2=batch $3=期望计数 $4=超时秒
  local rc
  for _ in $(seq 1 ${4:-120}); do
    LAST_COUNTS=$(state_counts "$1" "$2")
    rc=$?
    [ "$rc" -eq 0 ] || return 2
    [ "$LAST_COUNTS" = "$3" ] && return 0
    sleep 1
  done
  return 1
}

expect_counts() { # $1=标签 $2=dir $3=batch $4=expected $5=timeout $6=成功文案
  local rc
  wait_counts "$2" "$3" "$4" "$5"
  rc=$?
  if [ "$rc" -eq 0 ]; then
    ok "$6"
    return 0
  fi
  if [ "$rc" -eq 2 ]; then
    bad "$1: status --json 查询/解析失败"
    exit 1
  fi
  bad "$1: 期望=[$4] 实际=[$LAST_COUNTS]"
  return 1
}

wait_log() { # $1=log $2=pattern $3=超时秒
  for _ in $(seq 1 ${3:-60}); do
    grep -q "$2" "$1" 2>/dev/null && return 0
    sleep 1
  done
  return 1
}

daemon_pid() {
  local output pid
  output=$($PY -m gsched.cli daemon status) || return 2
  case "$output" in
    *"pid="*","*) ;;
    *) return 2 ;;
  esac
  pid=${output#*pid=}
  pid=${pid%%,*}
  [ -n "$pid" ] || return 2
  printf '%s\n' "$pid"
}

mk_config() {   # $1=dir  $2=colocate值("absent"/"false"/"true")
  local col_kv=""
  [ "$2" = "false" ] && col_kv='"colocate": false,'
  [ "$2" = "true" ] && col_kv='"colocate": true,'
  cat > $1/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$1", "gpus": [0],
  "co_locate": true,
  "projects": {"default": {"root": "$ROOT", $col_kv "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
}

mk_pair_batch() { # $1=dir $2=batch名 $3=sleep秒 (两个共享任务)
  cat > $1/$2.json << EOF
{
  "name": "$2", "project": "default", "mode": "mix",
  "tasks": [
    {"id": "ta", "cmd": ["{VENV:k}", "-c", "import time; time.sleep($3)"],
     "duration_min": 1, "resources": {"gpu_share": true, "vram_gib": 1.0}},
    {"id": "tb", "cmd": ["{VENV:k}", "-c", "import time; time.sleep($3)"],
     "duration_min": 1, "resources": {"gpu_share": true, "vram_gib": 1.0}}
  ]
}
EOF
}

echo "=== B12-b 项目级 colocate 开关验收 ==="

# ---------- S1a: 缺省(中立) -> 并发装箱 ----------
echo "--- S1a: 项目缺省 -> 共享装箱生效 ---"
sched_accept_make_root SA "sched-project-colocate-a"
mk_config $SA absent
mk_pair_batch $SA pa 30
export SCHED_STATE=$SA SCHED_CONFIG=$SA/config.json
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1 \
  || { bad "S1a daemon start 失败"; exit 1; }
wait_log "$SA/testnode/scheduler.log" "fake=True" 30 \
  || { bad "S1a daemon 未进入 fake 模式"; exit 1; }
OUT_A=$($PY -m gsched.cli submit $SA/pa.json 2>&1) \
  || { bad "S1a submit 失败: $OUT_A"; exit 1; }
echo "$OUT_A" | grep -q "已入队" || { bad "S1a submit 失败"; exit 1; }
expect_counts "S1a 并发" $SA pa "2 0 0 0 0 0 0 0 0" 90 \
  "缺省语义: 两任务同卡并发 running" || exit 1
expect_counts "S1a 收敛" $SA pa "0 0 2 0 0 0 0 0 0" 120 \
  "缺省共享任务全部 done" || exit 1
stop_daemon $SA || exit 1

# ---------- S1b: colocate=false -> 降级独占串行 ----------
echo "--- S1b: 项目 colocate=false -> 独占串行 ---"
sched_accept_make_root SB "sched-project-colocate-b"
mk_config $SB false
mk_pair_batch $SB pb 30
export SCHED_STATE=$SB SCHED_CONFIG=$SB/config.json
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1 \
  || { bad "S1b daemon start 失败"; exit 1; }
wait_log "$SB/testnode/scheduler.log" "fake=True" 30 \
  || { bad "S1b daemon 未进入 fake 模式"; exit 1; }
OUT_B=$($PY -m gsched.cli submit $SB/pb.json 2>&1) \
  || { bad "S1b submit 失败: $OUT_B"; exit 1; }
echo "$OUT_B" | grep -q "已禁用 colocate" \
  && ok "提交期打印降级告警" || bad "缺提交期告警: $OUT_B"
expect_counts "S1b 串行起始" $SB pb "1 1 0 0 0 0 0 0 0" 90 \
  "独占串行: 1 running + 1 pending" || exit 1
wait_log "$SB/testnode/scheduler.log" \
  "已禁用 colocate -> gpu_share 降级独占" 60 \
  && ok "dispatcher 派发日志记录降级" \
  || bad "缺派发侧降级日志"
expect_counts "S1b 串行交接" $SB pb "1 0 1 0 0 0 0 0 0" 150 \
  "首任务 done 后第二任务才 running" || exit 1
expect_counts "S1b 收敛" $SB pb "0 0 2 0 0 0 0 0 0" 150 \
  "colocate=false 下两任务串行完成" || exit 1
PID_BEFORE=$(daemon_pid) \
  || { bad "热更新前 daemon PID 不可验证"; exit 1; }

# ---------- S2: 原子热更新翻转开关 -> 行为随之改变 ----------
echo "--- S2: config set 热更新 colocate false->true, 不重启 ---"
cat > "$SB/enable-colocate.json" << EOF
{"projects": {"default": {"colocate": true}}}
EOF
CFG_OUT=$($PY -m gsched.cli config set -f "$SB/enable-colocate.json" --yes 2>&1) \
  || { bad "config set 失败: $CFG_OUT"; exit 1; }
echo "$CFG_OUT" | grep -q "已应用并请求热重载" \
  && ok "config set 原子写入并请求热重载" \
  || { bad "config set 未确认热重载: $CFG_OUT"; exit 1; }
wait_log "$SB/testnode/scheduler.log" "config_reload req" 60 \
  && ok "daemon 消费 config_reload 请求" \
  || { bad "daemon 未消费 config_reload"; exit 1; }
wait_log "$SB/testnode/scheduler.log" "配置已热更新" 60 \
  && ok "项目 colocate=true 已热生效" \
  || { bad "项目 colocate=true 未热生效"; exit 1; }
mk_pair_batch $SB pc 30
$PY -m gsched.cli submit $SB/pc.json >/dev/null 2>&1 \
  || { bad "pc submit 失败"; exit 1; }
expect_counts "S2 热更新并发" $SB pc "2 0 0 0 0 0 0 0 0" 90 \
  "热更新后新批次并发装箱" || exit 1
PID_AFTER=$(daemon_pid) \
  || { bad "热更新后 daemon PID 不可验证"; exit 1; }
[ "$PID_AFTER" = "$PID_BEFORE" ] \
  && ok "热更新与新批提交期间 daemon 未重启" \
  || { bad "daemon 意外重启 ($PID_BEFORE -> $PID_AFTER)"; exit 1; }
expect_counts "S2 收敛" $SB pc "0 0 2 0 0 0 0 0 0" 120 \
  "热更新并发任务全部 done" || exit 1
stop_daemon $SB || exit 1

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$PASS" -eq 13 ] || { echo "预期 PASS=13，实际 PASS=$PASS"; exit 1; }
[ $FAIL -eq 0 ] || exit 1
