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
set -u
cd "$(dirname "$0")/.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() {
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  pkill -f "gsched.dispatcher_main" 2>/dev/null
  sleep 1
}

count_status() { # $1=state_dir $2=batch_name $3=status -> 数量
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

wait_status() { # $1=dir $2=batch $3=status $4=期望 $5=超时秒
  for _ in $(seq 1 ${5:-40}); do
    [ "$(count_status $1 $2 $3)" = "$4" ] && return 0
    sleep 1
  done
  return 1
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

wait_for() { # $1=条件命令(返回0即命中) $2=超时秒
  for _ in $(seq 1 ${2:-25}); do
    if eval "$1"; then return 0; fi
    sleep 1
  done
  return 1
}

echo "=== B12-b 项目级 colocate 开关验收 ==="

# ---------- S1a: 缺省(中立) -> 并发装箱 ----------
echo "--- S1a: 项目缺省 -> 共享装箱生效 ---"
SA=/tmp/sched_pc_a; rm -rf $SA; mkdir -p $SA
mk_config $SA absent
mk_pair_batch $SA pa 25
export SCHED_STATE=$SA SCHED_CONFIG=$SA/config.json
OUT_A=$($PY -m gsched.cli submit $SA/pa.json 2>&1)
echo "$OUT_A" | grep -q "已入队" || { bad "S1a submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
wait_for '[ "$(count_status $SA pa running)" = "2" ]' 25 \
  && ok "缺省语义: 两任务同卡并发 running" \
  || bad "未并发 (running=$(count_status $SA pa running))"

# ---------- S1b: colocate=false -> 降级独占串行 ----------
echo "--- S1b: 项目 colocate=false -> 独占串行 ---"
SB=/tmp/sched_pc_b; rm -rf $SB; mkdir -p $SB
mk_config $SB false
mk_pair_batch $SB pb 25
export SCHED_STATE=$SB SCHED_CONFIG=$SB/config.json
OUT_B=$($PY -m gsched.cli submit $SB/pb.json 2>&1)
echo "$OUT_B" | grep -q "已禁用 colocate" \
  && ok "提交期打印降级告警" || bad "缺提交期告警: $OUT_B"
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
wait_for '[ "$(count_status $SB pb running)" = "1" ] && [ "$(count_status $SB pb pending)" = "1" ]' 25 \
  && ok "独占串行: 1 running + 1 pending" \
  || bad "非串行 (running=$(count_status $SB pb running) pending=$(count_status $SB pb pending))"
grep -q "已禁用 colocate -> gpu_share 降级独占" $SB/testnode/scheduler.log \
  && ok "dispatcher 派发日志记录降级" || bad "缺派发侧降级日志"

# ---------- S2: 热更新翻转开关 -> 行为随之改变 ----------
echo "--- S2: 热更新 colocate false->缺省, 不重启 ---"
python3 - << PYEOF
import json
p = "$SB/config.json"
cfg = json.load(open(p))
cfg["projects"]["default"].pop("colocate")   # false -> 中立
json.dump(cfg, open(p, "w"), indent=2)
PYEOF
> "$SB/testnode/scheduler.log"
wait_status $SB pb done 2 60 && ok "首批任务全部完成" || bad "首批未完成"
mk_pair_batch $SB pc 8
$PY -m gsched.cli submit $SB/pc.json >/dev/null 2>&1
wait_for '[ "$(count_status $SB pc running)" = "2" ]' 30 \
  && ok "热更新后新批次并发装箱 (行为随配置切换)" \
  || bad "热更新后仍串行 (running=$(count_status $SB pc running))"
stop_daemon $SB

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ $FAIL -eq 0 ] || exit 1
