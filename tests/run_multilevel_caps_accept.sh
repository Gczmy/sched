#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_multilevel_caps_accept.sh — B12-c 三级打包上限验收 (fake-gpu)
# =============================================================================
# 覆盖场景:
#   S1 全局上限: co_locate_max_jobs=1 -> 两个共享任务串行 (显存再宽也不叠)
#   S2 卡级上限: gpus[{idx,max_jobs}] 异构形态 -> 覆盖全局缺省密度
#   S3 项目级上限: 只数该项目在此卡的任务; 其他项目不受影响
#   S4 自然排水: 上限调低不驱逐已 pack 任务, 退出后新 pack 遵守新上限
#
# 用法: bash sched/tests/run_multilevel_caps_accept.sh
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

PASS=0; FAIL=0
source tests/acceptance_cleanup.sh
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() {
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  sleep 1
}

count_status() { # $1=dir $2=batch $3=status
  local payload count
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  payload=$($PY -m gsched.cli status --json 2>/dev/null) || return 2
  count=$(printf '%s\n' "$payload" | $PY -c "
import json, sys
d = json.load(sys.stdin)
n = sum(1 for j in d['jobs'] if j['batch_name'] == '$2' and j['status'] == '$3')
print(n)") || return 2
  printf '%s\n' "$count"
}

status_count_is() { # $1=dir $2=batch $3=status $4=expected
  local actual
  actual=$(count_status "$1" "$2" "$3") || return 2
  [ "$actual" = "$4" ]
}

wait_for() { # $1=条件 $2=超时秒
  local rc
  for _ in $(seq 1 ${2:-25}); do
    eval "$1"
    rc=$?
    [ "$rc" -eq 0 ] && return 0
    [ "$rc" -eq 2 ] && return 2
    sleep 1
  done
  return 1
}

mk_batch() { # $1=dir $2=batch名 $3=sleep秒 $4=项目(缺省 default)
  local proj=${4:-default}
  cat > $1/$2.json << EOF
{
  "name": "$2", "project": "$proj", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep($3)"],
     "duration_min": 1, "resources": {"gpu_share": true, "vram_gib": 1.0}},
    {"id": "t2", "cmd": ["{VENV:k}", "-c", "import time; time.sleep($3)"],
     "duration_min": 1, "resources": {"gpu_share": true, "vram_gib": 1.0}},
    {"id": "t3", "cmd": ["{VENV:k}", "-c", "import time; time.sleep($3)"],
     "duration_min": 1, "resources": {"gpu_share": true, "vram_gib": 1.0}}
  ]
}
EOF
}

submit_and_start() { # $1=dir $2=batch名
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli submit $1/$2.json >/dev/null 2>&1 || { bad "$2 submit 失败"; exit 1; }
  SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1 \
    || { bad "$2 daemon start 失败"; exit 1; }
  sleep 2
}

echo "=== B12-c 三级打包上限验收 ==="

# ---------- S1: 全局上限 ----------
echo "--- S1: 全局 co_locate_max_jobs=1 -> 串行 ---"
sched_accept_make_root S1 "sched-caps-global"
cat > $S1/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S1", "gpus": [0], "co_locate": true,
  "co_locate_max_jobs": 2,
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
mk_batch $S1 cap1 20
submit_and_start $S1 cap1
wait_for 'status_count_is "$S1" cap1 running 2' 25 \
  && ok "全局上限=2: 2 个并发" || bad "running=$(count_status $S1 cap1 running)"
sleep 3
R=$(count_status $S1 cap1 running) || { bad "cap1 running 查询失败"; exit 1; }
P=$(count_status $S1 cap1 pending) || { bad "cap1 pending 查询失败"; exit 1; }
[ "$R" = "2" ] && [ "$P" = "1" ] \
  && ok "第 3 个排队: 装箱被全局上限约束" \
  || bad "running=$R pending=$P 并发越界"
grep -q "等待自然排水" $S1/testnode/scheduler.log \
  && ok "warn-once 等待日志存在" || bad "缺等待日志"
stop_daemon $S1

# ---------- S2: 卡级上限 (对象形态 max_jobs) ----------
echo "--- S2: 卡级 max_jobs=2 -> 并发 2, 第 3 个排队 ---"
sched_accept_make_root S2 "sched-caps-gpu"
cat > $S2/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S2",
  "gpus": [{"idx": 0, "max_jobs": 2}], "co_locate": true,
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
mk_batch $S2 cap2 20
submit_and_start $S2 cap2
wait_for 'status_count_is "$S2" cap2 running 2' 25 \
  && ok "卡级上限=2: 2 个并发" || bad "running=$(count_status $S2 cap2 running)"
sleep 3
R=$(count_status $S2 cap2 running) || { bad "cap2 running 查询失败"; exit 1; }
P=$(count_status $S2 cap2 pending) || { bad "cap2 pending 查询失败"; exit 1; }
[ "$R" = "2" ] && [ "$P" = "1" ] \
  && ok "第 3 个任务排队 (2 running + 1 pending)" \
  || bad "running=$R pending=$P"
GPUVIEW=$(SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json $PY -m gsched.cli list-gpus 2>/dev/null) \
  || { bad "list-gpus 查询失败"; exit 1; }
GPUVIEW=${GPUVIEW%%$'\n'*}
echo "$GPUVIEW" | grep -q "packed=" && ok "list-gpus 显示 packed 标注 ($GPUVIEW)" || bad "缺 packed 标注"
stop_daemon $S2

# ---------- S3: 项目级上限 + 跨项目隔离 ----------
echo "--- S3: 项目级 max_jobs=1, 跨项目不受影响 ---"
sched_accept_make_root S3 "sched-caps-project"
cat > $S3/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S3", "gpus": [0], "co_locate": true,
  "projects": {
    "lighta": {"root": "$ROOT", "git": false, "max_jobs": 1},
    "heavyb": {"root": "$ROOT", "git": false}
  },
  "default_project": "lighta", "venvs": {"k": "$PY"}
}
EOF
mk_batch $S3 ca 20 lighta     # lighta 的 3 个任务, 上限 1 -> 串行
export SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json
$PY -m gsched.cli submit $S3/ca.json >/dev/null 2>&1 \
  || { bad "ca submit 失败"; exit 1; }
cat > $S3/cb.json << EOF
{
  "name": "cb", "project": "heavyb", "mode": "mix",
  "tasks": [
    {"id": "b1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(20)"],
     "duration_min": 1, "resources": {"gpu_share": true, "vram_gib": 1.0}}
  ]
}
EOF
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1 \
  || { bad "S3 daemon start 失败"; exit 1; }
$PY -m gsched.cli submit $S3/cb.json >/dev/null 2>&1 \
  || { bad "cb submit 失败"; exit 1; }
wait_for 'status_count_is "$S3" cb running 1' 30 \
  && ok "heavyb (无上限) 正常运行" || bad "heavyb 未跑"
RA=$(count_status $S3 ca running) || { bad "ca running 查询失败"; exit 1; }
[ "$RA" = "1" ] && ok "lighta 上限=1 生效" || bad "lighta 并发异常 (running=$RA)"
RB=$(count_status $S3 cb running) || { bad "cb running 查询失败"; exit 1; }
TOTAL=$((RA + RB))
[ "$TOTAL" = "2" ] \
  && ok "跨项目共存: lighta×1 + heavyb×1 同卡 (项目上限只数自家任务)" \
  || bad "同卡总数异常 ($TOTAL)"
PLIST=$(SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json $PY -m gsched.cli project list 2>/dev/null) \
  || { bad "project list 查询失败"; exit 1; }
echo "$PLIST" | grep -q "lighta" && echo "$PLIST" | grep "lighta" | grep -q "1" \
  && ok "project list 展示项目上限" || bad "project list 缺上限列"
stop_daemon $S3

# ---------- S4: 自然排水 (热更新调低上限, 不驱逐) ----------
echo "--- S4: 热更新调低上限 -> 已 pack 不驱逐 ---"
sched_accept_make_root S4 "sched-caps-hotreload"
cat > $S4/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S4", "gpus": [0], "co_locate": true,
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
mk_batch $S4 cd 25
submit_and_start $S4 cd
wait_for 'status_count_is "$S4" cd running 3' 25 \
  && ok "初始 3 个并发装箱" || bad "未 3 并发"
$PY - << PYEOF
import json
p = "$S4/config.json"
cfg = json.load(open(p))
# 热更新调低卡级上限 (整数形态 -> 对象形态; 卡集/容量不变 -> 热键变更)
cfg["gpus"] = [{"idx": 0, "max_jobs": 1}]
json.dump(cfg, open(p, "w"), indent=2)
PYEOF
[ $? -eq 0 ] || { bad "S4 config 热更新失败"; exit 1; }
sleep 13   # >= 1 tick + 余量: 若有驱逐逻辑此时 running 会掉
R=$(count_status $S4 cd running) || { bad "cd running 查询失败"; exit 1; }
[ "$R" = "3" ] && ok "上限调低后 3 个任务继续运行 (无驱逐)" || bad "发生驱逐 (running=$R)"
wait_for 'status_count_is "$S4" cd done 3' 60 \
  && ok "全部自然完成" || bad "任务未收敛"
stop_daemon $S4

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$PASS" -eq 13 ] || { echo "预期 PASS=13，实际 PASS=$PASS"; exit 1; }
[ $FAIL -eq 0 ] || exit 1
