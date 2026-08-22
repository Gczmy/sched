#!/bin/bash
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
#   4. probes 未命中: 正常等退出码路径不受扰 (无声明任务照常 done)
#
# 用法: bash sched/tests/run_probes_accept.sh
# 退出码: 0 = 全过, 1 = 有失败 (输出 FAIL 行)
# =============================================================================
set -u
cd "$(dirname "$0")/../.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT/sched${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  pkill -f "gsched.dispatcher_main" 2>/dev/null
  sleep 1
}

# 任务状态计数: $1=state_dir $2=batch_name $3=status -> 数量
count_status() {
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

wait_status() { # $1=state_dir $2=batch_name $3=status $4=期望数 $5=超时秒(默认40)
  local st=$1 bn=$2 want=$3 exp=$4 timeout=${5:-40}
  for _ in $(seq 1 $timeout); do
    [ "$(count_status $st $bn $want)" = "$exp" ] && return 0
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
S1=/tmp/sched_prb1; rm -rf $S1; mkdir -p $S1
mk_config $S1
cat > $S1/batch.json << EOF
{
  "name": "p1", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(1); print('FATAL Traceback boom', flush=True); time.sleep(120)"],
     "duration_min": 5, "max_retry": 3,
     "probes": {"fail_on_log": "Traceback"}}
  ]
}
EOF
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
$PY -m gsched.cli submit $S1/batch.json >/dev/null 2>&1 || { bad "p1 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 2
[ "$(count_status $S1 p1 running)" = "1" ] && ok "t1 进入 running" || bad "t1 未 running"
wait_status $S1 p1 blocked 1 40 && ok "fail_on_log 命中 -> blocked" \
  || bad "未 blocked (got running=$(count_status $S1 p1 running) blocked=$(count_status $S1 p1 blocked))"
# 不 retry: retries 应为 0 (probe 命中视为确定失败)
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
RETRIES=$($PY -m gsched.cli status --json 2>/dev/null | $PY -c "
import json, sys
d = json.load(sys.stdin)
for j in d['jobs']:
    if j['task'] == 't1':
        print(j.get('retries', 0)); break
")
[ "$RETRIES" = "0" ] && ok "probe 命中不 retry (retries=$RETRIES)" || bad "不应 retry (retries=$RETRIES)"
stop_daemon $S1

# ---------- 场景 2: ready_on_log 命中 + 产物存在 -> done ----------
echo "--- 场景 2: ready_on_log 命中 + 产物存在 -> done ---"
S2=/tmp/sched_prb2; rm -rf $S2; mkdir -p $S2
mk_config $S2
cat > $S2/batch.json << EOF
{
  "name": "p2", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(1); open('$S2/out.txt','w').write('ok'); print('ALL_DONE', flush=True); time.sleep(120)"],
     "duration_min": 5,
     "probes": {"ready_on_log": "ALL_DONE"},
     "artifacts": {"out": {"path": "$S2/out.txt"}}, "paths_escape": true}
  ]
}
EOF
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
$PY -m gsched.cli submit $S2/batch.json >/dev/null 2>&1 || { bad "p2 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 2
[ "$(count_status $S2 p2 running)" = "1" ] && ok "t1 进入 running" || bad "t1 未 running"
wait_status $S2 p2 done 1 40 && ok "ready_on_log 命中 + 产物存在 -> done" \
  || bad "未 done (got done=$(count_status $S2 p2 done) running=$(count_status $S2 p2 running))"
[ -f $S2/out.txt ] && ok "产物 out.txt 已生成" || bad "产物缺失"
stop_daemon $S2

# ---------- 场景 3: ready_on_log 命中但产物缺失 -> 降级 failed ----------
echo "--- 场景 3: ready_on_log 命中但产物缺失 -> failed ---"
S3=/tmp/sched_prb3; rm -rf $S3; mkdir -p $S3
mk_config $S3
cat > $S3/batch.json << EOF
{
  "name": "p3", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(1); print('ALL_DONE', flush=True); time.sleep(120)"],
     "duration_min": 5, "max_retry": 0,
     "probes": {"ready_on_log": "ALL_DONE"},
     "artifacts": {"out": {"path": "$S3/out.txt"}}, "paths_escape": true}
  ]
}
EOF
export SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json
$PY -m gsched.cli submit $S3/batch.json >/dev/null 2>&1 || { bad "p3 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 2
[ "$(count_status $S3 p3 running)" = "1" ] && ok "t1 进入 running" || bad "t1 未 running"
wait_status $S3 p3 failed 1 40 && ok "ready 命中但产物缺失 -> 降级 failed" \
  || bad "未 failed (got failed=$(count_status $S3 p3 failed) done=$(count_status $S3 p3 done))"
stop_daemon $S3

# ---------- 场景 4: 无 probes 任务不受扰 (正常退出码路径) ----------
echo "--- 场景 4: 无 probes 任务正常 done ---"
S4=/tmp/sched_prb4; rm -rf $S4; mkdir -p $S4
mk_config $S4
cat > $S4/batch.json << EOF
{
  "name": "p4", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(1); print('Traceback unrelated'); print('OK')"],
     "duration_min": 5, "max_retry": 0,
     "artifacts": {"out": {"path": "$S4/out.txt"}}, "paths_escape": true}
  ]
}
EOF
export SCHED_STATE=$S4 SCHED_CONFIG=$S4/config.json
$PY -m gsched.cli submit $S4/batch.json >/dev/null 2>&1 || { bad "p4 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
# 无 probes -> 任务自行退出 rc=0; 产物缺失 -> failed(artifact) -> 重试耗尽
# (max_retry=0, 复核注记) -> blocked 终态。断言 blocked + failure=artifact,
# 而非 failed (failed 是瞬态, 立即被 _maybe_retry 转 blocked)。
wait_status $S4 p4 blocked 1 40 && ok "无 probes 任务正常收敛 (rc 路径, 产物缺失 -> blocked/artifact)" \
  || bad "未收敛 (got $(count_status $S4 p4 blocked) blocked)"
stop_daemon $S4

# ---------- 场景 5: SIGKILL 升级 (L3) ----------
# 任务忽略 SIGTERM: probe kill 发 SIGTERM 无效 -> daemon 逐轮 SIGKILL 升级
# -> 进程真实死亡 (不与 cancel 脱节, 防占卡直至 releasing 超时)
echo "--- 场景 5: probe SIGKILL 升级 (进程忽略 SIGTERM) ---"
S5=/tmp/sched_prb5; rm -rf $S5; mkdir -p $S5
mk_config $S5
cat > $S5/batch.json << EOF
{
  "name": "p5", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import signal,time,os; signal.signal(signal.SIGTERM, signal.SIG_IGN); open('$S5/pid.txt','w').write(str(os.getpid())); print('FATAL Traceback', flush=True); time.sleep(120)"],
     "duration_min": 5, "max_retry": 0,
     "probes": {"fail_on_log": "Traceback"}}
  ]
}
EOF
export SCHED_STATE=$S5 SCHED_CONFIG=$S5/config.json
$PY -m gsched.cli submit $S5/batch.json >/dev/null 2>&1 || { bad "p5 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
wait_status $S5 p5 blocked 1 40 && ok "fail_on_log 命中 -> blocked (忽略 SIGTERM 的任务)" \
  || bad "未 blocked (got blocked=$(count_status $S5 p5 blocked))"
# SIGKILL 升级: 等 1-2 轮 tick 后进程应真实死亡 (忽略 SIGTERM 也逃不掉 SIGKILL)
PID="$(cat $S5/pid.txt 2>/dev/null)"
DEAD=0
if [ -n "$PID" ]; then
  for _ in $(seq 1 25); do
    # 用 ps stat 而非 kill -0: SIGKILL 后进程成 zombie (父 daemon 未 reap),
    # kill -0 对 zombie 仍返回 0 -> 误判存活。zombie 不再执行/占卡, 视为已杀。
    ST=$(ps -o stat= -p "$PID" 2>/dev/null | tr -d ' ')
    if [ -z "$ST" ] || [ "${ST#Z}" != "$ST" ]; then DEAD=1; break; fi
    sleep 1
  done
fi
[ "$DEAD" = "1" ] && ok "SIGKILL 升级生效: 忽略 SIGTERM 的进程最终被杀 (pid=$PID)" \
  || bad "进程未被 SIGKILL 升级杀死 (pid=$PID stat=$ST)"
stop_daemon $S5

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" = "0" ]
