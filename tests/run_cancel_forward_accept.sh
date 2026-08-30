#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_cancel_forward_accept.sh — cancel 转发 daemon 验收 (事故记录 4, 2026-08-17)
# =============================================================================
# 背景: CLI (登录节点) 看不到计算节点进程组 (PID namespace 跨节点, 定案 44 同类),
#   旧实现本地 os.killpg(pgid,0) 恒 ESRCH -> cancel 对 running 任务"恒判自然结束"
#   -> killpg 从未发生 -> 孤儿进程占卡 (cats_smoke 事故, 13 分钟 20.2GiB).
# 修复: CLI 写 control_requests 队列 (不本地 killpg), daemon 每轮 tick 在本地
#   完成 alive 预检 (O5) + 写 kill_reason + killpg + reap 释放 GPU; SIGTERM
#   未生效 -> 下轮 SIGKILL 升级; reap 标 cancelled 前二次 alive 校验兜底.
#
# 覆盖场景:
#   1. running cancel -> CLI 只写请求不杀进程; daemon 处理 -> killed -> cancelled
#   2. SIGTERM 抗杀进程 (signal 忽略) -> 下一轮 SIGKILL 升级 -> 仍收敛 cancelled
#   3. cancel 后 GPU 释放 (assigned -> releasing -> free)
#   4. 无 running 任务 -> 请求不产生 (pending 直标不变)
#
# 用法: bash sched/tests/run_cancel_forward_accept.sh
# 退出码: 0 = 全过, 1 = 有失败
# =============================================================================
set -u
cd "$(dirname "$0")/.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
source tests/acceptance_cleanup.sh
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
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
    if j['batch_name'] == bn and j['status'] == want:
        n += 1
print(n)
"
}

wait_status() { # $1=state_dir $2=batch_name $3=status $4=期望数 $5=超时秒(默认25)
  local st=$1 bn=$2 want=$3 exp=$4 timeout=${5:-25}
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

echo "=== cancel 转发 daemon 验收 (事故记录 4) ==="

# ---------- 场景 1: running cancel 转发 daemon + GPU 释放 ----------
echo "--- 场景 1: running cancel 转发 daemon, kill 生效, GPU 释放 ---"
sched_accept_make_root S1 "sched-cancel-forward-1"
mk_config $S1
cat > $S1/batch.json << EOF
{
  "name": "cf1",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(120)"], "duration_min": 5}
  ]
}
EOF
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
$PY -m gsched.cli submit $S1/batch.json >/dev/null 2>&1 || { bad "cf1 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 3  # 等 t1 running
[ "$(count_status $S1 cf1 running)" = "1" ] && ok "t1 进入 running" || bad "t1 未 running"
# 关键断言: CLI cancel 只写请求, 不本地杀进程 (进程仍存活直到 daemon 处理)
PGID_BEFORE=$($PY -c "
import sys; sys.path.insert(0, 'sched')
from gsched import state
with state.connect() as conn:
    j = state.get_job(conn, state.all_jobs(conn)[0]['id']) if state.all_jobs(conn) else None
    print(j['pgid'] if j and j['pgid'] else 0)
")
$PY -m gsched.cli cancel cf1 --yes > $S1/cancel_out.txt 2>&1
sleep 1
if [ "$PGID_BEFORE" != "0" ] && kill -0 "$PGID_BEFORE" 2>/dev/null; then
  ok "CLI cancel 后进程仍存活 (未本地 killpg, 转发 daemon)"
else
  bad "CLI cancel 后进程已死 (本地 killpg 仍发生?)"
fi
grep -q "已转发取消" $S1/cancel_out.txt && ok "输出指明'已转发取消' (daemon 执行)" \
  || bad "输出缺转发提示 (输出: $(cat $S1/cancel_out.txt))"
# daemon 处理: killed -> cancelled (reap 收敛)
wait_status $S1 cf1 cancelled 1 25 && ok "daemon killpg 生效, t1 -> cancelled" \
  || bad "t1 未 cancelled (got running=$(count_status $S1 cf1 running) cancelled=$(count_status $S1 cf1 cancelled))"
sleep 3  # 等 settle_releasing -> free
GPUS_JSON=$($PY -m gsched.cli status --json 2>/dev/null)
echo "$GPUS_JSON" | grep -q '"status": "free"' && ok "GPU 释放回 free (无孤儿占卡)" \
  || bad "GPU 未回 free (residual: $(echo "$GPUS_JSON" | grep -o '"gpus":.*' | head -c 120))"
stop_daemon $S1

# ---------- 场景 2: SIGTERM 抗杀进程 -> SIGKILL 升级 ----------
echo "--- 场景 2: SIGTERM 忽略进程 -> SIGKILL 升级收敛 cancelled ---"
sched_accept_make_root S2 "sched-cancel-forward-2"
mk_config $S2
cat > $S2/trap_sleep.py << 'EOF'
import signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
time.sleep(120)
EOF
cat > $S2/batch.json << EOF
{
  "name": "cf2",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "$S2/trap_sleep.py"], "duration_min": 5}
  ]
}
EOF
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
$PY -m gsched.cli submit $S2/batch.json >/dev/null 2>&1 || { bad "cf2 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 3
[ "$(count_status $S2 cf2 running)" = "1" ] && ok "t2 进入 running" || bad "t2 未 running"
$PY -m gsched.cli cancel cf2 --yes >/dev/null 2>&1
# SIGTERM 被忽略 -> daemon 下轮 SIGKILL -> 仍收敛 cancelled (绝不静默)
wait_status $S2 cf2 cancelled 1 25 && ok "SIGTERM 忽略仍收敛 cancelled (SIGKILL 升级)" \
  || bad "SIGTERM 抗杀进程未收敛 (SIGKILL 升级失效)"
stop_daemon $S2

# ---------- 场景 3: 请求表干净 (处理后 done) ----------
echo "--- 场景 3: control_requests 处理后全 done ---"
sched_accept_make_root S3 "sched-cancel-forward-3"
mk_config $S3
cat > $S3/batch.json << EOF
{
  "name": "cf3",
  "project": "default", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(60)"], "duration_min": 5},
    {"id": "t2", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(60)"], "duration_min": 5}
  ]
}
EOF
export SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json
$PY -m gsched.cli submit $S3/batch.json >/dev/null 2>&1 || { bad "cf3 submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 3  # t1 running, t2 pending (单卡)
$PY -m gsched.cli cancel cf3 --yes >/dev/null 2>&1
wait_status $S3 cf3 cancelled 2 25 && ok "t1 (转发) + t2 (pending 直标) 都 cancelled" \
  || bad "cancelled 数 != 2 (got $(count_status $S3 cf3 cancelled))"
# 请求处理是异步的 (job cancelled 后下一轮 tick 才 finish 请求), 轮询等待 pending=0
PENDING_OK=0
for _ in $(seq 1 20); do
  PENDING_REQ=$(SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json $PY -c "
import sys; sys.path.insert(0, 'sched')
from gsched import state
with state.connect() as conn:
    n = conn.execute(\"SELECT COUNT(*) FROM control_requests WHERE status='pending'\").fetchone()[0]
    print(n)
")
  [ "$PENDING_REQ" = "0" ] && { PENDING_OK=1; break; }
  sleep 1
done
[ "$PENDING_OK" = "1" ] && ok "control_requests 无残留 pending (全部处理完毕)" \
  || bad "control_requests 残留 pending=$PENDING_REQ"
stop_daemon $S3

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" = "0" ]
