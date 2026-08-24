#!/bin/bash
# =============================================================================
# run_discard_accept.sh — B16 sched discard 验收 (fake-gpu)
# =============================================================================
# 覆盖:
#   S1 失败任务 -> blocked; 无 --yes 拒绝执行
#   S2 discard --yes -> 批次 discarded, 任务证据保留
#   S3 settle 不复活 discarded (daemon tick 后仍 discarded)
#   S4 retry/resubmit 被守卫拒绝
#   S5 非 blocked 批次 (done) 拒绝退役; 下游依赖告警打印
#
# 用法: bash sched/tests/run_discard_accept.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ✅ $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  ❌ $1"; }
count_st(){ export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli status --json 2>/dev/null | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(-1); raise SystemExit
n=0
for j in d['jobs']:
    if j['batch'].split('-')[0]=='$2' and j['status']=='$3': n+=1
print(n)
"
}
wait_for(){ for _ in $(seq 1 ${2:-40}); do eval "$1" && return 0; sleep 2; done; return 1; }

S=/tmp/sched_discard; rm -rf $S; mkdir -p $S /tmp/sched_dc_out
cat > $S/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S", "gpus": [0],
  "projects": {
    "default": {"root": "$ROOT", "git": false},
    "downstream": {"root": "$ROOT", "git": false}
  },
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
export SCHED_STATE=$S SCHED_CONFIG=$S/config.json
sched(){ $PY -m gsched.cli "$@"; }

mk_batch(){ # $1=name $2=exit码 $3=depends(空或批次名)
  local dep=""
  [ -n "${3:-}" ] && dep="\"depends_on\": [\"$3\"],"
  cat > $S/$1.json << EOF
{"name":"$1","project":"default","mode":"mix",$dep
 "tasks":[{"id":"t1","duration_min":1,"max_retry":0,
  "cmd":["{VENV:k}","-c","import sys; print('boom'); sys.exit($2)"]}]}
EOF
}

echo "=== B16 sched discard 验收 ==="

# 准备: dc1 失败 -> blocked; dcok 成功; dc2 depends_on dc1 -> 也失败但依赖挂起语义
mk_batch dc1 1
mk_batch dcok 0
sched submit $S/dc1.json >/dev/null 2>&1 && ok "dc1 提交" || bad "dc1 提交失败"
sched submit $S/dcok.json >/dev/null 2>&1
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 3

mk_batch dcdep 1 dc1   # 下游依赖 dc1 -> 因上游 blocked 将永久挂起 (queued)
O=$(sched submit $S/dcdep.json >/dev/null 2>&1; echo done)

echo "--- S1: blocked + 无 --yes 拒绝 ---"
wait_for '[ "$(count_st $S dc1 blocked)" = "1" ]' 30 && ok "dc1 blocked" || bad "dc1 未 blocked"
if sched discard dc1 >/dev/null 2>&1; then bad "无 --yes 竟执行"; else ok "无 --yes 拒绝执行"; fi

echo "--- S2: discard --yes ---"
DOUT=$(sched discard dc1 --yes 2>&1)
echo "$DOUT" | grep -q "已退役" && ok "discard 执行成功" || bad "discard 失败: $DOUT"
echo "$DOUT" | grep -q "dcdep.*depends_on 本批次, 已退役, 下游将挂起" \
  && ok "下游依赖告警打印" || bad "缺下游告警: $DOUT"
BS=$(python3 -c "
import sqlite3
conn = sqlite3.connect('$S/testnode/state.db')
print(conn.execute(\"SELECT status FROM batches WHERE name='dc1'\").fetchone()[0])")
[ "$BS" = "discarded" ] && ok "批次状态=discarded" || bad "状态异常: $BS"
JF=$(python3 -c "
import sqlite3
conn = sqlite3.connect('$S/testnode/state.db')
print(conn.execute(\"SELECT COUNT(*) FROM jobs WHERE batch_id LIKE 'dc1%' AND status IN ('failed','blocked')\").fetchone()[0])")
[ "$JF" = "1" ] && ok "任务失败终态证据保留" || bad "任务证据被破坏: $JF"

echo "--- S3: settle 不复活 discarded ---"
sleep 12   # >=1 tick, _settle_batch_status 若误碰会翻回 blocked
BS2=$(python3 -c "
import sqlite3
conn = sqlite3.connect('$S/testnode/state.db')
print(conn.execute(\"SELECT status FROM batches WHERE name='dc1'\").fetchone()[0])")
[ "$BS2" = "discarded" ] && ok "daemon tick 后仍 discarded (settle 不复活)" || bad "被复活: $BS2"

echo "--- S4: retry/resubmit 守卫 ---"
if sched retry dc1 >/dev/null 2>&1; then bad "retry 未被拦截"; else ok "retry 被守卫拒绝"; fi
TREF=$(python3 -c "
import sqlite3
conn = sqlite3.connect('$S/testnode/state.db')
bid = conn.execute(\"SELECT id FROM batches WHERE name='dc1'\").fetchone()[0]
print(bid + ':t1')")
if sched resubmit "$TREF" >/dev/null 2>&1; then bad "resubmit 未被拦截"; else ok "resubmit 被守卫拒绝"; fi

echo "--- S5: done 批次拒绝退役 + queued 下游可退役 ---"
if sched discard dcok --yes >/dev/null 2>&1; then bad "done 批次竟可退役"; else ok "done 批次拒绝退役"; fi
DOUT2=$(sched discard dcdep --yes 2>&1)
echo "$DOUT2" | grep -q "已退役" && ok "queued 挂起批次可退役 (pending 一并取消)"   || bad "queued 退役失败: $DOUT2"

stop_daemon() {
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  pkill -f "gsched.dispatcher_main" 2>/dev/null
}
stop_daemon

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ $FAIL -eq 0 ] || exit 1
