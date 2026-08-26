#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_resubmit_batch_accept.sh — B17 resubmit 批量操作验收 (fake-gpu, 确定性设计)
# =============================================================================
# 设计要点: 失败任务置于队列尾 (t1/t2 先成功, t3 最后失败) —— 避免中段失败
# 触发批次 blocked 冻结兄弟任务的时序竞争。
#
# 覆盖:
#   S1 失败收敛: done=2 + blocked=1 + pending=0 (确定性稳态)
#   S2 --dry-run --failed: 只列 t3, 不写入
#   S3 --failed: 重跑 t3 -> 批次回 active -> t3 成功 -> 批次 done
#   S4 参数校验: 单任务+flag / 裸批次无flag / 双flag / 不存在批次
#   S5 --all --dry-run 列出全部 3 个
#
# 用法: bash sched/tests/run_resubmit_batch_accept.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ✅ $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  ❌ $1"; }
cnt(){ export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli status --json 2>/dev/null | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(-1); raise SystemExit
print(sum(1 for j in d['jobs'] if j['batch'].split('-')[0]=='$2' and j['status']=='$3'))"; }
wait_for(){ for _ in $(seq 1 ${2:-40}); do eval "$1" && return 0; sleep 2; done; return 1; }
wait_batch_done(){ for _ in $(seq 1 ${3:-30}); do
    SCHED_STATE=$1 SCHED_CONFIG=$1/config.json $PY -m gsched.cli status --json 2>/dev/null | \
      $PY -c "
import json,sys
d=json.load(sys.stdin)
bad=[b for b in d['batches'] if b['name'].split('-')[0]=='$2' and b['status'] not in ('done','blocked','cancelled')]
sys.exit(0 if not bad else 1)" && return 0
    sleep 2
  done; return 1; }

S=/tmp/sched_rsbatch; rm -rf $S $HOME/rs_t3_flag; mkdir -p $S
cat > $S/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S", "gpus": [0],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
export SCHED_STATE=$S SCHED_CONFIG=$S/config.json
sched(){ $PY -m gsched.cli "$@"; }

FLAG=$HOME/rs_t3_flag
cat > $S/rs.json << EOF
{"name":"rs","project":"default","mode":"mix",
 "tasks":[
  {"id":"t1","duration_min":1,"cmd":["{VENV:k}","-c","print('T1_OK')"]},
  {"id":"t2","duration_min":1,"cmd":["{VENV:k}","-c","print('T2_OK')"]},
  {"id":"t3","duration_min":1,"max_retry":0,
   "runtime":{"venv_alias":"k"},
   "cmd":["{VENV:k}","-c","import os,sys; p='$FLAG'; sys.exit(print('T3_OK')) if os.path.exists(p) else (open(p,'w').close(), sys.exit(1))"]}
 ]}
EOF

echo "=== B17 resubmit 批量操作验收 ==="
O=$(sched submit $S/rs.json 2>&1); echo "$O" | grep -q "已入队" || { bad "提交失败: $O"; exit 1; }
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1

# ---------- S1: 确定性失败收敛 ----------
echo "--- S1 失败收敛稳态 ---"
wait_for '[ "$(cnt $S rs blocked)" = "1" ]' 120 \
  && ok "t3 失败 -> blocked (t1/t2 已 done)" || bad "未收敛 ($(cnt $S rs blocked))"
D=$(cnt $S rs done); P=$(cnt $S rs pending)
[ "$D" = "2" ] && [ "$P" = "0" ] && ok "稳态分布: 2 done + 0 pending" || bad "D=$D P=$P"

# ---------- S2: --dry-run ----------
echo "--- S2 --dry-run 只列 t3 ---"
DRY=$(sched resubmit rs --failed --dry-run 2>&1)
echo "$DRY" | grep -q "dry-run" && echo "$DRY" | grep -q "t3" \
  && ! echo "$DRY" | grep -qw "t1" && ! echo "$DRY" | grep -qw "t2" \
  && ok "--dry-run 只列终态失败的 t3" || bad "--dry-run 清单异常: $DRY"
[ "$(cnt $S rs pending)" = "0" ] && ok "--dry-run 未写入" || bad "dry-run 竟写入"

# ---------- S3: --failed 实际执行 ----------
echo "--- S3 --failed -> 批次回 active -> t3 成功 ---"
O=$(sched resubmit rs --failed 2>&1)
echo "$O" | grep -q "已 resubmit 1 个任务" && ok "只重跑 t3" || bad "数量异常: $O"
echo "$O" | grep -q "批次已回 active" && ok "blocked 自动回 active" || bad "未回 active: $O"
wait_for '[ "$(cnt $S rs done)" = "3" ]' 120 \
  && ok "t3 v2 成功, 批次全绿 done" || bad "未全 done ($(cnt $S rs done))"

# ---------- S4: 参数校验 ----------
echo "--- S4 参数校验 ---"
sched resubmit rs:t3 --failed >/dev/null 2>&1 && bad "单任务+flag 竟通过" || ok "单任务+flag 拒绝"
sched resubmit rs >/dev/null 2>&1 && bad "裸批次无flag 竟通过" || ok "裸批次无 flag 拒绝"
sched resubmit rs --failed --all >/dev/null 2>&1 && bad "双flag 竟通过" || ok "双flag 互斥拒绝"
NORF=$(sched resubmit nosuch --failed 2>&1)
echo "$NORF" | grep -q "不存在" && ok "不存在批次报错清晰" || bad "报错异常: $NORF"

# ---------- S5: --all ----------
echo "--- S5 --all ---"
ALLD=$(sched resubmit rs --all --dry-run 2>&1)
echo "$ALLD" | grep -q "将 resubmit 3 个任务" && ok "--all 列出全部 3 个" || bad "--all 异常: $ALLD"

stop_daemon() {
  sched daemon stop >/dev/null 2>&1
  pkill -f "gsched.dispatcher_main" 2>/dev/null
}
stop_daemon

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ $FAIL -eq 0 ] || exit 1
