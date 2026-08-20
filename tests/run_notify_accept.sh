#!/bin/bash
# =============================================================================
# run_notify_accept.sh — 批次终态通知验收 (fake-gpu 快速回归)
# =============================================================================
# 用途: 通知功能 (docs/sched_notify_design.md §8) 端到端验证.
#       不烧 GPU (SCHED_FAKE_GPUS), file 渠道落 notify_inbox 断言.
#
# 覆盖场景:
#   1. 批次 done -> inbox 落 .done.json (事件字段完整)
#   2. 批次 blocked -> 落 .blocked.json (failures 带日志绝对路径)
#   3. retry 解除 -> active 不重发; 再 done -> 仅各一封 (一次性迁移点去重)
#   4. notify-inbox / notify-ack CLI: 列出 -> 确认 -> .acked
#   5. email 渠道异常 (SMTP 拒连) 不影响 file 渠道与批次收敛
#   6. 批次级 notify=false -> 不发
#
# 用法: bash sched/tests/run_notify_accept.sh
# 退出码: 0 = 全过, 1 = 有失败
# =============================================================================
set -u
cd "$(dirname "$0")/../.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT/sched${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

mk_config() { # $1=state_dir $2=notify_json (可空)
  cat > $1/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$1", "gpus": [0],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
  if [ -n "${2:-}" ]; then
    # 在末尾 } 前插入 notify 段 (heredoc 内嵌引号易踩坑, 用 python 稳妥)
    $PY - "$1/config.json" "$2" <<'PYEOF'
import json, sys
p, nf = sys.argv[1], json.loads(sys.argv[2])
cfg = json.load(open(p))
cfg["notify"] = nf
json.dump(cfg, open(p, "w"), indent=2)
PYEOF
  fi
}

start_fake() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  SCHED_FAKE_GPUS=0 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
}

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  pkill -f "gsched.dispatcher_main" 2>/dev/null
  sleep 1
}

inbox() { echo "$1/testnode/notify_inbox"; }  # $1=state_dir

count_inbox() { # $1=state_dir $2=glob (如 '*.done.json') -> 数量
  ls $(inbox $1)/$2 2>/dev/null | wc -l | tr -d ' '
}

wait_inbox() { # $1=state_dir $2=glob $3=期望数 $4=超时秒(默认30)
  local st=$1 pat=$2 exp=$3 timeout=${4:-30}
  for _ in $(seq 1 $timeout); do
    [ "$(count_inbox $st "$pat")" = "$exp" ] && return 0
    sleep 1
  done
  return 1
}

echo "=== 批次终态通知验收 (fake-gpu) ==="

# ---------- 场景 1: 批次 done -> .done.json ----------
echo "--- 场景 1: 批次 done -> inbox 落 .done.json ---"
S1=/tmp/sched_ntf1; rm -rf $S1; mkdir -p $S1
mk_config $S1 '{"file": {}}'
cat > $S1/batch.json << EOF
{
  "name": "n1", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "print('ok')"], "duration_min": 5, "max_retry": 0}
  ]
}
EOF
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
$PY -m gsched.cli submit $S1/batch.json >/dev/null 2>&1 || { bad "n1 submit 失败"; exit 1; }
start_fake $S1
wait_inbox $S1 '*.done.json' 1 30 && ok "批次 done -> inbox 落 .done.json" \
  || bad "未落 .done.json (inbox: $(ls $(inbox $S1) 2>/dev/null))"
F=$(ls $(inbox $S1)/*.done.json 2>/dev/null | head -1)
if [ -n "$F" ]; then
  $PY -c "
import json
ev = json.load(open('$F'))
assert ev['event'] == 'batch_done' and ev['batch'] == 'n1', ev
assert ev['counts'].get('done') == 1, ev['counts']
" && ok "事件字段完整 (event/batch/counts)" || bad "事件字段异常: $(cat $F)"
fi
stop_daemon $S1

# ---------- 场景 2: 批次 blocked -> .blocked.json (failures 带日志路径) ----------
echo "--- 场景 2: 批次 blocked -> .blocked.json ---"
S2=/tmp/sched_ntf2; rm -rf $S2; mkdir -p $S2
mk_config $S2 '{"file": {}}'
cat > $S2/batch.json << EOF
{
  "name": "n2", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import sys; sys.exit(1)"], "duration_min": 5, "max_retry": 0}
  ]
}
EOF
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
$PY -m gsched.cli submit $S2/batch.json >/dev/null 2>&1 || { bad "n2 submit 失败"; exit 1; }
start_fake $S2
wait_inbox $S2 '*.blocked.json' 1 30 && ok "批次 blocked -> inbox 落 .blocked.json" \
  || bad "未落 .blocked.json (inbox: $(ls $(inbox $S2) 2>/dev/null))"
F2=$(ls $(inbox $S2)/*.blocked.json 2>/dev/null | head -1)
if [ -n "$F2" ]; then
  $PY -c "
import json, os
ev = json.load(open('$F2'))
assert ev['event'] == 'batch_blocked', ev['event']
assert ev['failures'] and os.path.isabs(ev['failures'][0]['log']), ev['failures']
" && ok "failures 带日志绝对路径 (agent 可直接 Read)" || bad "failures 字段异常: $(cat $F2)"
fi
stop_daemon $S2

# ---------- 场景 3: retry 解除 -> active 不重发; 再 done -> 仅各一封 ----------
echo "--- 场景 3: unblock 不重发 + 终态各一封 ---"
S3=/tmp/sched_ntf3; rm -rf $S3; mkdir -p $S3
mk_config $S3 '{"file": {}}'
# 首跑失败 (建 flag 后 exit 1), retry 后成功
cat > $S3/batch.json << EOF
{
  "name": "n3", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import os,sys; f='$S3/flag'; sys.exit(0) if os.path.exists(f) else (open(f,'w').write('x'), sys.exit(1))[1]"],
     "duration_min": 5, "max_retry": 0}
  ]
}
EOF
export SCHED_STATE=$S3 SCHED_CONFIG=$S3/config.json
$PY -m gsched.cli submit $S3/batch.json >/dev/null 2>&1 || { bad "n3 submit 失败"; exit 1; }
start_fake $S3
wait_inbox $S3 '*.blocked.json' 1 30 && ok "首跑失败 -> .blocked.json" \
  || bad "未落 .blocked.json (inbox: $(ls $(inbox $S3) 2>/dev/null))"
$PY -m gsched.cli retry n3 >/dev/null 2>&1 || bad "retry n3 失败"
wait_inbox $S3 '*.done.json' 1 30 && ok "retry 后 done -> .done.json" \
  || bad "retry 后未 done"
sleep 2   # 多等两拍, 确认无重复通知
NB=$(count_inbox $S3 '*.blocked.json'); ND=$(count_inbox $S3 '*.done.json')
[ "$NB" = "1" ] && [ "$ND" = "1" ] && ok "blocked/done 各一封 (active 迁移不发, 一次性去重)" \
  || bad "通知数量异常 (blocked=$NB done=$ND, 期望各 1)"
stop_daemon $S3

# ---------- 场景 4: notify-inbox / notify-ack CLI ----------
echo "--- 场景 4: notify-inbox / notify-ack ---"
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
OUT=$($PY -m gsched.cli notify-inbox 2>&1)
echo "$OUT" | grep -q "n1" && ok "notify-inbox 列出未确认事件" \
  || bad "notify-inbox 未列出 n1 (输出: $OUT)"
F1=$(ls $(inbox $S1)/*.done.json 2>/dev/null | head -1)
$PY -m gsched.cli notify-ack "$F1" >/dev/null 2>&1 \
  && [ -f "${F1%.json}.json.acked" -o -f "$F1.acked" ] \
  && ok "notify-ack 确认 -> .acked" || bad "notify-ack 未生效"
OUT2=$($PY -m gsched.cli notify-inbox 2>&1)
echo "$OUT2" | grep -q "n1" && bad "ack 后 inbox 仍列出 n1" || ok "ack 后不再列入未确认"
$PY -m gsched.cli notify-inbox --all 2>&1 | grep -q "n1" \
  && ok "--all 可见已确认事件" || bad "--all 未见已确认事件"

# ---------- 场景 5: email 渠道异常不影响 file 渠道与批次收敛 ----------
echo "--- 场景 5: email 渠道异常隔离 ---"
S5=/tmp/sched_ntf5; rm -rf $S5; mkdir -p $S5
# smtp 指向 127.0.0.1:1 (拒连, 快速失败) + file 渠道
mk_config $S5 '{"file": {}, "email": {"smtp_host": "127.0.0.1", "smtp_port": 1, "from": "sched@test", "to": ["x@test"]}}'
cat > $S5/batch.json << EOF
{
  "name": "n5", "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "print('ok')"], "duration_min": 5, "max_retry": 0}
  ]
}
EOF
export SCHED_STATE=$S5 SCHED_CONFIG=$S5/config.json
$PY -m gsched.cli submit $S5/batch.json >/dev/null 2>&1 || { bad "n5 submit 失败"; exit 1; }
start_fake $S5
wait_inbox $S5 '*.done.json' 1 30 && ok "email 拒连下 file 渠道仍落盘" \
  || bad "email 异常拖垮了 file 渠道 (inbox: $(ls $(inbox $S5) 2>/dev/null))"
[ -f $S5/testnode/markers/n5.done ] && ok "批次正常收敛 done (marker 存在)" \
  || bad "批次未收敛 (markers: $(ls $S5/testnode/markers 2>/dev/null))"
grep -q "notify" $S5/testnode/scheduler.log 2>/dev/null \
  && ok "email 失败记入 scheduler.log" || bad "scheduler.log 无 notify 失败记录"
stop_daemon $S5

# ---------- 场景 6: 批次级 notify=false -> 不发 ----------
echo "--- 场景 6: 批次级 notify=false ---"
S6=/tmp/sched_ntf6; rm -rf $S6; mkdir -p $S6
mk_config $S6 '{"file": {}}'
cat > $S6/batch.json << EOF
{
  "name": "n6", "mode": "mix", "notify": false,
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "print('ok')"], "duration_min": 5, "max_retry": 0}
  ]
}
EOF
export SCHED_STATE=$S6 SCHED_CONFIG=$S6/config.json
$PY -m gsched.cli submit $S6/batch.json >/dev/null 2>&1 || { bad "n6 submit 失败"; exit 1; }
start_fake $S6
# 等批次收敛 (marker 出现) 再断言 inbox 为空
for _ in $(seq 1 30); do [ -f $S6/testnode/markers/n6.done ] && break; sleep 1; done
[ -f $S6/testnode/markers/n6.done ] && ok "批次 done (对照: 收敛正常)" || bad "n6 未收敛"
sleep 2
[ "$(count_inbox $S6 '*.json')" = "0" ] && ok "notify=false -> inbox 无文件" \
  || bad "notify=false 仍发通知 (inbox: $(ls $(inbox $S6) 2>/dev/null))"
stop_daemon $S6

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" = "0" ]
