#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_runtime_accept.sh — B15 runtime 声明与命令解耦验收 (fake-gpu)
# =============================================================================
# 覆盖场景:
#   S1 自由格式 cmd (bash 开头) -> 放行 + dry-run/submit 双警告
#   S2 runtime.conda_env 拼错名 -> 提交期即拒绝
#   S3 runtime.venv_alias 通道 -> 正常运行完成
#   S4 runtime 变更 -> 指纹变化触发重跑 (环境漂移可审计)
#
# 用法: bash sched/tests/run_runtime_accept.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
# submit 会自动 ensure_running；在任何非 dry-run 提交前固定单卡 fake 模式。
export SCHED_FAKE_GPUS=0:24

source tests/acceptance_cleanup.sh
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

count_st(){ # $1=dir $2=batch $3=status
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  sched() { $PY -m gsched.cli "$@"; }
  sched status --json 2>/dev/null | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(-1); raise SystemExit
print(sum(1 for j in d['jobs'] if j['batch_name']=='$2' and j['status']=='$3'))"
}
wait_for(){ for _ in $(seq 1 ${2:-40}); do eval "$1" && return 0; sleep 2; done; return 1; }

latest_batch_id() { # $1=batch name
  sched status "$1" --json 2>/dev/null | "$PY" -c '
import json, sys
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
print(batches[0].get("batch_id", "") if batches else "")'
}

task_status() { # $1=<batch id>:<task>
  sched task "$1" --json 2>/dev/null | "$PY" -c '
import json, sys
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    jobs = []
print(jobs[-1].get("status", "") if jobs else "")'
}

wait_task_status() { # $1=<batch id>:<task> $2=status $3=timeout
  local actual
  for _ in $(seq 1 ${3:-120}); do
    actual=$(task_status "$1")
    [ "$actual" = "$2" ] && return 0
    case "$actual" in
      done|skip|failed|blocked|cancelled|timed_out|interrupted) return 2 ;;
    esac
    sleep 1
  done
  return 1
}

batch_status() { # $1=batch id
  sched status "$1" --json 2>/dev/null | "$PY" -c '
import json, sys
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
print(batches[0].get("status", "") if batches else "")'
}

wait_batch_status() { # $1=batch id $2=status $3=timeout
  for _ in $(seq 1 ${3:-120}); do
    [ "$(batch_status "$1")" = "$2" ] && return 0
    sleep 1
  done
  return 1
}

sched_accept_make_root S "sched-runtime"
sched_accept_make_root CONDA_ROOT "sched-runtime-conda"
mkdir -p "$CONDA_ROOT/envs/timerxl2"
cat > $S/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S", "gpus": [0],
  "conda_envs_dirs": ["$CONDA_ROOT/envs"],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
export SCHED_STATE=$S SCHED_CONFIG=$S/config.json
sched(){ $PY -m gsched.cli "$@"; }

echo "=== B15 runtime 解耦验收 ==="

# ---------- S1: 自由格式 + 警告 ----------
echo "--- S1: 自由格式 cmd 放行 + 警告 ---"
cat > $S/free.json << EOF
{"name":"rt_free","project":"default","mode":"mix",
 "tasks":[{"id":"t1","cmd":["/bin/bash","-c","echo FREE_OK"],"duration_min":1}]}
EOF
DRY=$(sched submit $S/free.json --dry-run 2>&1)
echo "$DRY" | grep -q "未声明运行环境" && ok "dry-run 打印未声明警告" || bad "dry-run 缺警告"
OUT=$(sched submit $S/free.json 2>&1)
echo "$OUT" | grep -q "已入队" && ok "自由格式 cmd 放行入队" || bad "提交被拒: $OUT"
echo "$OUT" | grep -q "未声明运行环境" && ok "submit 也打印警告" || bad "submit 缺警告"
FREE_BID=$(latest_batch_id rt_free)
[ -n "$FREE_BID" ] || { bad "rt_free 批次 ID 不可见"; exit 1; }
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
wait_task_status "$FREE_BID:t1" done 120 \
  && ok "自由格式任务正常执行" \
  || { bad "任务未完成 (got $(task_status "$FREE_BID:t1"))"; sched daemon status; exit 1; }
wait_batch_status "$FREE_BID" done 120 \
  || { bad "rt_free 批次未收敛 done"; exit 1; }

# ---------- S2: 拼错 conda_env 拒绝 ----------
echo "--- S2: conda_env 拼错名 -> 提交期拒绝 ---"
python3 -c "
import json
spec={'name':'rt_bad','project':'default','mode':'mix','tasks':[
 {'id':'t1','runtime':{'conda_env':'nonexistent'},'cmd':['/bin/bash','-c','x'],'duration_min':1}]}
json.dump(spec,open('$S/bad.json','w'))"
if sched submit $S/bad.json >/dev/null 2>&1; then bad "拼错名竟通过"; else ok "拼错环境名在提交期拒绝"; fi

# ---------- S3: venv_alias 通道 ----------
echo "--- S3: runtime.venv_alias ---"
python3 -c "
import json
spec={'name':'rt_va','project':'default','mode':'mix','tasks':[
 {'id':'t1','runtime':{'venv_alias':'k'},
  'cmd':['/bin/bash','-c','printf VA_OK > "$S/va.out"'],'duration_min':1,
  'artifacts':{'result':{'path':'$S/va.out'}},'paths_escape':True}]}
json.dump(spec,open('$S/va.json','w'))"
VA=$(sched submit $S/va.json 2>&1)
echo "$VA" | grep -q "已入队" && ok "venv_alias 通道入队" || bad "venv_alias 提交失败: $VA"
VA_BID=$(latest_batch_id rt_va)
[ -n "$VA_BID" ] || { bad "首个 rt_va 批次 ID 不可见"; exit 1; }
wait_task_status "$VA_BID:t1" done 120 \
  && ok "venv_alias 任务执行完成" \
  || { bad "venv_alias 任务未在 120s 内完成"; exit 1; }
wait_batch_status "$VA_BID" done 120 \
  || { bad "首个 rt_va 批次未收敛 done"; exit 1; }

# 完全相同 runtime/cmd/artifact 的对照必须命中指纹 SKIP。
VA_SKIP=$(sched submit $S/va.json 2>&1)
echo "$VA_SKIP" | grep -q "已入队" \
  || { bad "runtime 不变对照提交失败: $VA_SKIP"; exit 1; }
VA_SKIP_BID=$(latest_batch_id rt_va)
[ -n "$VA_SKIP_BID" ] && [ "$VA_SKIP_BID" != "$VA_BID" ] \
  || { bad "runtime 不变对照批次 ID 异常"; exit 1; }
wait_task_status "$VA_SKIP_BID:t1" skip 120 \
  && ok "runtime 不变对照命中精确 SKIP" \
  || { bad "runtime 不变对照未 SKIP (got $(task_status "$VA_SKIP_BID:t1"))"; exit 1; }
wait_batch_status "$VA_SKIP_BID" done 120 \
  || { bad "runtime 不变对照批次未收敛 done"; exit 1; }

# ---------- S4: runtime 变更 -> 重跑 ----------
echo "--- S4: runtime 变更触发重跑 ---"
python3 -c "
import json
spec={'name':'rt_va','project':'default','mode':'mix','tasks':[
 {'id':'t1','runtime':{'conda_env':'timerxl2'},
  'cmd':['/bin/bash','-c','printf VA_OK > "$S/va.out"'],'duration_min':1,
  'artifacts':{'result':{'path':'$S/va.out'}},'paths_escape':True}]}
json.dump(spec,open('$S/va2.json','w'))"
V2=$(sched submit $S/va2.json 2>&1)
echo "$V2" | grep -q "已入队" || { bad "变更后提交失败: $V2"; exit 1; }
VA2_BID=$(latest_batch_id rt_va)
[ -n "$VA2_BID" ] && [ "$VA2_BID" != "$VA_SKIP_BID" ] \
  || { bad "runtime 变更后的精确批次 ID 异常"; exit 1; }
wait_task_status "$VA2_BID:t1" done 120 \
  && ok "仅 runtime 变更 -> 指纹变化触发精确新批次重跑" \
  || { bad "runtime 变更后的精确任务未 done (got $(task_status "$VA2_BID:t1"))"; exit 1; }
wait_batch_status "$VA2_BID" done 120 \
  || { bad "runtime 变更批次未收敛 done"; exit 1; }

stop_daemon() {
  export SCHED_STATE=$S SCHED_CONFIG=$S/config.json
  "$PY" -m gsched.cli daemon stop >/dev/null 2>&1
}
stop_daemon || { bad "daemon stop 失败"; exit 1; }

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ $FAIL -eq 0 ] || exit 1
