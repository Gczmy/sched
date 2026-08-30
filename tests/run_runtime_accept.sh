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
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 3
wait_for '[ "$(count_st $S rt_free done)" = "1" ]' 30 && ok "自由格式任务正常执行" || bad "任务未执行"

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
 {'id':'t1','runtime':{'venv_alias':'k'},'cmd':['/bin/bash','-c','echo VA_OK'],'duration_min':1}]}
json.dump(spec,open('$S/va.json','w'))"
VA=$(sched submit $S/va.json 2>&1)
echo "$VA" | grep -q "已入队" && ok "venv_alias 通道入队" || bad "venv_alias 提交失败: $VA"
wait_for '[ "$(count_st $S rt_va done)" = "1" ]' 30 && ok "venv_alias 任务执行完成" || bad "未执行"

# ---------- S4: runtime 变更 -> 重跑 ----------
echo "--- S4: runtime 变更触发重跑 ---"
python3 -c "
import json
spec={'name':'rt_va','project':'default','mode':'mix','tasks':[
 {'id':'t1','runtime':{'conda_env':'timerxl2'},'cmd':['/bin/bash','-c','echo VA_OK2'],'duration_min':1}]}
json.dump(spec,open('$S/va2.json','w'))"
V2=$(sched submit $S/va2.json 2>&1)
echo "$V2" | grep -q "已入队" || { bad "变更后提交失败: $V2"; exit 1; }
wait_for '[ "$(count_st $S rt_va skip)" = "0" ]' 5   # 不应出现新 SKIP
DONE2=$(count_st $S rt_va done)
[ "$DONE2" = "1" ] && ok "runtime 变更 -> 指纹变化触发重跑 (done=$DONE2)" \
  || bad "重跑异常 (done=$DONE2)"

stop_daemon() {
  export SCHED_STATE=$S SCHED_CONFIG=$S/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
}
stop_daemon

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ $FAIL -eq 0 ] || exit 1
