#!/bin/bash
# =============================================================================
# run_taskenv_accept.sh — B18 部署级任务环境缺省值验收 (fake-gpu)
# =============================================================================
# 覆盖:
#   S1 task_default_env 注入生效 (无 batch/task env 声明时)
#   S2 batch env 覆盖优先级
#   S3 非法配置拒绝
#
# 用法: bash sched/tests/run_taskenv_accept.sh
# =============================================================================
set -u
cd "$(dirname "$0")/.."
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ✅ $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  ❌ $1"; }
wait_for(){ for _ in $(seq 1 ${2:-40}); do eval "$1" && return 0; sleep 2; done; return 1; }

S=/tmp/sched_tenv; rm -rf $S; mkdir -p $S
cat > $S/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S", "gpus": [0],
  "task_default_env": {"SMOKE_MARK": "default_on", "PY_USERSITE_TEST": "1"},
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
export SCHED_STATE=$S SCHED_CONFIG=$S/config.json
sched(){ $PY -m gsched.cli "$@"; }

echo "=== B18 task_default_env 验收 ==="

cat > $S/t1.json << EOF
{"name":"te_def","project":"default","mode":"mix",
 "tasks":[{"id":"t1","duration_min":1,
  "cmd":["{VENV:k}","-c","import os;print('MARK',os.environ.get('SMOKE_MARK'));print('PYSITE',os.environ.get('PY_USERSITE_TEST'))"]}]}
EOF
python3 - << PYEOF
import json
spec = json.load(open("$S/t1.json"))
spec["name"] = "te_ovr"
spec["tasks"][0]["env"] = {"SMOKE_MARK": "batch_win"}
json.dump(spec, open("$S/t2.json", "w"), indent=2)
PYEOF

sched submit $S/t1.json >/dev/null 2>&1 && ok "t1 提交" || bad "t1 提交失败"
sched submit $S/t2.json >/dev/null 2>&1 && ok "t2 提交" || bad "t2 提交失败"
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 4
# 等 t2 完成 (单卡串行, 排在 t1 之后)
wait_for '[ -n "$(ls $S/testnode/logs/te_ovr-*/t1-v1.log 2>/dev/null)" ]' 40 \
  && ok "t2 已执行" || bad "t2 未执行"
L1=$(ls $S/testnode/logs/te_def-*/t1-v1.log 2>/dev/null | head -1)
L2=$(ls $S/testnode/logs/te_ovr-*/t1-v1.log 2>/dev/null | head -1)
grep -q "MARK default_on" "$L1" && ok "缺省值注入生效" || bad "缺省值未注入: $(cat $L1 2>/dev/null)"
grep -q "PYSITE 1" "$L1" && ok "第二键同时注入" || bad "第二键缺失"
grep -q "MARK batch_win" "$L2" && ok "batch env 覆盖优先" || bad "覆盖失败: $(cat $L2 2>/dev/null)"

echo "--- S3 非法配置拒绝 ---"
BADCFG='{"task_default_env": [1,2]}'
if python3 -c "
import sys; sys.path.insert(0,'$ROOT')
from gsched.config import load_config, ConfigError
import json
open('$S/bad.json','w').write(json.dumps(json.loads('$BADCFG')))
try:
    load_config('$S/bad.json'); print('PASS_THROUGH')
except ConfigError: print('REJECTED')" | grep -q REJECTED; then
  ok "非法 task_default_env 校验拒绝"
else
  bad "校验未拦截"
fi

$PY -m gsched.cli daemon stop >/dev/null 2>&1
pkill -f "gsched.dispatcher_main" 2>/dev/null

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ $FAIL -eq 0 ] || exit 1
