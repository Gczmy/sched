#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_b13_accept.sh — SelfDistOTS 改进批次验收 (环境净化/指纹/artifact/进度/批量取消)
# =============================================================================
# 覆盖场景:
#   S1 环境净化: daemon 的 conda 污染键不泄漏; VENV 感知注入 CONDA_PREFIX/PATH
#   S2 dirty-tree 指纹: 未提交改动 -> 不再 SKIP; 干净树 -> 正常 SKIP
#   S3 force_rerun: 声明后跳过 SKIP 判定强制重跑
#   S4 sched clean: 清指纹后重跑
#   S5 artifact 新规则: has_key 通过/缺失判定
#   S6 progress_regex: 运行中任务进度入库并显示于 status --json
#   S7 cancel --project 批量取消
#   S8 日志毫秒时间戳
#
# 用法: bash sched/tests/run_b13_accept.sh
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

count_status() { # $1=dir $2=batch $3=status
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli status --json 2>/dev/null | \
    $PY -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print(-1); raise SystemExit
n = sum(1 for j in d['jobs'] if j['batch'].split('-')[0] == '$2' and j['status'] == '$3')
print(n)"
}

wait_batch_done() { # $1=dir $2=batch名前缀 $3=超时秒 —— 等同名批次全部收敛终态
  for _ in $(seq 1 ${3:-30}); do
    SCHED_STATE=$1 SCHED_CONFIG=$1/config.json $PY -m gsched.cli status --json 2>/dev/null | \
      $PY -c "
import json, sys
d = json.load(sys.stdin)
bad = [b for b in d['batches'] if b['name'].split('-')[0] == '$2' and b['status'] not in ('done','blocked','cancelled')]
sys.exit(0 if not bad else 1)" && return 0
    sleep 2
  done
  return 1
}

wait_for() { # $1=条件 $2=超时秒
wait_batch_done() { # $1=dir $2=batch名 $3=超时秒 —— 等同名批次全部终态
  for _ in $(seq 1 ${3:-20}); do
    SCHED_STATE=$1 SCHED_CONFIG=$1/config.json $PY -m gsched.cli status --json 2>/dev/null | \
      $PY -c "
import json, sys
d = json.load(sys.stdin)
bad = [b for b in d['batches'] if b['name'].split('-')[0] == '$2' and b['status'] not in ('done','blocked','cancelled')]
sys.exit(0 if not bad else 1)" && return 0
    sleep 1
  done
  return 1
}

  for _ in $(seq 1 ${2:-30}); do
    eval "$1" && return 0
    sleep 1
  done
  return 1
}

echo "=== B13 SelfDistOTS 改进批次验收 ==="

# ---------- S1: 环境净化 ----------
echo "--- S1: conda 环境净化 + VENV 注入 ---"
S=/tmp/sched_b13_1; rm -rf $S; mkdir -p $S
mkdir -p $S/envs/myenv/bin
cat > $S/envs/myenv/bin/pyprobe << 'EOF'
#!/bin/bash
echo "PREFIX=[$CONDA_PREFIX]"
echo "DENV=[$CONDA_DEFAULT_ENV]"
echo "PATHFIRST=[${PATH%%:*}]"
EOF
chmod +x $S/envs/myenv/bin/pyprobe
cat > $S/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S", "gpus": [0],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"txl": "$S/envs/myenv/bin/pyprobe"}
}
EOF
cat > $S/b1.json << EOF
{
  "name": "envp", "project": "default", "mode": "mix",
  "tasks": [{"id": "t1", "cmd": ["{VENV:txl}"], "duration_min": 1}]
}
EOF
export SCHED_STATE=$S SCHED_CONFIG=$S/config.json
$PY -m gsched.cli submit $S/b1.json >/dev/null 2>&1
# 关键: 以被污染的父环境启动 daemon (模拟 kronos_ft 下启动)
CONDA_PREFIX=/poison/conda CONDA_DEFAULT_ENV=kronos_ft \
  SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
wait_for '[ "$(count_status $S envp done)" = "1" ]' 30 && ok "净化任务执行完成" || bad "任务未完成"
LOGF=$(ls $S/testnode/logs/envp-*/t1-v1.log)
grep -q "PREFIX=\[$S/envs/myenv\]" "$LOGF" && ok "CONDA_PREFIX 注入为任务自己的 env" || bad "PREFIX 异常: $(grep PREFIX= $LOGF)"
grep -q "DENV=\[\]" "$LOGF" && ok "污染键 CONDA_DEFAULT_ENV 已剥离" || bad "DENV 未剥离: $(grep DENV= $LOGF)"
grep -q "PATHFIRST=\[$S/envs/myenv/bin\]" "$LOGF" && ok "PATH 前缀注入 env bin" || bad "PATH 异常: $(grep PATHFIRST= $LOGF)"
stop_daemon $S

# ---------- S2/S3/S4: 指纹三件套 ----------
echo "--- S2-S4: dirty-tree 指纹 / force_rerun / clean ---"
W=/tmp/sched_b13_repo; rm -rf $W; mkdir -p $W
cd "$W"
git init -q . && git config user.email t@t && git config user.name t
cat > runner.py << 'EOF'
open("/tmp/sched_b13_out/res.json", "w").write('{"result": 1}')
EOF
git add -A && git commit -qm init
mkdir -p /tmp/sched_b13_out
S2=/tmp/sched_b13_2; rm -rf $S2; mkdir -p $S2
rm -rf /tmp/sched_b13_out; mkdir -p /tmp/sched_b13_out   # 清残留产物 (防首跑误 SKIP)
cat > $S2/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S2", "gpus": [0],
  "projects": {"default": {"root": "$W", "git": true}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
cat > $S2/b.json << EOF
{
  "name": "fp", "project": "default", "mode": "mix",
  "tasks": [{
    "id": "t1", "cmd": ["{VENV:k}", "runner.py"], "duration_min": 1,
    "artifacts": {"r": {"path": "/tmp/sched_b13_out/res.json"}}, "paths_escape": true
  }]
}
EOF
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
$PY -m gsched.cli submit $S2/b.json >/dev/null 2>&1
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
wait_for '[ "$(count_status $S2 fp done)" = "1" ]' 120 && ok "首跑完成" || bad "首跑未完成"
wait_batch_done $S2 fp 60   # 批次 settle 后再重提 (防同名未终态拒绝)
# 同内容重提 -> SKIP
$PY -m gsched.cli submit $S2/b.json >/dev/null 2>&1
wait_for '[ "$(count_status $S2 fp skip)" = "1" ]' 60 && ok "同内容重提正确 SKIP" || bad "未 SKIP"
wait_batch_done $S2 fp 60   # 批次 settle
# 工作区改动不提交 -> 重提应重跑 (dirty-tree 指纹)
echo "" >> $W/runner.py   # 未提交改动 -> dirty-tree 指纹必变
DIRTY_OUT=$($PY -m gsched.cli submit $S2/b.json 2>&1)
echo "$DIRTY_OUT" | grep -q "已入队" || bad "脏树提交失败: $DIRTY_OUT"
BEFORE=$(count_status $S2 fp done)
wait_for '[ "$(count_status $S2 fp done)" = '"$((BEFORE+1))"' ]' 30 \
  && ok "dirty-tree: 未提交改动触发重跑 (不再误 SKIP)" \
  || bad "脏树仍被 SKIP"
git -C $W add -A && git -C $W commit -qm change2
# S3: force_rerun —— 干净树也强制重跑
python3 - << PYEOF
import json
spec = json.load(open("$S2/b.json"))
spec["name"] = "fpf"
spec["force_rerun"] = True
json.dump(spec, open("$S2/bf.json", "w"), indent=2)
PYEOF
rm -f /tmp/sched_b13_out/res.json
$PY -m gsched.cli submit $S2/bf.json >/dev/null 2>&1
wait_for '[ "$(count_status $S2 fpf done)" = "1" ]' 30 \
  && ok "force_rerun: 干净树也强制重跑" || bad "force_rerun 未生效"
[ -f /tmp/sched_b13_out/res.json ] && ok "产物已重新生成" || bad "产物缺失"

# ---------- S4: sched clean ----------
echo "--- S4: sched clean 清指纹+删产物 ---"
S4OUT=$($PY -m gsched.cli submit $S2/b.json 2>&1)
echo "$S4OUT" | grep -q "已入队" || bad "S4 提交失败: $S4OUT"
wait_for '[ "$(count_status $S2 fp skip)" -ge 1 ]' 45 \
  && ok "clean 前正确 SKIP" || bad "clean 前 未 SKIP"
wait_batch_done $S2 fp 30
CLEANOUT=$($PY -m gsched.cli clean fp --yes 2>&1)
echo "$CLEANOUT" | grep -q "已清除" || bad "clean 异常: $CLEANOUT"
[ -f /tmp/sched_b13_out/res.json ] && bad "产物未被 clean 删除" || ok "clean 已删除产物文件"
$PY -m gsched.cli submit $S2/b.json >/dev/null 2>&1
BEFORE=$(count_status $S2 fp done)
wait_for '[ "$(count_status $S2 fp done)" = '"$((BEFORE+1))"' ]' 40 \
  && ok "clean 后重跑 (不再 SKIP)" || bad "clean 后仍 SKIP"

# ---------- S5: artifact has_key 规则 ----------
echo "--- S5: artifact has_key ---"
python3 - << PYEOF
import json
base = json.load(open("$S2/b.json"))
base["tasks"][0]["cmd"] = ["{VENV:k}", "-c",
    "import json; open('/tmp/sched_b13_out/hk.json','w').write(json.dumps({'avg_mse': 0.1}))"]
base["tasks"][0]["artifacts"] = {
    "hk": {"path": "/tmp/sched_b13_out/hk.json", "has_key": "avg_mse"}}
base["tasks"][0]["paths_escape"] = True
base["name"] = "hkok"
json.dump(base, open("$S2/hkok.json", "w"), indent=2)
base["tasks"][0]["artifacts"]["hk"]["has_key"] = "missing_key"
base["name"] = "hkbad"
json.dump(base, open("$S2/hkbad.json", "w"), indent=2)
PYEOF
HKOK=$($PY -m gsched.cli submit $S2/hkok.json 2>&1)
echo "$HKOK" | grep -q "已入队" || bad "hkok 提交失败: $HKOK"
wait_for '[ "$(count_status $S2 hkok done)" = "1" ]' 60 && ok "has_key 命中 -> done" || bad "has_key 未命中"
HBOUT=$($PY -m gsched.cli submit $S2/hkbad.json 2>&1)
echo "$HBOUT" | grep -q "已入队" || bad "hkbad 提交失败: $HBOUT"
wait_for '[ "$(count_status $S2 hkbad blocked)" -ge 1 ]' 120 && ok "has_key 缺失 -> blocked" || bad "缺键未被拦截"

# ---------- S6: progress_regex ----------
echo "--- S6: progress_regex 进度入库与展示 ---"
S6=/tmp/sched_b13_6; rm -rf $S6; mkdir -p $S6
cat > $S6/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S6", "gpus": [0],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
python3 - << PRGEOF
import json
spec = {
    "name": "prg", "project": "default", "mode": "mix",
    "tasks": [{
        "id": "t1", "duration_min": 1,
        "progress_regex": r"epoch \d+/5",
        "cmd": ["{VENV:k}", "-c",
                "import time\n" +
                "\n".join(f"print('epoch {i}/5', flush=True); time.sleep(3)" for i in range(1, 7))],
    }],
}
json.dump(spec, open("$S6/prg.json", "w"), indent=2)
PRGEOF
export SCHED_STATE=$S6 SCHED_CONFIG=$S6/config.json
$PY -m gsched.cli submit $S6/prg.json >/dev/null 2>&1
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
cat > $S6/unit_prog.py << 'UNITPY'
import sys, sqlite3
sys.path.insert(0, "$ROOT")
from gsched import state
conn = sqlite3.connect(state.db_path())
rows = conn.execute(
    "SELECT progress FROM jobs WHERE progress IS NOT NULL"
).fetchall()
print(rows[0][0] if rows else "")
UNITPY
PROG=""
for _ in $(seq 1 40); do
  PROG=$(SCHED_STATE=$S6 $PY $S6/unit_prog.py)
  [ -n "$PROG" ] && break
  sleep 1
done
[ -n "$PROG" ] && ok "progress_regex 解析入库 (progress=$PROG)" || bad "进度未解析"
stop_daemon $S6

# ---------- S7: cancel --project 批量取消 ----------
echo "--- S7: cancel --project 批量取消 ---"
S7=/tmp/sched_b13_7; rm -rf $S7; mkdir -p $S7
cat > $S7/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S7", "gpus": [0],
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default", "venvs": {"k": "$PY"}
}
EOF
for b in b1 b2; do
cat > $S7/$b.json << EOF
{"name": "$b", "project": "default", "mode": "mix",
 "tasks": [{"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(120)"], "duration_min": 2}]}
EOF
done
export SCHED_STATE=$S7 SCHED_CONFIG=$S7/config.json
$PY -m gsched.cli submit $S7/b1.json >/dev/null 2>&1
$PY -m gsched.cli submit $S7/b2.json >/dev/null 2>&1
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 3
COUT=$($PY -m gsched.cli cancel --project default --yes 2>&1)
wait_for '[ "$(count_status $S7 b1 cancelled)" = "1" ] && [ "$(count_status $S7 b2 cancelled)" = "1" ]' 30 \
  && ok "项目批量取消: 两批次全部收敛 cancelled" || bad "批量取消未收敛: $COUT"
stop_daemon $S7

# ---------- S8: 毫秒时间戳 ----------
echo "--- S8: 毫秒时间戳 ---"
grep -qE "\[[0-9-]+ [0-9:.]+\]" $S/testnode/scheduler.log && \
  grep -qE "\.[0-9]{3}\]" $S/testnode/scheduler.log \
  && ok "scheduler.log 含毫秒时间戳" || bad "时间戳无毫秒"

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ $FAIL -eq 0 ] || exit 1
