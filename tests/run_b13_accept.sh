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
source tests/acceptance_cleanup.sh
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
# submit 会自动 ensure_running；独立运行本验收时也必须先进入 fake 模式。
export SCHED_FAKE_GPUS=0:24

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() {
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
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
n = sum(1 for j in d['jobs'] if j['batch_name'] == '$2' and j['status'] == '$3')
print(n)"
}

wait_for() { # $1=条件 $2=超时秒
  for _ in $(seq 1 ${2:-30}); do
    eval "$1" && return 0
    sleep 1
  done
  return 1
}

latest_batch_id() { # $1=state dir $2=batch name
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli status "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
print(batches[0].get("batch_id", "") if batches else "")'
}

submit_batch_id() { # $1=state dir $2=spec path $3=batch name
  local output rc bid
  output=$(SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli submit "$2" 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "submit failed (rc=$rc): $output" >&2
    return "$rc"
  fi
  bid=$(latest_batch_id "$1" "$3")
  if [ -z "$bid" ]; then
    echo "submit succeeded but latest batch id was not visible: $output" >&2
    return 1
  fi
  printf '%s\n' "$bid"
}

dry_run_task_skip() { # $1=state dir $2=spec path
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli submit "$2" --dry-run --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    tasks = json.load(sys.stdin).get("tasks", [])
except Exception:
    tasks = []
if tasks:
    print("true" if tasks[0].get("skip") is True else "false")'
}

task_status() { # $1=state dir $2=<batch id>:<task>
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli task "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    jobs = []
print(jobs[-1].get("status", "") if jobs else "")'
}

batch_status() { # $1=state dir $2=batch id
  SCHED_STATE=$1 SCHED_CONFIG=$1/config.json \
    "$PY" -m gsched.cli status "$2" --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
print(batches[0].get("status", "") if batches else "")'
}

wait_task_terminal() { # $1=state dir $2=<batch id>:<task> $3=timeout seconds
  local status
  for _ in $(seq 1 ${3:-120}); do
    status=$(task_status "$1" "$2")
    case "$status" in
      done|skip|failed|blocked|cancelled|timed_out|interrupted) return 0 ;;
    esac
    sleep 1
  done
  return 1
}

wait_batch_terminal() { # $1=state dir $2=batch id $3=timeout seconds
  local status
  for _ in $(seq 1 ${3:-120}); do
    status=$(batch_status "$1" "$2")
    case "$status" in
      done|blocked|cancelled|discarded) return 0 ;;
    esac
    sleep 1
  done
  return 1
}

wait_batch_done() { # $1=state dir $2=batch id $3=timeout seconds
  if ! wait_batch_terminal "$1" "$2" "${3:-120}"; then
    return 1
  fi
  [ "$(batch_status "$1" "$2")" = "done" ]
}

assert_task_status() { # $1=state dir $2=batch id $3=expected $4=description
  local actual
  if ! wait_task_terminal "$1" "$2:t1" 120; then
    bad "$4 (等待精确任务 $2:t1 终态超时)"
    return 1
  fi
  actual=$(task_status "$1" "$2:t1")
  if [ "$actual" = "$3" ]; then
    ok "$4"
    return 0
  fi
  bad "$4 (期望=$3, 实际=$actual, 任务=$2:t1)"
  return 1
}

run_count() { # $1=count file
  if [ -f "$1" ]; then
    tr -d '[:space:]' < "$1"
  else
    echo 0
  fi
}

assert_count_increment() { # $1=before $2=after $3=description
  local expected
  case "$1:$2" in
    *[!0-9:]*|:*|*:)
      bad "$3 (非法计数 $1 -> $2)"
      return 1
      ;;
  esac
  expected=$(( $1 + 1 ))
  if [ "$2" -eq "$expected" ]; then
    ok "$3"
    return 0
  fi
  bad "$3 (计数异常 $1 -> $2)"
  return 1
}

echo "=== B13 SelfDistOTS 改进批次验收 ==="

# ---------- S1: 环境净化 ----------
echo "--- S1: conda 环境净化 + VENV 注入 ---"
sched_accept_make_root S "sched-b13-env"
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
ENV_BID=$(submit_batch_id "$S" "$S/b1.json" envp) || {
  bad "净化任务提交失败"
  exit 1
}
# 关键: 以被污染的父环境启动 daemon (模拟 kronos_ft 下启动)
CONDA_PREFIX=/poison/conda CONDA_DEFAULT_ENV=kronos_ft \
  SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
assert_task_status "$S" "$ENV_BID" done "净化任务执行完成" || {
  stop_daemon "$S"
  exit 1
}
wait_batch_done "$S" "$ENV_BID" 120 || {
  bad "净化任务批次未收敛"
  stop_daemon "$S"
  exit 1
}
LOGF=$(ls $S/testnode/logs/envp-*/t1-v1.log)
grep -q "PREFIX=\[$S/envs/myenv\]" "$LOGF" && ok "CONDA_PREFIX 注入为任务自己的 env" || bad "PREFIX 异常: $(grep PREFIX= $LOGF)"
grep -q "DENV=\[\]" "$LOGF" && ok "污染键 CONDA_DEFAULT_ENV 已剥离" || bad "DENV 未剥离: $(grep DENV= $LOGF)"
grep -q "PATHFIRST=\[$S/envs/myenv/bin\]" "$LOGF" && ok "PATH 前缀注入 env bin" || bad "PATH 异常: $(grep PATHFIRST= $LOGF)"
stop_daemon $S

# ---------- S2/S3/S4: 指纹三件套 ----------
echo "--- S2-S4: dirty-tree 指纹 / force_rerun / clean ---"
sched_accept_make_root W "sched-b13-repo"
cd "$W"
git init -q . && git config user.email t@t && git config user.name t
sched_accept_make_root OUT "sched-b13-out"
cat > runner.py << EOF
from pathlib import Path

counter = Path("$OUT/run_count")
count = int(counter.read_text()) if counter.exists() else 0
counter.write_text(str(count + 1))
Path("$OUT/res.json").write_text('{"result": 1}')
EOF
git add -A && git commit -qm init
sched_accept_make_root S2 "sched-b13-fingerprint"
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
    "artifacts": {"r": {"path": "$OUT/res.json"}}, "paths_escape": true
  }]
}
EOF
export SCHED_STATE=$S2 SCHED_CONFIG=$S2/config.json
FIRST_BID=$(submit_batch_id "$S2" "$S2/b.json" fp) || {
  bad "首跑提交失败"
  exit 1
}
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
assert_task_status "$S2" "$FIRST_BID" done "首跑完成" || {
  stop_daemon "$S2"
  exit 1
}
wait_batch_done "$S2" "$FIRST_BID" 120 || {
  bad "首跑批次未收敛"
  stop_daemon "$S2"
  exit 1
}
# 同内容重提 -> SKIP
SKIP_BID=$(submit_batch_id "$S2" "$S2/b.json" fp) || {
  bad "同内容重提失败"
  stop_daemon "$S2"
  exit 1
}
assert_task_status "$S2" "$SKIP_BID" skip "同内容重提正确 SKIP" || {
  stop_daemon "$S2"
  exit 1
}
wait_batch_done "$S2" "$SKIP_BID" 120 || {
  bad "SKIP 批次未收敛"
  stop_daemon "$S2"
  exit 1
}
# 工作区改动不提交 -> 重提应重跑 (dirty-tree 指纹)
echo "" >> $W/runner.py   # 未提交改动 -> dirty-tree 指纹必变
DIRTY_BEFORE=$(run_count "$OUT/run_count")
DIRTY_BID=$(submit_batch_id "$S2" "$S2/b.json" fp) || {
  bad "脏树提交失败"
  stop_daemon "$S2"
  exit 1
}
assert_task_status "$S2" "$DIRTY_BID" done \
  "dirty-tree: 未提交改动触发重跑 (不再误 SKIP)" || {
  stop_daemon "$S2"
  exit 1
}
DIRTY_AFTER=$(run_count "$OUT/run_count")
assert_count_increment "$DIRTY_BEFORE" "$DIRTY_AFTER" \
  "dirty-tree: 执行计数精确增加一次" || {
  stop_daemon "$S2"
  exit 1
}
wait_batch_done "$S2" "$DIRTY_BID" 120 || {
  bad "dirty-tree 批次未收敛"
  stop_daemon "$S2"
  exit 1
}
git -C $W add -A && git -C $W commit -qm change2
# 提交 dirty 变更后先建立当前 clean fingerprint 的可信 producer。
CLEAN_SEED_BID=$(submit_batch_id "$S2" "$S2/b.json" fp) || {
  bad "clean fingerprint producer 提交失败"
  stop_daemon "$S2"
  exit 1
}
assert_task_status "$S2" "$CLEAN_SEED_BID" done \
  "已建立 clean fingerprint producer" || {
  stop_daemon "$S2"
  exit 1
}
wait_batch_done "$S2" "$CLEAN_SEED_BID" 120 || {
  bad "clean fingerprint producer 批次未收敛"
  stop_daemon "$S2"
  exit 1
}
if [ "$(dry_run_task_skip "$S2" "$S2/b.json")" = "true" ]; then
  ok "force_rerun 前普通提交确实可 SKIP"
else
  bad "force_rerun 前未建立可 SKIP 基线"
  stop_daemon "$S2"
  exit 1
fi

# S3: force_rerun —— 保留有效产物和同一指纹，仍必须再执行一次。
"$PY" - << PYEOF
import json
spec = json.load(open("$S2/b.json"))
spec["force_rerun"] = True
json.dump(spec, open("$S2/bf.json", "w"), indent=2)
PYEOF
FORCE_BEFORE=$(run_count "$OUT/run_count")
FORCE_BID=$(submit_batch_id "$S2" "$S2/bf.json" fp) || {
  bad "force_rerun 提交失败"
  stop_daemon "$S2"
  exit 1
}
assert_task_status "$S2" "$FORCE_BID" done \
  "force_rerun: 有效产物与同指纹仍强制重跑" || {
  stop_daemon "$S2"
  exit 1
}
FORCE_AFTER=$(run_count "$OUT/run_count")
assert_count_increment "$FORCE_BEFORE" "$FORCE_AFTER" \
  "force_rerun: 执行计数精确增加一次" || {
  stop_daemon "$S2"
  exit 1
}
[ -f "$OUT/res.json" ] && ok "产物已重新生成" || bad "产物缺失"
wait_batch_done "$S2" "$FORCE_BID" 120 || {
  bad "force_rerun 批次未收敛"
  stop_daemon "$S2"
  exit 1
}

# ---------- S4: sched clean ----------
echo "--- S4: sched clean 清指纹+删产物 ---"
S4_SKIP_BID=$(submit_batch_id "$S2" "$S2/b.json" fp) || {
  bad "S4 提交失败"
  stop_daemon "$S2"
  exit 1
}
assert_task_status "$S2" "$S4_SKIP_BID" skip "clean 前正确 SKIP" || {
  stop_daemon "$S2"
  exit 1
}
wait_batch_done "$S2" "$S4_SKIP_BID" 120 || {
  bad "clean 前 SKIP 批次未收敛"
  stop_daemon "$S2"
  exit 1
}
CLEAN_BEFORE=$(run_count "$OUT/run_count")
stop_daemon "$S2"
CLEANOUT=$("$PY" -m gsched.cli clean "$S4_SKIP_BID" --yes 2>&1)
CLEAN_RC=$?
if [ "$CLEAN_RC" -ne 0 ]; then
  bad "clean 异常 (rc=$CLEAN_RC): $CLEANOUT"
  exit 1
fi
[ -f "$OUT/res.json" ] && {
  bad "产物未被 clean 删除"
  exit 1
} || ok "clean 已删除产物文件"
SCHED_FAKE_GPUS=0:24 "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1 || {
  bad "clean 后 daemon 重启失败"
  stop_daemon "$S2"
  exit 1
}
assert_task_status "$S2" "$S4_SKIP_BID" done "clean 后同一任务重跑 (不再 SKIP)" || {
  stop_daemon "$S2"
  exit 1
}
CLEAN_AFTER=$(run_count "$OUT/run_count")
assert_count_increment "$CLEAN_BEFORE" "$CLEAN_AFTER" \
  "clean 后执行计数精确增加一次" || {
  stop_daemon "$S2"
  exit 1
}

# ---------- S5: artifact has_key 规则 ----------
echo "--- S5: artifact has_key ---"
python3 - << PYEOF
import json
base = json.load(open("$S2/b.json"))
base["tasks"][0]["cmd"] = ["{VENV:k}", "-c",
    "import json; open('$OUT/hk.json','w').write(json.dumps({'avg_mse': 0.1}))"]
base["tasks"][0]["artifacts"] = {
    "hk": {"path": "$OUT/hk.json", "has_key": "avg_mse"}}
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

stop_daemon $S2

# ---------- S6: progress_regex ----------
echo "--- S6: progress_regex 进度入库与展示 ---"
sched_accept_make_root S6 "sched-b13-progress"
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
sched_accept_make_root S7 "sched-b13-cancel"
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
