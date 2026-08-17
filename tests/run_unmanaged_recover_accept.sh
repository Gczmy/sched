#!/bin/bash
# =============================================================================
# run_unmanaged_recover_accept.sh — unmanaged 卡自动恢复验收 (fake-gpu 快速回归)
# =============================================================================
# 背景 (2026-08-15 排雷): 非 sched 外部进程占卡触发孤儿防线 (probe_free) 误判为
#   unmanaged 后, 外部进程退出但状态不恢复 -> GPU 永久空置, 曾需人工 sched gpu-free.
#   修复: allocator.probe_unmanaged() 让 unmanaged 卡物理真实空闲后自动回 free.
#
# 覆盖场景:
#   1. 正常批次 done 后 GPU 回 free (probe_unmanaged 不误伤正常路径)
#   2. 手动置 unmanaged -> daemon tick 自动回 free (核心新增逻辑)
#   3. unmanaged 期间任务不派发, 恢复后新批次正常派发完成 (回归)
#
# 用法: bash sched/tests/run_unmanaged_recover_accept.sh
# 退出码: 0 = 全过, 1 = 有失败 (输出 FAIL 行)
# =============================================================================
set -u
cd "$(dirname "$0")/../.."   # 仓库根
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT/sched${PYTHONPATH:+:$PYTHONPATH}"   # sched 包零依赖, 无需 pip install
HOST=testnode   # 定案 43 (P6): hostname() 读 config node 字段, 测试 config 统一 node=testnode

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

run_batch() { # $1=state_dir  $2=batch -> daemon log 路径
  local st=$1 batch=$2
  export SCHED_STATE=$st SCHED_CONFIG=$st/config.json
  $PY -m gsched.cli submit "$batch" >/dev/null 2>&1 || return 1
  env SCHED_STATE=$st SCHED_CONFIG=$st/config.json SCHED_FAKE_GPUS=0 \
      $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
  echo "$st/$HOST/scheduler.log"
}

gpu_status() { # $1=state_dir -> GPU 状态行 (idx|status)
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli status 2>/dev/null | grep -E '^  GPU0' | awk '{print $2}' | tr -d '[]'
}

set_gpu_status() { # $1=state_dir $2=status
  $PY -c "
import sqlite3, sys
db = '$1/$HOST/state.db'
st = '$2'
c = sqlite3.connect(db)
c.execute(\"UPDATE gpus SET status=?, job_id=NULL, updated_at=datetime('now') WHERE idx=0\", (st,))
c.commit(); c.close()
"
}

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  pkill -f "gsched.dispatcher_main" 2>/dev/null
  sleep 1
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

echo "=== unmanaged 自动恢复验收 (fake-gpu) ==="

# ---------- 场景 1: 正常批次 done 后 GPU 回 free (不误伤) ----------
echo "--- 场景 1: 正常路径 GPU 回 free ---"
S1=/tmp/sched_acc_u1; rm -rf $S1; mkdir -p $S1
mk_config $S1
cat > $S1/batch.json << EOF
{
  "name": "u1",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S1/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S1/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S1 $S1/batch.json)
for _ in $(seq 1 30); do
  [ "$(gpu_status $S1)" = "free" ] && break
  sleep 1
done
if [ "$(gpu_status $S1)" = "free" ]; then
  ok "场景1: 正常批次完成后 GPU0=free (probe_unmanaged 不误伤)"
else
  bad "场景1: GPU0 未回 free (实际: $(gpu_status $S1))"
fi
stop_daemon $S1

# ---------- 场景 2: 手动置 unmanaged -> 自动回 free (核心) ----------
echo "--- 场景 2: unmanaged 卡自动恢复 ---"
S2=/tmp/sched_acc_u2; rm -rf $S2; mkdir -p $S2
mk_config $S2
cat > $S2/batch.json << EOF
{
  "name": "u2",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S2/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S2/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S2 $S2/batch.json)
for _ in $(seq 1 30); do
  [ "$(gpu_status $S2)" = "free" ] && break
  sleep 1
done
set_gpu_status $S2 unmanaged   # 模拟孤儿防线误判
ST0=$(gpu_status $S2)
if [ "$ST0" != "unmanaged" ]; then
  bad "场景2: 前置失败, 未能置为 unmanaged (实际: $ST0)"
else
  # fake 模式 probe_unmanaged 单 tick 即恢复 (confirm 恒 True); 等 2 tick 余量
  for _ in $(seq 1 8); do
    [ "$(gpu_status $S2)" = "free" ] && break
    sleep 2
  done
  if [ "$(gpu_status $S2)" = "free" ]; then
    ok "场景2: unmanaged -> 自动回 free (probe_unmanaged 生效)"
  else
    bad "场景2: unmanaged 未自动恢复 (实际: $(gpu_status $S2))"
  fi
fi
stop_daemon $S2# ---------- 场景 3: 恢复后新批次正常派发完成 ----------
echo "--- 场景 3: unmanaged 恢复后新批次可派发 ---"
S3=/tmp/sched_acc_u3; rm -rf $S3; mkdir -p $S3
mk_config $S3
cat > $S3/batch.json << EOF
{
  "name": "u3",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S3/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S3/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S3 $S3/batch.json)
for _ in $(seq 1 30); do
  if [ -f "$S3/t1.txt" ]; then break; fi
  sleep 1
done
if [ -f "$S3/t1.txt" ]; then
  ok "场景3: 批次任务正常派发完成 (产物生成)"
else
  bad "场景3: 批次任务未完成"
fi
stop_daemon $S3

# ---------- 场景 5: blocked 批次 retry 后自动回 active (定案 37) ----------
echo "--- 场景 5: blocked 批次 retry 后自动回 active ---"
S5=/tmp/sched_acc_u5; rm -rf $S5; mkdir -p $S5
mk_config $S5
cat > $S5/batch.json << EOF
{
  "name": "u5",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(5); open('$S5/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S5/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S5 $S5/batch.json)
for _ in $(seq 1 15); do
  [ "$(gpu_status $S5)" = "assigned" ] && break
  sleep 1
done
if [ "$(gpu_status $S5)" != "assigned" ]; then
  bad "场景5: 前置失败, 任务未 running"
else
  # stop 杀任务 -> 批次 blocked; 重启 daemon
  export SCHED_STATE=$S5 SCHED_CONFIG=$S5/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1; sleep 1
  env SCHED_STATE=$S5 SCHED_CONFIG=$S5/config.json SCHED_FAKE_GPUS=0 \
      $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
  sleep 3
  if ! $PY -m gsched.cli status --json 2>/dev/null | grep -q '"status": "blocked"'; then
    bad "场景5: 前置失败, 批次未 blocked"
  else
    # 人工 retry -> 任务 pending -> daemon 下一轮自动回 active 并重跑
    export SCHED_STATE=$S5 SCHED_CONFIG=$S5/config.json
    $PY -m gsched.cli retry u5:t1 >/dev/null 2>&1
    # retry 后需等 daemon tick (10s) 回 active + 派发 + 任务跑 5s
    for _ in $(seq 1 40); do
      if [ -f "$S5/t1.txt" ]; then break; fi
      sleep 1
    done
    if [ -f "$S5/t1.txt" ]; then
      ok "场景5: retry 后批次自动回 active 并重跑完成 (无手工 UPDATE)"
    else
      bad "场景5: retry 后未自动重跑 (批次可能仍 blocked)"
    fi
  fi
fi
stop_daemon $S5

# ---------- 场景 6: 空转自动退出 + submit 自动拉起 (定案 38) ----------
echo "--- 场景 6: idle 自动退出 + submit 自动拉起 daemon ---"
daemon_alive() { # $1=state_dir -> 1 alive / 0 dead
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -c "from gsched import daemon; import sys; sys.exit(0 if daemon.is_running() else 1)" 2>/dev/null && echo 1 || echo 0
}
S6=/tmp/sched_acc_u6; rm -rf $S6; mkdir -p $S6
cat > $S6/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$S6", "gpus": [0], "idle_timeout_min": 1,
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
# u6a: 提交即自动拉起 daemon (ensure_running, SCHED_FAKE_GPUS 驱动 fake)
cat > $S6/batch_a.json << EOF
{
  "name": "u6a",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S6/a.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S6/a.txt"}}, "paths_escape": true}
  ]
}
EOF
env SCHED_STATE=$S6 SCHED_CONFIG=$S6/config.json SCHED_FAKE_GPUS=0 \
    $PY -m gsched.cli submit $S6/batch_a.json >/dev/null 2>&1
for _ in $(seq 1 20); do
  [ -f "$S6/a.txt" ] && break
  sleep 1
done
if [ ! -f "$S6/a.txt" ]; then
  bad "场景6: u6a 未完成 (自动拉起失败?)"
else
  # 等 idle 超时 (idle_timeout_min=1 -> 60s + tick 边界)
  for _ in $(seq 1 30); do
    [ "$(daemon_alive $S6)" = "0" ] && break
    sleep 3
  done
  if [ "$(daemon_alive $S6)" = "0" ]; then
    ok "场景6a: 连续 idle 1min 后 daemon 自动退出"
  else
    bad "场景6a: daemon 未自动退出 (idle_timeout 未生效)"
  fi
fi
# u6b: 提交新批次 -> ensure_running 自动拉起 daemon -> 完成
cat > $S6/batch_b.json << EOF
{
  "name": "u6b",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S6/b.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S6/b.txt"}}, "paths_escape": true}
  ]
}
EOF
env SCHED_STATE=$S6 SCHED_CONFIG=$S6/config.json SCHED_FAKE_GPUS=0 \
    $PY -m gsched.cli submit $S6/batch_b.json >/dev/null 2>&1
for _ in $(seq 1 20); do
  [ -f "$S6/b.txt" ] && break
  sleep 1
done
if [ -f "$S6/b.txt" ] && [ "$(daemon_alive $S6)" = "1" ]; then
  ok "场景6b: submit 自动拉起 daemon 并完成新批次 (idle 退出后自愈)"
else
  bad "场景6b: 自动拉起未生效 (产物=$( [ -f "$S6/b.txt" ] && echo yes || echo no ), daemon=$(daemon_alive $S6))"
fi
stop_daemon $S6

# ---------- 场景 4: daemon stop 收尾不残留 assigned 卡 (N11 修复) ----------
echo "--- 场景 4: daemon stop 后 GPU 释放不残留 ---"
S4=/tmp/sched_acc_u4; rm -rf $S4; mkdir -p $S4
mk_config $S4
cat > $S4/batch.json << EOF
{
  "name": "u4",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(30); open('$S4/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S4/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S4 $S4/batch.json)
for _ in $(seq 1 15); do
  [ "$(gpu_status $S4)" = "assigned" ] && break
  sleep 1
done
if [ "$(gpu_status $S4)" != "assigned" ]; then
  bad "场景4: 前置失败, 任务未 running (实际: $(gpu_status $S4))"
else
  export SCHED_STATE=$S4 SCHED_CONFIG=$S4/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1; sleep 1
  # 重启 daemon 让 settle_releasing 把 releasing 转 free (fake 立即)
  env SCHED_STATE=$S4 SCHED_CONFIG=$S4/config.json SCHED_FAKE_GPUS=0 \
      $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
  for _ in $(seq 1 15); do
    [ "$(gpu_status $S4)" = "free" ] && break
    sleep 1
  done
  if [ "$(gpu_status $S4)" = "free" ]; then
    ok "场景4: daemon stop 后 GPU 释放回 free (不残留 assigned)"
  else
    bad "场景4: GPU 残留 (实际: $(gpu_status $S4))"
  fi
fi
stop_daemon $S4

# ---------- 场景 7: gpu_jobs 迁移 + 独占生命周期 (§3.2e A2/B, 定案 39) ----------
echo "--- 场景 7: gpu_jobs 迁移 + 独占模式零行为变化 ---"
# 7a 迁移验证: 独立目录, 手工造旧库 (gpus.job_id 非空, 无 gpu_jobs 表)
S7=/tmp/sched_acc_u7a; rm -rf $S7; mkdir -p $S7
SCHED_STATE=$S7 $PY - <<EOF
import os, sqlite3, sys
sys.path.insert(0, '$ROOT/sched')
st = __import__('gsched.state', fromlist=['x'])
db = '$S7/$HOST/state.db'
os.makedirs(os.path.dirname(db), exist_ok=True)
c = sqlite3.connect(db)
c.executescript("""
CREATE TABLE batches (id TEXT PRIMARY KEY, name TEXT NOT NULL, mode TEXT NOT NULL DEFAULT 'mix',
 depends_on TEXT NOT NULL DEFAULT '[]', gpus TEXT, cwd TEXT, env TEXT, status TEXT NOT NULL DEFAULT 'queued', created_at TEXT NOT NULL);
CREATE TABLE tasks (batch_id TEXT NOT NULL, id TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1,
 spec TEXT NOT NULL, order_idx INTEGER NOT NULL, PRIMARY KEY (batch_id, id, version));
CREATE TABLE jobs (id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, task_id TEXT NOT NULL, version INTEGER NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending', gpu INTEGER, pgid INTEGER, kill_reason TEXT, rc INTEGER, failure TEXT,
 retries INTEGER NOT NULL DEFAULT 0, fingerprint TEXT, stage_fingerprints TEXT, git_rev TEXT,
 submitted_at TEXT, started_at TEXT, finished_at TEXT, UNIQUE (batch_id, task_id, version));
CREATE TABLE gpus (idx INTEGER PRIMARY KEY, status TEXT NOT NULL, job_id TEXT,
 quarantined INTEGER NOT NULL DEFAULT 0, ignore_until TEXT, updated_at TEXT);
INSERT INTO gpus VALUES (0,'assigned','job_A',0,NULL,'2026-08-15 12:00:00');
INSERT INTO gpus VALUES (1,'free',NULL,0,NULL,'2026-08-15 12:00:00');
""")
c.commit(); c.close()
st.init_db()
c = sqlite3.connect(db); c.row_factory = sqlite3.Row
rows = [(r['gpu_id'], r['job_id']) for r in c.execute("SELECT * FROM gpu_jobs").fetchall()]
c.close()
assert rows == [(0, 'job_A')], rows
print('MIGRATE_OK')
EOF
if [ $? -eq 0 ]; then ok "场景7a: gpus.job_id 存量行迁移到 gpu_jobs (每卡 1 行)"; else bad "场景7a: 迁移失败"; fi
# 7b 独占生命周期: 干净目录, 正常批次 -> assigned 有 gpu_jobs 行 -> 完成后行清空 + 回 free
S7B=/tmp/sched_acc_u7b; rm -rf $S7B; mkdir -p $S7B
mk_config $S7B
cat > $S7B/batch.json << EOF
{
  "name": "u7b",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import time; time.sleep(2); open('$S7B/t1.txt','w').write('ok')"], "duration_min": 1, "artifacts": {"a": {"path": "$S7B/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S7B $S7B/batch.json)
GPJ=0
for _ in $(seq 1 30); do
  N=$($PY -c "
import sqlite3
db='$S7B/$HOST/state.db'
c=sqlite3.connect(db)
n=c.execute(\"SELECT COUNT(*) FROM gpu_jobs\").fetchone()[0]
c.close()
print(n)" 2>/dev/null)
  [ "$N" = "0" ] && [ "$(gpu_status $S7B)" = "free" ] && { GPJ=1; break; }
  sleep 1
done
if [ "$GPJ" = "1" ]; then
  ok "场景7b: 独占任务完成后 gpu_jobs 计数归零 + GPU 回 free (零行为变化)"
else
  bad "场景7b: gpu_jobs 残留或 GPU 未回 free (rows=$N, gpu=$(gpu_status $S7B))"
fi
stop_daemon $S7B

# ---------- 场景 8: profile 消费 (定案 39, daemon 侧) ----------
echo "--- 场景 8: SCHED_PROFILE_OUT 注入 + upsert + 删临时 ---"
S8=/tmp/sched_acc_u8; rm -rf $S8; mkdir -p $S8
mk_config $S8
# 任务: 训练侧模拟 - 写 SCHED_PROFILE_OUT 指向的文件 (peak_gib), 声明 profile_key
cat > $S8/batch.json << EOF
{
  "name": "u8",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import json,os; json.dump({'peak_gib': 3.25}, open(os.environ['SCHED_PROFILE_OUT'],'w')); open('$S8/t1.txt','w').write('ok')"], "duration_min": 1, "resources": {"profile_key": "raft/b158/bs4096"}, "artifacts": {"a": {"path": "$S8/t1.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S8 $S8/batch.json)
U8=0
for _ in $(seq 1 30); do
  R=$($PY -c "
import sqlite3
db='$S8/$HOST/state.db'
c=sqlite3.connect(db)
r=c.execute(\"SELECT peak_gib, git_rev FROM profile_cache WHERE profile_key='raft/b158/bs4096'\").fetchone()
c.close()
print(r[0] if r else 'NONE')" 2>/dev/null)
  [ "$R" != "NONE" ] && { U8=1; PEAK=$R; break; }
  sleep 1
done
if [ "$U8" = "1" ]; then
  ok "场景8a: rc=0 后 upsert profile_cache (peak=$PEAK GiB)"
else
  bad "场景8a: profile_cache 未 upsert (peak=$R)"
fi
# 临时文件应已删除
if [ ! -f "$S8/$HOST/profiles/"*u8*t1*.json ] 2>/dev/null; then
  ok "场景8b: 临时 profile 文件已删除"
else
  bad "场景8b: 临时文件残留: $(ls $S8/$HOST/profiles/ 2>/dev/null)"
fi
stop_daemon $S8

# ---------- 场景 8c: 失败任务只删不 upsert ----------
echo "--- 场景 8c: 失败任务 profile 不入库 ---"
S8C=/tmp/sched_acc_u8c; rm -rf $S8C; mkdir -p $S8C
mk_config $S8C
cat > $S8C/batch.json << EOF
{
  "name": "u8c",
  "mode": "mix",
  "tasks": [
    {"id": "t1", "cmd": ["{VENV:k}", "-c", "import json,os; json.dump({'peak_gib': 9.9}, open(os.environ['SCHED_PROFILE_OUT'],'w')); exit(3)"], "duration_min": 1, "resources": {"profile_key": "raft/b158/bs4096_fail"}, "artifacts": {"a": {"path": "$S8C/none.txt"}}, "paths_escape": true}
  ]
}
EOF
LOG=$(run_batch $S8C $S8C/batch.json)
U8C=0
for _ in $(seq 1 30); do
  R=$($PY -c "
import sqlite3
db='$S8C/$HOST/state.db'
c=sqlite3.connect(db)
r=c.execute(\"SELECT COUNT(*) FROM profile_cache WHERE profile_key='raft/b158/bs4096_fail'\").fetchone()[0]
c.close()
print(r)" 2>/dev/null)
  [ "$R" = "0" ] && [ "$(gpu_status $S8C)" = "free" ] && { U8C=1; break; }
  sleep 1
done
if [ "$U8C" = "1" ]; then
  ok "场景8c: 失败任务 profile 未入库 + 临时已清 + GPU 回 free"
else
  bad "场景8c: 失败任务 profile 异常入库或未清理 (rows=$R, gpu=$(gpu_status $S8C))"
fi
stop_daemon $S8C

# ---------- 场景 9: M8 releasing 判据 (compute-apps pid 归属对照, §3.2e C) ----------
echo "--- 场景 9: M8 pid 归属判据 (残留框架进程等 / 外部进程判净) ---"
S9=/tmp/sched_acc_u9; rm -rf $S9; mkdir -p $S9
SCHED_STATE=$S9 SCHED_FAKE_GPUS=0 $PY - <<'EOF'
import os, sys
sys.path.insert(0, os.getcwd() + '/sched')
import gsched.state as st
st.init_db()
with st.connect() as conn:
    st.init_gpus(conn, [0])
    # 造一个已知 job (pgid=111, 框架记录在案): M8 归属对照用
    conn.execute("INSERT INTO jobs (id,batch_id,task_id,version,status,pgid,submitted_at) "
                 "VALUES ('known','b','t',1,'done',111,'2026-08-15 12:00:00')")
from gsched.allocator import Allocator
al = Allocator([0], fake=True)

def set_releasing():
    with st.connect() as conn:
        # 注意: 必须用本地时间 (datetime('now','localtime')) —— 框架 state.now() 是本地,
        # settle_releasing 用 time.mktime 解析 (假定本地); 用 UTC 会算出 ~8h  elapsed
        # 误触发 5min -> unmanaged 分支
        conn.execute("UPDATE gpus SET status='releasing', job_id=NULL, updated_at=datetime('now','localtime') WHERE idx=0")

def gpu_status():
    with st.connect() as conn:
        return conn.execute("SELECT status FROM gpus WHERE idx=0").fetchone()['status']

# 9a: 残留框架进程 (pid 111 的 pgid 属已知 job pgid=111) -> 卡不转 free
set_releasing()
os.environ['SCHED_FAKE_COMPUTE_APPS'] = '0:111'
assert al._card_has_compute(0) is True, al._card_has_compute(0)  # 残留框架进程
assert al.settle_releasing() == ([], [])
assert gpu_status() == 'releasing', gpu_status()
print('9a OK')
# 9b: 外部进程 (pid 999 不属于任何已知 job) -> 判干净 -> free
os.environ['SCHED_FAKE_COMPUTE_APPS'] = '0:999'
assert al._card_has_compute(0) is False, al._card_has_compute(0)  # 外部进程
assert al.settle_releasing() == ([0], [])
assert gpu_status() == 'free', gpu_status()
print('9b OK')
# 9c: 无进程 -> free (常规路径不受影响)
set_releasing()
os.environ['SCHED_FAKE_COMPUTE_APPS'] = ''
assert al._card_has_compute(0) is False
assert al.settle_releasing() == ([0], [])
assert gpu_status() == 'free', gpu_status()
print('9c OK')
print('M8_OK')
EOF
if [ $? -eq 0 ]; then ok "场景9: M8 pid 归属判据 (残留框架进程等 / 外部进程判净 / 无进程即 free)"; else bad "场景9: M8 判据失败"; fi

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" -eq 0 ] || exit 1
