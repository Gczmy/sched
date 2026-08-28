#!/bin/bash
export SCHED_ALLOW_FOREIGN_WRITE=1  # 测试在本机跑, config node 写死远端名 — 跳过 B24d 守卫
# =============================================================================
# run_incident_accept.sh — F2 事故快照验收 (fake-gpu 快速回归)
# =============================================================================
# 用途: dispatcher._capture_incident / incidents 表 / CLI 查询 的验证.
#       不烧 GPU (SCHED_FAKE_GPUS).
#
# 覆盖场景:
#   1. 共享装箱 OOM -> 快照生成: kind=oom, co_runners 非空, 结构完整
#   2. diag 集成: sched diag 输出 incident 摘要
#   3. 外部进程判定 (unit): SCHED_FAKE_COMPUTE_APPS 里的陌生 pid -> external
#   4. 裁剪 (unit): 条数/TTL 双限 + blocked 引用豁免
#
# 用法: bash sched/tests/run_incident_accept.sh
# 退出码: 0 = 全过, 1 = 有失败
# =============================================================================
set -u
cd "$(dirname "$0")/.."   # 仓库根 (standalone 布局: tests/ 的上一级)
PY=${PY:-$(command -v python3 || echo python3)}
ROOT=$(pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

stop_daemon() { # $1=state_dir
  export SCHED_STATE=$1 SCHED_CONFIG=$1/config.json
  $PY -m gsched.cli daemon stop >/dev/null 2>&1
  pkill -f "gsched.dispatcher_main" 2>/dev/null
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
    if j['batch'].split('-')[0] == bn and j['status'] == want:
        n += 1
print(n)
"
}

wait_status() { # $1=state_dir $2=batch_name $3=status $4=期望数 $5=超时秒(默认60)
  local st=$1 bn=$2 want=$3 exp=$4 timeout=${5:-60}
  for _ in $(seq 1 $timeout); do
    [ "$(count_status $st $bn $want)" = "$exp" ] && return 0
    sleep 1
  done
  return 1
}

incident_count() { # $1=state_dir -> 行数
  SCHED_STATE=$1 $PY -c "
from gsched import state
import sqlite3, os
p = state.db_path()
if not os.path.exists(p):
    print(0); raise SystemExit
conn = sqlite3.connect(p)
print(conn.execute('SELECT COUNT(*) FROM incidents').fetchone()[0])
"
}

mk_config() { # $1=state_dir  (co_locate 开启)
  cat > $1/config.json << EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "testnode",
  "state_dir": "$1", "gpus": [0],
  "co_locate": true, "co_locate_safety": 0.7,
  "projects": {"default": {"root": "$ROOT", "git": false}},
  "default_project": "default",
  "venvs": {"k": "$PY"}
}
EOF
}

echo "=== F2 事故快照验收 (fake-gpu) ==="

# ---------- 场景 1+2: 共享装箱 OOM -> 快照 + diag 集成 ----------
echo "--- 场景 1: 共享装箱 OOM -> 快照 (co_runners 非空) ---"
S1=/tmp/sched_inc1; rm -rf $S1; mkdir -p $S1
mk_config $S1
cat > $S1/batch.json << EOF
{
  "name": "inc", "mode": "mix",
  "project": "default",
  "tasks": [
    {"id": "oomer",
     "cmd": ["{VENV:k}", "-c", "import time; time.sleep(1); print('CUDA out of memory. Tried to allocate 2.50 GiB', flush=True); raise SystemExit(1)"],
     "duration_min": 5, "max_retry": 0,
     "resources": {"gpu_share": true, "vram_gib": 1.0}},
    {"id": "neighbor",
     "cmd": ["{VENV:k}", "-c", "import time; time.sleep(90)"],
     "duration_min": 5,
     "resources": {"gpu_share": true, "vram_gib": 1.0}}
  ]
}
EOF
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
$PY -m gsched.cli submit $S1/batch.json >/dev/null 2>&1 || { bad "submit 失败"; exit 1; }
SCHED_FAKE_GPUS=0:24 $PY -m gsched.cli daemon start --fake >/dev/null 2>&1
sleep 3
[ "$(count_status $S1 inc running)" = "2" ] && ok "两任务同卡共享 running" \
  || bad "未双 running (running=$(count_status $S1 inc running))"
wait_status $S1 inc blocked 1 && ok "oomer 进入 blocked (max_retry=0)" || bad "oomer 未 blocked"

# 等 reap 完成后查快照
SNAP=$(SCHED_STATE=$S1 $PY -c "
from gsched import state
import json, sqlite3, os
conn = sqlite3.connect(state.db_path())
conn.row_factory = sqlite3.Row
rows = conn.execute(\"SELECT * FROM incidents WHERE kind='oom'\").fetchall()
if not rows:
    print('NONE'); raise SystemExit
r = rows[0]
p = json.loads(r['payload'])
checks = {
  'job_id': r['job_id'],
  'mode': p['failed']['dispatch_mode'],
  'n_co': len(p['co_runners']),
  'has_ext_field': 'external_pids' in p['memory'],
  'has_declared': p['failed']['declared_vram_gib'] == 1.0,
  'cap': p['memory']['cap_gib'],
  'excerpt_ok': bool(p['log_excerpt'] and 'CUDA out of memory' in p['log_excerpt']),
}
print(json.dumps(checks))
")
stop_daemon $S1

[ "$SNAP" != "NONE" ] && ok "快照已生成" || bad "无 oom 快照"
if [ "$SNAP" != "NONE" ]; then
  echo "$SNAP" | grep -q '"mode": "shared"' && ok "dispatch_mode=shared" || bad "mode 不是 shared: $SNAP"
  echo "$SNAP" | grep -q '"n_co": 1' && ok "co_runners 含邻居 (1)" || bad "co_runners 异常: $SNAP"
  echo "$SNAP" | grep -q '"has_ext_field": true' && ok "external_pids 字段存在" || bad "缺 external_pids"
  echo "$SNAP" | grep -q '"has_declared": true' && ok "declared_vram 记录正确" || bad "declared_vram 错误"
  echo "$SNAP" | grep -q '"cap": 24.0' && ok "容量记录 (24 GiB)" || bad "容量缺失"
  echo "$SNAP" | grep -q '"excerpt_ok": true' && ok "日志摘录含 OOM 特征" || bad "摘录缺特征"
fi

echo "--- 场景 2: diag 集成事故判读 ---"
export SCHED_STATE=$S1 SCHED_CONFIG=$S1/config.json
DIAG_OUT=$($PY -m gsched.cli diag "inc:oomer" 2>/dev/null)
echo "$DIAG_OUT" | grep -q "incident #" && ok "diag 输出 incident 摘要" || bad "diag 缺 incident 块"
echo "$DIAG_OUT" | grep -qE "判读假设|邻居:" && ok "diag 有判读/邻居明细" || bad "diag 无判读内容"

# ---------- 场景 3: 外部进程判定 (unit, 不起 daemon) ----------
echo "--- 场景 3: external pid 判定 ---"
S3=/tmp/sched_inc3; rm -rf $S3; mkdir -p $S3
mk_config $S3
cat > $S3/unit_ext.py << 'UNITPY'
import json, os
os.environ["SCHED_FAKE_GPUS"] = "0:24"
os.environ["SCHED_FAKE_COMPUTE_APPS"] = "0:999001"
from gsched import state
state.init_db()
from gsched.allocator import Allocator
a = Allocator([0], fake=True)
ext, degraded = a.incident_external_pids(0)
print(json.dumps({"ext": [e["pid"] for e in ext], "degraded": degraded}))
UNITPY
EXT_OUT=$(SCHED_STATE=$S3 $PY $S3/unit_ext.py)
echo "$EXT_OUT" | grep -q '"ext": \[999001\]' && ok "陌生 pid 判为外部 ($EXT_OUT)" || bad "外部判定失败: $EXT_OUT"

cat > $S3/unit_clean.py << 'UNITPY'
import os
os.environ["SCHED_FAKE_GPUS"] = "0:24"
os.environ.pop("SCHED_FAKE_COMPUTE_APPS", None)
from gsched.allocator import Allocator
a = Allocator([0], fake=True)
ext, degraded = a.incident_external_pids(0)
print(f"{len(ext)}:{degraded}")
UNITPY
CLEAN_OUT=$(SCHED_STATE=$S3 $PY $S3/unit_clean.py)
[ "$CLEAN_OUT" = "0:False" ] && ok "无 compute-apps 时外部为空且非降级" || bad "空场景异常: $CLEAN_OUT"

# ---------- 场景 4: 裁剪 (unit) — 双限 + blocked 豁免 ----------
echo "--- 场景 4: prune 裁剪 ---"
S4=/tmp/sched_inc4; rm -rf $S4; mkdir -p $S4
mk_config $S4
cat > $S4/unit_prune.py << 'UNITPY'
from gsched import state
state.init_db()
with state.connect() as conn:
    state.migrate_incidents(conn)
    conn.execute(
        "INSERT INTO jobs (id,batch_id,task_id,version,status,pgid)"
        " VALUES ('jb','bb','tb',1,'blocked',111)"
    )
    for i in range(12):
        jid = "jb" if i >= 10 else None   # 最后 2 条引用 blocked job
        state.insert_incident(conn, "2026-08-24 00:00:%02d" % i, "oom", 0,
                              jid, "bb", "{}")
    n_del = state.prune_incidents(conn, max_rows=5, ttl_days=30)
    remain = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    kept_blocked = conn.execute(
        "SELECT COUNT(*) FROM incidents WHERE job_id='jb'").fetchone()[0]
print(f"{n_del}:{remain}:{kept_blocked}")
UNITPY
PRUNE_OUT=$(SCHED_STATE=$S4 $PY $S4/unit_prune.py)
[ "$PRUNE_OUT" = "7:5:2" ] && ok "裁剪到 5 条且 blocked 引用全保留 ($PRUNE_OUT)" \
  || bad "裁剪结果异常: $PRUNE_OUT (期望 7:5:2)"

echo "--- 场景 5: TTL 删除也保留 blocked 证据 ---"
S5=/tmp/sched_inc5; rm -rf $S5; mkdir -p $S5
mk_config $S5
cat > $S5/unit_ttl.py << 'UNITPY'
from gsched import state
state.init_db()
with state.connect() as conn:
    conn.execute(
        "INSERT INTO jobs (id,batch_id,task_id,version,status,pgid)"
        " VALUES ('blocked-job','bb','tb',1,'blocked',111)"
    )
    state.insert_incident(conn, "2020-01-01 00:00:00", "oom", 0, "blocked-job", "bb", "{}")
    state.insert_incident(conn, "2020-01-01 00:00:01", "oom", 0, None, "bb", "{}")
    n_del = state.prune_incidents(conn, max_rows=200, ttl_days=1)
    remain = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    kept = conn.execute("SELECT COUNT(*) FROM incidents WHERE job_id='blocked-job'").fetchone()[0]
print(f"{n_del}:{remain}:{kept}")
UNITPY
TTL_OUT=$(SCHED_STATE=$S5 $PY $S5/unit_ttl.py)
[ "$TTL_OUT" = "0:1:1" ] && ok "TTL 裁剪保留 blocked 事故证据 ($TTL_OUT)" \
  || bad "TTL 裁剪错误: $TTL_OUT (期望 0:1:1; return 值仅统计条数裁剪)"

echo
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ $FAIL -eq 0 ] || exit 1
