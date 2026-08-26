#!/bin/bash
# B26: GPU 健康自愈验收 —— 探测失败计数/幽灵卡检测/自动熔断
set -u
cd "$(dirname "$0")/.."
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

export SCHED_STATE="$(mktemp -d)"
export SCHED_ALLOW_FOREIGN_WRITE=1

NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" << EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "gpus": [0,1,2,3], "venvs": {"k": "/bin/true"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF

python3 - << 'RPEOF'
import sys, os, json, sqlite3
sys.path.insert(0, ".")
from gsched import state as st
from gsched.allocator import Allocator

st.init_db()
db = st.db_path()
_conn0 = sqlite3.connect(db)
for _i in range(4):
    _conn0.execute(
        "INSERT INTO gpus (idx, status, quarantined, ignore_until, updated_at, mem_total_gib)"
        " VALUES (?, 'free', 0, NULL, datetime('now'), 22.5)", (_i,))
_conn0.commit(); _conn0.close()

a2 = Allocator.__new__(Allocator)
a2.fake = False
a2._probe_fail_streak = {}
a2._auto_quarantine_threshold = 5
for i in range(5):
    a2._note_probe_fail(2)

conn = sqlite3.connect(db)
r = conn.execute("SELECT quarantined FROM gpus WHERE idx=2").fetchone()
if not (r and r[0] == 1):
    print("❌ S1 熔断未生效"); sys.exit(1)
print("✅ S1 连续 5 次探测失败 -> 自动熔断")

n = conn.execute("SELECT COUNT(*) FROM incidents WHERE kind='gpu_probe_failed' AND gpu_idx=2").fetchone()[0]
print("✅ S2 incident 已记录" if n == 1 else f"❌ S2 incident 数={n}")

for i in range(4):  # 未达阈值
    a2._note_probe_fail(1)
r = conn.execute("SELECT quarantined FROM gpus WHERE idx=1").fetchone()
print("✅ S3 未达阈值不熔断" if r[0] == 0 else "❌ S3 误熔断")
sys.exit(0)
RPEOF

if [ $? -eq 0 ]; then ok "B26 自愈主流程"; else bad "B26 自愈主流程"; fi

echo ""
echo "=== 结果: PASS=$PASS FAIL=$FAIL ==="
[ "$FAIL" -eq 0 ]
