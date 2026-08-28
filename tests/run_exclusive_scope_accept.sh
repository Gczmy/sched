#!/bin/bash
# L7: a hard-affinity exclusive miss must not block unrelated projects this tick.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
export SCHED_FAKE_GPUS="0:24,1:24"
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "gpus": [0, 1], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false, "gpu_affinity": [0], "gpu_affinity_hard": true}}
}
EOF
python3 - <<'PY'
import sqlite3, sys
sys.path.insert(0, ".")
from gsched import state
from gsched.allocator import Allocator
from gsched.dispatcher import Dispatcher
state.init_db()
with state.connect() as conn:
    conn.execute("INSERT INTO gpus (idx,status,job_id,quarantined,updated_at,mem_total_gib) VALUES (0,'assigned','busy',0,datetime('now'),24)",)
    conn.execute("INSERT INTO gpus (idx,status,job_id,quarantined,updated_at,mem_total_gib) VALUES (1,'free',NULL,0,datetime('now'),24)")
d = Dispatcher.__new__(Dispatcher)
d.cfg = {"co_locate": False}
d._projects = {"p": {"gpu_affinity": [0], "gpu_affinity_hard": True}}
d.allocator = Allocator([0, 1], fake=True)
d._project_affinity = lambda project: [0] if project == "p" else []
d._project_affinity_hard = lambda project: project == "p"
with state.connect() as conn:
    selected = Dispatcher._assign_in_tx(d, conn, "p-job", {"resources": {"gpu": 1}}, "p")
    assert selected is None
    assert d._assign_reject_scope == "project", d._assign_reject_scope
print("L7 hard-affinity exclusive miss scopes rejection to project")
PY
