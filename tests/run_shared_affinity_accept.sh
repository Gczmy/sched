#!/bin/bash
# M5: shared packing must prefer a project's soft-affinity cards before load balance.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-shared-affinity"
export SCHED_STATE
export SCHED_FAKE_GPUS="0:24,1:24"
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "gpus": [0, 1], "co_locate": true, "co_locate_safety": 0.7,
  "co_locate_max_jobs": 8, "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false, "gpu_affinity": [1]}}
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
    for idx in (0, 1):
        conn.execute(
            "INSERT INTO gpus (idx, status, job_id, quarantined, updated_at, mem_total_gib)"
            " VALUES (?, 'assigned', ?, 0, datetime('now'), 24.0)",
            (idx, f"existing-{idx}"),
        )
    conn.execute(
        "INSERT INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at) VALUES (0, 'existing-0', 0.0, datetime('now'))"
    )
    conn.execute(
        "INSERT INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at) VALUES (1, 'existing-1', 4.0, datetime('now'))"
    )
    spec = {"resources": {"gpu": 1, "gpu_share": True, "vram_gib": 1.0}}
    d = Dispatcher.__new__(Dispatcher)
    d.cfg = {"co_locate": True, "co_locate_safety": 0.7, "co_locate_max_jobs": 8}
    d._projects = {"p": {"gpu_affinity": [1]}}
    d._gpu_max_jobs = {}
    d._frozen_gpus = set()
    d._cap_warned = set()
    d.allocator = Allocator([0, 1], fake=True)
    d.log_line = lambda _msg: None
    d._project_affinity = lambda project: [1] if project == "p" else []
    d._project_affinity_hard = lambda _project: False
    selected = Dispatcher._assign_in_tx(d, conn, "new-job", spec, "p")
    assert selected == 1, f"soft-affinity card was not preferred: GPU{selected}"
print("M5 shared packing prefers soft-affinity card before least-load fallback")
PY
