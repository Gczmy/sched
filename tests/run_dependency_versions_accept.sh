#!/bin/bash
# H5: dependency unlock and batch settlement must use current task versions safely.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-dependency-versions"
export SCHED_STATE
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import sqlite3, sys
sys.path.insert(0, ".")
from gsched import state
from gsched.dispatcher import Dispatcher
state.init_db()
spec = {
    "id": "t1", "cmd": ["echo", "ok"], "stages": None, "cwd_abs": "/tmp",
    "git": None, "env": {}, "resources": {"gpu": 0}, "duration_min": None,
    "max_retry": 0, "artifacts": {}, "retry_transform": None, "probes": None,
}

def add_versions(batch_id, name, old_status, latest_status, batch_status="active"):
    with state.connect() as conn:
        state.insert_batch(conn, batch_id, name, "mix", [], None, "/tmp", None, project="p")
        conn.execute("UPDATE batches SET status=? WHERE id=?", (batch_status, batch_id))
        state.insert_task(conn, batch_id, "t1", 1, spec, 0, "p")
        state.insert_task(conn, batch_id, "t1", 2, spec, 0, "p")
        state.insert_job(conn, f"{batch_id}-t1-v1", batch_id, "t1", 1, "fp1", None, "p")
        state.insert_job(conn, f"{batch_id}-t1-v2", batch_id, "t1", 2, "fp2", None, "p")
        conn.execute("UPDATE jobs SET status=? WHERE id=?", (old_status, f"{batch_id}-t1-v1"))
        conn.execute("UPDATE jobs SET status=? WHERE id=?", (latest_status, f"{batch_id}-t1-v2"))

add_versions("unlock-batch", "unlock_batch", "failed", "done")
add_versions("settle-batch", "settle_batch", "running", "done")

conn = sqlite3.connect(state.db_path())
conn.row_factory = sqlite3.Row
assert Dispatcher._batch_successful(None, conn, "unlock_batch") is True
assert Dispatcher._batch_successful(None, conn, "settle_batch") is False

d = Dispatcher.__new__(Dispatcher)
d.log_line = lambda _msg: None
d._write_marker = lambda *_args: None
d._notify_batch = lambda *_args: None
Dispatcher._settle_batch_status(d)
row = conn.execute("SELECT status FROM batches WHERE id='settle-batch'").fetchone()
assert row["status"] == "active", row
print("H5 latest-version dependency unlock and stale-running settlement guard pass")
PY
