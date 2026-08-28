#!/bin/bash
# M2: resubmit must refuse a running or pending target.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import argparse, json, os, sqlite3, sys
sys.path.insert(0, ".")
from gsched import cli, daemon, state
state.init_db()
spec = {
    "id": "t1", "cmd": ["echo", "ok"], "stages": None, "cwd_abs": os.path.realpath("/tmp"),
    "git": False, "env": {}, "resources": {"gpu": 0}, "duration_min": None,
    "max_retry": 0, "artifacts": {}, "retry_transform": None, "probes": None,
}
with state.connect() as conn:
    state.insert_batch(conn, "running-batch", "running_batch", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, "running-batch", "t1", 1, spec, 0, "p")
    state.insert_job(conn, "running-batch-t1-v1", "running-batch", "t1", 1, "fp", None, "p")
    conn.execute("UPDATE batches SET status='active' WHERE id=?", ("running-batch",))
    conn.execute("UPDATE jobs SET status='running', pgid=4242 WHERE id=?", ("running-batch-t1-v1",))
daemon.ensure_running = lambda: "test daemon"
args = argparse.Namespace(task="running_batch:t1", failed=False, resubmit_all=False, dry_run=False)
assert cli.cmd_resubmit(args) == 1
conn = sqlite3.connect(state.db_path())
assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
assert conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
print("M2 running resubmit is refused without creating a new version")

with state.connect() as conn:
    batch_id = "stale-running-batch"
    state.insert_batch(conn, batch_id, "stale_running", "mix", [], None, "/tmp", None, project="p")
    conn.execute("UPDATE batches SET status='active' WHERE id=?", (batch_id,))
    state.insert_task(conn, batch_id, "t1", 1, spec, 0, "p")
    state.insert_task(conn, batch_id, "t1", 2, spec, 0, "p")
    state.insert_job(conn, f"{batch_id}-t1-v1", batch_id, "t1", 1, "fp1", None, "p")
    state.insert_job(conn, f"{batch_id}-t1-v2", batch_id, "t1", 2, "fp2", None, "p")
    conn.execute("UPDATE jobs SET status='running', pgid=4243 WHERE id=?", (f"{batch_id}-t1-v1",))
    conn.execute("UPDATE jobs SET status='done' WHERE id=?", (f"{batch_id}-t1-v2",))
args = argparse.Namespace(task="stale_running:t1", failed=False, resubmit_all=False, dry_run=False)
assert cli.cmd_resubmit(args) == 1
conn = sqlite3.connect(state.db_path())
assert conn.execute("SELECT COUNT(*) FROM jobs WHERE batch_id=?", ("stale-running-batch",)).fetchone()[0] == 2
assert conn.execute("SELECT COUNT(*) FROM tasks WHERE batch_id=?", ("stale-running-batch",)).fetchone()[0] == 2
print("M2 older running version also blocks resubmit")
PY
