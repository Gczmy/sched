#!/bin/bash
# L9: cancel --project must visit every active same-name batch instance.
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
import argparse, contextlib, io, os, sqlite3, sys
sys.path.insert(0, ".")
from gsched import cli, state
state.init_db()
spec = {
    "id": "t1", "cmd": ["echo", "ok"], "stages": None, "cwd_abs": "/tmp",
    "git": False, "env": {}, "resources": {"gpu": 0}, "duration_min": None,
    "max_retry": 0, "artifacts": {}, "retry_transform": None, "probes": None,
}
with state.connect() as conn:
    for n, bid, jid in (("old", "same-name-old", "same-name-old-t1-v1"), ("new", "same-name-new", "same-name-new-t1-v1")):
        state.insert_batch(conn, bid, "same_name", "mix", [], None, "/tmp", None, project="p")
        state.insert_task(conn, bid, "t1", 1, spec, 0, "p")
        state.insert_job(conn, jid, bid, "t1", 1, "fp", None, "p")
        conn.execute("UPDATE batches SET status='active' WHERE id=?", (bid,))
        conn.execute("UPDATE jobs SET status='running', pgid=? WHERE id=?", (4242 if n == "old" else 4243, jid))
args = argparse.Namespace(batch="", yes=True, project=None, bulk_project="p")
out = io.StringIO()
with contextlib.redirect_stdout(out):
    assert cli.cmd_cancel(args) == 0, out.getvalue()
conn = sqlite3.connect(state.db_path())
rows = conn.execute("SELECT id,status FROM jobs ORDER BY id").fetchall()
assert rows == [("same-name-new-t1-v1", "running"), ("same-name-old-t1-v1", "running")], rows
reqs = conn.execute("SELECT job_id FROM control_requests ORDER BY id").fetchall()
assert {r[0] for r in reqs} == {"same-name-old-t1-v1", "same-name-new-t1-v1"}, reqs
print("L9 cancel --project forwards every same-name batch instance")
PY
