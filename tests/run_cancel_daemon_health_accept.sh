#!/bin/bash
# M7: cancel forwarding must warn when the daemon heartbeat is stale/missing.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-cancel-health"
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
    state.insert_batch(conn, "cancel-health-batch", "cancel_health", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, "cancel-health-batch", "t1", 1, spec, 0, "p")
    state.insert_job(conn, "cancel-health-batch-t1-v1", "cancel-health-batch", "t1", 1, "fp", None, "p")
    conn.execute("UPDATE batches SET status='active' WHERE id=?", ("cancel-health-batch",))
    conn.execute("UPDATE jobs SET status='running', pgid=4242 WHERE id=?", ("cancel-health-batch-t1-v1",))
args = argparse.Namespace(batch="cancel_health", yes=True, project=None, bulk_project=None)
out = io.StringIO()
with contextlib.redirect_stdout(out):
    rc = cli.cmd_cancel(args)
assert rc == 0, f"rc={rc} output={out.getvalue()}"
text = out.getvalue()
assert "daemon 未运行" in text or "心跳" in text, text
original_load_cfg = cli._load_cfg
def bad_load_cfg():
    raise SystemExit(1)
cli._load_cfg = bad_load_cfg
try:
    assert cli._daemon_health() == {}
finally:
    cli._load_cfg = original_load_cfg
print("M7 cancel forwarding warns when daemon health is unavailable")
PY
