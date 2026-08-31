#!/bin/bash
# M7: cancel forwarding must warn when the daemon heartbeat is stale/missing.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
PY=${PY:-python3}
sched_accept_make_root SCHED_STATE "sched-cancel-health"
export SCHED_STATE
export SCHED_CONFIG="$SCHED_STATE/config.json"
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [0], "venvs": {"k": "$PY"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
"$PY" - <<'PY'
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
    conn.execute("UPDATE jobs SET status='running', pgid=NULL WHERE id=?", ("cancel-health-batch-t1-v1",))
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

# The fixture intentionally leaves a synthetic running row while no daemon is
# available.  Start the isolated fake daemon afterwards so the queued cancel
# request can converge through the public scheduler path before safe cleanup.
if ! SCHED_FAKE_GPUS=0:24 "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1; then
  echo "failed to start isolated daemon for cancel convergence" >&2
  exit 1
fi
terminal=0
for _ in $(seq 1 120); do
  task_status=$("$PY" -m gsched.cli task cancel-health-batch:t1 --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    jobs = []
print(jobs[-1].get("status", "") if jobs else "")')
  batch_status=$("$PY" -m gsched.cli status cancel-health-batch --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
print(batches[0].get("status", "") if batches else "")')
  if [ "$task_status" = "cancelled" ] \
    && [ "$batch_status" = "blocked" ] \
    && [ -f "$SCHED_STATE/$NODE/markers/cancel_health.blocked" ]; then
    terminal=1
    break
  fi
  sleep 1
done
if [ "$terminal" != "1" ]; then
  echo "synthetic running job/cancel request did not converge to task=cancelled/batch=blocked" >&2
  exit 1
fi
"$PY" -m gsched.cli daemon stop >/dev/null 2>&1 || exit 1
