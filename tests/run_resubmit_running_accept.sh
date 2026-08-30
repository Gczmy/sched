#!/bin/bash
# M2: resubmit must refuse a running or pending target.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
PY=${PY:-python3}
sched_accept_make_root SCHED_STATE "sched-resubmit-running"
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
    conn.execute("UPDATE jobs SET status='running', pgid=NULL WHERE id=?", ("running-batch-t1-v1",))
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
    conn.execute("UPDATE jobs SET status='running', pgid=NULL WHERE id=?", (f"{batch_id}-t1-v1",))
    conn.execute("UPDATE jobs SET status='done' WHERE id=?", (f"{batch_id}-t1-v2",))
args = argparse.Namespace(task="stale_running:t1", failed=False, resubmit_all=False, dry_run=False)
assert cli.cmd_resubmit(args) == 1
conn = sqlite3.connect(state.db_path())
assert conn.execute("SELECT COUNT(*) FROM jobs WHERE batch_id=?", ("stale-running-batch",)).fetchone()[0] == 2
assert conn.execute("SELECT COUNT(*) FROM tasks WHERE batch_id=?", ("stale-running-batch",)).fetchone()[0] == 2
print("M2 older running version also blocks resubmit")
PY

if ! SCHED_FAKE_GPUS=0:24 "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1; then
  echo "failed to start isolated daemon for resubmit-fixture convergence" >&2
  exit 1
fi
settled=0
for _ in $(seq 1 120); do
  current_ok=1
  stale_ok=1
  batches_ok=1
  "$PY" -m gsched.cli task running-batch:t1 --json 2>/dev/null | "$PY" -c '
import json, sys
jobs = json.load(sys.stdin).get("jobs", [])
ok = [(j.get("version"), j.get("status")) for j in jobs] == [(1, "blocked")]
raise SystemExit(0 if ok else 1)' || current_ok=0
  "$PY" -m gsched.cli task stale-running-batch:t1 --json 2>/dev/null | "$PY" -c '
import json, sys
jobs = json.load(sys.stdin).get("jobs", [])
observed = [(j.get("version"), j.get("status")) for j in jobs]
raise SystemExit(0 if observed == [(1, "blocked"), (2, "done")] else 1)' || stale_ok=0
  "$PY" -m gsched.cli status --json 2>/dev/null | "$PY" -c '
import json, sys
d = json.load(sys.stdin)
batches = {b["batch_id"]: b["status"] for b in d.get("batches", [])}
ok = (
    batches.get("running-batch") == "blocked"
    and batches.get("stale-running-batch") == "done"
)
raise SystemExit(0 if ok else 1)' || batches_ok=0
  if [ "$current_ok" = "1" ] \
    && [ "$stale_ok" = "1" ] \
    && [ "$batches_ok" = "1" ] \
    && [ -f "$SCHED_STATE/$NODE/markers/running_batch.blocked" ] \
    && [ ! -e "$SCHED_STATE/$NODE/markers/running_batch.done" ] \
    && [ -f "$SCHED_STATE/$NODE/markers/stale_running.done" ] \
    && [ ! -e "$SCHED_STATE/$NODE/markers/stale_running.blocked" ]; then
    settled=1
    break
  fi
  sleep 1
done
if [ "$settled" != "1" ]; then
  echo "resubmit running fixtures did not fully converge" >&2
  exit 1
fi
"$PY" -m gsched.cli daemon stop >/dev/null 2>&1 || exit 1
