#!/bin/bash
# H5: dependency unlock and batch settlement must use current task versions safely.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
PY=${PY:-python3}
sched_accept_make_root SCHED_STATE "sched-dependency-versions"
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
add_versions(
    "active-stale-pending-batch",
    "active_stale_pending",
    "pending",
    "done",
)
add_versions(
    "done-stale-pending-batch",
    "done_stale_pending",
    "pending",
    "pending",
    batch_status="done",
)

conn = sqlite3.connect(state.db_path())
conn.row_factory = sqlite3.Row
d = Dispatcher.__new__(Dispatcher)
d.host_dir = state.host_dir()
d.log_line = lambda _msg: None
d._write_marker = lambda *_args: None
d._remove_marker = lambda *_args: None
d._notify_batch = lambda *_args: None
assert d._batch_successful(conn, "unlock_batch") is True
assert d._batch_successful(conn, "settle_batch") is False
assert d._batch_successful(conn, "active_stale_pending") is True
Dispatcher._settle_batch_status(d)
row = conn.execute("SELECT status FROM batches WHERE id='settle-batch'").fetchone()
assert row["status"] == "active", row
row = conn.execute(
    "SELECT status FROM batches WHERE id='active-stale-pending-batch'"
).fetchone()
assert row["status"] == "done", row
# Let the real isolated daemon perform the same convergence so marker creation
# is covered as well as the direct settlement predicate.
conn.execute(
    "UPDATE batches SET status='active'"
    " WHERE id='active-stale-pending-batch'"
)
conn.commit()
print("H5 latest-version dependency unlock and stale-running settlement guard pass")
PY

if ! SCHED_FAKE_GPUS=0:24 "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1; then
  echo "failed to start isolated daemon for stale-version convergence" >&2
  exit 1
fi
settled=0
for _ in $(seq 1 120); do
  task_ok=1
  batch_ok=1
  "$PY" -m gsched.cli task settle-batch:t1 --json 2>/dev/null | "$PY" -c '
import json, sys
jobs = json.load(sys.stdin).get("jobs", [])
observed = [(j.get("version"), j.get("status")) for j in jobs]
raise SystemExit(0 if observed == [(1, "blocked"), (2, "done")] else 1)' || task_ok=0
  "$PY" -m gsched.cli status settle-batch --json 2>/dev/null | "$PY" -c '
import json, sys
batches = json.load(sys.stdin).get("batches", [])
ok = len(batches) == 1 and batches[0].get("status") == "done"
raise SystemExit(0 if ok else 1)' || batch_ok=0
  if [ "$task_ok" = "1" ] \
    && [ "$batch_ok" = "1" ] \
    && [ -f "$SCHED_STATE/$NODE/markers/settle_batch.done" ] \
    && [ ! -e "$SCHED_STATE/$NODE/markers/settle_batch.blocked" ]; then
    settled=1
    break
  fi
  sleep 1
done
if [ "$settled" != "1" ]; then
  echo "stale running version did not fully converge" >&2
  exit 1
fi
if ! "$PY" -m gsched.cli task active-stale-pending-batch:t1 --json 2>/dev/null \
  | "$PY" -c '
import json, sys
jobs = json.load(sys.stdin).get("jobs", [])
observed = [(j.get("version"), j.get("status"), j.get("started_at")) for j in jobs]
raise SystemExit(0 if observed == [(1, "pending", None), (2, "done", None)] else 1)'; then
  echo "active batch dispatched an obsolete pending version" >&2
  exit 1
fi
if ! "$PY" -m gsched.cli status active-stale-pending-batch --json 2>/dev/null \
  | "$PY" -c '
import json, sys
batches = json.load(sys.stdin).get("batches", [])
raise SystemExit(0 if len(batches) == 1 and batches[0].get("status") == "done" else 1)' \
  || [ ! -f "$SCHED_STATE/$NODE/markers/active_stale_pending.done" ]; then
  echo "obsolete pending version blocked batch settlement or done marker" >&2
  exit 1
fi
if ! "$PY" -m gsched.cli task done-stale-pending-batch:t1 --json 2>/dev/null \
  | "$PY" -c '
import json, sys
jobs = json.load(sys.stdin).get("jobs", [])
observed = [(j.get("version"), j.get("status"), j.get("started_at")) for j in jobs]
raise SystemExit(0 if observed == [(1, "pending", None), (2, "pending", None)] else 1)'; then
  echo "terminal batch dispatched a pending job" >&2
  exit 1
fi
"$PY" -m gsched.cli daemon stop >/dev/null 2>&1 || exit 1
