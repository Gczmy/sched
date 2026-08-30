#!/bin/bash
# M3: duplicate cancel requests for one job must not escalate to SIGKILL in one tick.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
PY=${PY:-python3}
sched_accept_make_root SCHED_STATE "sched-cancel-dedupe"
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
import os, signal, sqlite3, sys
sys.path.insert(0, ".")
from gsched import state
from gsched.dispatcher import Dispatcher
state.init_db()
spec = {
    "id": "t1", "cmd": ["echo", "ok"], "stages": None, "cwd_abs": "/tmp",
    "git": False, "env": {}, "resources": {"gpu": 0}, "duration_min": None,
    "max_retry": 0, "artifacts": {}, "retry_transform": None, "probes": None,
}
job_id = "cancel-dedupe-t1-v1"
synthetic_pgid = 2_147_483_647  # Above Linux pid_max; never signal a real group.
with state.connect() as conn:
    state.insert_batch(conn, "cancel-dedupe", "cancel_dedupe", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, "cancel-dedupe", "t1", 1, spec, 0, "p")
    state.insert_job(conn, job_id, "cancel-dedupe", "t1", 1, "fp", None, "p")
    conn.execute(
        "UPDATE jobs SET status='running', pgid=? WHERE id=?",
        (synthetic_pgid, job_id),
    )
    state.insert_control_request(conn, job_id)
    state.insert_control_request(conn, job_id)

signals = []
class Exec:
    def alive(self, _pgid): return True
    def kill_pgid(self, _pgid, sig=signal.SIGTERM):
        signals.append(sig)
        return True
d = Dispatcher.__new__(Dispatcher)
d.host_dir = os.path.join(os.environ["SCHED_STATE"], state.hostname())
d.executor = Exec()
d.log_line = lambda _msg: None
d._proc_start_time = lambda _pgid: "proc:1"
marker = d._launch_marker_path({"id": job_id, "pgid": synthetic_pgid})
os.makedirs(os.path.dirname(marker), exist_ok=True)
with open(marker, "w", encoding="utf-8") as stream:
    stream.write(f"{synthetic_pgid} proc:1\n")
Dispatcher._process_control_requests(d)
conn = sqlite3.connect(state.db_path())
statuses = [r[0] for r in conn.execute("SELECT status FROM control_requests ORDER BY id")]
assert signals == [signal.SIGTERM], signals
assert statuses == ["pending", "done"], statuses
print("M3 duplicate cancel requests are deduplicated within one tick")
PY

# Reconcile the synthetic dead process through the isolated public daemon
# lifecycle so no running row, request, or launch marker leaks into cleanup.
if ! SCHED_FAKE_GPUS=0:24 "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1; then
  echo "failed to start isolated daemon for cancel-dedupe convergence" >&2
  exit 1
fi
terminal=0
for _ in $(seq 1 120); do
  task_status=$("$PY" -m gsched.cli task cancel-dedupe:t1 --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    jobs = []
print(jobs[-1].get("status", "") if jobs else "")')
  batch_status=$("$PY" -m gsched.cli status cancel-dedupe --json 2>/dev/null | \
    "$PY" -c '
import json, sys
try:
    batches = json.load(sys.stdin).get("batches", [])
except Exception:
    batches = []
print(batches[0].get("status", "") if batches else "")')
  if [ "$task_status" = "cancelled" ] \
    && [ "$batch_status" = "blocked" ] \
    && [ -f "$SCHED_STATE/$NODE/markers/cancel_dedupe.blocked" ]; then
    terminal=1
    break
  fi
  sleep 1
done
if [ "$terminal" != "1" ]; then
  echo "synthetic cancel-dedupe job/request did not fully converge" >&2
  exit 1
fi
"$PY" -m gsched.cli daemon stop >/dev/null 2>&1 || exit 1
