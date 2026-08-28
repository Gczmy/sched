#!/bin/bash
# M3: duplicate cancel requests for one job must not escalate to SIGKILL in one tick.
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
with state.connect() as conn:
    state.insert_batch(conn, "cancel-dedupe", "cancel_dedupe", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, "cancel-dedupe", "t1", 1, spec, 0, "p")
    state.insert_job(conn, job_id, "cancel-dedupe", "t1", 1, "fp", None, "p")
    conn.execute("UPDATE jobs SET status='running', pgid=4242 WHERE id=?", (job_id,))
    state.insert_control_request(conn, job_id)
    state.insert_control_request(conn, job_id)

signals = []
class Exec:
    def alive(self, _pgid): return True
    def kill_pgid(self, _pgid, sig=signal.SIGTERM): signals.append(sig)
d = Dispatcher.__new__(Dispatcher)
d.executor = Exec()
d.log_line = lambda _msg: None
Dispatcher._process_control_requests(d)
conn = sqlite3.connect(state.db_path())
statuses = [r[0] for r in conn.execute("SELECT status FROM control_requests ORDER BY id")]
assert signals == [signal.SIGTERM], signals
assert statuses == ["pending", "done"], statuses
print("M3 duplicate cancel requests are deduplicated within one tick")
PY
