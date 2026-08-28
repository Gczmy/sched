#!/bin/bash
# L11: completed control requests and notification thread references stay bounded.
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
import datetime, sqlite3, threading
from gsched import state
from gsched.dispatcher import Dispatcher
state.init_db()
old = (datetime.datetime.now() - datetime.timedelta(days=31)).strftime("%Y-%m-%d %H:%M:%S")
with state.connect() as conn:
    for idx in range(3):
        conn.execute(
            "INSERT INTO control_requests (job_id,op,status,created_at,processed_at,result)"
            " VALUES (?, 'cancel', 'done', ?, ?, 'ok')",
            (f"old-{idx}", old, old),
        )
    conn.execute(
        "INSERT INTO control_requests (job_id,op,status,created_at)"
        " VALUES ('pending', 'cancel', 'pending', datetime('now'))"
    )
d = Dispatcher.__new__(Dispatcher)
d._notify_threads = [threading.Thread(target=lambda: None)]
d._prune_control_requests()
d._prune_notify_threads()
conn = sqlite3.connect(state.db_path())
assert conn.execute("SELECT COUNT(*) FROM control_requests WHERE status='done'").fetchone()[0] == 0
assert conn.execute("SELECT COUNT(*) FROM control_requests WHERE status='pending'").fetchone()[0] == 1
assert d._notify_threads == []
print("L11 completed requests and dead notification threads are pruned")
PY
