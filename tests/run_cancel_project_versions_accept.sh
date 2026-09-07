#!/bin/bash
# L9: cancel --project must visit every active same-name batch instance.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
PY=${PY:-$(command -v python3)}
sched_accept_make_root SCHED_STATE "sched-cancel-project"
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
    for n, bid, jid in (("old", "same-name-old", "same-name-old-t1-v1"), ("new", "same-name-new", "same-name-new-t1-v1")):
        state.insert_batch(conn, bid, "same_name", "mix", [], None, "/tmp", None, project="p")
        state.insert_task(conn, bid, "t1", 1, spec, 0, "p")
        state.insert_job(conn, jid, bid, "t1", 1, "fp", None, "p")
        conn.execute("UPDATE batches SET status='active' WHERE id=?", (bid,))
        conn.execute("UPDATE jobs SET status='running', pgid=NULL WHERE id=?", (jid,))
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

if ! SCHED_FAKE_GPUS=0:24 "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1; then
  echo "failed to start isolated daemon for project-cancel convergence" >&2
  exit 1
fi
settled=0
for _ in $(seq 1 120); do
  if "$PY" -m gsched.cli status --json 2>/dev/null | "$PY" -c '
import json, os, sys
d = json.load(sys.stdin)
batches = {b["batch_id"]: b["status"] for b in d.get("batches", [])}
jobs = {(j["batch_id"], j["task"]): j["status"] for j in d.get("jobs", [])}
ok = (
    batches.get("same-name-old") == "blocked"
    and batches.get("same-name-new") == "blocked"
    and jobs.get(("same-name-old", "t1")) == "cancelled"
    and jobs.get(("same-name-new", "t1")) == "cancelled"
)
raise SystemExit(0 if ok else 1)' \
    && [ -f "$SCHED_STATE/$NODE/markers/same_name.blocked" ] \
    && [ ! -e "$SCHED_STATE/$NODE/markers/same_name.done" ]; then
    settled=1
    break
  fi
  sleep 1
done
if [ "$settled" != "1" ]; then
  echo "project cancel requests did not converge to task=cancelled/batch=blocked" >&2
  exit 1
fi
"$PY" -m gsched.cli daemon stop >/dev/null 2>&1 || exit 1
