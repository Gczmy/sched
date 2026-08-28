#!/bin/bash
# C2: gateway submission must use the file-only inbox channel; no NFS DB write.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
unset SCHED_ALLOW_FOREIGN_WRITE || true
cat > "$SCHED_STATE/config.json" <<'EOF'
{
  "schema_version": 1,
  "user": "t",
  "node": "compute-node",
  "state_dir": "PLACEHOLDER",
  "default_project": "p",
  "gpus": [0],
  "venvs": {"k": "/opt/venvs/k/bin/python"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
sed -i.bak "s|PLACEHOLDER|$SCHED_STATE|" "$SCHED_STATE/config.json"
rm -f "$SCHED_STATE/config.json.bak"
BATCH="$SCHED_STATE/batch.json"
cat > "$BATCH" <<'EOF'
{
  "schema_version": 1,
  "name": "single_writer",
  "project": "p",
  "cwd": "/tmp",
  "tasks": [{"id": "t1", "cmd": ["echo", "ok"]}]
}
EOF
python3 -m gsched.cli submit "$BATCH"
python3 - <<'PY'
import glob, json, os, sqlite3, sys
sys.path.insert(0, ".")
from gsched import state
conn = sqlite3.connect(state.db_path())
count = conn.execute("SELECT COUNT(*) FROM control_requests").fetchone()[0]
assert count == 0, f"gateway wrote {count} control_requests rows"
inbox = os.path.join(os.environ["SCHED_STATE"], "compute-node", "submit_inbox")
files = glob.glob(os.path.join(inbox, "submit-*.json"))
assert len(files) == 1, files
with open(files[0], encoding="utf-8") as f:
    envelope = json.load(f)
assert envelope["spec"]["name"] == "single_writer"

from gsched.dispatcher import Dispatcher
d = Dispatcher.__new__(Dispatcher)
d.cfg = json.load(open(os.path.join(os.environ["SCHED_STATE"], "config.json")))
d.log_line = lambda _msg: None
Dispatcher._drain_submit_inbox(d)
assert conn.execute("SELECT COUNT(*) FROM control_requests").fetchone()[0] == 1
Dispatcher._process_control_requests(d)
row = conn.execute("SELECT status FROM batches WHERE name='single_writer'").fetchone()
assert row and row[0] == "queued", row
assert not os.path.exists(files[0]), "daemon did not consume inbox payload"
print("C2 gateway submit writes payload only; daemon owns request rows")
PY
