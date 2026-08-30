#!/bin/bash
# C1: inbox consumer must expand the same command templates as direct submit.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-inbox-template"
export SCHED_STATE
export SCHED_ALLOW_FOREIGN_WRITE=1
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" << EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [0], "venvs": {"k": "/opt/venvs/k/bin/python"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import json, os, sqlite3, sys
sys.path.insert(0, ".")
from gsched import state
state.init_db()
payload_dir = os.path.join(os.environ["SCHED_STATE"], state.hostname(), "submit_inbox")
os.makedirs(payload_dir, exist_ok=True)
payload = os.path.join(payload_dir, "submit-inbox-template-TEST.json")
spec = {
    "schema_version": 1,
    "name": "inbox_template",
    "project": "p",
    "cwd": os.environ["SCHED_STATE"],
    "tasks": [{
        "id": "t1",
        "stages": [
            {"cmd": ["{VENV:k}", "-c", "print('stage0')"], "artifacts": {"ckpt": {"path": "out/model.bin"}}},
            {"cmd": ["{VENV:k}", "{stage0_ckpt}"], "artifacts": {}},
        ],
    }],
}
with open(payload, "w", encoding="utf-8") as f:
    json.dump({"spec": spec, "bid": "inbox-template-TEST0001"}, f)
with state.connect() as conn:
    conn.execute(
        "INSERT INTO control_requests (job_id, op, status, created_at) VALUES (?, 'batch_submit', 'pending', datetime('now'))",
        (payload,),
    )
from gsched.dispatcher import Dispatcher
d = Dispatcher.__new__(Dispatcher)
d.cfg = json.load(open(os.path.join(os.environ["SCHED_STATE"], "config.json")))
d.log_line = lambda _msg: None
Dispatcher._process_control_requests(d)
conn = sqlite3.connect(state.db_path())
conn.row_factory = sqlite3.Row
row = conn.execute("SELECT spec FROM tasks WHERE batch_id='inbox-template-TEST0001'").fetchone()
assert row, "task was not inserted"
task = json.loads(row["spec"])
assert task["stages"][0]["cmd"][0] == "/opt/venvs/k/bin/python", task
assert task["stages"][1]["cmd"][0] == "/opt/venvs/k/bin/python", task
assert task["stages"][1]["cmd"][1] == os.path.realpath(os.path.join(os.environ["SCHED_STATE"], "out", "model.bin")), task
job = conn.execute("SELECT fingerprint FROM jobs WHERE id='inbox-template-TEST0001-t1-v1'").fetchone()
assert job and job["fingerprint"], "fingerprint missing"
req = conn.execute("SELECT status FROM control_requests WHERE job_id=?", (payload,)).fetchone()
assert req["status"] == "done", req
assert not os.path.exists(payload), "payload was not consumed"
print("C1 inbox consumer expands VENV and stage references")
PY
