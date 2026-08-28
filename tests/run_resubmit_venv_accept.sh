#!/bin/bash
# M1: resubmit fingerprints must resolve the configured VENV aliases.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "/opt/venvs/k/bin/python"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import argparse, json, os, sqlite3, sys
sys.path.insert(0, ".")
from gsched import cli, daemon, state
from gsched.fingerprint import compute_fingerprint
state.init_db()
spec = {
    "id": "t1", "cmd": ["{VENV:k}", "-c", "print(1)"], "stages": None,
    "cwd_abs": os.path.realpath("/tmp"), "git": False, "env": {},
    "resources": {"gpu": 0}, "duration_min": None, "max_retry": 0,
    "artifacts": {}, "retry_transform": None, "probes": None,
}
with state.connect() as conn:
    state.insert_batch(conn, "resubmit-venv-batch", "resubmit_venv", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, "resubmit-venv-batch", "t1", 1, spec, 0, "p")
    old_fp, _, _ = compute_fingerprint(spec["cmd"], None, spec["cwd_abs"], False, {"k": "/opt/venvs/k/bin/python"})
    state.insert_job(conn, "resubmit-venv-batch-t1-v1", "resubmit-venv-batch", "t1", 1, old_fp, None, "p")
    conn.execute("UPDATE jobs SET status='failed', failure='launch' WHERE id=?", ("resubmit-venv-batch-t1-v1",))
    conn.execute("UPDATE batches SET status='active' WHERE id=?", ("resubmit-venv-batch",))
daemon.ensure_running = lambda: "test daemon"
args = argparse.Namespace(task="resubmit_venv:t1", failed=False, resubmit_all=False, dry_run=False)
assert cli.cmd_resubmit(args) == 0
with state.connect() as conn:
    row = conn.execute("SELECT fingerprint FROM jobs WHERE id=?", ("resubmit-venv-batch-t1-v2",)).fetchone()
    got = row["fingerprint"]
expected, _, _ = compute_fingerprint(spec["cmd"], None, spec["cwd_abs"], False, {"k": "/opt/venvs/k/bin/python"})
assert got == expected, (got, expected)
print("M1 resubmit fingerprint resolves configured VENV")
PY
