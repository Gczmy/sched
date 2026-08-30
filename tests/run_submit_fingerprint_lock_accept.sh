#!/bin/bash
# M9: submit must compute git fingerprints before opening the write transaction.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-submit-fingerprint"
export SCHED_STATE
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
cat > "$SCHED_STATE/batch.json" <<'EOF'
{
  "schema_version": 1,
  "name": "fingerprint_boundary",
  "project": "p",
  "cwd": "/tmp",
  "tasks": [{"id": "t1", "cmd": ["echo", "ok"], "resources": {"gpu": 0}}]
}
EOF
python3 - <<'PY'
import argparse, json, os, sys
sys.path.insert(0, ".")
from gsched import cli, daemon, fingerprint, state
state.init_db()
write_started = False
original_insert = state.insert_batch
original_fp = fingerprint.compute_fingerprint

def insert_batch(*args, **kwargs):
    global write_started
    write_started = True
    return original_insert(*args, **kwargs)

def compute_fingerprint(*args, **kwargs):
    assert not write_started, "fingerprint ran after state write transaction began"
    return original_fp(*args, **kwargs)
state.insert_batch = insert_batch
fingerprint.compute_fingerprint = compute_fingerprint
daemon.ensure_running = lambda: "test daemon"
try:
    args = argparse.Namespace(batch=os.path.join(os.environ["SCHED_STATE"], "batch.json"), dry_run=False, json=False)
    assert cli.cmd_submit(args) == 0
finally:
    state.insert_batch = original_insert
    fingerprint.compute_fingerprint = original_fp
print("M9 submit computes fingerprints before DB writes")
PY
