#!/bin/bash
# H1: sched run must persist the project in batches.project.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-cmd-project"
export SCHED_STATE
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [0], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import argparse, json, os, sqlite3, sys
sys.path.insert(0, ".")
from gsched import cli, state
state.init_db()
from gsched import daemon
daemon.ensure_running = lambda: "test daemon"
args = argparse.Namespace(
    cmd=["echo", "ok"], venv="k", cwd="/tmp", gpus=1, cpu_only=False,
    cpus=None, duration=None, out=None, dry_run=False, project="p",
)
assert cli.cmd_run(args) == 0
conn = sqlite3.connect(state.db_path())
row = conn.execute("SELECT project FROM batches ORDER BY rowid DESC LIMIT 1").fetchone()
assert row and row[0] == "p", row
print("H1 sched run persists project in batches.project")
PY
