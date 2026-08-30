#!/bin/bash
# L1: migrate_project_columns must execute as a normal migration function.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-state-migration"
export SCHED_STATE
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import sqlite3
from gsched import state
state.init_db()
conn = sqlite3.connect(state.db_path())
for table in ("tasks", "batches", "jobs"):
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    assert "project" in cols, (table, cols)
print("L1 project-column migration executes during init_db")
PY
