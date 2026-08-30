#!/bin/bash
# H3: a transient nvidia-smi failure must not quarantine every configured GPU.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-probe-capacity"
export SCHED_STATE
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "gpus": [0, 1], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import os, sqlite3, sys
sys.path.insert(0, ".")
from gsched import state
from gsched.allocator import Allocator
import gsched.allocator as allocator_module
state.init_db()
with state.connect() as conn:
    for idx in (0, 1):
        conn.execute(
            "INSERT INTO gpus (idx, status, quarantined, updated_at) VALUES (?, 'free', 0, datetime('now'))",
            (idx,),
        )

class FailedProbe:
    returncode = 1
    stdout = ""
    stderr = "nvidia-smi transient failure"

original_run = allocator_module.subprocess.run
allocator_module.subprocess.run = lambda *args, **kwargs: FailedProbe()
try:
    Allocator([0, 1]).probe_capacity()
finally:
    allocator_module.subprocess.run = original_run

conn = sqlite3.connect(state.db_path())
rows = conn.execute("SELECT idx, quarantined FROM gpus ORDER BY idx").fetchall()
assert rows == [(0, 0), (1, 0)], rows
assert conn.execute("SELECT COUNT(*) FROM incidents WHERE kind='gpu_ghost'").fetchone()[0] == 0
print("H3 failed nvidia-smi probe leaves GPU registry unchanged")
PY
