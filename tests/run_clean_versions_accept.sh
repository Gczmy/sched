#!/bin/bash
# L10: clean must select the latest version independently for each task.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-clean-versions"
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
import argparse, os, sqlite3, sys
sys.path.insert(0, ".")
from gsched import cli, state
state.init_db()
out_dir = os.path.join(os.environ["SCHED_STATE"], "artifacts")
os.makedirs(out_dir, exist_ok=True)

def spec(path):
    return {
        "id": "t", "cmd": ["echo", "ok"], "stages": None, "cwd_abs": "/tmp",
        "git": False, "env": {}, "resources": {"gpu": 0}, "duration_min": None,
        "max_retry": 0, "paths_escape": True,
        "artifacts": {"out": {"path": path}},
        "retry_transform": None, "probes": None,
    }
t1_old = os.path.join(out_dir, "t1-old.txt")
t1_new = os.path.join(out_dir, "t1-new.txt")
t2 = os.path.join(out_dir, "t2.txt")
for path in (t1_old, t1_new, t2): open(path, "w").write("x")
with state.connect() as conn:
    state.insert_batch(conn, "clean-versions", "clean_versions", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, "clean-versions", "t1", 1, spec(t1_old), 0, "p")
    state.insert_task(conn, "clean-versions", "t1", 2, spec(t1_new), 0, "p")
    state.insert_task(conn, "clean-versions", "t2", 1, spec(t2), 1, "p")
args = argparse.Namespace(batch="clean_versions", yes=True)
assert cli.cmd_clean(args) == 0
assert not os.path.exists(t1_new), "latest t1 artifact was not removed"
assert not os.path.exists(t2), "latest t2 artifact was skipped by global MAX(version)"
print("L10 clean selects latest task version independently")
PY
