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
from unittest import mock
sys.path.insert(0, ".")
from gsched import cli, state
from gsched.dispatcher import Dispatcher
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
    conn.execute("UPDATE batches SET status='done' WHERE id='clean-versions'")
    state.insert_task(conn, "clean-versions", "t1", 1, spec(t1_old), 0, "p")
    state.insert_task(conn, "clean-versions", "t1", 2, spec(t1_new), 0, "p")
    state.insert_task(conn, "clean-versions", "t2", 1, spec(t2), 1, "p")
    state.insert_job(conn, "clean-versions-t1-v1", "clean-versions", "t1", 1, "fp-t1-v1", None, "p")
    state.insert_job(conn, "clean-versions-t1-v2", "clean-versions", "t1", 2, "fp-t1-v2", None, "p")
    state.insert_job(conn, "clean-versions-t2-v1", "clean-versions", "t2", 1, "fp-t2-v1", None, "p")
    conn.execute("UPDATE jobs SET status='skip' WHERE batch_id='clean-versions'")
marker_dir = os.path.join(os.environ["SCHED_STATE"], os.uname().nodename, "markers")
os.makedirs(marker_dir, exist_ok=True)
done_marker = os.path.join(marker_dir, "clean_versions.done")
open(done_marker, "w").write("terminal")
args = argparse.Namespace(batch="clean_versions", yes=True)
with mock.patch.object(cli, "_ensure_running_locked", return_value="test daemon") as wake:
    assert cli.cmd_clean(args) == 0
wake.assert_called_once_with()
d = Dispatcher.__new__(Dispatcher)
d.host_dir = state.host_dir()
with state.connect() as conn:
    ready = conn.execute(
        "SELECT j.*, b.name AS batch_name FROM jobs j"
        " JOIN batches b ON b.id=j.batch_id"
        " WHERE j.batch_id='clean-versions' AND j.status='pending'"
    ).fetchall()
    d._reconcile_ready_batch_markers(conn, ready)
assert not os.path.exists(t1_new), "latest t1 artifact was not removed"
assert not os.path.exists(t2), "latest t2 artifact was skipped by global MAX(version)"
assert os.path.exists(t1_old), "obsolete t1 artifact should not be removed"
with state.connect() as conn:
    rows = conn.execute(
        "SELECT id, status, fingerprint FROM jobs WHERE batch_id='clean-versions' ORDER BY version, id"
    ).fetchall()
    observed = {row["id"]: (row["status"], row["fingerprint"]) for row in rows}
    batch_status = conn.execute(
        "SELECT status FROM batches WHERE id='clean-versions'"
    ).fetchone()["status"]
assert observed == {
    "clean-versions-t1-v1": ("skip", None),
    "clean-versions-t1-v2": ("pending", None),
    "clean-versions-t2-v1": ("pending", None),
}, observed
assert batch_status == "active", batch_status
assert not os.path.exists(done_marker), "clean left a stale done marker"
print("L10 clean selects latest task version and only requeues current generations")
PY
