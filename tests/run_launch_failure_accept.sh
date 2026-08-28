#!/bin/bash
# L17: a DB write failure after Popen must kill the just-launched process group.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import json, os
from gsched import state
from gsched.dispatcher import Dispatcher
from gsched.executor import Executor
import gsched.executor as executor_mod
import signal
state.init_db()
spec = {
    "id": "t1", "cmd": ["echo", "ok"], "stages": None, "cwd_abs": "/tmp",
    "git": False, "env": {}, "resources": {"gpu": 0}, "duration_min": None,
    "max_retry": 0, "artifacts": {}, "retry_transform": None, "probes": None,
}
with state.connect() as conn:
    state.insert_batch(conn, "launch-failure", "launch_failure", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, "launch-failure", "t1", 1, spec, 0, "p")
    state.insert_job(conn, "launch-failure-t1-v1", "launch-failure", "t1", 1, "fp", None, "p")
    job = conn.execute("SELECT * FROM jobs WHERE id='launch-failure-t1-v1'").fetchone()

d = Dispatcher.__new__(Dispatcher)
d.cfg = {"default_project": "p", "projects": {"p": {"root": "/tmp"}}}
d.host_dir = os.path.join(os.environ["SCHED_STATE"], state.hostname())
d.venv_paths = {}
d.log_line = lambda _msg: None
d._get_task_spec = lambda _conn, _job: json.dumps(spec)
d._should_skip = lambda _conn, _spec, _job: False
d._clean_stale_artifacts = lambda _conn, _spec, _job: None
d._job_log_path = lambda _job: os.path.join(d.host_dir, "logs", "launch.log")
killed = []
class Exec:
    def launch(self, **_kwargs): return 515151
    def kill_pgid(self, pgid, sig): killed.append((pgid, sig))
    def alive(self, _pgid): return False
d.executor = Exec()
original_update = state.update_job
def fail_update(*_args, **_kwargs):
    raise RuntimeError("simulated DB write failure")
state.update_job = fail_update
try:
    try:
        with state.connect() as conn:
            Dispatcher._launch_job(d, conn, job, None)
    except RuntimeError:
        pass
finally:
    state.update_job = original_update
assert killed and killed[0][0] == 515151, killed
with state.connect() as conn:
    row = conn.execute("SELECT status, pgid FROM jobs WHERE id='launch-failure-t1-v1'").fetchone()
marker = d._launch_marker_path({"id": "launch-failure-t1-v1"})
os.makedirs(os.path.dirname(marker), exist_ok=True)
with open(marker, "w", encoding="utf-8") as f:
    f.write("515151\n")
d._recover_launch_markers()
assert not os.path.exists(marker), marker
assert row[0] == "pending" and row[1] is None, row
e = Executor.__new__(Executor)
e._procs = {515151: object()}
original_killpg = executor_mod.os.killpg
def missing_kill(*_args):
    raise ProcessLookupError()
executor_mod.os.killpg = missing_kill
try:
    e.kill_pgid(515151, signal.SIGKILL)
finally:
    executor_mod.os.killpg = original_killpg
assert not e._procs, e._procs
print("L17 launch DB failure kills orphan process group and rolls back state")
PY
