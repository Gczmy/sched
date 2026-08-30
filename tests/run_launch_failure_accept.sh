#!/bin/bash
# L17: a DB write failure after Popen must kill the just-launched process group
# while retaining the committed running/pgid=NULL recovery claim.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
PY=${PY:-python3}
sched_accept_make_root SCHED_STATE "sched-launch-failure"
export SCHED_STATE
export SCHED_CONFIG="$SCHED_STATE/config.json"
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [0], "venvs": {"k": "$PY"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
"$PY" - <<'PY'
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
d._should_skip = lambda _conn, _spec, _job, current_fingerprint: False
d._clean_stale_artifacts = lambda _conn, _spec, _job, current_fingerprint, stage_fingerprints: None
d._job_log_path = lambda _job: os.path.join(d.host_dir, "logs", "launch.log")
killed = []
class Exec:
    def __init__(self):
        self.running = False
    def launch(self, **_kwargs):
        marker = d._launch_marker_path(job)
        os.makedirs(os.path.dirname(marker), exist_ok=True)
        with open(marker, "w", encoding="utf-8") as stream:
            stream.write("515151 proc:1\n")
        self.running = True
        return 515151
    def kill_pgid(self, pgid, sig):
        killed.append((pgid, sig))
        self.running = False
        return True
    def alive(self, _pgid):
        return self.running
d.executor = Exec()
d._proc_start_time = lambda _pgid: "proc:1"
original_update = state.update_job
def fail_update(conn, job_id, **fields):
    if fields.get("pgid") == 515151:
        raise RuntimeError("simulated post-launch DB write failure")
    return original_update(conn, job_id, **fields)
state.update_job = fail_update
try:
    try:
        with state.connect() as conn:
            Dispatcher._launch_job(d, conn, job, None)
    except RuntimeError:
        pass
finally:
    state.update_job = original_update
assert killed and killed[0] == (515151, signal.SIGKILL), killed
with state.connect() as conn:
    row = conn.execute(
        "SELECT status, pgid FROM jobs WHERE id='launch-failure-t1-v1'"
    ).fetchone()
marker = d._launch_marker_path({"id": "launch-failure-t1-v1"})
assert not os.path.exists(marker), marker
assert row[0] == "running" and row[1] is None, row
e = Executor.__new__(Executor)
e._procs = {515151: object()}
e._dead_pgroups = set()
original_killpg = executor_mod.os.killpg
def missing_kill(*_args):
    raise ProcessLookupError()
executor_mod.os.killpg = missing_kill
try:
    e.kill_pgid(515151, signal.SIGKILL)
finally:
    executor_mod.os.killpg = original_killpg
assert not e._procs, e._procs
print("L17 launch DB failure kills orphan process group and retains durable recovery claim")
PY

if ! SCHED_FAKE_GPUS=0:24 "$PY" -m gsched.cli daemon start --fake >/dev/null 2>&1; then
  echo "failed to start isolated daemon for launch-failure recovery" >&2
  exit 1
fi
settled=0
for _ in $(seq 1 120); do
  if "$PY" -m gsched.cli status launch-failure --json 2>/dev/null | "$PY" -c '
import json, sys
d = json.load(sys.stdin)
batches = d.get("batches", [])
jobs = d.get("jobs", [])
ok = (
    len(batches) == 1
    and batches[0].get("batch_id") == "launch-failure"
    and batches[0].get("status") == "blocked"
    and len(jobs) == 1
    and jobs[0].get("status") == "blocked"
)
raise SystemExit(0 if ok else 1)' \
    && [ -f "$SCHED_STATE/$NODE/markers/launch_failure.blocked" ] \
    && [ ! -e "$SCHED_STATE/$NODE/markers/launch_failure.done" ]; then
    settled=1
    break
  fi
  sleep 1
done
if [ "$settled" != "1" ]; then
  echo "launch-failure recovery claim did not fully converge" >&2
  exit 1
fi
"$PY" -m gsched.cli daemon stop >/dev/null 2>&1 || exit 1
