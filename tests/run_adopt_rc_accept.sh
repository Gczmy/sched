#!/bin/bash
# H2: a daemon restart must preserve a completed job's real exit code.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-adopt-rc"
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
import hashlib, json, os, sqlite3, sys, time
sys.path.insert(0, ".")
from gsched import state
from gsched.executor import Executor, _is_strong_start_token
state.init_db()

# The launch wrapper must leave a durable rc marker before its process group exits.
rc_dir = os.path.join(os.environ["SCHED_STATE"], "rc-launch")
os.makedirs(rc_dir, exist_ok=True)
log_path = os.path.join(os.environ["SCHED_STATE"], "launch.log")
launch_marker = os.path.join(rc_dir, "launch.marker")
e = Executor()
pid = e.launch(
    ["/bin/sh", "-c", "exit 0"],
    None,
    "/tmp",
    {
        "SCHED_RC_DIR": rc_dir,
        "SCHED_RC_PREFIX": "launch",
        "SCHED_LAUNCH_MARKER": launch_marker,
        "PATH": "/nonexistent",
    },
    None,
    log_path,
)
rc = e.poll_rc(pid)
while rc is None:
    time.sleep(0.01)
    rc = e.poll_rc(pid)
assert rc == 0
rc_path = os.path.join(rc_dir, f"launch-{pid}.rc")
with open(rc_path, encoding="utf-8") as f:
    assert f.read().strip() == "0"

# Simulate a daemon restart: Popen is gone, but the adopted job's rc marker is 0.
with open(launch_marker, encoding="utf-8") as f:
    launch_identity = f.read().split()
assert launch_identity[0] == str(pid)
assert len(launch_identity) >= 2
assert _is_strong_start_token(launch_identity[1]), launch_identity
batch_id = "adopt-rc-batch"
job_id = "adopt-rc-batch-t1-v1"
spec = {
    "id": "t1", "cmd": ["echo", "ok"], "stages": None, "cwd_abs": "/tmp",
    "git": None, "env": {}, "resources": {"gpu": 0}, "duration_min": None,
    "max_retry": 0, "artifacts": {}, "retry_transform": None, "probes": None,
}
with state.connect() as conn:
    state.insert_batch(conn, batch_id, "adopt_rc", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, batch_id, "t1", 1, spec, 0, "p")
    state.insert_job(conn, job_id, batch_id, "t1", 1, "fp", None, "p")
    state.update_job(conn, job_id, status="running", pgid=424242)

from gsched.dispatcher import Dispatcher
d = Dispatcher.__new__(Dispatcher)
d.cfg = json.load(open(os.path.join(os.environ["SCHED_STATE"], "config.json")))
d.host_dir = os.path.join(os.environ["SCHED_STATE"], state.hostname())
d.log_line = lambda _msg: None
class DeadExecutor:
    def alive(self, _pgid): return False
d.executor = DeadExecutor()
d._release_gpu_for_job = lambda _conn, _job: None
d._drop_profile = lambda _job: None
d._consume_profile = lambda _conn, _job, _spec: None
d._maybe_retry = lambda _conn, _job: None
marker = d._job_rc_path({"id": job_id, "pgid": 424242})
assert marker is not None
os.makedirs(os.path.dirname(marker), exist_ok=True)
with open(marker, "w", encoding="utf-8") as f:
    f.write("0\n")
Dispatcher._adopt_running(d)
conn = sqlite3.connect(state.db_path())
row = conn.execute("SELECT status,rc FROM jobs WHERE id=?", (job_id,)).fetchone()

# A restart after SIGTERM may have no durable rc marker; kill_reason still
# decides the terminal state and must never fall through to artifact skip/retry.
reason_jobs = []
for index, reason, expected in [
    (1, "cancelled", "cancelled"),
    (2, "timed_out", "timed_out"),
]:
    reason_batch = f"adopt-reason-{index}"
    reason_job = f"{reason_batch}-t1-v1"
    with state.connect() as c:
        state.insert_batch(c, reason_batch, reason_batch, "mix", [], None, "/tmp", None, project="p")
        state.insert_task(c, reason_batch, "t1", 1, spec, 0, "p")
        state.insert_job(c, reason_job, reason_batch, "t1", 1, "fp", None, "p")
        state.update_job(c, reason_job, status="running", pgid=424200 + index, kill_reason=reason)
    reason_jobs.append((reason_job, expected))
Dispatcher._adopt_running(d)
with sqlite3.connect(state.db_path()) as reason_conn:
    for reason_job, expected in reason_jobs:
        reason_row = reason_conn.execute(
            "SELECT status, rc FROM jobs WHERE id=?", (reason_job,)
        ).fetchone()
        assert reason_row == (expected, 137), reason_row

# Per-attempt markers are keyed by the process group, so retry attempts cannot
# consume a marker produced by an older process group with the same job id.
stale_batch = "stale-marker-batch"
stale_job = "stale-marker-batch-t1-v1"
stale_spec = {
    "id": "t1", "cmd": ["echo", "ok"], "stages": None, "cwd_abs": "/tmp",
    "git": None,
    "env": {
        "SCHED_BATCH_ID": "spoof-batch",
        "SCHED_TASK_ID": "spoof-task",
        "SCHED_RUN_ID": "spoof-run",
        "SCHED_PROJECT": "spoof-project",
        "SCHED_RC_PREFIX": "spoof-rc-prefix",
    },
    "resources": {"gpu": 0}, "duration_min": None,
    "max_retry": 0, "artifacts": {}, "retry_transform": None, "probes": None,
}
with state.connect() as conn:
    state.insert_batch(conn, stale_batch, "stale_marker", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, stale_batch, "t1", 1, stale_spec, 0, "p")
    state.insert_job(conn, stale_job, stale_batch, "t1", 1, "fp", None, "p")
    stale_row = conn.execute("SELECT * FROM jobs WHERE id=?", (stale_job,)).fetchone()
old_marker = d._job_rc_path({"id": stale_job, "pgid": 424240})
new_marker = d._job_rc_path({"id": stale_job, "pgid": 515151})
assert old_marker != new_marker
os.makedirs(os.path.dirname(old_marker), exist_ok=True)
with open(old_marker, "w", encoding="utf-8") as f:
    f.write("0\n")

class LaunchExecutor:
    def __init__(self):
        self.last_env = None
    def launch(self, **kwargs):
        self.last_env = dict(kwargs["env"])
        return 515151

d2 = Dispatcher.__new__(Dispatcher)
d2.cfg = d.cfg
d2.host_dir = d.host_dir
d2.venv_paths = {}
d2.executor = LaunchExecutor()
d2.log_line = lambda _msg: None
d2._get_task_spec = lambda _conn, _job: json.dumps(stale_spec)
d2._should_skip = lambda _conn, _spec, _job, current_fingerprint: False
d2._clean_stale_artifacts = lambda _conn, _spec, _job, current_fingerprint, stage_fingerprints: None
d2._job_log_path = lambda _job: os.path.join(d.host_dir, "logs", f"{stale_job}.log")
with state.connect() as conn:
    assert Dispatcher._launch_job(d2, conn, stale_row, None)
launch_env = d2.executor.last_env
assert launch_env is not None
assert launch_env["SCHED_BATCH_ID"] == "stale_marker" != stale_batch
assert launch_env["SCHED_TASK_ID"] == "t1"
assert launch_env["SCHED_RUN_ID"] == stale_job
assert launch_env["SCHED_PROJECT"] == "p"
expected_rc_prefix = hashlib.sha256(stale_job.encode("utf-8")).hexdigest()[:24]
assert launch_env["SCHED_RC_PREFIX"] == expected_rc_prefix
assert d2._read_job_rc({"id": stale_job, "pgid": 515151}) is None
assert os.path.exists(old_marker)
Dispatcher._adopt_running(d)
with state.connect() as conn:
    settled_stale = state.get_job(conn, stale_job)
assert settled_stale["status"] == "failed", settled_stale["status"]
print("dispatcher-owned scheduler identity overrides task env spoofing")
print("H2 retry process group gets a distinct exit marker")
assert row == ("done", 0), row
print("H2 adopted completed job uses durable rc=0 and becomes done")
PY
