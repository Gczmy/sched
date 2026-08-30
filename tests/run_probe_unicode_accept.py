import json
import os
import sqlite3
import sys

sys.path.insert(0, ".")
from gsched import state
from gsched.dispatcher import Dispatcher

state.init_db()
log_path = os.path.join(os.environ["SCHED_STATE"], "job.log")
spec = {
    "id": "t1", "cmd": ["echo", "ok"], "stages": None, "cwd_abs": "/tmp",
    "git": False, "env": {}, "resources": {"gpu": 0}, "duration_min": None,
    "max_retry": 0, "artifacts": {}, "retry_transform": None,
    "probes": {"fail_on_log": "错误"},
}
with state.connect() as conn:
    state.insert_batch(conn, "probe-unicode", "probe_unicode", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, "probe-unicode", "t1", 1, spec, 0, "p")
    state.insert_job(conn, "probe-unicode-t1-v1", "probe-unicode", "t1", 1, "fp", None, "p")
    conn.execute("UPDATE jobs SET status='running', pgid=4242 WHERE id=?", ("probe-unicode-t1-v1",))
with open(log_path, "wb") as f:
    f.write("阶段✓错".encode("utf-8"))

class Executor:
    def alive(self, _pgid): return True
    def kill_pgid(self, _pgid, _sig=None): return True

d = Dispatcher.__new__(Dispatcher)
d.host_dir = os.path.join(os.environ["SCHED_STATE"], state.hostname())
d.executor = Executor()
d._probe_offsets = {}
d._job_log_path = lambda _job: log_path
d._get_task_spec = lambda _conn, _job: json.dumps(spec)
d._release_gpu_for_job = lambda _conn, _job: None
d._drop_profile = lambda _job: None
d.log_line = lambda _msg: None
d._proc_start_time = lambda _pgid: "proc:1"
marker = d._launch_marker_path(
    {"id": "probe-unicode-t1-v1", "pgid": 4242}
)
os.makedirs(os.path.dirname(marker), exist_ok=True)
with open(marker, "w", encoding="utf-8") as stream:
    stream.write("4242 proc:1\n")
Dispatcher._check_probes(d)
with open(log_path, "ab") as f:
    f.write("误\r\n".encode("utf-8"))
Dispatcher._check_probes(d)
with state.connect() as conn:
    running = state.get_job(conn, "probe-unicode-t1-v1")
    assert (
        running["status"],
        running["failure"],
        running["kill_reason"],
    ) == ("running", "probe", "probe_failed"), dict(running)
    d.executor.alive = lambda _pgid: False
    Dispatcher._handle_job_done(d, conn, running, 137)
with state.connect() as conn:
    row = conn.execute(
        "SELECT status, failure FROM jobs WHERE id=?",
        ("probe-unicode-t1-v1",),
    ).fetchone()
assert tuple(row) == ("blocked", "probe"), tuple(row)
print("L6 UTF-8 probe offsets detect appended failures")
