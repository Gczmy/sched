#!/bin/bash
# L16: re-check heartbeat before killing a supposedly stale daemon.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "gpus": [], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import os
import gsched.dispatcher as dispatcher_module
from gsched.dispatcher import Dispatcher

d = Dispatcher.__new__(Dispatcher)
d.lock_dir = os.path.join(os.environ["SCHED_STATE"], "lock")
d.pid_file = os.path.join(os.environ["SCHED_STATE"], "daemon.pid")
d.heartbeat_file = os.path.join(os.environ["SCHED_STATE"], "daemon.heartbeat")
os.makedirs(d.lock_dir)
with open(d.pid_file, "w") as f:
    f.write("123")
checks = iter([False, True])
d._is_running = lambda: False
d._read_pid = lambda: 123
d._pid_exists = lambda _pid: True
d._heartbeat_fresh = lambda: next(checks)
d.log_line = lambda _msg: None
d._cleanup_lock = lambda: None
original_matches = dispatcher_module.pid_cmdline_matches
dispatcher_module.pid_cmdline_matches = lambda _pid, _needle: True
killed = []
original_kill = dispatcher_module.os.kill
original_sleep = dispatcher_module.time.sleep
dispatcher_module.os.kill = lambda pid, sig: killed.append((pid, sig))
dispatcher_module.time.sleep = lambda _seconds: None
try:
    assert Dispatcher.acquire_lock(d) is False
finally:
    dispatcher_module.os.kill = original_kill
    dispatcher_module.time.sleep = original_sleep
    dispatcher_module.pid_cmdline_matches = original_matches
assert not killed, killed
print("L16 fresh second heartbeat prevents stale-daemon kill")
PY
