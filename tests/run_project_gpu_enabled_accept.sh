#!/bin/bash
# ND-01: real local daemon with fake GPU, public CLI writes only.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
export PYTHONDONTWRITEBYTECODE=1
export SCHED_FAKE_GPUS=0:24
PY=${PY:-$(command -v python3)}
source tests/acceptance_cleanup.sh
sched_accept_make_root ST "sched-project-gpu-state"
sched_accept_make_root WORK "sched-project-gpu-work"
export SCHED_STATE=$ST SCHED_CONFIG=$ST/config.json
sched() { "$PY" -m gsched.cli "$@"; }
fail() {
  printf 'FAIL: %s\n' "$*" >&2
  sched status --json || true
  sched daemon status || true
  if [ -n "${BID:-}" ]; then sched diag "$BID" || true; fi
  tail -n 40 "$ST/$(hostname)/scheduler.log" 2>/dev/null || true
  exit 1
}
job_status() {
  sched status "$1" --json | "$PY" -c 'import json,sys; d=json.load(sys.stdin); print(next((j["status"] for j in d["jobs"] if j["task"]==sys.argv[1]), "absent"))' "$2"
}
wait_job() {
  for _ in $(seq 1 120); do
    [ "$(job_status "$1" "$2")" = "$3" ] && return 0
    sleep 1
  done
  fail "$1:$2 did not reach $3"
}
batch_id() {
  sched status "$1" --json | "$PY" -c 'import json,sys; print(json.load(sys.stdin)["batches"][0]["id"])'
}

"$PY" - "$ST" "$WORK" <<'PY'
import getpass, json, os, socket, sys
state_root, work = sys.argv[1:]
cfg = {"schema_version": 1, "user": getpass.getuser(), "node": socket.gethostname(),
       "state_dir": state_root, "gpus": [0], "default_project": "p",
       "projects": {"p": {"root": work, "git": False, "gpu_quota": 1}},
       "venvs": {"test": sys.executable}}
with open(os.path.join(state_root, "config.json"), "w") as stream:
    json.dump(cfg, stream)
live = "from pathlib import Path; import time; p=Path('release');\nwhile not p.exists(): time.sleep(.1)"
batch = {"name": "gpu-policy", "project": "p", "tasks": [
    {"id": "live", "cmd": ["{VENV:test}", "-c", live], "duration_min": 3},
    {"id": "held", "cmd": ["{VENV:test}", "-c", "from pathlib import Path; Path('held-ran').touch()"], "duration_min": 1}]}
for name, value in (("gpu.json", batch), ("disable.json", {"projects": {"p": {"gpu_enabled": False}}}),
                    ("enable.json", {"projects": {"p": {"gpu_enabled": True}}})):
    with open(os.path.join(work, name), "w") as stream:
        json.dump(value, stream)
cpu = {"name": "cpu-policy", "project": "p", "tasks": [
    {"id": "cpu", "cmd": ["{VENV:test}", "-c", "pass"], "resources": {"gpu": 0}, "duration_min": 1}]}
with open(os.path.join(work, "cpu.json"), "w") as stream:
    json.dump(cpu, stream)
PY

sched daemon start --fake
sched submit "$WORK/gpu.json" > "$WORK/submit.log"
BID=$(batch_id gpu-policy)
wait_job "$BID" live running
[ "$(job_status "$BID" held)" = pending ] || fail 'second GPU task was not queued'
sched config set -f "$WORK/disable.json" --yes
[ "$(job_status "$BID" live)" = running ] || fail 'disable stopped the running GPU task'
touch "$WORK/release"
wait_job "$BID" live done
[ "$(job_status "$BID" held)" = pending ] || fail 'disabled GPU task was dispatched'
[ ! -e "$WORK/held-ran" ] || fail 'held GPU task executed'
sched status --json > "$WORK/status.json"
sched project list --json > "$WORK/projects.json"
"$PY" - "$WORK/status.json" "$WORK/projects.json" <<'PY'
import json, sys
status, projects = [json.load(open(path)) for path in sys.argv[1:]]
held = next(job for job in status["jobs"] if job["task"] == "held")
assert held["status"] == "pending" and held["wait_reason"] == "project_gpu_disabled", held
p = projects["projects"][0]
assert p["gpu_enabled"] is False and p["gpu_access"] == "disabled" and p["gpu_quota"] == 1, p
PY
if [ -n "${SCHED_GPU_ACCESS_STATUS_OUT:-}" ]; then
  cp "$WORK/status.json" "$SCHED_GPU_ACCESS_STATUS_OUT"
fi
if sched submit "$WORK/gpu.json" > "$WORK/rejected.log" 2>&1; then
  fail 'disabled project accepted a GPU submission'
fi
grep -q 'gpu_enabled' "$WORK/rejected.log" || fail 'GPU rejection lacks policy reason'
if sched resubmit "$BID:live" > "$WORK/resubmit-rejected.log" 2>&1; then
  fail 'disabled project accepted GPU resubmit'
fi
sched submit "$WORK/cpu.json" > "$WORK/cpu-submit.log"
CPUBID=$(batch_id cpu-policy)
wait_job "$CPUBID" cpu done
echo 'PASS: disable preserves running work, holds queued GPU work, and permits CPU-only submission'

sched config set -f "$WORK/enable.json" --yes
wait_job "$BID" held done
[ -f "$WORK/held-ran" ] || fail 'enable did not resume the original GPU task'
sched status "$BID" --json | "$PY" -c 'import json,sys; d=json.load(sys.stdin); assert all(j["version"] == 1 and j["status"] == "done" for j in d["jobs"]), d'
sched daemon stop
echo 'PASS: enabling resumes the existing queue without new versions; daemon stopped through CLI'
