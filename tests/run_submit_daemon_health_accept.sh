#!/bin/bash
# H4: gateway submit must warn when the remote daemon heartbeat is stale/missing.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
PY=${PY:-$(command -v python3)}
unset SCHED_ALLOW_FOREIGN_WRITE || true
export SCHED_FAKE_GPUS=0
sched_accept_make_root SCHED_STATE "sched-submit-health"
export SCHED_STATE
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "$(whoami)", "node": "compute-node", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "$PY"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
cat > "$SCHED_STATE/batch.json" <<'EOF'
{
  "schema_version": 1,
  "name": "daemon_health_warning",
  "project": "p",
  "cwd": "/tmp",
  "tasks": [{"id": "t1", "cmd": ["echo", "ok"]}]
}
EOF
"$PY" - <<'PY'
import os, subprocess, sys
result = subprocess.run(
    [sys.executable, "-m", "gsched.cli", "submit", os.path.join(os.environ["SCHED_STATE"], "batch.json")],
    text=True, capture_output=True,
)
assert result.returncode == 0, result.stderr + result.stdout
output = result.stdout + result.stderr
assert "daemon 未运行" in output, output
assert "sched verify" in output, output
print("H4 foreign submit warns when daemon heartbeat is unavailable")
PY
