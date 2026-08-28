#!/bin/bash
# H4: gateway submit must warn when the remote daemon heartbeat is stale/missing.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
CUSTOM_STATE="$SCHED_STATE/custom-state"
mkdir -p "$CUSTOM_STATE/compute-node"
touch "$CUSTOM_STATE/compute-node/daemon.heartbeat" "$CUSTOM_STATE/compute-node/daemon.tick_ok"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "compute-node", "state_dir": "$CUSTOM_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "/bin"},
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
python3 - <<'PY'
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
