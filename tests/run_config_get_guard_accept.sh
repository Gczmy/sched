#!/bin/bash
# L4: read-only config get must work from a login node.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-config-guard"
export SCHED_STATE
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "compute-node", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import os, subprocess, sys
result = subprocess.run([sys.executable, "-m", "gsched.cli", "config", "get"], text=True, capture_output=True)
assert result.returncode == 0, result.stderr + result.stdout
assert '"node": "compute-node"' in result.stdout, result.stdout
assert not os.path.exists(os.path.join(os.environ["SCHED_STATE"], "compute-node", "state.db"))
print("L4 config get bypasses foreign write guard")
PY
