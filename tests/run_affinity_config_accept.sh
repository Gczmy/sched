#!/bin/bash
# L8: hard GPU affinity without cards must fail configuration validation.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-affinity-config"
export SCHED_STATE
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "gpus": [0], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false, "gpu_affinity_hard": true}}
}
EOF
python3 - <<'PY'
from gsched.config import ConfigError, load_config
try:
    load_config()
except ConfigError as exc:
    assert "gpu_affinity_hard" in str(exc), exc
else:
    raise AssertionError("empty hard affinity was accepted")
print("L8 empty hard GPU affinity is rejected")
PY
