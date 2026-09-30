#!/bin/bash
# L2: ordinary mix admission is preserved; legacy strict is read-only compatibility.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-mode-consistency"
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
from copy import deepcopy
import os
from gsched.schema import SchemaError, validate_batch
cfg = {"default_project": "p", "venvs": {"k": "/bin"}, "projects": {"p": {"root": os.getcwd(), "git": False}}}
base = {"name": "mode", "project": "p", "tasks": [{"id": "t1", "cmd": ["/bin/echo", "ok"], "resources": {"gpu": 0, "cpus": 1}}]}
assert validate_batch(base, cfg)["mode"] == "mix"
for mode in ("gpu", "cpu"):
    spec = deepcopy(base)
    spec["mode"] = mode
    try:
        validate_batch(spec, cfg)
    except SchemaError:
        continue
    raise AssertionError(f"unsupported mode accepted: {mode}")
strict = deepcopy(base)
strict["mode"] = "strict"
strict["tasks"][0]["max_retry"] = 0
strict["tasks"][0]["git"] = False
try:
    validate_batch(strict, cfg)
except SchemaError:
    pass
else:
    raise AssertionError("legacy strict accepted as a new submission")

profile_cfg = deepcopy(cfg)
profile_cfg["native_exec_profiles"] = {
    "mode-test-v1": {
        "mode": "strict",
        "project": "p",
        "batch_name": "mode",
        "task_id": "t1",
        "submitted_argv": ["/bin/echo", "ok"],
    }
}
try:
    validate_batch(strict, profile_cfg)
except SchemaError:
    pass
else:
    raise AssertionError("legacy cold profile re-enabled strict admission")
ordinary = deepcopy(base)
ordinary["name"] = "ordinary-mode"
assert validate_batch(ordinary, cfg)["mode"] == "mix"
print("L2 mix admission preserved; legacy strict admission rejected")
PY
