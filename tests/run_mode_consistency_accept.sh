#!/bin/bash
# L2: batch mode documentation and validation must expose the implemented mode only.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
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
from gsched.schema import SchemaError, validate_batch
cfg = {"default_project": "p", "venvs": {"k": "/bin"}, "projects": {"p": {"root": "/tmp", "git": False}}}
base = {"name": "mode", "project": "p", "tasks": [{"id": "t1", "cmd": ["echo", "ok"]}]}
assert validate_batch(base, cfg)["mode"] == "mix"
for mode in ("gpu", "cpu", "strict"):
    spec = deepcopy(base)
    spec["mode"] = mode
    try:
        validate_batch(spec, cfg)
    except SchemaError:
        continue
    raise AssertionError(f"unsupported mode accepted: {mode}")
print("L2 mode validation exposes implemented mix mode only")
PY
