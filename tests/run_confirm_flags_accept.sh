#!/bin/bash
# L12: releasing and unmanaged confirmation counters must not share a flag.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "gpus": [0], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
import os
from gsched.allocator import Allocator

a = Allocator([0], fake=False)
assert a._confirm_release(0) is False
assert a._confirm_unmanaged(0) is False
assert a._confirm_unmanaged(0) is True
assert os.path.exists(a._confirm_flag("release_confirm", 0))
assert not os.path.exists(a._confirm_flag("unmanaged_confirm", 0))
print("L12 releasing and unmanaged confirmation flags are isolated")
PY
