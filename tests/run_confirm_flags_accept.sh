#!/bin/bash
# L12: releasing and unmanaged confirmation counters must not share a flag.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-confirm-flags"
export SCHED_STATE
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "gpus": [0], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
# 本场景只测两个确认计数文件的隔离；显式关闭 runner 的 fake 短路，
# 不调用任何 nvidia-smi/派发路径。
env -u SCHED_FAKE_GPUS python3 - <<'PY'
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
