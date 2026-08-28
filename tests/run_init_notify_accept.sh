#!/bin/bash
# M6: cmd_init must emit the command channel in schema-compatible form.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
python3 - <<'PY'
import builtins, json, os, tempfile, sys
sys.path.insert(0, ".")
from gsched import cli
from gsched.config import ConfigError, load_config
config_path = os.path.join(os.environ["SCHED_STATE"], "config.json")
answers = iter(["t", "node", os.environ["SCHED_STATE"], "/tmp", "/bin/python", "y", "3", "/bin/echo"])
original_input = builtins.input
builtins.input = lambda _prompt="": next(answers)
try:
    assert cli.cmd_init(type("Args", (), {"config": config_path})()) == 0
finally:
    builtins.input = original_input
try:
    cfg = load_config(config_path)
except ConfigError as exc:
    raise AssertionError(f"generated config is invalid: {exc}")
assert cfg["notify"]["command"] == ["/bin/echo"], cfg["notify"]
print("M6 cmd_init emits schema-compatible notify command array")
PY
