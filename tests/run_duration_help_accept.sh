#!/bin/bash
# L5: run help must describe the actual one-times duration watchdog.
set -u
cd "$(dirname "$0")/.."
python3 - <<'PY'
import subprocess, sys
result = subprocess.run([sys.executable, "-m", "gsched.cli", "run", "--help"], text=True, capture_output=True)
assert result.returncode == 0, result.stderr
text = result.stdout + result.stderr
assert "2x" not in text and "2×" not in text, text
assert "超过该时长" in text or "duration_min" in text, text
print("L5 run help matches duration watchdog semantics")
PY
