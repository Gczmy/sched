#!/bin/bash
# L15: agent-facing documentation references must point to files in this repo.
set -u
cd "$(dirname "$0")/.."
python3 - <<'PY'
import re
from pathlib import Path
text = Path("AGENTS.md").read_text(encoding="utf-8")
refs = sorted(set(re.findall(r"`(docs/[A-Za-z0-9_./-]+)`", text)))
missing = [ref for ref in refs if not Path(ref).is_file()]
assert not missing, missing
print("L15 AGENTS documentation references all resolve")
PY
