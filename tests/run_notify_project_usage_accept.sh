#!/bin/bash
# L14: disabled file notifications and CPU-only project usage must be honored.
set -u
cd "$(dirname "$0")/.."
source tests/acceptance_cleanup.sh
sched_accept_make_root SCHED_STATE "sched-notify-usage"
export SCHED_STATE
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}},
  "notify": {"on": ["batch_done"]}
}
EOF
python3 - <<'PY'
import argparse, contextlib, io, os, sqlite3
from gsched import cli, notify, state
state.init_db()
original_file = notify.CHANNELS["file"]
calls = []
notify.CHANNELS["file"] = lambda _event, _cfg: calls.append(True) or "sent"
try:
    assert notify.send({"event": "batch_done"}, {"notify": {"on": ["batch_done"], "file": {"enabled": False}}}) == []
    assert not calls
    assert notify.send({"event": "batch_done"}, {"notify": {"on": ["batch_done"], "file": {"enabled": True}}}) == ["ok: sent"]
    assert len(calls) == 1
finally:
    notify.CHANNELS["file"] = original_file

spec = {"id": "t1", "cmd": ["echo", "ok"], "stages": None, "cwd_abs": "/tmp", "git": False,
        "env": {}, "resources": {"gpu": 0}, "duration_min": None, "max_retry": 0,
        "artifacts": {}, "retry_transform": None, "probes": None}
with state.connect() as conn:
    state.insert_batch(conn, "usage-batch", "usage", "mix", [], None, "/tmp", None, project="p")
    state.insert_task(conn, "usage-batch", "cpu", 1, spec, 0, "p")
    state.insert_task(conn, "usage-batch", "gpu", 1, {**spec, "id": "gpu", "resources": {"gpu": 1}}, 1, "p")
    state.insert_job(conn, "usage-cpu-v1", "usage-batch", "cpu", 1, "fp", None, "p")
    state.insert_job(conn, "usage-gpu-v1", "usage-batch", "gpu", 1, "fp", None, "p")
    conn.execute("UPDATE jobs SET status='running', gpu=NULL WHERE id='usage-cpu-v1'")
    conn.execute("UPDATE jobs SET status='running', gpu=0 WHERE id='usage-gpu-v1'")
out = io.StringIO()
with contextlib.redirect_stdout(out):
    assert cli.cmd_project_list(argparse.Namespace()) == 0
text = out.getvalue()
assert "1/∞" in text, text
print("L14 disabled file notifications skip and project usage counts GPU jobs only")
PY
