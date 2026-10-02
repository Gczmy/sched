"""Generic progress fixture: no GPU, framework, project import or scientific logic."""
import json
import os
from pathlib import Path
import sys
import time

from gsched.recovery import CheckpointStore

store = CheckpointStore.from_environment()
if os.environ.get("CHECK_IDENTITY_FD") == "1":
    identity = json.loads(os.read(4, 8192))
    if identity["schema"] == "sched_execution_owner_identity/v1":
        assert set(identity) == {"schema", "attempt", "owner"}
        identity = identity["attempt"]
    assert identity["schema"] == "sched_execution_identity/v1"
    assert identity["job_id"] == store.value["job_id"]
settings = json.loads(Path("settings.json").read_text())
if len(sys.argv) > 1:
    settings.update(settings.get("groups", {}).get(sys.argv[1], {}))
if store.value["mode"] == "smoke":
    store.save({"next": 1, "results": [0]})
    assert store.load() == {"next": 1, "results": [0]}
    try:
        raise MemoryError("simulated recoverable OOM")
    except MemoryError:
        store.save({"next": 2, "results": [0, 1]})
    assert store.load()["next"] == 2
    store.report("smoke_ok")
    sys.exit(0)
with Path("starts.jsonl").open("a") as stream:
    stream.write(json.dumps({"job_id": store.value["job_id"], "pid": os.getpid()}) + "\n")
progress = store.load() or {"next": 0, "results": []}
for step in range(progress["next"], settings["total"]):
    progress["results"].append(step)
    progress["next"] = step + 1
    store.save(progress)
    if settings.get("hold_step") == progress["next"]:
        deadline = time.monotonic() + 120
        while not Path("release").exists() and time.monotonic() < deadline:
            time.sleep(.05)
    if settings.get("step_sec"):
        time.sleep(settings["step_sec"])
    if progress["next"] in settings.get("oom_at", []):
        store.report("oom")
        if settings.get("oom_log", True):
            print("CUDA out of memory", flush=True)
        sys.exit(42)
Path("result.json").write_text(json.dumps(progress))
