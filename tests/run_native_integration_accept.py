"""Linux CPU-only acceptance for native V1 and the current daemon controls."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time


def main():
    with tempfile.TemporaryDirectory(prefix="sched-native-integration-") as temporary:
        root = Path(temporary)
        program = root / "job.py"
        program.write_text(
            "import json,os,time\n"
            "from pathlib import Path\n"
            "Path('observed.json').write_text(json.dumps(dict(os.environ)))\n"
            "while not Path('release').exists(): time.sleep(.1)\n"
        )
        argv = [sys.executable, "-I", "-S", str(program)]
        cfg = {
            "schema_version": 1, "node": socket.gethostname(),
            "user": os.environ["USER"], "state_dir": str(root / "state"),
            "gpus": [], "cpus_total": 1, "default_project": "test",
            "projects": {"test": {"root": str(root), "git": False, "gpu_enabled": False}},
            "venvs": {}, "task_default_env": {"DEFAULT_POISON": "must-not-inherit"},
            "native_exec_profiles": {"local-v1": {
                "mode": "strict", "project": "test", "batch_name": "native-control",
                "task_id": "native", "submitted_argv": argv,
            }},
        }
        (root / "config.json").write_text(json.dumps(cfg))
        spec = {"name": "native-control", "mode": "strict", "project": "test",
                "tasks": [{"id": "native", "cmd": argv, "git": False,
                           "resources": {"gpu": 0, "cpus": 1}, "max_retry": 0}]}
        batch = root / "batch.json"
        batch.write_text(json.dumps(spec))
        env = dict(os.environ, SCHED_STATE=str(root / "state"),
                   SCHED_CONFIG=str(root / "config.json"),
                   PYTHONPATH=str(Path(__file__).resolve().parents[1]),
                   DAEMON_POISON="must-not-inherit", PYTHONDONTWRITEBYTECODE="1")
        env.pop("SCHED_ALLOW_FOREIGN_WRITE", None)

        def cli(*args, success=True):
            result = subprocess.run([sys.executable, "-m", "gsched.cli", *args],
                                    env=env, capture_output=True, text=True, timeout=45)
            assert (result.returncode == 0) == success, (args, result.stdout, result.stderr)
            return result.stdout

        def wait(status, reason=None):
            deadline = time.monotonic() + 90
            snapshot = None
            while time.monotonic() < deadline:
                snapshot = json.loads(cli("status", "--json"))
                jobs = snapshot["jobs"]
                if len(jobs) == 1 and jobs[0]["status"] == status:
                    if reason is None or jobs[0]["wait_reason"] == reason:
                        return jobs[0]
                time.sleep(.5)
            raise AssertionError(snapshot)

        try:
            cli("daemon", "drain")
            cli("submit", str(batch))
            wait("pending", "draining")
            cli("daemon", "resume")
            running = wait("running")
            observed_path = root / "observed.json"
            deadline = time.monotonic() + 10
            while not observed_path.exists() and time.monotonic() < deadline:
                time.sleep(.1)
            observed = json.loads(observed_path.read_text())
            assert "DEFAULT_POISON" not in observed
            assert "DAEMON_POISON" not in observed
            assert "PYTHONPATH" not in observed
            assert observed["CUDA_VISIBLE_DEVICES"] == ""
            assert observed["SCHED_BATCH_ID"] == spec["name"]
            assert observed["SCHED_TASK_ID"] == "native"
            cli("daemon", "drain", "--stop-when-idle")
            (root / "release").touch()
            completed = wait("done")
            assert completed["version"] == running["version"]
            cli("retry", running["batch_id"], success=False)
            cli("resubmit", running["batch_id"], "--all", success=False)
            cli("submit", str(batch), success=False)
            print("PASS: native V1 obeys drain, runs with GPU disabled, isolates env, completes once and rejects replay")
        finally:
            cli("daemon", "stop")


if __name__ == "__main__":
    main()
