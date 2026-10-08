"""Manual two-host acceptance: shared disposable state, CPU only, CLI only.

Run prepare/consume/finish/cleanup inside an existing compute-node allocation.
Run deliver/ticket/confirm on its gateway in separate SSH sessions. No SSH
endpoint, node, allocation, production config or state is embedded here.
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time


REQUEST_ID = "gateway-feedback-once"


class Fixture:
    def __init__(self, root):
        self.root = root
        self.repository = Path(__file__).resolve().parents[1]
        self.config = root / "config.json"
        self.manifest = root / "fixture.json"
        self.env = dict(os.environ, SCHED_CONFIG=str(self.config),
                        SCHED_STATE=str(root / "state"), SCHED_FAKE_GPUS="0:24",
                        PYTHONPATH=str(self.repository), PYTHONDONTWRITEBYTECODE="1")
        self.env.pop("SCHED_ALLOW_FOREIGN_WRITE", None)

    def cli(self, *args):
        result = subprocess.run([sys.executable, "-m", "gsched.cli", *map(str, args)],
                                cwd=self.root, env=self.env, capture_output=True,
                                text=True, timeout=75)
        assert result.returncode == 0, (args, result.returncode, result.stdout, result.stderr)
        return result.stdout

    def data(self, *args):
        return json.loads(self.cli(*args))

    def wait(self, query, predicate):
        deadline = time.monotonic() + 120
        while True:
            value = query()
            if predicate(value):
                return value
            if time.monotonic() >= deadline:
                raise AssertionError(value)
            time.sleep(0.5)

    def prepare(self):
        assert sys.platform == "linux" and os.environ.get("SLURM_JOB_ID"), "Existing compute allocation required"
        self.root.mkdir(mode=0o700)  # Never reuse a production/previous root.
        config = {"schema_version": 1, "node": socket.gethostname(), "user": getpass.getuser(),
                  "state_dir": str(self.root / "state"), "gpus": [0], "cpus_total": 1,
                  "max_cpu_jobs": 1, "idle_timeout_min": 0, "default_project": "example",
                  "host_mem_default_gib": 0.125, "host_mem_reserve_gib": 0,
                  "projects": {"example": {"root": str(self.root), "git": False}},
                  "venvs": {"python": sys.executable}}
        self.config.write_text(json.dumps(config))
        program = ("from pathlib import Path; Path('runs.txt').open('a').write('run\\n'); "
                   "Path('result.json').write_text('{\"ok\":true}')")
        batch = {"name": "gateway-feedback", "project": "example", "tasks": [{
            "id": "task", "cmd": [sys.executable, "-I", "-c", program], "max_retry": 0,
            "resources": {"gpu": 0, "cpus": 1},
            "artifacts": {"result": {"path": "result.json", "json_equals": {"ok": True}}}}]}
        (self.root / "batch.json").write_text(json.dumps(batch))
        self.cli("daemon", "drain")
        identity = self.data("identity", "--json")
        assert identity["available"] and self.data("daemon", "status", "--json")["health_state"] == "stopped"
        self.manifest.write_text(json.dumps({"fixture": "sched-gateway-feedback-v1",
            "node": config["node"], "instance_id": identity["instance_id"], "request_id": REQUEST_ID}))
        print("PASS: new independent instance prepared, private daemon stopped and drained", flush=True)

    def load(self, *, compute):
        assert not self.root.is_symlink() and self.root.is_dir()
        self.binding = json.loads(self.manifest.read_text())
        config = json.loads(self.config.read_text())
        assert self.binding["fixture"] == "sched-gateway-feedback-v1" and self.binding["request_id"] == REQUEST_ID
        assert config["state_dir"] == str(self.root / "state") and config["node"] == self.binding["node"]
        assert config["projects"] == {"example": {"root": str(self.root), "git": False}}
        assert (socket.gethostname() == self.binding["node"]) == compute, "Wrong host for this role"
        if compute:
            assert sys.platform == "linux" and os.environ.get("SLURM_JOB_ID"), "Existing compute allocation required"
        assert self.data("identity", "--json")["instance_id"] == self.binding["instance_id"]

    def receipt(self, wait=0):
        return self.data("request-status", REQUEST_ID, "--expect-instance", self.binding["instance_id"],
                         "--wait-sec", str(wait), "--json")

    def submit(self):
        return self.data("submit", self.root / "batch.json", "--request-id", REQUEST_ID,
                         "--expect-instance", self.binding["instance_id"], "--expect-project", "example", "--json")

    def delivered(self, *, pause):
        assert self.receipt()["phase"] == "not_found", "Do not redeliver an unknown original request"
        result = self.submit()
        assert result["phase"] == "delivered" and not result["persisted"] and result["delivery"] == "inbox"
        # Do not emit the submit reply/batch ID: reconnecting callers recover it
        # exclusively through the frozen RID, not a newly generated operation.
        print("PASS: gateway CLI committed delivery; submit reply intentionally withheld", flush=True)
        if pause:
            print("TRANSPORT_DISCONNECT_WINDOW", flush=True)
            time.sleep(60)

    def ticket(self):
        before = self.receipt(wait=0.2)
        assert before["phase"] == "delivered" and before["receipt_source"] == "ticket"
        assert before["delivery_confirmed"] is True and before["batch_persisted"] is False
        assert before["wait_timed_out"]
        result = self.submit()  # Explicit same logical operation, never another RID.
        assert result["replayed"] and result["phase"] == "delivered"
        assert self.receipt()["binding_sha256"] == before["binding_sha256"]
        (self.root / "ticket-observation.json").write_text(json.dumps(before))
        assert not (self.root / "runs.txt").exists()
        print("PASS: fresh gateway session recovers original delivered ticket; bounded wait does not claim acceptance", flush=True)

    def consume(self):
        self.cli("daemon", "start", "--fake")
        receipt = self.wait(self.receipt, lambda r: r["phase"] == "done")
        assert receipt["code"] == 0 and receipt["receipt_source"] == "database" and receipt["batch_persisted"]
        self.assert_one_pending(receipt["result"]["batch_id"])
        print("PASS: compute daemon consumes inbox while drained; exactly one pending job and no worker run", flush=True)

    def assert_one_pending(self, batch):
        status = self.data("status", "--json")
        assert not any(status["truncated"].values()) and len(status["batches"]) == len(status["jobs"]) == 1
        assert status["batches"][0]["id"] == status["jobs"][0]["batch_id"] == batch
        assert status["jobs"][0]["status"] == "pending" and not (self.root / "runs.txt").exists()

    def confirm(self):
        receipt = self.receipt(wait=2)
        assert receipt["phase"] == "done" and receipt["receipt_source"] == "database"
        assert receipt["code"] == 0 and receipt["batch_persisted"] and not receipt["wait_timed_out"]
        ticket = json.loads((self.root / "ticket-observation.json").read_text())
        assert receipt["binding_sha256"] == ticket["binding_sha256"]
        result = self.submit()
        assert result["replayed"] and result["batch_id"] == receipt["result"]["batch_id"]
        self.assert_one_pending(result["batch_id"])
        (self.root / "accepted-observation.json").write_text(json.dumps(receipt))
        print("PASS: gateway DB/WAL snapshot confirms original RID; same-RID replay still one pending job", flush=True)

    def finish(self):
        batch = self.receipt()["result"]["batch_id"]
        self.cli("daemon", "resume")
        self.wait(lambda: self.data("task", f"{batch}:task", "--json"),
                  lambda t: t["jobs"][0]["status"] == "done")
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda h: h["health_state"] == "stopped")
        assert (self.root / "runs.txt").read_text() == "run\n"
        validations = self.data("artifact-validations", f"{batch}:task", "--json")["validations"]
        assert len(validations) == 1 and validations[0]["passed"] and validations[0]["wait_verified"]
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda h: h["health_state"] == "healthy")
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda h: h["health_state"] == "stopped")
        print("PASS: one real CPU run, verified wait/artifact; private daemon restart does not replay worker", flush=True)

    def final(self):
        before = json.loads((self.root / "accepted-observation.json").read_text())
        current = self.receipt(wait=2)
        stable = lambda r: {k: v for k, v in r.items() if k != "observed_at"}
        assert stable(current) == stable(before)
        replay = self.submit()
        batch = current["result"]["batch_id"]
        assert replay["replayed"] and replay["batch_id"] == batch
        status = self.data("status", "--json")
        assert not any(status["truncated"].values()) and len(status["batches"]) == len(status["jobs"]) == 1
        assert status["jobs"][0]["status"] == "done" and (self.root / "runs.txt").read_text() == "run\n"
        print("PASS: new gateway connection after compute restart preserves instance/RID/receipt and one worker run", flush=True)

    def cleanup(self):
        self.cli("daemon", "stop")
        assert self.data("daemon", "status", "--json")["health_state"] == "stopped"
        # This root was created exclusively by prepare. Retain it on any earlier
        # failure; cleanup is an explicit role and never targets deployed state.
        shutil.rmtree(self.root)
        print("PASS: only the verified stopped, disposable fixture removed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=("prepare", "deliver", "ticket", "consume", "confirm", "finish", "final", "cleanup"))
    parser.add_argument("--root", type=Path, required=True, help="new absolute directory on compute/gateway shared storage")
    parser.add_argument("--pause-after-delivery", action="store_true", help="deliver only: allow an SSH disconnect after committed delivery")
    args = parser.parse_args()
    if not args.root.is_absolute() or args.root == Path(args.root.anchor) or args.root == Path.home():
        parser.error("root must be a new dedicated absolute child directory")
    if args.pause_after_delivery and args.role != "deliver":
        parser.error("pause is only valid for deliver")
    fixture = Fixture(args.root)
    if args.role == "prepare":
        fixture.prepare()
    else:
        fixture.load(compute=args.role in ("consume", "finish", "cleanup"))
        if args.role == "deliver":
            fixture.delivered(pause=args.pause_after_delivery)
        else:
            getattr(fixture, args.role)()


if __name__ == "__main__":
    main()
