"""Isolated Linux CPU/CLI lease acceptance. Never cancels a real Slurm job.

Actual read-only Slurm provenance is observed separately. A fixture scontrol
then changes answers, not allocations/cgroups; unknown_policy=allow is explicit
only in this private CPU fixture so it can exercise invalidation while running.
"""
import json
import os
from pathlib import Path
import shutil
import socket
import sys
import tempfile
import time

from run_exact_dependencies_accept import ExactAcceptance


class LeaseAcceptance(ExactAcceptance):
    def snapshot(self, lease_id=None):
        args = ["daemon-lease", "--json"]
        if lease_id:
            args += ["--lease-id", lease_id]
        return self.data(*args)["leases"]

    def controller(self, state_name, *, start=1):
        raw = {"errors": [], "warnings": [], "jobs": [{"job_id": int(self.fixture_job),
               "job_state": [state_name], "user_id": os.getuid(), "nodes": socket.gethostname(),
               "start_time": start, "end_time": int(time.time()) + 3600, "cpus": 2, "restart_cnt": 0}]}
        temporary = self.root / "controller-next.json"
        temporary.write_text(json.dumps(raw))
        temporary.replace(self.root / "controller.json")  # Fixture input, outside scheduler state.

    def private_drain(self):
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")

    def run(self):
        # Observe the real shell/Slurm context without claiming membership from
        # environment alone. No real query is performed on the gateway.
        self.cfg["lease_validation"] = {"mode": "enforce", "interval_sec": 1}
        self.config.write_text(json.dumps(self.cfg))
        self.cli("daemon", "drain")
        actual_batch = self.submit("actual-lease-observation", [self.worker("held", "actual-held")])
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        actual = self.wait(self.snapshot, lambda r: bool(r))[-1]
        actual_id = actual["origin"]["lease_id"]
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "healthy")
        assert actual["origin"]["physical_host"] == socket.gethostname()
        assert actual["origin"]["uid"] == os.getuid()
        assert actual["origin"]["affinity"] == sorted(os.sched_getaffinity(0))
        assert set(actual["origin"]["slurm_environment"]) <= {
            "SLURM_JOB_ID", "SLURM_STEP_ID", "SLURM_CPUS_ON_NODE", "SLURM_CPUS_PER_TASK", "SLURM_NTASKS", "SLURM_CLUSTER_NAME"}
        print("OBSERVED: real shell provenance recorded; allocation_state=" + actual["recorded_check"]["data"]["allocation_state"], flush=True)
        if actual["recorded_check"]["data"]["allocation_state"] == "unknown":
            assert self.job(actual_batch, "held")["jobs"][0]["status"] == "pending"
            assert not (self.root / "actual-held/runs.txt").exists()
            assert self.data("allocations", f"{actual_batch}:held", "--json")["allocations"] == []
            self.cli("cancel", f"{actual_batch}:held", "--yes")
            print("PASS: real unknown provenance with default pause blocks CPU launch and allocation before any fixture worker runs", flush=True)
        else:
            self.wait(lambda: self.job(actual_batch, "held"), lambda r: r["jobs"][0]["status"] in ("done", "cancelled"))
        self.private_drain()
        actual_after = self.snapshot(actual_id)[0]
        assert actual_after["origin"] == actual["origin"] and actual_after["recorded_exit"]
        assert actual_after["allocation_state"] == "unknown"
        print("PASS: normal private daemon exit retains real origin and exit context without inventing worker wait", flush=True)

        binary = self.root / "bin"
        binary.mkdir()
        command = binary / "scontrol"
        command.write_text("#!" + sys.executable + "\nimport sys\nfrom pathlib import Path\n"
                           "assert sys.argv[1:4] == ['--json','show','job']\n"
                           "print(Path(" + repr(str(self.root / "controller.json")) + ").read_text())\n")
        command.chmod(0o700)
        self.fixture_job = os.environ.get("SLURM_JOB_ID", "42")
        self.env.update(PATH=str(binary) + os.pathsep + os.environ.get("PATH", ""),
                        SLURM_JOB_ID=self.fixture_job, SLURM_STEP_ID="0")
        self.cfg.update(cpus_total=2, max_cpu_jobs=1, lease_validation={"mode": "enforce", "unknown_policy": "allow", "interval_sec": 1},
                        notify={"on": ["lease_invalid"], "file": {"enabled": True}})
        self.config.write_text(json.dumps(self.cfg))  # Cold fixture config with its daemon stopped.
        self.controller("RUNNING")
        self.cli("daemon", "resume")
        self.cli("daemon", "drain")
        first = self.worker("first", "first")
        first["resources"]["cpus"] = 2
        program = first["cmd"][-1]
        first["cmd"][-1] = ("import time\n" + program + "\nend=time.monotonic()+80\n"
                              "while not Path(" + repr(str(self.root / "release")) + ").exists():\n"
                              " if time.monotonic()>end: raise SystemExit(2)\n time.sleep(.05)\n")
        batch = self.submit("lease-fixture", [first, self.worker("second", "second")], failure_policy="continue_independent")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "first"), lambda r: r["jobs"][0]["status"] == "running" and (self.root / "first/runs.txt").exists())
        candidate = next(item for item in self.snapshot() if item["origin"]["lease_id"] != actual_id)
        lease_id = candidate["origin"]["lease_id"]
        bound = candidate["origin"]["initial_slurm_binding"]
        allocation = self.data("allocations", f"{batch}:first", "--json")["allocations"][0]
        exact = self.data("allocations", f"{batch}:first", "--allocation-id", allocation["allocation_id"], "--json")["allocations"][0]
        assert exact["lease_identity"]["lease_id"] == lease_id
        assert "lease" not in self.data("daemon", "status", "--json")
        assert self.data("daemon", "status", "--json", "--include-lease")["lease"]["leases"][0]["origin"]["lease_id"] == lease_id
        self.controller("CANCELLED")
        invalid = self.wait(lambda: self.snapshot(lease_id)[0], lambda r: r["recorded_check"]["data"]["invalid_latched"])
        assert not invalid["recorded_check"]["data"]["dispatch_allowed"]
        assert self.job(batch, "first")["jobs"][0]["status"] == "running"
        assert self.job(batch, "second")["jobs"][0]["status"] == "pending"
        events = self.wait(lambda: self.data("notify-inbox", "--json"), lambda r: bool(r))
        assert len(events) == 1 and events[0]["event"] == "lease_invalid"
        self.cli("notify-ack", events[0]["_file"])
        print("PASS: fixture controller cancellation pauses dispatch but keeps the running CPU child; allocation binds exact daemon lease; one CLI-acked notification", flush=True)

        self.controller("RUNNING", start=int(time.time()) + 1)  # Same ID reused/restarted; not a real new lease.
        (self.root / "release").touch()
        self.wait(lambda: self.job(batch, "first"), lambda r: r["jobs"][0]["status"] == "done")
        retained = self.snapshot(lease_id)[0]
        assert retained["origin"]["initial_slurm_binding"] == bound
        assert retained["recorded_check"]["data"]["invalid_latched"]
        assert self.job(batch, "second")["jobs"][0]["status"] == "pending"
        assert not (self.root / "second/runs.txt").exists()
        assert self.data("allocations", f"{batch}:second", "--json")["allocations"] == []
        assert self.data("notify-inbox", "--json") == []
        assert (self.root / "first/runs.txt").read_text() == "run\n"
        print("PASS: later RUNNING/reused job cannot rebind or resume old daemon; original child settles normally once; pending has no launch allocation", flush=True)
        self.private_drain()
        assert self.snapshot(lease_id)[0]["recorded_exit"]["data"]["invalid_latched"]
        self.controller("RUNNING")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "second"), lambda r: r["jobs"][0]["status"] == "done")
        assert (self.root / "second/runs.txt").read_text() == "run\n"
        self.private_drain()
        assert len(self.snapshot()) == 3
        assert self.snapshot(lease_id)[0]["origin"] == candidate["origin"]
        print("PASS: only explicit private daemon restart creates a new origin and permits queued work; old provenance remains immutable; no Slurm mutation or real GPU", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run on a Linux compute node, not a laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-lease-")
    acceptance = LeaseAcceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private acceptance daemon not stopped; retained {temporary}")
    shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
