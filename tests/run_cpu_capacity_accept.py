"""Linux CPU/fake-GPU CLI acceptance in private state, never a real Slurm mutation."""
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

from run_cluster_lease_accept import LeaseAcceptance


class CpuAcceptance(LeaseAcceptance):
    def capacity(self):
        return self.data("cpu-capacity", "--json")

    def patch(self, **values):
        patch = self.root / "cpu-patch.json"
        patch.write_text(json.dumps(values))  # Outside scheduler state.
        self.cli("config", "set", "-f", patch, "--yes")
        self.cfg.update(values)

    def hold(self, task, cpus, *, gpu=False):
        worker = self.worker(task, task)
        worker["resources"] = {"gpu": 1 if gpu else 0, "cpus": cpus}
        if gpu:
            worker["resources"].update(vram_gib=1, gpu_share=True)
        worker["cmd"][-1] = ("import time\n" + worker["cmd"][-1] + "\nend=time.monotonic()+100\n"
            "while not Path(" + repr(str(self.root / (task + "-release"))) + ").exists():\n"
            " if time.monotonic()>end: raise SystemExit(2)\n time.sleep(.05)\n")
        return worker

    def run(self):
        # Observe actual enclosing lease first. This does not certify cgroup
        # membership or change its Slurm allocation.
        self.cfg.update(cpus_total="auto", cpus_auto_max=2,
                        lease_validation={"mode": "enforce", "interval_sec": 1})
        self.config.write_text(json.dumps(self.cfg))
        self.cli("daemon", "drain")
        actual = self.submit("actual-auto", [self.worker("held", "actual-auto")])
        self.cli("daemon", "start", "--fake")
        report = self.wait(self.capacity, lambda r: r["observation"]["error"] is None)
        assert report["configured"] == "auto" and report["effective_total"] <= 2
        assert report["sources"][0]["cpus"] == len(os.sched_getaffinity(0))
        self.cli("daemon", "resume")
        if not report["available"]:
            assert self.job(actual, "held")["jobs"][0]["status"] == "pending"
            assert self.data("allocations", f"{actual}:held", "--json")["allocations"] == []
            assert "cpu" not in self.data("status", "--json")
            self.cli("cancel", f"{actual}:held", "--yes")
        else:
            self.wait(lambda: self.job(actual, "held"), lambda r: r["jobs"][0]["status"] == "done")
        print("PASS: actual auto origin/affinity observed; uncertainty pauses rather than using zero/unlimited", flush=True)
        self.private_drain()

        binary = self.root / "bin"
        binary.mkdir()
        command = binary / "scontrol"
        command.write_text("#!" + sys.executable + "\nimport sys\nfrom pathlib import Path\n"
            "assert sys.argv[1:4] == ['--json','show','job']\n"
            "print(Path(" + repr(str(self.root / "controller.json")) + ").read_text())\n")
        command.chmod(0o700)
        self.fixture_job = os.environ.get("SLURM_JOB_ID", "42")
        self.env.update(PATH=str(binary) + os.pathsep + os.environ.get("PATH", ""),
                        SLURM_JOB_ID=self.fixture_job, SLURM_STEP_ID="0",
                        SLURM_CPUS_ON_NODE="4", SLURM_CPUS_PER_TASK="2")
        self.cfg.update(lease_validation={"mode": "enforce", "unknown_policy": "allow", "interval_sec": 1})
        self.config.write_text(json.dumps(self.cfg))
        self.controller("RUNNING")
        self.cli("daemon", "drain")
        first = self.hold("first", 2, gpu=True)
        second = self.worker("second", "second")
        big = self.worker("big", "big")
        big["resources"]["cpus"] = 3
        batch = self.submit("auto-budget", [first, second, big], failure_policy="continue_independent")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "first"), lambda r: r["jobs"][0]["status"] == "running" and (self.root / "first/runs.txt").exists())
        report = self.capacity()
        assert report["available"] and report["used"] == report["effective_total"] == 2
        assert "cpu_capacity_sources_disagree" in report["warnings"]
        assert self.data("status", "--json")["cpu"] == {"used": 2, "total": 2}
        assert self.data("status", "--json", "--include-cpu-capacity")["cpu_capacity"]["configured"] == "auto"
        assert self.job(batch, "second")["jobs"][0]["status"] == "pending"
        detail = self.data("allocations", f"{batch}:first", "--json")["allocations"][0]
        detail = self.data("allocations", f"{batch}:first", "--allocation-id", detail["allocation_id"], "--json")["allocations"][0]
        assert detail["cpu_capacity"]["effective_total"] == detail["cpu_reservation"] == 2
        self.patch(cpus_auto_max=1, gpu_job_cpus=1)
        report = self.wait(self.capacity, lambda r: r["observation"]["error"] is None and r["effective_total"] == 1)
        assert report["used"] == 2 and self.job(batch, "first")["jobs"][0]["status"] == "running"
        assert self.job(batch, "second")["jobs"][0]["status"] == "pending"
        (self.root / "first-release").touch()
        self.wait(lambda: self.job(batch, "second"), lambda r: r["jobs"][0]["status"] == "done")
        assert self.job(batch, "big")["jobs"][0]["status"] == "pending"
        assert self.data("allocations", f"{batch}:big", "--json")["allocations"] == []
        print("PASS: min sources/admin cap; GPU and CPU share frozen CPU reservations; hot shrink keeps running child and holds oversized pending before allocation", flush=True)

        # Unknown controller must not use max_cpu_jobs, even under allow.
        (self.root / "controller.json").write_text(json.dumps({"errors": ["fixture unavailable"], "jobs": []}))
        report = self.wait(self.capacity, lambda r: r["observation"]["error"] is None and not r["available"])
        assert "original_slurm_capacity_unverified" in report["unknown"]
        assert "cpu" not in self.data("status", "--json")
        self.controller("RUNNING")
        self.wait(self.capacity, lambda r: r["observation"]["error"] is None and r["available"])
        self.controller("CANCELLED")
        self.wait(self.capacity, lambda r: r["invalid_latched"] is True)
        self.controller("RUNNING")
        # Bound until observation after the replacement controller input.
        before = self.capacity()["observation"]["captured_at"]
        report = self.wait(self.capacity, lambda r: r["observation"]["captured_at"] > before)
        assert not report["available"] and report["invalid_latched"]
        assert self.job(batch, "big")["jobs"][0]["status"] == "pending"
        self.cli("cancel", f"{batch}:big", "--yes")
        self.private_drain()
        print("PASS: controller uncertainty blocks auto; confirmed cancellation is sticky despite later RUNNING, no fallback or automatic rebind", flush=True)

        # Explicit restart and legacy zero still retain CPU-only concurrency.
        self.patch(cpus_total=0, cpus_auto_max=None, max_cpu_jobs=1)
        zero = self.submit("zero-compatible", [self.hold("zero-first", 1000), self.worker("zero-second", "zero-second")])
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(zero, "zero-first"), lambda r: r["jobs"][0]["status"] == "running")
        assert self.data("status", "--json")["cpu"] == {"used": 1000, "total": 0}
        assert self.job(zero, "zero-second")["jobs"][0]["status"] == "pending"
        (self.root / "zero-first-release").touch()
        self.wait(lambda: self.job(zero, "zero-second"), lambda r: r["jobs"][0]["status"] == "done")
        self.private_drain()
        for task in ("first", "second", "zero-first", "zero-second"):
            assert (self.root / task / "runs.txt").read_text() == "run\n"
        print("PASS: explicit restart only; zero stays unlimited declarations plus max_cpu_jobs, exactly one CPU execution each; no real GPU or Slurm mutation", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run on a Linux compute node, not a laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-cpu-capacity-")
    acceptance = CpuAcceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private daemon not stopped; retained {temporary}")
    shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
