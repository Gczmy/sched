"""Compute-only CPU affinity/CLI acceptance; private state and fake GPU."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

from run_cluster_lease_accept import LeaseAcceptance


class IsolationAcceptance(LeaseAcceptance):
    def remember_daemon(self):
        if hasattr(self, "adopted"):
            self.adopted.add(self.data("daemon", "status", "--json")["pid"])

    def reap(self, pid):
        import time
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                done, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                return
            if done:
                self.adopted.discard(pid)
                return
            time.sleep(.1)
        raise RuntimeError(f"private adopted process not reaped: {pid}")

    def claims(self):
        return self.data("cpu-isolation", "--json")

    def hold(self, task, *, gpu=False, cpus=1, kind=None):
        worker = self.worker(task, task)
        worker["resources"] = {"gpu": 1 if gpu else 0, "cpus": cpus}
        if gpu:
            worker["resources"].update(vram_gib=1, gpu_share=True)
        program = ("import os,json,subprocess,sys,time\n" + worker["cmd"][-1]
            + "\nmask=sorted(os.sched_getaffinity(0))\n"
            "child=json.loads(subprocess.check_output([sys.executable,'-I','-c','import json,os; print(json.dumps(sorted(os.sched_getaffinity(0))))'],text=True))\n"
            "Path('affinity.json').write_text(json.dumps([mask,child]))\n"
            "end=time.monotonic()+150\n"
            "while not Path(" + repr(str(self.root / (task + "-release"))) + ").exists():\n"
            " if time.monotonic()>end: raise SystemExit(2)\n time.sleep(.05)\n")
        worker["cmd"][-1] = program
        if kind:
            # Configured execution must start at the registered project root.
            # The fixed worker program chooses its private output subdirectory.
            worker["cwd"] = "."
            worker["cmd"][-1] = "import os\nos.chdir(" + repr(str(self.root / task)) + ")\n" + program
            executable = os.path.realpath(sys.executable)
            worker["cmd"][0] = executable
            self.cfg.setdefault("execution_backends", {})[task] = {"kind": kind, "executable": executable,
                "sha256": hashlib.sha256(Path(executable).read_bytes()).hexdigest(), "argv": worker["cmd"],
                "env": {}, "projects": ["example"], "input_slots": {}}
            worker["execution"] = {"backend": task, "inputs": {}}
        return worker

    def mask(self, task):
        path = self.root / task / "affinity.json"
        return self.wait(lambda: json.loads(path.read_text()) if path.exists() else None, bool)

    def exact(self, batch, task):
        summary = self.data("allocations", f"{batch}:{task}", "--json")["allocations"][-1]
        return self.data("allocations", f"{batch}:{task}", "--allocation-id", summary["allocation_id"], "--json")["allocations"][0]

    def run(self):
        if len(os.sched_getaffinity(0)) < 2:
            raise RuntimeError("acceptance needs two authorized logical CPUs")
        self.cfg.update(cpu_isolation={"mode": "affinity"}, cpus_total=0, max_cpu_jobs=10,
                        lease_validation={"mode": "enforce", "interval_sec": 1})
        self.config.write_text(json.dumps(self.cfg))
        self.cli("daemon", "drain")
        self.cli("daemon", "start", "--fake")
        actual = self.wait(self.snapshot, bool)[-1]
        assert actual["origin"]["affinity"] == sorted(os.sched_getaffinity(0))
        assert not self.claims()["claims"]
        root_health = self.data("scope-health", "--json")
        assert root_health["status"] == "disabled" and root_health["recorded_origin"] is None, root_health
        assert root_health["contract"] == "sched-scope-health-state-v1"
        assert not root_health["runtime_probed"] and not root_health["physical_boundary_verified"]
        self.private_drain()
        print("PASS: actual lease/affinity recorded separately with private dispatch drained; passive root health reports disabled, never invents delegation", flush=True)

        binary = self.root / "bin"
        binary.mkdir()
        command = binary / "scontrol"
        command.write_text("#!" + sys.executable + "\nimport sys\nfrom pathlib import Path\n"
            "assert sys.argv[1:4] == ['--json','show','job']\n"
            "print(Path(" + repr(str(self.root / "controller.json")) + ").read_text())\n")
        command.chmod(0o700)
        self.fixture_job = os.environ.get("SLURM_JOB_ID", "42")
        self.env.update(PATH=str(binary) + os.pathsep + os.environ.get("PATH", ""),
            SLURM_JOB_ID=self.fixture_job, SLURM_STEP_ID="0", SLURM_CPUS_ON_NODE="2", SLURM_CPUS_PER_TASK="2")
        self.cfg["lease_validation"] = {"mode": "enforce", "unknown_policy": "allow", "interval_sec": 1}
        self.controller("RUNNING")
        tasks = [self.hold("gpu", gpu=True), self.hold("cpu"), self.hold("large", cpus=3), self.hold("next")]
        from gsched.execution import BackendUnavailable, LinuxFdBackend
        try:
            LinuxFdBackend()
        except BackendUnavailable:
            if os.environ.get("SCHED_REQUIRE_NATIVE") == "1":
                raise
            native = []
        else:
            native = [self.hold("fd", kind="linux_fd"), self.hold("owner", kind="linux_fd_owner")]
        self.config.write_text(json.dumps(self.cfg))  # Cold fixture, daemon stopped.
        batch = self.submit("affinity", tasks + native, failure_policy="continue_independent")
        check = self.data("daemon", "check", "--fake", "--json")
        assert any(c["id"] == "cpu_affinity_primitives" and c["level"] == "ok" for c in check["checks"])
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        left, right = self.mask("gpu"), self.mask("cpu")
        assert left[0] == left[1] and right[0] == right[1]
        assert len(left[0]) == len(right[0]) == 1 and not set(left[0]) & set(right[0])
        report = self.claims()
        assert len(report["claims"]) == 2 and not report["hard_isolation"] and not report["runtime_probed"]
        assert self.job(batch, "next")["jobs"][0]["status"] == "pending"
        assert self.data("allocations", f"{batch}:next", "--json")["allocations"] == []
        explanation = self.wait(lambda: self.data("admission-explain", f"{batch}:next", "--json"),
                                lambda r: not r["unknown"])
        assert not explanation["resource_fit"] and "cpu" in explanation["reasons"]
        assert explanation["cpu_isolation"]["reason"] == "cpu_pool_exhausted"
        assert self.exact(batch, "gpu")["cpu_binding"]["cpus"] == left[0]
        before = self.data("batch-policy", batch, "--json")["batch_revision"]
        assert self.claims() == report and self.data("batch-policy", batch, "--json")["batch_revision"] == before
        print("PASS: ordinary GPU/CPU workers and descendants get distinct immutable masks; exhausted pool holds pending before allocation; queries are passive", flush=True)

        (self.root / "gpu-release").touch()
        self.wait(lambda: self.job(batch, "gpu"), lambda r: r["jobs"][0]["status"] == "done")
        following = self.mask("next")
        assert following[0] == following[1] == left[0]
        assert self.job(batch, "large")["jobs"][0]["status"] == "pending"
        released = self.exact(batch, "gpu")
        assert any(e["data"].get("event") == "cpu_affinity_released" for e in released["events"])
        self.cli("cancel", f"{batch}:next", "--yes")
        self.wait(lambda: self.job(batch, "next"), lambda r: r["jobs"][0]["status"] == "cancelled")
        (self.root / "cpu-release").touch()
        self.wait(lambda: self.job(batch, "cpu"), lambda r: r["jobs"][0]["status"] == "done")
        self.cli("cancel", f"{batch}:large", "--yes")
        print("PASS: exact group cleanup releases CPU claim without changing history; oversized task cannot block a smaller one; cancellation converges", flush=True)

        for worker in native:
            task = worker["id"]
            mask = self.mask(task)
            assert mask[0] == mask[1] == self.exact(batch, task)["cpu_binding"]["cpus"]
            (self.root / (task + "-release")).touch()
            self.wait(lambda: self.job(batch, task), lambda r: r["jobs"][0]["status"] == "done")
            attempt = self.data("execution", f"{batch}:{task}", "--json")["attempts"][0]
            assert attempt["observation"]["returncode"] == 0 and attempt["observation"]["group_clean"] is True
        self.wait(self.claims, lambda r: not r["claims"])
        self.private_drain()
        print("PASS: explicit native backends keep their original child wait and CPU mask; all completed claims released", flush=True)

        if native:
            import ctypes
            import signal
            import time
            if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
                raise OSError(ctypes.get_errno(), "private child adoption unavailable")
            self.adopted = set()
            reconnect = self.hold("reconnect", cpus=2, kind="linux_fd_owner")
            self.config.write_text(json.dumps(self.cfg))  # Private daemon stopped.
            rebound = self.submit("reconnect", [reconnect, self.worker("after", "after")],
                                  failure_policy="continue_independent")
            self.cli("daemon", "resume")
            self.cli("daemon", "start", "--fake")
            self.remember_daemon()
            initial_mask = self.mask("reconnect")
            original = self.data("execution", f"{rebound}:reconnect", "--json")["attempts"][0]
            original_claim = self.claims()["claims"]
            scheduler = self.data("daemon", "status", "--json")["pid"]
            assert scheduler == original["identity"]["scheduler_pid"]
            self.adopted.add(original["owner"]["pid"])
            os.kill(scheduler, signal.SIGKILL)  # Exact private daemon only.
            self.reap(scheduler)
            self.wait(lambda: self.data("daemon", "status", "--json"),
                      lambda r: r["process_state"] == "stopped" and r["read_error"] is None
                      and (r["heartbeat_age_s"] is None or r["heartbeat_age_s"] > 61), timeout=180)
            assert self.claims()["claims"] == original_claim
            assert self.job(rebound, "after")["jobs"][0]["status"] == "pending"
            self.cli("daemon", "stop")
            self.cli("daemon", "start", "--fake")
            self.remember_daemon()
            restored = self.wait(lambda: self.data("execution", f"{rebound}:reconnect", "--json"),
                                 lambda r: r["attempts"][0]["owner_health"]["connection_status"] == "responsive")
            assert restored["attempts"][0]["attempt_id"] == original["attempt_id"]
            assert restored["attempts"][0]["owner"] == original["owner"]
            assert self.claims()["claims"] == original_claim and self.mask("reconnect") == initial_mask
            assert (self.root / "reconnect/runs.txt").read_text() == "run\n"
            (self.root / "reconnect-release").touch()
            self.wait(lambda: self.job(rebound, "reconnect"), lambda r: r["jobs"][0]["status"] == "done")
            self.wait(lambda: self.job(rebound, "after"), lambda r: r["jobs"][0]["status"] == "done")
            final = self.data("execution", f"{rebound}:reconnect", "--json")["attempts"][0]
            assert final["attempt_id"] == original["attempt_id"] and final["observation"]["returncode"] == 0
            self.wait(self.claims, lambda r: not r["claims"])
            self.private_drain()
            for pid in list(self.adopted):
                self.reap(pid)
            print("PASS: persistent owner and immutable CPU claim survive exact private daemon crash/reconnect; successor does not replay or migrate; original wait releases CPUs for queued work", flush=True)

        # Cold configuration must not implicitly enable/disable existing daemons.
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.remember_daemon()
        patch = self.root / "cold-patch.json"
        patch.write_text(json.dumps({"cpu_isolation": {"mode": "off"}}))
        self.cli("config", "set", "-f", patch, "--yes")
        cold = self.submit("cold-wait", [self.worker("cold", "cold")])
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "healthy")
        # Wait for the changed policy to be considered on a completed tick.
        import time
        time.sleep(12)
        assert self.job(cold, "cold")["jobs"][0]["status"] == "pending"
        assert not (self.root / "cold/runs.txt").exists()
        self.private_drain()
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.remember_daemon()
        self.wait(lambda: self.job(cold, "cold"), lambda r: r["jobs"][0]["status"] == "done")
        assert (self.root / "cold/runs.txt").read_text() == "run\n"
        assert "cpu_binding" not in self.exact(cold, "cold")
        self.private_drain()
        assert self.exact(batch, "gpu")["cpu_binding"] == released["cpu_binding"]
        print("PASS: cold policy change pauses new dispatch until explicit private restart; off preserves original behavior; old binding/history unchanged", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run on a Linux compute node, never the laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-cpu-isolation-")
    acceptance = IsolationAcceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        # Only bounded fixture workers, release them before CLI shutdown.
        for task in ("gpu", "cpu", "next", "large", "fd", "owner", "reconnect"):
            (acceptance.root / (task + "-release")).touch()
        acceptance.cli("daemon", "stop", timeout=150)
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"private fixture not stopped; retained {temporary}")
        for pid in list(getattr(acceptance, "adopted", ())):
            acceptance.reap(pid)
    shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
