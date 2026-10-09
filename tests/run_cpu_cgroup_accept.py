"""Compute-only CLI scope acceptance; never prepare a parent delegation.

Default checks only denial on a private ordinary directory. Positive scheduler
join requires --positive and SCHED_TEST_CPU_SCOPE_ROOT supplied externally,
plus the explicitly built native backend. No production state or GPU is used.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

from run_exact_dependencies_accept import ExactAcceptance


class CgroupAcceptance(ExactAcceptance):
    daemon_flags = ("--fake",)

    def verify_scoped_worker(self, batch, task, scope, *, released):
        """Optional extra evidence for the independently gated device lane."""

    def save_config(self, delegated_root):
        self.cfg.update(cpu_isolation={"mode": "cgroup", "delegated_root": delegated_root},
                        cpus_total=2, max_cpu_jobs=2,
                        lease_validation={"mode": "enforce", "interval_sec": 1})
        self.config.write_text(json.dumps(self.cfg))

    def deny(self):
        parent = self.root / "not-a-cgroup"
        parent.mkdir()
        self.save_config(str(parent))
        self.cli("daemon", "drain")
        batch = self.submit("denied", [self.worker("task", "worker")])
        self.cli("daemon", "resume")
        check = self.data("daemon", "check", "--fake", "--json", expect=None)
        assert any(c["id"] == "cpu_cgroup_delegation" and c["level"] == "fail" for c in check["checks"]), check
        before = self.job(batch, "task")
        result = self.cli("daemon", "start", "--fake", expect=None)
        assert "拒绝启动" in result.stdout, result
        assert self.data("daemon", "status", "--json")["health_state"] == "stopped"
        assert self.job(batch, "task") == before
        assert self.data("cpu-isolation", "--json")["claims"] == []
        assert self.data("cpu-scopes", "--json")["scopes"] == []
        root_health = self.data("scope-health", "--json")
        assert root_health["status"] == "unknown" and root_health["recorded_origin"] is None
        assert not root_health["runtime_probed"] and not root_health["admission_granted"]
        assert self.data("allocations", f"{batch}:task", "--json")["allocations"] == []
        assert not (self.root / "worker/runs.txt").exists() and list(parent.iterdir()) == []
        print("PASS: ordinary-directory delegation rejected before daemon/allocation/worker; no affinity fallback or parent writes", flush=True)

    def scoped_worker(self, task, kind=None):
        worker = self.worker(task, task)
        program = ("import os,json,subprocess,sys,time\nfrom pathlib import Path\n"
            "Path('runs.txt').open('a').write('run\\n')\n"
            "initial=sorted(os.sched_getaffinity(0))\n"
            "os.sched_setaffinity(0," + repr(sorted(os.sched_getaffinity(0))) + ")\n"
            "expanded=sorted(os.sched_getaffinity(0))\n"
            "child=json.loads(subprocess.check_output([sys.executable,'-I','-c','import json,os; print(json.dumps(sorted(os.sched_getaffinity(0))))'],text=True))\n"
            "Path('mask.json').write_text(json.dumps([initial,expanded,child,Path('/proc/self/cgroup').read_text()]))\n"
            "end=time.monotonic()+90\n"
            "while not Path(" + repr(str(self.root / (task + "-release"))) + ").exists():\n"
            " if time.monotonic()>end: raise SystemExit(2)\n time.sleep(.05)\n")
        worker["cmd"][-1] = program
        if kind:
            worker["cwd"] = "."
            worker["cmd"][-1] = "import os\nos.chdir(" + repr(str(self.root / task)) + ")\n" + program
            executable = os.path.realpath(sys.executable)
            worker["cmd"][0] = executable
            self.cfg.setdefault("execution_backends", {})[task] = {"kind": kind, "executable": executable,
                "sha256": hashlib.sha256(Path(executable).read_bytes()).hexdigest(), "argv": worker["cmd"],
                "env": {}, "projects": ["example"], "input_slots": {}}
            worker["execution"] = {"backend": task, "inputs": {}}
        return worker

    def positive(self, delegated_root):
        from gsched.execution import LinuxFdBackend
        LinuxFdBackend()  # Explicit positive matrix must not silently drop native.
        self.save_config(delegated_root)
        tasks = [self.scoped_worker("ordinary"), self.scoped_worker("fd", "linux_fd"),
                 self.scoped_worker("owner", "linux_fd_owner")]
        self.config.write_text(json.dumps(self.cfg))
        self.cli("daemon", "drain")
        batch = self.submit("scoped", tasks, failure_policy="continue_independent")
        check = self.data("daemon", "check", *self.daemon_flags, "--json")
        assert any(c["id"] == "cpu_cgroup_delegation" and c["level"] == "ok" for c in check["checks"])
        self.cli("daemon", "resume")
        self.cli("daemon", "start", *self.daemon_flags)
        root_health = self.wait(lambda: self.data("scope-health", "--json"), lambda r: r["status"] == "ready")
        assert root_health["recorded_origin"]["data"]["facts"]["parent"]["path"] == delegated_root
        assert not root_health["admission_granted"] and not root_health["physical_boundary_verified"]
        snapshots = {}
        for task in ("ordinary", "fd", "owner"):
            output = self.root / task / "mask.json"
            mask = self.wait(lambda: json.loads(output.read_text()) if output.exists() else None, bool)
            summary = self.data("allocations", f"{batch}:{task}", "--json")["allocations"][-1]
            claim = summary["cpu_binding"]
            scope = next(row for row in self.data("cpu-scopes", "--json")["scopes"] if row["allocation_id"] == summary["allocation_id"])
            assert mask[0] == mask[1] == mask[2] == claim["cpus"] and len(mask[0]) == 1
            assert "/sched-cpu-" + scope["scope_id"] in mask[3]
            assert scope["recorded_phase"] == "launch_intent" and scope["launch_consumed"]
            assert not scope["cpu_release_recorded_ready"]
            self.verify_scoped_worker(batch, task, scope, released=False)
            if task == "ordinary":
                other_output = self.root / "fd/mask.json"
                other = self.wait(lambda: json.loads(other_output.read_text()) if other_output.exists() else None, bool)
                assert not set(mask[0]) & set(other[0])
                assert self.job(batch, "owner")["jobs"][0]["status"] == "pending"
                assert self.data("allocations", f"{batch}:owner", "--json")["allocations"] == []
                self.cli("cancel", f"{batch}:ordinary", "--yes")
                terminal = "cancelled"
            else:
                (self.root / (task + "-release")).touch()
                terminal = "done"
            self.wait(lambda: self.job(batch, task), lambda r: r["jobs"][0]["status"] == terminal)
            frozen = self.data("cpu-scopes", "--scope-id", scope["scope_id"], "--json")
            assert frozen["scopes"][0]["recorded_phase"] == "removed"
            assert not Path(delegated_root, "sched-cpu-" + scope["scope_id"]).exists()
            self.verify_scoped_worker(batch, task, frozen["scopes"][0], released=True)
            assert (self.root / task / "runs.txt").read_text() == "run\n"
            if task != "ordinary":
                attempt = self.data("execution", f"{batch}:{task}", "--json")["attempts"][0]
                assert attempt["observation"]["returncode"] == 0 and attempt["observation"]["group_clean"] is True
            snapshots[scope["scope_id"]] = frozen
        assert self.data("cpu-isolation", "--json")["claims"] == []
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        self.cli("daemon", "start", *self.daemon_flags)
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        for identifier, frozen in snapshots.items():
            assert self.data("cpu-scopes", "--scope-id", identifier, "--json") == frozen
        print("PASS: three scheduler backends join original cpusets; widening capped, descendants inherit, cancellation/wait precede original removal/release; stopped restart never replays", flush=True)


def run(positive=False):
    if sys.platform != "linux":
        raise RuntimeError("Run on an authorized Linux compute node, not a laptop/gateway.")
    root = os.environ.get("SCHED_TEST_CPU_SCOPE_ROOT")
    if positive and not root:
        raise RuntimeError("Positive acceptance requires externally delegated SCHED_TEST_CPU_SCOPE_ROOT")
    temporary = tempfile.mkdtemp(prefix="sched-cpu-cgroup-")
    acceptance = CgroupAcceptance(Path(temporary))
    try:
        acceptance.positive(root) if positive else acceptance.deny()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private daemon not stopped; retained {temporary}")
    shutil.rmtree(temporary)


if __name__ == "__main__":
    if sys.argv[1:] not in ([], ["--positive"]):
        raise SystemExit("usage: run_cpu_cgroup_accept.py [--positive]")
    run(bool(sys.argv[1:]))
