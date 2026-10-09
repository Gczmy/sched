"""Joint real launch provenance and CPU affinity in an existing compute lease.

Only the harness/private daemon affinity is narrowed; the real Slurm anchor,
allocation, production state and parent cgroups are never modified.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import sys
import tempfile

from run_cpu_isolation_accept import IsolationAcceptance
from run_launch_ancestry_accept import AncestryAcceptance


class LaunchAffinityAcceptance(IsolationAcceptance):
    controller = AncestryAcceptance.controller

    def lease(self):
        return self.wait(lambda: next((r for r in self.snapshot() if r["current_owner_binding"]), None),
                         lambda r: r is not None and r["recorded_check"]["data"]["allocation_state"] == "valid")

    def configure(self):
        self.cfg.update(cpus_total=120, max_cpu_jobs=4, cpu_isolation={"mode": "affinity"},
                        device_isolation={"mode": "off"},
                        lease_validation={"membership": "launch_ancestry", "mode": "enforce",
                                          "unknown_policy": "pause", "interval_sec": 1})
        self.config.write_text(json.dumps(self.cfg))
        self.cli("daemon", "drain")
        self.cli("daemon", "start", "--fake")
        actual = self.lease()
        check = actual["recorded_check"]["data"]
        assert actual["origin"]["launch_ancestry"]["known"]
        assert check["slurm_observation"]["launch_tracking"]["present"]
        assert check["protection_level"] == "launch_ancestry_affinity"
        assert check["hard_isolation"] is False and check["job_cgroup_verified"] is False
        assert check["current_daemon_slurm_membership_verified"] is False
        capacity = self.data("cpu-capacity", "--json")
        assert capacity["mode"] == "fixed" and capacity["effective_total"] == 120
        assert capacity["observed_upper_bound"] == 2 and not capacity["hard_isolation"]
        self.original_job = copy.deepcopy(check["slurm_observation"]["job"])
        self.private_drain()
        print("PASS: real original job/step and launch anchor verified with explicit compatibility, fixed 120 budget and two-CPU private affinity pool", flush=True)

    def lifecycle(self):
        self.cli("daemon", "drain")
        tasks = [self.hold("left"), self.hold("right"), self.hold("large", cpus=3), self.hold("following")]
        batch = self.submit("joint-affinity", tasks, failure_policy="continue_independent")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        original = self.lease()
        left, right = self.mask("left"), self.mask("right")
        assert left[0] == left[1] and right[0] == right[1]
        assert len(left[0]) == len(right[0]) == 1 and not set(left[0]) & set(right[0])
        assert sorted(left[0] + right[0]) == sorted(os.sched_getaffinity(0))
        claims = self.claims()
        assert len(claims["claims"]) == 2 and not claims["hard_isolation"]
        for task in ("large", "following"):
            assert self.job(batch, task)["jobs"][0]["status"] == "pending"
            assert self.data("allocations", f"{batch}:{task}", "--json")["allocations"] == []
        explanation = self.wait(lambda: self.data("admission-explain", f"{batch}:following", "--json"),
                                lambda r: not r["unknown"])
        assert explanation["cpu_isolation"]["reason"] == "cpu_pool_exhausted"
        for task, mask in (("left", left), ("right", right)):
            allocation = self.exact(batch, task)
            assert allocation["cpu_binding"]["cpus"] == mask[0]
            assert allocation["cpu_binding"]["lease_id"] == original["origin"]["lease_id"]
        (self.root / "left-release").touch()
        self.wait(lambda: self.job(batch, "left"), lambda r: r["jobs"][0]["status"] == "done")
        assert self.mask("following")[0] == left[0]
        self.cli("cancel", f"{batch}:following", "--yes")
        self.wait(lambda: self.job(batch, "following"), lambda r: r["jobs"][0]["status"] == "cancelled")
        self.cli("cancel", f"{batch}:large", "--yes")
        (self.root / "right-release").touch()
        self.wait(lambda: self.job(batch, "right"), lambda r: r["jobs"][0]["status"] == "done")
        self.wait(self.claims, lambda r: not r["claims"])
        self.private_drain()
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.lease()
        for task in ("left", "right", "following"):
            assert (self.root / task / "runs.txt").read_text() == "run\n"
        self.private_drain()
        print("PASS: joint original masks/descendants, disjoint claims, finite pool, backfill, cancellation/release and restart without duplicate execution", flush=True)

    def native_waits(self):
        from gsched.execution import BackendUnavailable, LinuxFdBackend
        try:
            LinuxFdBackend()
        except BackendUnavailable:
            raise RuntimeError("Joint native acceptance requires a build matching the test interpreter")
        tasks = [self.hold("joint-fd", kind="linux_fd"), self.hold("joint-owner", kind="linux_fd_owner")]
        self.config.write_text(json.dumps(self.cfg))
        self.cli("daemon", "drain")
        batch = self.submit("joint-native", tasks, failure_policy="continue_independent")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.lease()
        for task in tasks:
            name = task["id"]
            mask = self.mask(name)
            assert mask[0] == mask[1] == self.exact(batch, name)["cpu_binding"]["cpus"]
            (self.root / (name + "-release")).touch()
            self.wait(lambda: self.job(batch, name), lambda r: r["jobs"][0]["status"] == "done")
            attempt = self.data("execution", f"{batch}:{name}", "--json")["attempts"][0]
            assert attempt["observation"]["returncode"] == 0 and attempt["observation"]["group_clean"]
            assert (self.root / name / "runs.txt").read_text() == "run\n"
        self.wait(self.claims, lambda r: not r["claims"])
        self.private_drain()
        print("PASS: both native backends preserve original lease/CPU binding, actual child masks and original wait/cleanup", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-native", action="store_true", help="also verify both explicitly built native backends")
    args = parser.parse_args()
    if sys.platform != "linux" or not os.environ.get("SLURM_JOB_ID") or not os.environ.get("SLURM_STEP_ID"):
        raise SystemExit("Run in the user-designated existing Linux Slurm compute lease, not gateway/standalone")
    original_mask = os.sched_getaffinity(0)
    if len(original_mask) < 2:
        raise SystemExit("Two existing authorized logical CPUs are required")
    root = Path(tempfile.mkdtemp(prefix="sched-launch-affinity-"))
    acceptance = None
    succeeded = False
    try:
        os.sched_setaffinity(0, sorted(original_mask)[:2])
        acceptance = LaunchAffinityAcceptance(root)
        acceptance.configure()
        acceptance.lifecycle()
        if args.require_native:
            acceptance.native_waits()
        succeeded = True
    finally:
        if acceptance is not None:
            acceptance.cli("daemon", "stop")
            if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
                raise RuntimeError("Private daemon not stopped; fixture retained at " + str(root))
        os.sched_setaffinity(0, original_mask)
        print(("PASSED" if succeeded else "FAILED") + ": private state/logs retained at " + str(root), flush=True)


if __name__ == "__main__":
    main()
