"""Joint real launch provenance and CPU affinity in an existing compute lease.

Only the harness/private daemon affinity is narrowed; the real Slurm anchor,
allocation, production state and parent cgroups are never modified.
"""
import argparse
import copy
import ctypes
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import time

from run_cpu_isolation_accept import IsolationAcceptance
from run_launch_ancestry_accept import AncestryAcceptance


class LaunchAffinityAcceptance(IsolationAcceptance):
    def controller(self, state_name="RUNNING", *, unavailable=False, timeout=False, identity_drift=False):
        AncestryAcceptance.controller(self, state_name, unavailable=unavailable)
        path = self.root / "controller.json"
        value = json.loads(path.read_text())
        value["timeout"] = timeout
        if identity_drift:
            value["response"]["jobs"][0]["start_time"] += 1
        temporary = self.root / "controller-next.json"
        temporary.write_text(json.dumps(value))
        temporary.replace(path)

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

    def install_controller_fixture(self):
        real = shutil.which("scontrol")
        assert real
        binary = self.root / "bin"
        binary.mkdir()
        command = binary / "scontrol"
        command.write_text("#!" + sys.executable + "\nimport json,os,sys,time\nfrom pathlib import Path\n"
            "if sys.argv[1:4] == ['--json','show','job']:\n"
            " data=json.loads(Path(" + repr(str(self.root / "controller.json")) + ").read_text())\n"
            " if data['unavailable']: raise SystemExit(1)\n"
            " if data['timeout']: time.sleep(4)\n"
            " print(json.dumps(data['response']))\n"
            "else:\n"
            " assert sys.argv[1] in ('listpids','show')\n"
            " os.execv(" + repr(real) + ",[" + repr(real) + ",*sys.argv[1:]])\n")
        command.chmod(0o700)
        self.env["PATH"] = str(binary) + os.pathsep + self.env.get("PATH", "")
        self.controller()

    def pending(self, name):
        batch = self.submit(name, [self.worker(name, name)])
        return batch

    def unstarted(self, batch, name):
        assert self.job(batch, name)["jobs"][0]["status"] == "pending"
        assert self.data("allocations", f"{batch}:{name}", "--json")["allocations"] == []
        assert not (self.root / name / "runs.txt").exists()

    def once(self, batch, name):
        self.wait(lambda: self.job(batch, name), lambda r: r["jobs"][0]["status"] == "done")
        assert (self.root / name / "runs.txt").read_text() == "run\n"

    def faults(self):
        self.install_controller_fixture()
        self.cli("daemon", "drain")
        held = self.submit("unknown-held", [self.hold("unknown-held")])
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        original = self.lease()
        lease_id = original["origin"]["lease_id"]
        birth = copy.deepcopy(original["origin"])
        mask = self.mask("unknown-held")
        claims = self.claims()["claims"]
        for kind in ("unavailable", "timeout"):
            self.controller(**{kind: True})
            self.wait(lambda: self.snapshot(lease_id)[0],
                      lambda r: r["recorded_check"]["data"]["allocation_state"] == "unknown")
            name = "paused-" + kind
            pending = self.pending(name)
            self.unstarted(pending, name)
            assert self.job(held, "unknown-held")["jobs"][0]["status"] == "running"
            assert self.claims()["claims"] == claims and self.mask("unknown-held") == mask
            self.controller()
            self.lease()
            self.once(pending, name)
            assert self.snapshot(lease_id)[0]["origin"] == birth
        print("PASS: controller failure and actual helper timeout pause eligible new CPU work, preserve running masks/claims, and recover only the original birth", flush=True)

        self.cli("daemon", "drain")
        stale = self.pending("paused-stale")
        scheduler = self.data("daemon", "status", "--json")["pid"]
        os.kill(scheduler, signal.SIGSTOP)  # Exact private daemon only; worker remains alive.
        try:
            from gsched.cluster_lease import MAX_AGE
            time.sleep(MAX_AGE + 2)
            observed = self.snapshot(lease_id)[0]
            assert observed["allocation_state"] == "unknown"
            assert observed["recorded_check"]["data"]["allocation_state"] == "valid"
            explanation = self.data("admission-explain", f"{stale}:paused-stale", "--json")
            assert explanation["unknown"]
            self.unstarted(stale, "paused-stale")
            self.controller(unavailable=True)
        finally:
            os.kill(scheduler, signal.SIGCONT)
        self.cli("daemon", "resume")
        self.wait(lambda: self.snapshot(lease_id)[0],
                  lambda r: r["recorded_check"]["data"]["allocation_state"] == "unknown")
        self.unstarted(stale, "paused-stale")
        self.controller()
        self.lease()
        self.once(stale, "paused-stale")
        (self.root / "unknown-held-release").touch()
        self.once(held, "unknown-held")
        self.private_drain()
        print("PASS: actual private daemon suspension expires passive lease/admission evidence; stale valid cannot authorize launch; original running worker settles once", flush=True)

        for kind in ("cancelled", "identity", "mask"):
            name = "held-" + kind
            self.controller()
            self.cli("daemon", "drain")
            held = self.submit(name, [self.hold(name)])
            self.cli("daemon", "resume")
            self.cli("daemon", "start", "--fake")
            original = self.lease()
            identifier = original["origin"]["lease_id"]
            mask = self.mask(name)
            claims = self.claims()["claims"]
            scheduler = self.data("daemon", "status", "--json")["pid"]
            if kind == "cancelled":
                self.controller("CANCELLED")
            elif kind == "identity":
                self.controller(identity_drift=True)
            else:
                os.sched_setaffinity(scheduler, mask[0])
            self.wait(lambda: self.snapshot(identifier)[0], lambda r: r["recorded_check"]["data"]["invalid_latched"])
            queued_name = "after-" + kind
            queued = self.pending(queued_name)
            self.unstarted(queued, queued_name)
            assert self.claims()["claims"] == claims and self.mask(name) == mask
            self.controller()
            if kind == "mask":
                os.sched_setaffinity(scheduler, os.sched_getaffinity(0))
            sequence = self.snapshot(identifier)[0]["recorded_check"]["seq"]
            retained = self.wait(lambda: self.snapshot(identifier)[0], lambda r: r["recorded_check"]["seq"] > sequence)
            assert retained["recorded_check"]["data"]["invalid_latched"]
            assert retained["origin"] == original["origin"]
            (self.root / (name + "-release")).touch()
            self.once(held, name)
            self.unstarted(queued, queued_name)
            self.private_drain()
            self.cli("daemon", "resume")
            self.cli("daemon", "start", "--fake")
            self.lease()
            self.once(queued, queued_name)
            self.wait(self.claims, lambda r: not r["claims"])
            self.private_drain()
        print("PASS: fixture lease end/identity drift and actual private-daemon mask drift latch invalid; restored answers/mask do not migrate, cancel or replay; explicit private restart alone dispatches once", flush=True)

    def owner_reconnect(self):
        if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "private child adoption unavailable")
        self.adopted = set()
        self.controller()
        task = self.hold("joint-reconnect", cpus=2, kind="linux_fd_owner")
        self.config.write_text(json.dumps(self.cfg))
        self.cli("daemon", "drain")
        batch = self.submit("joint-reconnect", [task, self.worker("joint-after", "joint-after")],
                            failure_policy="continue_independent")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.lease()
        self.remember_daemon()
        mask = self.mask("joint-reconnect")
        original = self.data("execution", f"{batch}:joint-reconnect", "--json")["attempts"][0]
        claims = self.claims()["claims"]
        scheduler = self.data("daemon", "status", "--json")["pid"]
        assert scheduler == original["identity"]["scheduler_pid"]
        self.adopted.add(original["owner"]["pid"])
        os.kill(scheduler, signal.SIGKILL)
        self.reap(scheduler)
        self.wait(lambda: self.data("daemon", "status", "--json"),
                  lambda r: r["process_state"] == "stopped" and r["read_error"] is None
                  and (r["heartbeat_age_s"] is None or r["heartbeat_age_s"] > 61), timeout=180)
        assert self.claims()["claims"] == claims
        self.unstarted(batch, "joint-after")
        self.cli("daemon", "stop")
        self.cli("daemon", "start", "--fake")
        self.lease()
        self.remember_daemon()
        restored = self.wait(lambda: self.data("execution", f"{batch}:joint-reconnect", "--json"),
                             lambda r: r["attempts"][0]["owner_health"]["connection_status"] == "responsive")
        assert restored["attempts"][0]["attempt_id"] == original["attempt_id"]
        assert restored["attempts"][0]["owner"] == original["owner"]
        assert self.claims()["claims"] == claims and self.mask("joint-reconnect") == mask
        assert (self.root / "joint-reconnect/runs.txt").read_text() == "run\n"
        (self.root / "joint-reconnect-release").touch()
        self.once(batch, "joint-reconnect")
        self.once(batch, "joint-after")
        final = self.data("execution", f"{batch}:joint-reconnect", "--json")["attempts"][0]
        assert final["attempt_id"] == original["attempt_id"]
        assert final["observation"]["returncode"] == 0 and final["observation"]["group_clean"]
        self.wait(self.claims, lambda r: not r["claims"])
        self.private_drain()
        for pid in list(self.adopted):
            self.reap(pid)
        print("PASS: actual private daemon SIGKILL reconnects the original owner/attempt/child/mask/claim under compatible lease checks; original wait settles once, without new start", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-native", action="store_true", help="also verify both explicitly built native backends")
    parser.add_argument("--faults", action="store_true", help="private controller/timeout/staleness/identity/mask faults; no real lease change")
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
        if args.faults:
            acceptance.faults()
            if args.require_native:
                acceptance.owner_reconnect()
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
