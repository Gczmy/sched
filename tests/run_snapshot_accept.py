"""Compute-only recovery-point CLI/fence acceptance; private state, CPU only."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from run_cluster_lease_accept import LeaseAcceptance


class SnapshotAcceptance(LeaseAcceptance):
    def run(self):
        self.cli("daemon", "drain")
        instance = self.data("identity", "--json")["instance_id"]
        batch = self.submit("snapshot-pending", [self.worker("once", "once")])
        busy = self.data("snapshot", "create", "--writers-quiesced", "--yes", "--json", expect=1)
        assert "busy" in busy["error"]
        assert not self.data("snapshot", "status", "--json")["maintenance_open"]
        self.private_drain()
        assert self.job(batch, "once")["jobs"][0]["status"] == "pending"
        created = self.data("snapshot", "create", "--writers-quiesced", "--yes", "--json")
        identifier = created["snapshot_id"]
        assert created["maintenance_open"] and not created["daemon_started"]
        for command in (("daemon", "start", "--fake"), ("config", "reload"),
                        ("daemon", "foreground", "--fake", "--supervise")):
            self.cli(*command, expect=2)
        direct = subprocess.run([sys.executable, "-m", "gsched.dispatcher_main", "--daemon"],
                                env=self.env, capture_output=True, text=True, timeout=10)
        assert direct.returncode != 0 and "maintenance window" in direct.stderr, direct
        assert self.data("daemon", "status", "--json")["health_state"] == "stopped"
        assert not (self.root / "once" / "runs.txt").exists()
        print("PASS: complete CLI recovery point; independent writers/start/foreground/direct dispatcher blocked before task launch", flush=True)

        verified = self.data("snapshot", "verify", identifier, "--json")
        assert verified["instance_id"] == instance and not verified["rollback_authorized"]
        assert self.data("snapshot", "migrate", identifier, "--yes", "--json")["phase"] == "migrated"
        assert self.data("snapshot", "rollback", identifier, "--yes", "--json")["phase"] == "restored"
        assert self.data("snapshot", "rollback", identifier, "--yes", "--json")["phase"] == "restored"
        assert self.data("identity", "--json")["instance_id"] == instance
        assert self.job(batch, "once")["jobs"][0]["status"] == "pending"
        assert not (self.root / "once" / "runs.txt").exists()
        self.data("snapshot", "close", identifier, "--yes", "--json")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "once"), lambda r: r["jobs"][0]["status"] == "done")
        self.private_drain()
        assert (self.root / "once" / "runs.txt").read_text() == "run\n"
        self.data("snapshot", "rollback", identifier, "--yes", "--json", expect=1)
        assert self.data("snapshot", "verify", identifier, "--json")["verified"]
        assert len(self.job(batch, "once")["jobs"]) == 1
        print("PASS: same-instance pending preserved across migration/rollback; close permanently consumes authority; explicit resume/start executes once", flush=True)

        later = self.data("snapshot", "create", "--writers-quiesced", "--yes", "--json")["snapshot_id"]
        self.data("snapshot", "rollback", identifier, "--yes", "--json", expect=1)
        assert self.data("snapshot", "status", "--json")["snapshot_id"] == later
        self.data("snapshot", "close", later, "--yes", "--json")
        print("PASS: a later maintenance window cannot revive a historical recovery point; private daemon remains stopped", flush=True)


def main():
    if sys.platform != "linux" or (os.environ.get("GITHUB_ACTIONS") != "true" and not os.environ.get("SLURM_JOB_ID")):
        raise SystemExit("Run on a Linux compute-node lease or Linux CI, not a laptop/gateway.")
    root = Path(tempfile.mkdtemp(prefix="sched-snapshot-"))
    acceptance = SnapshotAcceptance(root)
    try:
        acceptance.run()
    finally:
        # A failed/crashed rollback must remain fenced for inspection. Never
        # bypass a window or erase its original images in a cleanup handler.
        current = acceptance.data("snapshot", "status", "--json")
        if current["maintenance_open"]:
            raise RuntimeError(f"Private recovery window retained for inspection: {root}")
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private daemon not stopped; retained {root}")
    # Recovery-point/state cleanup has no CLI yet. Retain its evidence rather
    # than deleting scheduler-managed images directly through the test harness.
    print(f"RETAINED: stopped private recovery evidence {root}", flush=True)


if __name__ == "__main__":
    main()
