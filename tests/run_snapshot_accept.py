"""Compute-only recovery-point CLI/fence acceptance; private state, CPU only."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

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
        # Synthetic legacy directory within this script's independent state.
        legacy = Path(self.cfg["state_dir"]) / self.cfg["node"] / "legacy-permissions"
        legacy.mkdir(mode=0o755)
        legacy.chmod(0o755)
        (legacy / "original.log").write_text("unknown wait is retained\n")
        preview = self.data("snapshot", "permissions", "--dry-run", "--json")
        assert preview["repair_count"] == 1 and not preview["permissions_changed"]
        assert legacy.stat().st_mode & 0o777 == 0o755
        repaired = self.data("snapshot", "permissions", "--writers-quiesced", "--expect-plan",
                             preview["plan_sha256"], "--yes", "--json")
        assert repaired["phase"] == "completed" and legacy.stat().st_mode & 0o777 == 0o700
        assert (legacy / "original.log").read_text() == "unknown wait is retained\n"
        print("PASS: independent CLI permission preview/CAS repair preserves pending version and original log", flush=True)
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

        first = self.data("snapshot", "list", "--limit", "1", "--json")
        assert first["contract"] == "sched-upgrade-snapshot-management/v1" and first["effect"] == "none"
        assert first["truncated"] and first["next_cursor"]
        second = self.data("snapshot", "list", "--limit", "1", "--cursor", first["next_cursor"], "--json")
        assert not second["truncated"]
        assert {first["snapshots"][0]["snapshot_id"], second["snapshots"][0]["snapshot_id"]} == {identifier, later}
        retained = self.data("snapshot", "prune", identifier, "--dry-run", "--json")
        assert not retained["eligible"] and "retention_period_not_elapsed" in retained["reasons"]
        time.sleep(1.1)  # Move beyond the recorded fractional close epoch without changing a clock.
        preview = self.data("snapshot", "prune", identifier, "--retention-days", "0", "--keep-last", "0",
                            "--dry-run", "--json")
        assert preview["eligible"] and preview["effect"] == "none"
        command = ("snapshot", "prune", identifier, "--retention-days", "0", "--keep-last", "0",
                   "--as-of", preview["as_of"], "--expect-plan", preview["plan_sha256"], "--json")
        self.cli(*command, expect=1)  # Missing --yes never deletes.
        result = self.data(*command, "--yes")
        assert result["phase"] == "pruned" and result["audit_retained"] and not result["current_state_modified"]
        assert self.data(*command, "--yes") == result
        rows = self.data("snapshot", "list", "--json")["snapshots"]
        assert next(r for r in rows if r["snapshot_id"] == identifier)["phase"] == "pruned"
        assert next(r for r in rows if r["snapshot_id"] == later)["phase"] == "closed"
        self.data("snapshot", "verify", identifier, "--json", expect=1)
        self.data("snapshot", "rollback", identifier, "--yes", "--json", expect=1)
        assert self.data("identity", "--json")["instance_id"] == instance
        assert self.job(batch, "once")["jobs"][0]["status"] == "done"
        assert (self.root / "once" / "runs.txt").read_text() == "run\n"
        print("PASS: passive bounded catalog, retention preview, explicit original-plan prune/replay and permanent audit preserve current instance/task/wait and later point", flush=True)


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
    # Only eligible closed copies were pruned through CLI. Retain current state,
    # audit records and the remaining recovery point for inspection.
    print(f"RETAINED: stopped private recovery evidence {root}", flush=True)


if __name__ == "__main__":
    main()
