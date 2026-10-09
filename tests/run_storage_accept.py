"""Linux compute-only storage acceptance; isolated state, CPU workers, fake GPUs.

Never fill disks, change kernel quota, mount filesystems, or delete science data.
"""
import json
from pathlib import Path
import shutil
import sys
import tempfile

from run_exact_dependencies_accept import ExactAcceptance


class StorageAcceptance(ExactAcceptance):
    def patch(self, **values):
        path = self.root / "storage-patch.json"
        path.write_text(json.dumps({"storage_admission": values}))
        self.cli("config", "set", "-f", path, "--yes")

    def explanation(self, batch, task):
        return self.data("storage-explain", f"{batch}:{task}", "--json")

    def run(self):
        self.cfg["storage_admission"] = {"enabled": True, "reserve_gib": 0, "reserve_inodes": 0,
            "control_reserve_gib": 0.01, "control_reserve_inodes": 16}
        self.config.write_text(json.dumps(self.cfg))  # Fixture config before CLI initialization.
        self.cli("daemon", "drain")
        whale = self.worker("whale", "whale")
        whale["resources"].update(gpu=1, vram_gib=1, gpu_share=True, disk_gib=2 ** 30)
        whale["artifacts"] = {"science": {"path": "science.txt"}}
        sentinel = self.root / "whale" / "science.txt"
        sentinel.write_text("PRESERVE_FIXTURE\n")
        inode = self.worker("inode", "inode")
        inode["resources"]["disk_inodes"] = 2 ** 63 - 1
        small = self.worker("small", "small")
        small["resources"].update(gpu=1, vram_gib=1, gpu_share=True, disk_gib=0.001, disk_inodes=1)
        batch = self.submit("storage", [whale, inode, small], failure_policy="continue_independent")
        assert self.explanation(batch, "whale")["allowed"] is None
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "small"), lambda value: value["jobs"][0]["status"] == "done")
        whale_report = self.wait(lambda: self.explanation(batch, "whale"),
                                 lambda value: value["allowed"] is False and "disk_space" in value["reasons"])
        inode_report = self.wait(lambda: self.explanation(batch, "inode"),
                                 lambda value: value["allowed"] is False and "inode_space" in value["reasons"])
        for task in ("whale", "inode"):
            assert self.job(batch, task)["jobs"][0]["status"] == "pending"
            assert self.data("allocations", f"{batch}:{task}", "--json")["allocations"] == []
            assert not (self.root / task / "runs.txt").exists()
        assert sentinel.read_text() == "PRESERVE_FIXTURE\n"
        assert (self.root / "small" / "runs.txt").read_text() == "run\n"
        intent = self.data("allocations", f"{batch}:small", "--json")["allocations"][0]
        intent = self.data("allocations", f"{batch}:small", "--allocation-id", intent["allocation_id"], "--json")["allocations"][0]
        assert intent["storage_filesystems"] and intent["storage_observation_sha256"]
        print("PASS: disk/inode rejection occurs before fake GPU allocation, worker start and artifact cleanup; later small CPU worker runs once", flush=True)
        assert whale_report["quota_scope"] == "current_uid_only"
        assert whale_report["other_quota_scopes"] == "group_project_remote_not_verified"
        assert inode_report["hard_isolation"] is False
        self.cli("cancel", f"{batch}:whale", "--yes")
        self.cli("cancel", f"{batch}:inode", "--yes")

        self.patch(control_reserve_gib=2 ** 30)
        held = self.submit("control", [self.worker("held", "control")])
        control = self.wait(lambda: self.explanation(held, "held"),
                            lambda value: value["allowed"] is False and "disk_space" in value["reasons"])
        assert any("control" in fs["roles"] and "disk_space" in fs["reasons"] for fs in control["filesystems"])
        assert not (self.root / "control" / "runs.txt").exists()
        self.patch(control_reserve_gib=0.01)
        self.wait(lambda: self.job(held, "held"), lambda value: value["jobs"][0]["status"] == "done")
        assert (self.root / "control" / "runs.txt").read_text() == "run\n"
        print("PASS: control-plane free-space floor blocks new dispatch; CLI hot update resumes exactly one execution without deleting files", flush=True)

        unknown_quota = any(q.get("known") is not True for fs in control["filesystems"] for q in fs["quota"])
        if unknown_quota:
            self.patch(require_user_quota=True)
            quota_batch = self.submit("quota", [self.worker("held", "quota")])
            self.wait(lambda: self.explanation(quota_batch, "held"),
                      lambda value: value["allowed"] is False and any("user_quota_" in r for r in value["unknown"]))
            assert not (self.root / "quota" / "runs.txt").exists()
            self.patch(require_user_quota=False)
            self.wait(lambda: self.job(quota_batch, "held"), lambda value: value["jobs"][0]["status"] == "done")
            assert (self.root / "quota" / "runs.txt").read_text() == "run\n"
            print("PASS: actual unavailable local-user quota is unknown, require_user_quota pauses, explicit optional policy resumes once", flush=True)
        else:
            print("OBSERVED: local-user quota readable; quota-unknown branch covered by pure fixtures, not claimed as real failure injection", flush=True)

        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        for task in ("whale", "inode"):
            assert not (self.root / task / "runs.txt").exists()
        assert sentinel.read_text() == "PRESERVE_FIXTURE\n"
        print("PASS: private daemon stopped and rejected fixtures preserved; no real GPU or kernel quota mutation", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run only on a Linux compute node, not a laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-storage-")
    acceptance = StorageAcceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private acceptance daemon not stopped; retained {temporary}")
    shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
