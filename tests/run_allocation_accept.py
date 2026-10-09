"""Isolated allocation/layer acceptance: Linux compute node, fake GPUs, CPU children."""
import json
from pathlib import Path
import shutil
import sys
import tempfile

from run_exact_dependencies_accept import ExactAcceptance


class AllocationAcceptance(ExactAcceptance):
    def allocations(self, batch, task, identifier=None):
        args = ["allocations", f"{batch}:{task}", "--json"]
        if identifier:
            args.extend(["--allocation-id", identifier])
        return self.data(*args)

    def item(self, batch, task):
        rows = self.allocations(batch, task)["allocations"]
        assert len(rows) == 1, rows
        return self.allocations(batch, task, rows[0]["allocation_id"])["allocations"][0]

    def run(self):
        self.cli("daemon", "drain")
        failed = self.worker("retry", "retry", fails_until_allowed=True)
        artifact = self.worker("artifact", "artifact")
        artifact["artifacts"] = {"result": {"path": "missing.json", "check": "json"}}
        ready = self.worker("ready", "ready")
        ready["cmd"][-1] += "; import time; print('CPU_READY', flush=True); time.sleep(60)"
        ready["probes"] = {"ready_on_log": "CPU_READY"}
        ready["resources"].update(gpu=1, vram_gib=1, gpu_share=True)
        batch = self.submit("allocation", [failed, artifact, ready], failure_policy="continue_independent")
        assert self.allocations(batch, "retry")["allocations"] == []
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "retry"), lambda r: r["jobs"][0]["status"] == "blocked")
        self.wait(lambda: self.job(batch, "artifact"), lambda r: r["jobs"][0]["status"] == "blocked")
        self.wait(lambda: self.job(batch, "ready"), lambda r: r["jobs"][0]["status"] == "done")
        before_retry = self.item(batch, "retry")
        artifact_item = self.item(batch, "artifact")
        ready_item = self.item(batch, "ready")
        for item, rc in ((before_retry, 1), (artifact_item, 0)):
            facts = [e["data"]["wait"] for e in item["events"] if e["layer"] == "process"]
            assert len(facts) == 1 and facts[0]["verified"] and facts[0]["returncode"] == rc, facts
            assert facts[0]["subject"] == "scheduler_supervisor_command_chain"
            assert item["worker_identity"] is None and item["hard_isolation"] is False
            assert len(item["artifact_validation_ids"]) == 1
        evidence = self.data("artifact-validations", f"{batch}:artifact", "--validation-id", artifact_item["artifact_validation_ids"][0], "--json")["validations"][0]
        assert not evidence["passed"] and evidence["payload"]["allocation_id"] == artifact_item["allocation_id"]
        assert evidence["payload"]["checks"][0]["reason_code"] == "missing_file"
        print("PASS: raw rc=1 and raw rc=0/artifact failure retain separate immutable allocation, wait and artifact references", flush=True)

        assert any(e["layer"] == "monitor" for e in ready_item["events"])
        wait = next(e["data"]["wait"] for e in ready_item["events"] if e["layer"] == "process")
        assert wait["verified"] and wait["returncode"] != 0, wait
        assert wait["subject"] == "scheduler_supervisor_command_chain"
        assert ready_item["gpu_reservations"][0]["simulated"]
        assert any(e["data"].get("event") == "reservation_released" for e in ready_item["events"])
        print("PASS: ready monitor/done does not relabel the actual nonzero supervisor wait; fake GPU reservation/release is not physical ownership", flush=True)

        (self.root / "allow").touch()
        self.cli("retry", f"{batch}:retry")
        self.wait(lambda: self.job(batch, "retry"), lambda r: r["jobs"][0]["status"] == "done")
        attempts = self.allocations(batch, "retry")["allocations"]
        assert {item["ordinal"] for item in attempts} == {1, 2}
        assert len({item["allocation_id"] for item in attempts}) == 2
        preserved = self.allocations(batch, "retry", before_retry["allocation_id"])["allocations"][0]
        assert {k: v for k, v in preserved.items() if k != "events"} == {k: v for k, v in before_retry.items() if k != "events"}
        assert preserved["events"][:len(before_retry["events"])] == before_retry["events"]
        assert (self.root / "retry/runs.txt").read_text() == "run\nrun\n"
        print("PASS: ordinary retry reuses v1 but creates a distinct allocation; first failed wait and validation remain byte-equivalent", flush=True)

        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        snapshot = self.data("status", "--json")
        frozen = self.allocations(batch, "ready", ready_item["allocation_id"])
        for _ in range(2):
            assert frozen == self.allocations(batch, "ready", ready_item["allocation_id"])
        assert snapshot["jobs"] == self.data("status", "--json")["jobs"]
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "healthy")
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        assert frozen == self.allocations(batch, "ready", ready_item["allocation_id"])
        for directory in ("artifact", "ready"):
            assert (self.root / directory / "runs.txt").read_text() == "run\n"
        print("PASS: passive queries and private daemon restart preserve allocations, layered facts, revisions and execution counts", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run on a Linux compute node, not a laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-allocation-")
    acceptance = AllocationAcceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private acceptance daemon not stopped; retained {temporary}")
        shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
