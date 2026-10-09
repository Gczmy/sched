"""Isolated compute-node admission explanation acceptance, fake GPUs / real CPU."""
import json
from pathlib import Path
import shutil
import sys
import tempfile

from run_exact_dependencies_accept import ExactAcceptance


class AdmissionAcceptance(ExactAcceptance):
    def __init__(self, root):
        super().__init__(root)
        self.cfg.update(gpus=[0, 1], cpus_total=4, host_mem_total_gib=2,
                        host_mem_reserve_gib=0, host_mem_default_gib=0.125)
        self.config.write_text(json.dumps(self.cfg))
        self.env["SCHED_FAKE_GPUS"] = "0:16,1:24"

    def explain(self, batch, task):
        return self.data("admission-explain", f"{batch}:{task}", "--json")

    def run(self):
        self.cli("daemon", "drain")
        whale = self.worker("whale", "whale")
        whale["resources"] = {"gpu": 1, "cpus": 120, "host_mem_gib": 128, "vram_gib": 64}
        gpu_only = self.worker("gpu-only", "gpu-only")
        gpu_only["resources"] = {"gpu": 1, "cpus": 1, "host_mem_gib": 0.125, "vram_gib": 64}
        small = self.worker("small", "small")
        small["resources"] = {"gpu": 1, "cpus": 1, "host_mem_gib": 0.125, "vram_gib": 1}
        batch = self.submit("admission", [whale, gpu_only, small], failure_policy="continue_independent")
        initial = self.explain(batch, "whale")
        assert initial["resource_fit"] is None and initial["unknown"]
        assert "cpu" in initial["reasons"] and "host_memory" in initial["reasons"]
        assert initial["admission_granted"] is False and initial["effect"] == "none"
        print("PASS: pre-observation query reports all static budget rejections and unknown runtime, never grants admission", flush=True)

        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "small"), lambda r: r["jobs"][0]["status"] == "done")
        actual = self.wait(lambda: self.explain(batch, "whale"), lambda r: not r["unknown"])
        assert {"cpu", "host_memory", "gpu"} <= set(actual["reasons"]), actual
        assert actual["resource_fit"] is False and actual["gpu"]["selected"] is None
        assert len(actual["gpu"]["candidates"]) == 2
        assert all("declared_vram_exceeds_capacity" in card["reasons"] for card in actual["gpu"]["candidates"])
        assert actual["runtime_observation"]["usage_snapshot_matches"]
        print("PASS: recorded compute observations explain CPU/memory/both GPU capacities simultaneously using the real dispatch decisions", flush=True)

        gpu_wait = self.explain(batch, "gpu-only")
        assert gpu_wait["budget"]["allowed"] and gpu_wait["reasons"] == ["gpu"]
        assert (self.root / "small/runs.txt").read_text() == "run\n"
        assert not (self.root / "whale/runs.txt").exists() and not (self.root / "gpu-only/runs.txt").exists()
        assert self.job(batch, "whale")["jobs"][0]["status"] == "pending"
        print("PASS: a later small real CPU child backfills once; unfit GPU/CPU/memory candidates never start or reserve a GPU", flush=True)

        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        before = self.data("status", "--json")
        revision = self.data("batch-policy", batch, "--json")["batch_revision"]
        for _ in range(2):
            result = self.explain(batch, "whale")
            assert result["effect"] == "none" and not result["admission_granted"] and "draining" in result["reasons"]
        assert revision == self.data("batch-policy", batch, "--json")["batch_revision"]
        after = self.data("status", "--json")
        assert before["jobs"] == after["jobs"] and before["gpus"] == after["gpus"]
        assert (self.root / "small/runs.txt").read_text() == "run\n"
        print("PASS: repeated explanation after private stop changes no revisions, task fields, allocations, worker counts or artifacts", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run on a Linux compute node, not a laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-admission-")
    acceptance = AdmissionAcceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private acceptance daemon not stopped; retained {temporary}")
        shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
