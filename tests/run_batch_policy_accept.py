"""Real CPU/CLI failure-isolation acceptance; disposable state, no production."""
from pathlib import Path
import json
import shutil
import sys
import tempfile

from run_feedback_accept import Acceptance, worker_program


class PolicyAcceptance(Acceptance):
    def __init__(self, root):
        super().__init__(root)
        self.cfg.update(cpus_total=1, max_cpu_jobs=1)
        self.config.write_text(json.dumps(self.cfg))

    def submit_pair(self, name, policy=None):
        directory = self.root / name
        directory.mkdir()
        tasks = []
        for task in ("a", "b"):
            work = directory / task
            work.mkdir()
            program = worker_program(None) + ("; raise SystemExit(1)" if task == "a" else "")
            tasks.append({"id": task, "cwd": f"{name}/{task}", "max_retry": 0,
                          "cmd": [sys.executable, "-I", "-c", program],
                          "resources": {"gpu": 0, "cpus": 1}})
        spec = {"name": name, "project": "example", "tasks": tasks}
        if policy is not None:
            spec["failure_policy"] = policy
        path = self.root / f"{name}.json"
        path.write_text(json.dumps(spec))
        return self.data("submit", path, "--request-id", "submit-" + name, "--json")["batch_id"]

    def policy_request(self, batch, rid, *, reopen=False):
        current = self.data("batch-policy", batch, "--json")
        return ["request", rid, "--json", "--expect-kind", "batch", "--expect-id", batch,
                "--expect-status", current["status"], "--expect-revision", str(current["batch_revision"]),
                "--expect-instance", current["instance_id"], "--expect-project", "example", "--",
                "batch-policy", batch, "--failure-policy", "continue_independent", "--yes",
                *(["--reopen"] if reopen else [])]

    def run(self):
        self.cli("daemon", "drain")
        frozen = self.submit_pair("freeze")
        assert self.data("batch-policy", frozen, "--json")["failure_policy"] == "freeze"
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.data("batch-policy", frozen, "--json"),
                  lambda value: value["status"] == "blocked")
        before = self.data("task", f"{frozen}:b", "--json")
        assert before["jobs"][0]["status"] == "pending"
        assert not (self.root / "freeze" / "b" / "runs.txt").exists()
        args = self.policy_request(frozen, "change-only")
        result = self.data(*args)
        assert result["result"]["effect"]["status"] == "blocked"
        assert self.data("task", f"{frozen}:b", "--json")["jobs"] == before["jobs"]
        assert self.data(*args)["replayed"]
        reopen = self.policy_request(frozen, "explicit-reopen", reopen=True)
        assert self.data(*reopen)["result"]["effect"]["status"] == "active"
        assert self.data(*reopen)["replayed"]
        self.wait(lambda: self.data("task", f"{frozen}:b", "--json"),
                  lambda value: value["jobs"][0]["status"] == "done")
        self.wait(lambda: self.data("batch-policy", frozen, "--json"),
                  lambda value: value["status"] == "blocked")
        for task in ("a", "b"):
            assert (self.root / "freeze" / task / "runs.txt").read_text() == "run\n"
            assert len(self.data("task", f"{frozen}:{task}", "--json")["jobs"]) == 1
        print("PASS: default freeze preserves pending; CAS policy alone stays blocked; explicit reopen runs once", flush=True)

        independent = self.submit_pair("independent", "continue_independent")
        self.wait(lambda: self.data("task", f"{independent}:b", "--json"),
                  lambda value: value["jobs"][0]["status"] == "done")
        self.wait(lambda: self.data("batch-policy", independent, "--json"),
                  lambda value: value["status"] == "blocked")
        for task in ("a", "b"):
            assert (self.root / "independent" / task / "runs.txt").read_text() == "run\n"
        a = self.data("task", f"{independent}:a", "--json")["jobs"][0]
        b = self.data("task", f"{independent}:b", "--json")["jobs"][0]
        assert a["rc"] == 1 and a["status"] == "blocked"
        assert b["rc"] == 0 and b["status"] == "done"
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"),
                  lambda value: value["health_state"] == "stopped")
        print("PASS: opt-in independent task follows failed task without retry; final batch stays blocked", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run on a Linux compute node, not a laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-policy-")
    acceptance = PolicyAcceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private policy daemon not stopped; retained {temporary}")
    shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
