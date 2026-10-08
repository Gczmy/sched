"""Task DAG CPU/CLI acceptance: private Linux compute state, no real GPU."""
import json
from pathlib import Path
import shutil
import sys
import tempfile

from run_exact_dependencies_accept import ExactAcceptance
from run_feedback_accept import worker_program


class DagAcceptance(ExactAcceptance):
    def request_update(self, rid, batch, task, selectors):
        record = self.data("batch-policy", batch, "--json")
        version = self.job(batch, task)["jobs"][-1]["version"]
        return ["request", rid, "--json", "--expect-kind", "task", "--expect-id", f"{batch}:{task}",
                "--expect-status", "pending", "--expect-version", str(version),
                "--expect-revision", str(record["batch_revision"]),
                "--expect-instance", self.data("identity", "--json")["instance_id"],
                "--", "dependency-update", f"{batch}:{task}", "--dependencies-json", json.dumps(selectors), "--yes"]

    def facts(self, batch, task, *extra):
        return self.data("task-dependencies", f"{batch}:{task}", *extra, "--json")

    def run(self):
        self.cli("daemon", "drain")
        tasks = [self.worker("a", "dag/a", fails_until_allowed=True), self.worker("b", "dag/b"),
                 self.worker("c", "dag/c"), self.worker("d", "dag/d")]
        tasks[2]["depends_on"] = [{"task_id": "a", "version": 1}]
        tasks[2]["cmd"] = [sys.executable, "-I", "-c", worker_program("{}")]
        tasks[2]["artifacts"] = {"result": {"path": "result.json", "check": "json"}}
        tasks[3]["depends_on"] = [{"task_id": "b", "version": 1}]
        batch = self.submit("dag", tasks, failure_policy="continue_independent")
        original = self.facts(batch, "c")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "a"), lambda r: r["jobs"][0]["status"] == "blocked")
        self.wait(lambda: self.job(batch, "d"), lambda r: r["jobs"][0]["status"] == "done")
        assert self.job(batch, "c")["jobs"][0]["status"] == "waiting_dep"
        displayed = self.data("status", batch, "--json")["jobs"]
        assert next(job for job in displayed if job["task"] == "c")["wait_reason"] == "dependency"
        assert not (self.root / "dag/c/runs.txt").exists()
        assert self.facts(batch, "c")["blocking_paths"][0]["reason"] == "source_blocked"
        assert (self.root / "dag/b/runs.txt").read_text() == "run\n"
        print("PASS: A failure holds C; independent B/D run once and exact blocking path identifies A v1", flush=True)

        (self.root / "allow").touch()
        self.cli("resubmit", f"{batch}:a")
        self.wait(lambda: self.job(batch, "a"), lambda r: r["jobs"][-1]["version"] == 2 and r["jobs"][-1]["status"] == "done")
        assert self.facts(batch, "c")["binding_sha256"] == original["binding_sha256"]
        assert not (self.root / "dag/c/runs.txt").exists()
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        request = self.request_update("explicit-c-v2", batch, "c", self.selector(batch, "a", 2))
        result = self.data(*request)
        event_id = result["result"]["effect"]["dependency_event_id"]
        revision = self.data("batch-policy", batch, "--json")["batch_revision"]
        replay = self.data(*request)
        assert replay["result"] == result["result"]
        assert self.data("batch-policy", batch, "--json")["batch_revision"] == revision
        old = self.facts(batch, "c", "--event-id", original["event_id"])
        assert not old["effective"] and old["event"]["bindings"][0]["tasks"][0]["version"] == 1
        assert self.facts(batch, "c")["event_id"] == event_id
        print("PASS: successful A v2 does not replace v1; explicit CAS update retains old binding and same RID replays without a second event", flush=True)

        cyclic = [self.worker("a", "cycle/a"), self.worker("c", "cycle/c")]
        cyclic[1]["depends_on"] = [{"task_id": "a", "version": 1}]
        second = self.submit("cycle", cyclic, failure_policy="continue_independent")
        before = self.data("batch-policy", second, "--json")["batch_revision"]
        reject = self.cli(*self.request_update("cycle-reject", second, "a", self.selector(second, "c", 1)), expect=65)
        assert "成环" in reject.stderr
        assert self.data("batch-policy", second, "--json")["batch_revision"] == before
        assert self.facts(second, "a")["event_id"] is None
        self.cli("cancel", second, "--yes")
        print("PASS: cycle mutation rolls back binding/revision atomically, with durable rejected request", flush=True)

        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "c"), lambda r: r["jobs"][0]["status"] == "done")
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        assert self.job(batch, "c")["jobs"][0]["version"] == 1
        for directory in ("b", "c", "d"):
            assert (self.root / "dag" / directory / "runs.txt").read_text() == "run\n"
        assert self.job(batch, "a")["jobs"][0]["status"] == "blocked"
        assert self.facts(batch, "c")["event_id"] == event_id
        print("PASS: private daemon restart honors updated immutable dependency, runs C once without a new version, retains failed A v1", flush=True)

        self.cli("resubmit", f"{batch}:c")
        self.data(*self.request_update("change-c-cache-context", batch, "c", self.selector(batch, "b", 1)))
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "c"), lambda r: r["jobs"][-1]["version"] == 2 and r["jobs"][-1]["status"] == "done")
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        assert (self.root / "dag/c/runs.txt").read_text() == "run\nrun\n"
        assert self.facts(batch, "c")["bindings"][0]["tasks"][0]["task_id"] == "b"
        print("PASS: changed frozen dependency cannot SKIP a previous successful artifact with identical command fingerprint", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run on a Linux compute node, not a laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-task-dag-")
    acceptance = DagAcceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private acceptance daemon not stopped; retained {temporary}")
    shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
