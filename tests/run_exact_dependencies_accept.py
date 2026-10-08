"""Exact dependency acceptance on Linux compute node, independent CPU state."""
from pathlib import Path
import json
import shutil
import sys
import tempfile

from run_feedback_accept import Acceptance, worker_program


class ExactAcceptance(Acceptance):
    def selector(self, batch, task, version):
        return [{"instance_id": self.data("identity", "--json")["instance_id"],
                 "batch_id": batch, "tasks": [{"task_id": task, "version": version}]}]

    def submit(self, name, tasks, **options):
        path = self.root / (name + "-submit.json")
        path.write_text(json.dumps({"name": name, "project": "example", "tasks": tasks, **options}))
        return self.data("submit", path, "--request-id", "submit-" + name + "-" + str(len(self.data("status", "--json")["batches"])), "--json")["batch_id"]

    def worker(self, task, directory, *, fails_until_allowed=False):
        (self.root / directory).mkdir(parents=True)
        program = worker_program(None)
        if fails_until_allowed:
            program += f"; raise SystemExit(0 if Path({str(self.root / 'allow')!r}).exists() else 1)"
        return {"id": task, "cwd": directory, "cmd": [sys.executable, "-I", "-c", program],
                "max_retry": 0, "resources": {"gpu": 0, "cpus": 1}}

    def job(self, batch, task):
        return self.data("task", f"{batch}:{task}", "--json")

    def run(self):
        self.cli("daemon", "drain")
        source = self.submit("source", [self.worker("a", "source/a", fails_until_allowed=True),
                                        self.worker("b", "source/b")], failure_policy="continue_independent")
        exact = self.submit("exact-old", [self.worker("task", "exact-old")],
                            depends_on_exact=self.selector(source, "a", 1))
        subset = self.submit("exact-subset", [self.worker("task", "exact-subset")],
                             depends_on_exact=self.selector(source, "b", 1))
        legacy = self.submit("legacy", [self.worker("task", "legacy")], depends_on=["source"])
        before = self.data("batch-dependencies", exact, "--json")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(source, "a"), lambda r: r["jobs"][0]["status"] == "blocked")
        self.wait(lambda: self.job(subset, "task"), lambda r: r["jobs"][0]["status"] == "done")
        assert not (self.root / "exact-old" / "runs.txt").exists()
        assert not (self.root / "legacy" / "runs.txt").exists()
        print("PASS: selected successful subset runs despite unrelated source failure; failed exact version and legacy whole batch wait", flush=True)

        (self.root / "allow").touch()  # Fixture input, not state or a scientific artifact.
        self.cli("resubmit", f"{source}:a")
        self.wait(lambda: self.job(source, "a"), lambda r: r["jobs"][-1]["version"] == 2 and r["jobs"][-1]["status"] == "done")
        self.wait(lambda: self.job(legacy, "task"), lambda r: r["jobs"][0]["status"] == "done")
        assert not (self.root / "exact-old" / "runs.txt").exists()
        assert self.job(source, "a")["jobs"][0]["status"] == "blocked"
        frozen = self.data("batch-dependencies", exact, "--json")
        assert frozen["binding_sha256"] == before["binding_sha256"] and frozen["dependencies"][0]["version"] == 1
        print("PASS: same-batch v2 success releases legacy but cannot replace bound failed v1", flush=True)

        self.wait(lambda: self.data("batch-policy", source, "--json"), lambda r: r["status"] == "done")
        newer = self.submit("source", [self.worker("a", "new-source/a")])
        self.wait(lambda: self.job(newer, "a"), lambda r: r["jobs"][0]["status"] == "done")
        new_exact = self.submit("exact-v2", [self.worker("task", "exact-v2")],
                                depends_on_exact=self.selector(source, "a", 2))
        self.wait(lambda: self.job(new_exact, "task"), lambda r: r["jobs"][0]["status"] == "done")
        assert not (self.root / "exact-old" / "runs.txt").exists()
        legacy_fact = self.data("batch-dependencies", legacy, "--json")["dependencies"][0]
        assert legacy_fact["dynamic_latest"] and legacy_fact["resolved_batch_id"] == newer
        assert self.data("batch-dependencies", exact, "--json")["dependencies"][0]["batch_id"] == source
        print("PASS: same-name new batch does not redirect exact ID; explicit original v2 selector runs once", flush=True)

        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "healthy")
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        assert self.data("batch-dependencies", exact, "--json")["binding_sha256"] == before["binding_sha256"]
        assert not (self.root / "exact-old" / "runs.txt").exists()
        for directory in ("exact-subset", "legacy", "exact-v2"):
            assert (self.root / directory / "runs.txt").read_text() == "run\n"
        print("PASS: private daemon restart preserves exact binding, waits and one-run downstream counts", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run on a Linux compute node, not a laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-exact-dependencies-")
    acceptance = ExactAcceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private acceptance daemon not stopped; retained {temporary}")
    shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
