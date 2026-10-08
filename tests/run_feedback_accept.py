"""Feedback CLI acceptance on a Linux compute node; private state, CPU only.

All scheduler queries/mutations use the public CLI. This does not emulate SSH,
prove gateway delivery, or authorize settlement from an artifact predicate.
"""
from __future__ import annotations

import getpass
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time


def worker_program(content, *, pathological_regex=False):
    content_expression = "'a' * 100_000 + '!'" if pathological_regex else repr(content)
    return ("from pathlib import Path; "
            "Path('runs.txt').open('a').write('run\\n'); "
            + (f"Path('result.json').write_text({content_expression})" if content is not None else "pass"))


class Acceptance:
    def __init__(self, root):
        self.root = root
        self.repository = Path(__file__).resolve().parents[1]
        self.cfg = {
            "schema_version": 1, "node": socket.gethostname(), "user": getpass.getuser(),
            "state_dir": str(root / "state"), "gpus": [0], "cpus_total": 6,
            "max_cpu_jobs": 6, "idle_timeout_min": 0, "default_project": "example",
            "host_mem_default_gib": 0.125, "host_mem_reserve_gib": 0,
            "projects": {"example": {"root": str(root), "git": False}},
            "venvs": {"python": sys.executable},
        }
        self.config = root / "config.json"
        self.config.write_text(json.dumps(self.cfg))
        self.env = dict(os.environ, SCHED_CONFIG=str(self.config),
                        SCHED_STATE=str(root / "state"), SCHED_FAKE_GPUS="0:24",
                        PYTHONPATH=str(self.repository), PYTHONDONTWRITEBYTECODE="1")
        self.env.pop("SCHED_ALLOW_FOREIGN_WRITE", None)

    def cli(self, *args, expect=0, env=None, timeout=45):
        result = subprocess.run([sys.executable, "-m", "gsched.cli", *map(str, args)],
                                cwd=self.root, env=self.env if env is None else env,
                                capture_output=True, text=True, timeout=timeout)
        if expect is not None:
            assert result.returncode == expect, (args, result.returncode, result.stdout, result.stderr)
        return result

    def data(self, *args, **kwargs):
        return json.loads(self.cli(*args, **kwargs).stdout)

    def wait(self, query, predicate, timeout=100):
        deadline = time.monotonic() + timeout
        while True:
            result = query()
            if predicate(result):
                return result
            if time.monotonic() >= deadline:
                raise AssertionError(result)
            time.sleep(0.5)

    def task(self, batch):
        return self.data("task", f"{batch}:task", "--json")

    def request(self, rid, batch, revision, status, *, verb="request"):
        return [verb, rid, "--json", "--expect-kind", "batch", "--expect-id", batch,
                "--expect-status", status, "--expect-revision", str(revision),
                "--", "cancel", batch, "--yes"]

    def run(self):
        # Format rejection must work even before there is a state database.
        missing = self.data("request", "missing", "--json", "--expect-kind", "batch",
                            "--expect-id", "absent", "--expect-revision", "0", "--",
                            "cancel", "absent", "--yes", expect=64)
        assert missing["error"]["missing_fields"] == ["expect_status"]
        assert missing["error"]["effect_of_this_invocation"] == "none"
        assert not (self.root / "state").exists()
        valid_format = self.data(*self.request("preview", "absent", 0, "active",
                                               verb="request-validate"))
        assert valid_format["valid"] and not valid_format["state_checked"]
        assert not (self.root / "state").exists()
        print("PASS: request format checks do not initialize/reserve state", flush=True)

        self.cli("daemon", "drain")
        instance = self.data("identity", "--json")["instance_id"]
        cases = {
            "valid": ('{"status":"complete","ok":true}',
                      {"json_equals": {"status": "complete", "ok": True}}, "passed"),
            "invalid": ("not json", {"check": "json"}, "invalid_json"),
            "mismatch": ('{"ok":true}', {"json_equals": {"ok": 1}}, "json_value_mismatch"),
            "nomatch": ("abc", {"regex": "never"}, "regex_no_match"),
            "timeout": ("a" * 100_000 + "!", {"regex": "^(a+)+$"}, "regex_timeout"),
            "missing": (None, {}, "missing_file"),
        }
        submitted = {}
        for name, (content, rules, _) in cases.items():
            directory = self.root / name
            directory.mkdir()
            # The worker is bounded and self-contained, not customer code.
            program = worker_program(content, pathological_regex=name == "timeout")
            spec = {"name": name, "project": "example", "cwd": name,
                    "tasks": [{"id": "task", "max_retry": 0,
                               "cmd": [sys.executable, "-I", "-c", program],
                               "resources": {"gpu": 0, "cpus": 1},
                               "artifacts": {"result": {"path": "result.json", **rules}}}]}
            path = self.root / f"{name}.json"
            path.write_text(json.dumps(spec))
            args = ["submit", str(path), "--request-id", f"submit-{name}",
                    "--expect-instance", instance, "--expect-project", "example", "--json"]
            first = self.data(*args)
            again = self.data(*args)
            assert first["batch_id"] == again["batch_id"]
            submitted[name] = first["batch_id"]
            original = self.task(first["batch_id"])
            assert original["jobs"][0]["status"] == "pending"

        snapshot = self.data("status", "--json")
        assert len(snapshot["batches"]) == len(cases)
        assert not any(snapshot["truncated"].values())
        batch_states = {row["id"]: row["status"] for row in snapshot["batches"]}
        # Before the first daemon tick, accepted batches are still queued.
        # Drain does not bypass dependency/activation state in the query.
        assert all(row["status"] == "pending" and row["wait_reason"] ==
                   ("dependency" if batch_states[row["batch_id"]] == "queued" else "draining")
                   for row in snapshot["jobs"]), snapshot
        assert all(not (self.root / name / "runs.txt").exists() for name in cases)
        missing_receipt = self.data("request-status", "missing", "--json")
        assert missing_receipt["phase"] == "not_found"
        assert self.data("request-status", "preview", "--json")["phase"] == "not_found"
        batch = submitted["valid"]
        current = next(row for row in snapshot["batches"] if row["id"] == batch)
        stale = self.data(*self.request("stale", batch, current["revision"] - 1, current["status"]), expect=65)
        assert stale["result"]["error"]["conflict_reason"] == "revision_changed"
        assert self.task(batch)["jobs"][0]["status"] == "pending"
        replay = self.data(*self.request("stale", batch, current["revision"] - 1, current["status"]), expect=65)
        assert replay["replayed"] and stale["result"] == replay["result"]
        print("PASS: same-RID submits create one batch; stale CAS stays rejected on replay", flush=True)

        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.data("status", "--json"),
                  lambda value: len(value["jobs"]) == len(cases)
                  and all(row["status"] in ("done", "blocked") for row in value["jobs"]))
        # Take the observations with daemon stopped; read-only checks cannot
        # be confused with a simultaneous tick's legitimate status update.
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"),
                  lambda value: value["health_state"] == "stopped")
        validations = {}
        for name, (_, _, reason) in cases.items():
            before = self.task(submitted[name])
            job = before["jobs"][0]
            assert job["rc"] == 0
            assert job["status"] == ("done" if name == "valid" else "blocked")
            assert (self.root / name / "runs.txt").read_text() == "run\n"
            path = self.root / name / "result.json"
            digest = hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None
            summary = self.data("artifact-validations", f"{submitted[name]}:task", "--version", "1", "--json")
            assert summary["available"] and not summary["truncated"]
            assert len(summary["validations"]) == 1 and not summary["evidence_included"]
            record = summary["validations"][0]
            assert record["passed"] == (name == "valid") and record["wait_verified"]
            frozen = self.data("artifact-validations", f"{submitted[name]}:task", "--version", "1",
                               "--validation-id", record["validation_id"], "--json")
            payload = frozen["validations"][0]["payload"]
            assert payload["recorded_rc"] == 0 and payload["wait"]["returncode"] == 0
            assert payload["wait"]["subject"] == "scheduler_supervisor_command_chain"
            assert payload["checks"][0]["reason_code"] == reason
            validations[name] = frozen
            for _ in range(2):
                detail = self.data("artifact-check", f"{submitted[name]}:task", "--version", "1",
                                   "--json", expect=0 if name == "valid" else 1)
                assert detail["recorded_rc"] == 0 and detail["effect"] == "none"
                assert detail["checks"][0]["reason_code"] == reason
                assert self.task(submitted[name]) == before
                assert (self.root / name / "runs.txt").read_text() == "run\n"
                if digest is not None:
                    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
                assert self.data("artifact-validations", f"{submitted[name]}:task", "--version", "1",
                                 "--validation-id", record["validation_id"], "--json") == frozen
        print("PASS: actual rc=0, distinct artifact failures, read-only checks preserve history/files/runs", flush=True)

        ids = [f"submit-{name}" for name in cases]
        receipts = self.data("request-status-many", *ids, "--expect-instance", instance,
                             "--wait-sec", "1", "--json")
        assert not receipts["wait_timed_out"] and not receipts["ticket_fallback_atomic"]
        assert [row["request_id"] for row in receipts["requests"]] == ids
        assert all(row["phase"] == "done" and row["receipt_source"] == "database"
                   and row["batch_persisted"] for row in receipts["requests"])
        unknown = self.data("request-status", "never-submitted", "--wait-sec", "0.1", "--json")
        assert unknown["phase"] == "not_found" and unknown["wait_timed_out"]
        # Restart only the private daemon, then reread the same immutable RIDs.
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.data("daemon", "status", "--json"),
                  lambda value: value["health_state"] == "healthy")
        assert self.data("identity", "--json")["instance_id"] == instance
        reread = self.data("request-status-many", *ids, "--json")["requests"]
        stable = lambda row: {key: value for key, value in row.items() if key != "observed_at"}
        assert list(map(stable, reread)) == list(map(stable, receipts["requests"]))
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"),
                  lambda value: value["health_state"] == "stopped")
        assert len(self.data("status", "--json")["batches"]) == len(cases)
        for name, frozen in validations.items():
            assert self.data("artifact-validations", f"{submitted[name]}:task", "--version", "1",
                             "--validation-id", frozen["validations"][0]["validation_id"], "--json") == frozen
        assert all((self.root / name / "runs.txt").read_text() == "run\n" for name in cases)
        print("PASS: bounded queries and daemon restart keep original instance/RIDs/batches/validations", flush=True)

        patch = self.root / "patch.json"
        patch.write_text(json.dumps({"projects": {"example": {
            "gpu_affinity": [1], "gpu_affinity_hard": True}}}))
        original_config = self.config.read_bytes()
        self.cli("config", "set", "-f", str(patch), "--yes", expect=1)
        assert self.config.read_bytes() == original_config
        print("PASS: disjoint hard affinity is rejected without changing config", flush=True)
        self.revalidation()

    def revalidation(self):
        from revalidation_accept_support import StopFirstRegex
        root = self.root / "revalidation"
        root.mkdir()
        # A bounded but non-instant regex gives the pidfd injector a window.
        # The unchanged input passes when the deliberately stopped checker is
        # replaced by a fresh checker; the training worker is never stopped.
        pattern = "^(a+)+$|ok"
        content = "a" * 21 + "!ok"
        spec = {"name": "revalidation", "project": "example", "cwd": "revalidation", "tasks": [{
            "id": "task", "max_retry": 0, "resources": {"gpu": 0, "cpus": 1},
            "cmd": [sys.executable, "-I", "-c", worker_program(content)],
            "artifacts": {"result": {"path": "result.json", "regex": pattern}}}]}
        path = self.root / "revalidation.json"
        path.write_text(json.dumps(spec))
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        fault = StopFirstRegex(self.data("daemon", "status", "--json")["pid"], pattern)
        try:
            batch = self.data("submit", str(path), "--json")["batch_id"]
            fault.check()
            original_job = self.wait(lambda: self.task(batch), lambda r: r["jobs"][0]["status"] == "blocked")
            self.cli("daemon", "drain", "--stop-when-idle")
            self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        finally:
            fault.close()
        summary = self.data("artifact-validations", f"{batch}:task", "--version", "1", "--json")
        assert len(summary["validations"]) == 1
        source = summary["validations"][0]
        frozen = self.data("artifact-validations", f"{batch}:task", "--validation-id", source["validation_id"], "--json")
        assert source["wait_verified"] and not source["passed"]
        assert frozen["validations"][0]["payload"]["checks"][0]["reason_code"] == "regex_timeout"
        before_bytes = (root / "result.json").read_bytes()
        instance = self.data("identity", "--json")["instance_id"]
        def request(rid, settle=False):
            current = self.task(batch)
            return ["request", rid, "--json", "--expect-kind", "task", "--expect-id", f"{batch}:task",
                "--expect-status", "blocked", "--expect-version", "1", "--expect-revision", str(current["batch_revision"]),
                "--expect-instance", instance, "--", "artifact-revalidate", f"{batch}:task",
                "--validation-id", source["validation_id"], *(["--settle"] if settle else []), "--yes"]
        only = self.data(*request("only-revalidate"))["result"]["effect"]
        assert only["artifact_rules_passed"] and not only["settled"]
        assert self.task(batch) == original_job
        args = request("settle-revalidation", settle=True)
        first = self.data(*args)
        again = self.data(*args)
        assert first["result"]["effect"]["settled"] and again["replayed"] and first["result"] == again["result"]
        final = self.task(batch)["jobs"][0]
        assert final["status"] == "done" and final["rc"] == 0 and final["version"] == 1
        assert (root / "runs.txt").read_text() == "run\n" and (root / "result.json").read_bytes() == before_bytes
        assert self.data("artifact-validations", f"{batch}:task", "--validation-id", source["validation_id"], "--json") == frozen
        events = self.data("artifact-revalidations", f"{batch}:task", "--json")
        assert len(events["events"]) == 2 and sum(e["settled"] for e in events["events"]) == 1
        print("PASS: real injected regex timeout, artifact-only recovery, original wait/failure retained, one training run and same-RID replay", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run this acceptance on a Linux compute node, not on a laptop/gateway.")
    # Keep evidence if ownership/stop cannot be proved; do not silently remove
    # a live daemon's state via TemporaryDirectory's unconditional cleanup.
    temporary = tempfile.mkdtemp(prefix="sched-feedback-")
    acceptance = Acceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        acceptance.cli("daemon", "stop")
        health = acceptance.data("daemon", "status", "--json")
        if health["health_state"] != "stopped":
            raise RuntimeError(f"Private acceptance daemon not stopped; retained {temporary}")
    # Only this locally created, stopped fixture is disposable, never production.
    import shutil
    shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
