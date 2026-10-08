"""Pending-only cancellation CPU/CLI acceptance, isolated Linux compute state."""
import json
from pathlib import Path
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import time

from run_exact_dependencies_accept import ExactAcceptance


class GroupAcceptance(ExactAcceptance):
    def facts(self, batch, *tasks):
        selectors = [{"task_id": task, "version": version} for task, version in tasks]
        return self.data("task-facts", batch, "--tasks-json", json.dumps(selectors), "--json")

    def request_group(self, rid, facts):
        return ["request", rid, "--json", "--expect-kind", "batch", "--expect-id", facts["batch_id"],
                "--expect-status", facts["batch_status"], "--expect-revision", str(facts["batch_revision"]),
                "--expect-instance", facts["instance_id"], "--", "cancel-pending", facts["batch_id"],
                "--tasks-json", json.dumps([task["binding"] for task in facts["tasks"]]), "--yes"]

    def execute_fault(self, args, *, after_commit):
        # The actual CLI is the only state entry point. A test-only wrapper
        # stops its own process at a known transaction/reply window. No daemon,
        # worker, arbitrary PID, production path or scheduler file is changed.
        before = """
from gsched import pending_cancel
original = pending_cancel.perform
def hold(*args, **kwargs):
    result = original(*args, **kwargs)
    os.write(2, b'GROUP_FAULT_WINDOW\\n')
    os.kill(os.getpid(), signal.SIGSTOP)
    return result
pending_cancel.perform = hold
raise SystemExit(cli.main(sys.argv[1:]))
"""
        after = """
with contextlib.redirect_stdout(io.StringIO()):
    code = cli.main(sys.argv[1:])
if code:
    raise SystemExit(code)
os.write(2, b'GROUP_FAULT_WINDOW\\n')
os.kill(os.getpid(), signal.SIGSTOP)
"""
        program = "import contextlib,io,os,signal,sys\nfrom gsched import cli\n" + (after if after_commit else before)
        child = subprocess.Popen([sys.executable, "-c", program, *map(str, args)],
                                 cwd=self.root, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        seen = False
        try:
            deadline = time.monotonic() + 30
            diagnostic = []
            while time.monotonic() < deadline:
                readable, _, _ = select.select([child.stderr], [], [], 0.5)
                if readable:
                    line = child.stderr.readline()
                    diagnostic.append(line)
                    if line.strip() == "GROUP_FAULT_WINDOW":
                        seen = True
                        break
                    if not line and child.poll() is not None:
                        break
            assert seen, (child.poll(), diagnostic)
            child.kill()  # Exact test-owned CLI process, not its lease/shell.
            output, error = child.communicate(timeout=10)
            assert child.returncode == -signal.SIGKILL, (child.returncode, output, error)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
            child.stdout.close()
            child.stderr.close()

    def run(self):
        self.cli("daemon", "drain")
        batch = self.submit("group", [self.worker(task, "group/" + task) for task in ("a", "b", "c", "d")], failure_policy="continue_independent")
        facts = self.facts(batch, ("a", 1), ("c", 1))
        assert facts["truncated"] is False and facts["cancel_ready"] is None
        assert all(task["recorded_never_started"] for task in facts["tasks"])
        request = self.request_group("atomic-group", facts)
        result = self.data(*request)
        assert result["result"]["effect"]["count"] == 2
        assert all(self.job(batch, task)["jobs"][0]["status"] == "cancelled" for task in ("a", "c"))
        assert all(self.job(batch, task)["jobs"][0]["status"] == "pending" for task in ("b", "d"))
        revision = self.data("batch-policy", batch, "--json")["batch_revision"]
        assert self.data(*request)["result"] == result["result"]
        assert self.data("batch-policy", batch, "--json")["batch_revision"] == revision
        self.cli(*self.request_group("stale-group", facts), expect=65)
        print("PASS: one batch CAS cancels exactly A/C atomically, keeps B/D pending and same RID replays without a second revision change", flush=True)

        rollback = self.submit("rollback", [self.worker(task, "rollback/" + task) for task in ("a", "b")])
        rollback_facts = self.facts(rollback, ("a", 1), ("b", 1))
        rollback_request = self.request_group("before-commit", rollback_facts)
        self.execute_fault(rollback_request, after_commit=False)
        assert self.data("request-status", "before-commit", "--json")["phase"] == "not_found"
        assert all(self.job(rollback, task)["jobs"][0]["status"] == "pending" for task in ("a", "b"))
        assert self.data("batch-policy", rollback, "--json")["batch_revision"] == rollback_facts["batch_revision"]
        self.data(*rollback_request)  # Same logical RID/bytes, previous transaction rolled back.
        print("PASS: kill of exact test CLI before commit rolls back both jobs/RID/revision; same RID completes one atomic group", flush=True)

        reply = self.submit("lost-reply", [self.worker(task, "lost-reply/" + task) for task in ("a", "b")])
        reply_request = self.request_group("after-commit", self.facts(reply, ("a", 1), ("b", 1)))
        self.execute_fault(reply_request, after_commit=True)
        receipt = self.data("request-status", "after-commit", "--json")
        assert receipt["phase"] == "done" and receipt["code"] == 0 and receipt["result"]["effect"]["count"] == 2
        revision = self.data("batch-policy", reply, "--json")["batch_revision"]
        assert self.data(*reply_request)["result"] == receipt["result"]
        assert self.data("batch-policy", reply, "--json")["batch_revision"] == revision
        print("PASS: interrupted reply after commit is recovered from original durable RID; replay never recancels", flush=True)

        race = self.submit("race", [self.worker(task, "race/" + task) for task in ("a", "b")])
        race_facts = self.facts(race, ("a", 1), ("b", 1))
        children = []
        try:
            for rid in ("race-one", "race-two"):
                children.append(subprocess.Popen([sys.executable, "-m", "gsched.cli", *self.request_group(rid, race_facts)],
                                                 cwd=self.root, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
            codes = []
            for child in children:
                output, error = child.communicate(timeout=30)
                codes.append(child.returncode)
                assert child.returncode in (0, 65), (child.returncode, output, error)
            assert sorted(codes) == [0, 65]
            assert all(self.job(race, task)["jobs"][0]["status"] == "cancelled" for task in ("a", "b"))
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)
                child.stdout.close()
                child.stderr.close()
        print("PASS: concurrent different RIDs with one source snapshot yield one commit and one conflict, not partial cancellation", flush=True)

        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "d"), lambda r: r["jobs"][0]["status"] == "done")
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        for task in ("a", "c"):
            assert not (self.root / "group" / task / "runs.txt").exists()
        for task in ("b", "d"):
            assert (self.root / "group" / task / "runs.txt").read_text() == "run\n"
        for directory in ("rollback", "lost-reply", "race"):
            assert not any((self.root / directory / task / "runs.txt").exists() for task in ("a", "b"))
        print("PASS: actual private CPU dispatch runs untouched B/D once and never starts any cancelled member", flush=True)

        history = self.submit("history", [self.worker("a", "history/a", fails_until_allowed=True)], failure_policy="continue_independent")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(history, "a"), lambda r: r["jobs"][0]["status"] == "blocked")
        self.cli("daemon", "drain", "--stop-when-idle")
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "stopped")
        self.cli("retry", f"{history}:a")
        historical = self.facts(history, ("a", 1))
        assert not historical["tasks"][0]["recorded_never_started"]
        self.cli(*self.request_group("history-refusal", historical), expect=65)
        assert self.job(history, "a")["jobs"][0]["status"] == "pending"
        assert (self.root / "history/a/runs.txt").read_text() == "run\n"
        print("PASS: real failed CPU attempt reset to pending by retry cannot be reclassified as never started", flush=True)


def main():
    if sys.platform != "linux":
        raise SystemExit("Run on a Linux compute node, not a laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-pending-group-")
    acceptance = GroupAcceptance(Path(temporary))
    try:
        acceptance.run()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private acceptance daemon not stopped; retained {temporary}")
        shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
