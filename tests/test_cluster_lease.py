"""Pure lease decisions and private synthetic DB; no Slurm or worker execution."""
import copy
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
import unittest
from unittest import mock

from gsched import cli, cluster_lease as lease, daemon, lease_probe, state
from gsched.dispatcher import Dispatcher
from gsched.integration import instance_id
from test_review_cli_state import TempStateCase


def context():
    return {"pid": 100, "start_token": "proc:123", "physical_host": "compute-a", "uid": 1234,
            "affinity": [0, 1], "cpus_allowed_list": "0-1",
            "cgroups": [{"hierarchy": "0", "controllers": "", "path": "/slurm/job_42/step_0"}]}


def sample():
    return {"known": True, "observed_at": time.time(), "job": {"job_id": "42", "states": ["RUNNING"],
            "user_id": 1234, "start_time": int(time.time()) - 60, "end_time": int(time.time()) + 3600,
            "restart_count": 0, "nodes": "compute-a", "hosts": ["compute-a"], "cpus": 2}}


def origin(**settings):
    return {**context(), "started_at": time.time(), "slurm_environment": {"SLURM_JOB_ID": "42", "SLURM_STEP_ID": "0"},
            "policy": lease.policy({"lease_validation": settings})}


class LeaseDecisionTests(unittest.TestCase):
    def test_verified_running_job_and_matching_job_step_are_observational_valid(self):
        value = lease.decide(origin(), context(), sample())
        self.assertEqual("valid", value["allocation_state"])
        self.assertTrue(value["dispatch_allowed"])
        self.assertFalse(value["hard_isolation"])

    def test_generic_system_cgroup_is_unknown_not_slurm_membership(self):
        initial = origin()
        initial["cgroups"][0]["path"] = "/system.slice/slurmstepd.scope/system"
        current = {k: copy.deepcopy(initial[k]) for k in context()}
        value = lease.decide(initial, current, sample())
        self.assertEqual("unknown", value["allocation_state"])
        self.assertFalse(value["dispatch_allowed"])
        self.assertIn("job_cgroup_membership_not_verified", value["unknown"])
        initial["policy"]["unknown_policy"] = "allow"
        self.assertTrue(lease.decide(initial, current, sample())["dispatch_allowed"])

    def test_step_and_hostname_are_not_inferred_from_env(self):
        initial = origin()
        initial["slurm_environment"]["SLURM_STEP_ID"] = "1"
        observation = sample()
        observation["job"]["hosts"] = ["alias-only"]
        value = lease.decide(initial, context(), observation)
        self.assertEqual({"step_cgroup_membership_not_verified", "slurm_node_hostname_not_verified"}, set(value["unknown"]))
        self.assertFalse(value["dispatch_allowed"])

    def test_missing_cancelled_or_replaced_job_cannot_resume_after_later_running(self):
        initial, current, observed = origin(unknown_policy="allow"), context(), sample()
        bound = lease.binding(observed["job"])
        cases = [{"known": True, "observed_at": time.time(), "missing": True, "job_id": "42"}]
        for key, value in (("states", ["CANCELLED"]), ("start_time", int(time.time()) + 30),
                           ("restart_count", 1), ("cpus", 120), ("user_id", 999), ("end_time", 1)):
            changed = copy.deepcopy(observed)
            changed["job"][key] = value
            cases.append(changed)
        for observation in cases:
            with self.subTest(observation=observation):
                rejected = lease.decide(initial, current, observation, frozen_binding=bound)
                self.assertEqual("invalid", rejected["allocation_state"])
                self.assertFalse(rejected["dispatch_allowed"])
                later = lease.decide(initial, current, observed, frozen_binding=bound, invalid_latched=True)
                self.assertFalse(later["dispatch_allowed"])
                self.assertIn("allocation_invalid_latched", later["reasons"])

    def test_kernel_identity_or_affinity_cgroup_drift_is_invalid(self):
        for key, changed in (("pid", 101), ("start_token", "proc:new"), ("physical_host", "compute-b"),
                             ("uid", 999), ("affinity", [2]), ("cpus_allowed_list", "2"), ("cgroups", [])):
            current = context()
            current[key] = changed
            with self.subTest(key=key):
                value = lease.decide(origin(), current, sample())
                self.assertFalse(value["dispatch_allowed"])
                self.assertIn("kernel_" + key + "_changed", value["reasons"])

    def test_controller_failure_stale_or_wrong_id_is_unknown(self):
        for observed in ({"known": False}, {**sample(), "observed_at": time.time() - 46},
                         {**sample(), "job": {**sample()["job"], "job_id": "99"}}):
            with self.subTest(observed=observed):
                value = lease.decide(origin(), context(), observed)
                self.assertEqual("unknown", value["allocation_state"])
                self.assertFalse(value["dispatch_allowed"])

    def test_standalone_auto_compatibility_and_malformed_slurm_enforcement(self):
        initial = origin()
        initial["slurm_environment"] = {}
        self.assertTrue(lease.decide(initial, context(), {"known": False})["dispatch_allowed"])
        initial["slurm_environment"]["SLURM_JOB_ID"] = None
        self.assertFalse(lease.decide(initial, context(), {"known": False})["dispatch_allowed"])
        initial["policy"]["mode"] = "observe"
        value = lease.decide(initial, context(), {"known": False}, invalid_latched=True)
        self.assertTrue(value["dispatch_allowed"])
        self.assertEqual("invalid", value["allocation_state"])

    def test_policy_is_cold_strict_and_environment_is_whitelisted(self):
        for patch in (None, {"mode": True}, {"mode": []}, {"unknown_policy": "unlimited"},
                      {"interval_sec": True}, {"interval_sec": 0}, {"interval_sec": 31}, {"secret": "x"}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                lease.policy({"lease_validation": patch})
        with mock.patch.dict(os.environ, {"SLURM_JOB_ID": "42", "SLURM_STEP_ID": "0", "SCHED_TOKEN": "SECRET", "SLURM_CLUSTER_NAME": "bad\nvalue"}, clear=True):
            recorded = lease.slurm_environment()
        self.assertEqual({"SLURM_JOB_ID": "42", "SLURM_STEP_ID": "0", "SLURM_CLUSTER_NAME": None}, recorded)
        self.assertNotIn("SECRET", json.dumps(recorded))

    def test_slurm_json_normalization_is_bounded_and_diagnostics_unknown(self):
        raw = {"jobs": [{"job_id": 42, "job_state": ["RUNNING"], "nodes": "compute-a", "user_id": 1234,
                        "start_time": {"set": True, "infinite": False, "number": 1}, "end_time": 9999999999,
                        "cpus": {"set": True, "infinite": False, "number": 2}, "restart_cnt": 0}]}
        with mock.patch.object(lease_probe, "command", return_value=json.dumps(raw)) as query:
            result = lease_probe.sample("42")
            self.assertTrue(result["known"])
            self.assertEqual(2, result["job"]["cpus"])
            query.assert_called_once_with(["--json", "show", "job", "42"])
        for raw in ({"jobs": [], "errors": [], "warnings": []}, {"jobs": [], "errors": [{"error": "missing"}]},
                    {"jobs": [], "warnings": ["unknown"]}, {"jobs": [{}]}, {"jobs": [] ,"errors": "error"}):
            with mock.patch.object(lease_probe, "command", return_value=json.dumps(raw)):
                value = lease_probe.sample("42")
                self.assertEqual(not raw.get("errors") and not raw.get("warnings") and raw.get("jobs") == [], value["known"])
        with mock.patch.object(lease_probe, "command", side_effect=subprocess.TimeoutExpired("scontrol", 2)):
            self.assertFalse(lease_probe.sample("42")["known"])

    def test_timeout_retains_single_helper_without_unbounded_wait(self):
        child = mock.Mock(returncode=None)
        child.communicate.side_effect = subprocess.TimeoutExpired("probe", 5)
        child.poll.return_value = None
        with mock.patch.object(lease, "_pending_probe", None), mock.patch.object(lease.subprocess, "Popen", return_value=child) as spawn:
            self.assertFalse(lease.probe("42")["known"])
            self.assertFalse(lease.probe("42")["known"])
            spawn.assert_called_once()
            child.kill.assert_called_once()
            child.wait.assert_not_called()


class LeaseEvidenceTests(TempStateCase):
    def setUp(self):
        super().setUp()
        self.current = context()
        self.observed = sample()
        self.owner = {"schema_version": 1, "lease_id": "a" * 32, **{k: self.current[k] for k in ("pid", "start_token", "physical_host")}}
        for target, value in (("kernel_context", lambda: copy.deepcopy(self.current)),
                              ("slurm_environment", lambda: {"SLURM_JOB_ID": "42", "SLURM_STEP_ID": "0"}),
                              ("probe", lambda jid: copy.deepcopy(self.observed))):
            patcher = mock.patch.object(lease, target, side_effect=value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.health = {"query_host": "compute-a", "process_state": "running"}
        for target, fn in (("_read_lease_owner", lambda: copy.deepcopy(self.owner)),
                           ("health_snapshot", lambda: copy.deepcopy(self.health))):
            patcher = mock.patch.object(daemon, target, side_effect=fn)
            patcher.start()
            self.addCleanup(patcher.stop)

    def monitor(self, **settings):
        return lease.Monitor({"lease_validation": settings}, self.owner)

    def report(self, **options):
        with state.connect() as conn:
            return lease.query(conn, **options)

    def test_origin_check_and_exit_are_retained_and_immutable(self):
        monitor = self.monitor()
        first = self.report(lease_id=self.owner["lease_id"])["leases"][0]
        self.assertEqual("valid", first["allocation_state"])
        monitor.finish("fixture_finished")
        monitor.finish("duplicate")
        after = self.report(lease_id=self.owner["lease_id"])["leases"][0]
        self.assertEqual(first["origin"], after["origin"])
        self.assertEqual("unknown", after["allocation_state"])
        self.assertEqual(["check", "exit"], [e["kind"] for e in after["events"]])
        self.assertEqual("daemon_lease_release_not_worker_wait", after["recorded_exit"]["data"]["semantics"])
        for table in ("daemon_leases", "daemon_lease_events"):
            for sql in (f"DELETE FROM {table}", f"UPDATE {table} SET payload='{{}}'"):
                with self.subTest(sql=sql), state.connect() as conn, self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(sql)

    def test_controller_invalid_is_sticky_and_incident_is_once(self):
        monitor = self.monitor()
        self.observed["job"]["states"] = ["CANCELLED"]
        self.assertFalse(monitor.update(force=True))
        self.observed = sample()
        self.assertFalse(monitor.update(force=True))
        self.assertFalse(monitor.update(force=True))
        with state.connect() as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM incidents WHERE kind='lease_invalid'").fetchone()[0])
        self.assertEqual("42", self.report()["leases"][0]["origin"]["initial_slurm_binding"]["job_id"])

    def test_failed_invalid_audit_is_retried_without_forgetting_latch(self):
        monitor = self.monitor()
        self.observed["job"]["states"] = ["CANCELLED"]
        with mock.patch.object(monitor, "_incident", side_effect=sqlite3.OperationalError("fixture write failure")), self.assertRaises(sqlite3.OperationalError):
            monitor.update(force=True)
        self.assertTrue(monitor.invalid_latched)
        self.assertFalse(monitor.update())
        self.assertFalse(monitor.update())
        with state.connect() as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM incidents WHERE kind='lease_invalid'").fetchone()[0])

    def test_initial_invalid_is_recorded_once_and_new_lease_does_not_rewrite_old(self):
        self.observed["job"]["states"] = ["COMPLETED"]
        first = self.monitor()
        first.update(force=True)
        first.finish("old_end")
        original = self.report(lease_id="a" * 32)["leases"][0]["origin"]
        self.owner["lease_id"] = "b" * 32
        self.observed = sample()
        second = self.monitor()
        self.assertTrue(second.update(force=True))
        self.assertEqual(original, self.report(lease_id="a" * 32)["leases"][0]["origin"])
        self.assertEqual("unknown", self.report(lease_id="a" * 32)["leases"][0]["allocation_state"])
        with state.connect() as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM incidents WHERE kind='lease_invalid'").fetchone()[0])

    def test_birth_and_first_check_are_atomic_and_binding_mismatch_cannot_publish(self):
        with mock.patch.object(lease, "event", side_effect=ValueError("record failure")), self.assertRaises(ValueError):
            self.monitor()
        self.assertEqual([], self.report()["leases"])
        self.owner["start_token"] = "proc:forged"
        with self.assertRaises(ValueError):
            self.monitor()
        self.assertEqual([], self.report()["leases"])

    def test_stale_pid_reused_or_unknown_local_owner_never_displays_valid(self):
        self.monitor()
        for process_state in ("stopped", "unknown"):
            self.health["process_state"] = process_state
            self.assertEqual("unknown", self.report()["leases"][0]["allocation_state"])
        self.health["process_state"] = "running"
        with mock.patch.object(lease.time, "time", return_value=time.time() + 46):
            self.assertEqual("unknown", self.report()["leases"][0]["allocation_state"])

    def test_query_paging_is_live_bounded_and_chain_bound(self):
        monitor = self.monitor()
        monitor.update(force=True)
        first = self.report(lease_id=self.owner["lease_id"], limit=1)["leases"][0]
        self.assertTrue(first["events_truncated"])
        self.assertEqual(1, first["next_seq"])
        second = self.report(lease_id=self.owner["lease_id"], after_seq=1, limit=1)["leases"][0]
        self.assertEqual(2, second["events"][0]["seq"])
        self.assertFalse(second["events_truncated"])
        for args in ({"limit": True}, {"limit": 0}, {"limit": 101}, {"after_seq": 1},
                     {"after_seq": -1}, {"lease_id": "bad"}, {"lease_id": "a" * 32, "cursor": "a" * 32}):
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.report(**args)
        with state.connect() as conn:
            for i in range(16):
                lease.event(conn, self.owner["lease_id"], "fixture", {"padding": "x" * 240000})
        with self.assertRaises(ValueError):
            self.report(lease_id=self.owner["lease_id"], limit=100)

    def test_queries_use_recorded_facts_only_and_default_health_contract_unchanged(self):
        self.monitor()
        with mock.patch.object(lease, "probe", side_effect=AssertionError("query probed Slurm")), mock.patch.object(lease, "kernel_context", side_effect=AssertionError("query sampled resources")):
            rc, output, error = self.capture(cli.main, ["daemon-lease", "--json"])
            self.assertEqual(0, rc, error)
            self.assertEqual("sched-daemon-lease-v1", json.loads(output)["contract"])
            rc, output, error = self.capture(cli.main, ["daemon", "status", "--json"])
            self.assertEqual(0, rc, error)
            self.assertNotIn("lease", json.loads(output))
            rc, output, error = self.capture(cli.main, ["daemon", "status", "--json", "--include-lease"])
            self.assertEqual(0, rc, error)
            self.assertEqual(self.owner["lease_id"], json.loads(output)["lease"]["leases"][0]["origin"]["lease_id"])

    def test_v16_read_is_not_migrated_or_backfilled_and_writer_migration_is_empty(self):
        with state.connect() as conn:
            for name in ("daemon_lease_immutable", "daemon_lease_retained", "daemon_lease_event_immutable", "daemon_lease_event_retained"):
                conn.execute(f"DROP TRIGGER {name}")
            conn.execute("DROP TABLE daemon_lease_events")
            conn.execute("DROP TABLE daemon_leases")
            conn.execute("PRAGMA user_version=16")
            expected = instance_id(conn)
        rc, output, error = self.capture(cli.main, ["daemon-lease", "--json"])
        self.assertEqual(0, rc, error)
        self.assertEqual("migration_required", json.loads(output)["reason"])
        state.set_read_only(True)
        try:
            with state.connect() as conn:
                self.assertEqual(16, conn.execute("PRAGMA user_version").fetchone()[0])
        finally:
            state.set_read_only(False)
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertEqual(expected, instance_id(conn))
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM daemon_leases").fetchone()[0])

    def test_final_lease_pause_precedes_marker_and_allocation(self):
        jid = self.seed_batch(job_status="pending")
        dispatcher = Dispatcher(self.cfg, fake=True)
        self.addCleanup(dispatcher.log.close)
        dispatcher._cluster_lease = mock.Mock()
        dispatcher._cluster_lease.update.return_value = False
        with state.connect() as conn, mock.patch.object(dispatcher, "_prepare_launch_marker") as marker, mock.patch.object(dispatcher.executor, "launch") as launch:
            self.assertFalse(dispatcher._launch_job(conn, state.get_job(conn, jid), None))
            marker.assert_not_called()
            launch.assert_not_called()
            self.assertEqual("pending", state.get_job(conn, jid)["status"])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM allocations").fetchone()[0])

    def test_final_lease_drift_after_fingerprint_still_precedes_claim_and_cleanup(self):
        jid = self.seed_batch(job_status="pending")
        dispatcher = Dispatcher(self.cfg, fake=True)
        self.addCleanup(dispatcher.log.close)
        dispatcher._cluster_lease = mock.Mock()
        dispatcher._cluster_lease.update.side_effect = [True, False]
        # This narrow fixture exercises the later lease fence, not CPU source
        # resolution. The CPU gate has its own real synthetic-context tests.
        dispatcher._cpu_launch_allowed = mock.Mock(return_value=True)
        dispatcher._ready_fingerprint_snapshots = {jid: ("new-fp", None, None)}
        with state.connect() as conn, mock.patch.object(dispatcher, "_prepare_launch_marker", return_value=False), mock.patch.object(dispatcher, "_clean_stale_artifacts") as cleanup, mock.patch.object(dispatcher.executor, "launch") as launch:
            self.assertFalse(dispatcher._launch_job(conn, state.get_job(conn, jid), None))
            dispatcher._cpu_launch_allowed.assert_called_once()
            cleanup.assert_not_called()
            launch.assert_not_called()
            self.assertEqual("pending", state.get_job(conn, jid)["status"])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM allocations").fetchone()[0])

    def test_missing_state_query_never_initializes_it(self):
        missing = Path(self.tmp.name) / "never-created"
        with mock.patch.dict(os.environ, {"SCHED_STATE": str(missing)}):
            rc, _, _ = self.capture(cli.main, ["daemon-lease", "--json"])
            self.assertEqual(1, rc)
            self.assertFalse(missing.exists())

    def test_replaced_owner_cannot_remove_successor_or_write_exit_for_it(self):
        monitor = self.monitor()
        dispatcher = Dispatcher(self.cfg, fake=True)
        self.addCleanup(dispatcher.log.close)
        dispatcher._lease_owner = self.owner
        dispatcher._cluster_lease = monitor
        with mock.patch.object(dispatcher, "_remove_exact_lock", return_value=False):
            dispatcher._cleanup_lock()
        self.assertIsNone(self.report(lease_id=self.owner["lease_id"])["leases"][0]["recorded_exit"])

    def test_birth_precedes_ready_and_publication_failure_retains_exit(self):
        dispatcher = Dispatcher(self.cfg, fake=True)
        self.addCleanup(dispatcher.log.close)
        self.current.update(pid=os.getpid())
        def failed_ready():
            with state.connect() as conn:
                self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM daemon_leases").fetchone()[0])
                self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM daemon_lease_events").fetchone()[0])
            raise OSError("fixture ready publication failure")
        with mock.patch("socket.gethostname", return_value="compute-a"), mock.patch.object(dispatcher, "_proc_start_time", return_value=self.current["start_token"]), mock.patch.object(dispatcher, "_touch_heartbeat", side_effect=failed_ready), self.assertRaises(OSError):
            dispatcher.acquire_lock()
        recorded = self.report()["leases"][0]
        self.assertEqual("startup_publication_failed_not_worker_wait", recorded["recorded_exit"]["data"]["reason"])
        self.assertIsNone(dispatcher._lease_owner)


if __name__ == "__main__":
    unittest.main()
