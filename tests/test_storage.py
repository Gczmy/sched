"""Synthetic state and pure storage decisions; no filesystem helper or workers."""
import copy
import json
from pathlib import Path
import subprocess
import time
import unittest
from unittest import mock

from gsched import allocation, cli, config, resources, state, storage, storage_probe
from gsched.dispatcher import Dispatcher
from gsched.execution_policy import digest
from test_review_cli_state import TempStateCase


def observation(key="1", *, available=100, inodes=100, quota=None):
    return {"filesystem_id": key, "bytes_available": available, "inodes_available": inodes,
            "readonly": False, "quota": quota or {k: {"known": False, "status": "unknown"} for k in ("bytes", "inodes")}}


class StoragePlanTests(unittest.TestCase):
    def setUp(self):
        self.settings = storage.policy({"storage_admission": {"enabled": True, "reserve_gib": 0,
            "reserve_inodes": 10, "control_reserve_gib": 0, "control_reserve_inodes": 5}})

    def plan(self, sample=None, *, request=None, used=None, paths=None, unresolved=None):
        return storage.decide(self.settings, request or {"bytes": 20, "inodes": 20},
            paths or {"/state": ["control"], "/out": ["task"]},
            {"paths": sample or {"/state": observation(), "/out": observation()}}, used or {}, unresolved or [])

    def test_same_filesystem_floors_use_max_and_running_reservation_once(self):
        value = self.plan(used={"1": {"bytes": 30, "inodes": 5}})
        self.assertTrue(value["allowed"])
        self.assertEqual(1, len(value["filesystems"]))
        self.assertEqual({"bytes": 50, "inodes": 35}, value["filesystems"][0]["needed"])
        value = self.plan(used={"1": {"bytes": 81, "inodes": 90}})
        self.assertEqual(["disk_space", "inode_space"], value["reasons"])

    def test_distinct_control_filesystem_does_not_charge_task_declaration(self):
        value = self.plan({"/state": observation("2", available=0, inodes=5), "/out": observation()})
        self.assertTrue(value["allowed"])
        control = next(f for f in value["filesystems"] if f["filesystem_id"] == "2")
        self.assertEqual({"bytes": 0, "inodes": 5}, control["needed"])
        self.settings["control_reserve_inodes"] = 6
        self.assertIn("inode_space", self.plan({"/state": observation("2", inodes=5), "/out": observation()})["reasons"])

    def test_known_user_quota_is_enforced_without_require_flag(self):
        quota = {"bytes": {"known": True, "status": "bounded", "headroom": 19},
                 "inodes": {"known": True, "status": "bounded", "headroom": 29}}
        value = self.plan({"/state": observation(quota=quota), "/out": observation(quota=quota)})
        self.assertFalse(value["allowed"])
        self.assertEqual(["user_quota_bytes", "user_quota_inodes"], value["reasons"])
        self.assertEqual("group_project_remote_not_verified", value["other_quota_scopes"])

    def test_unknown_quota_is_explicit_and_only_required_flag_blocks(self):
        value = self.plan()
        self.assertTrue(value["allowed"])
        self.assertTrue(all(q["known"] is False for q in value["filesystems"][0]["quota"]))
        self.settings["require_user_quota"] = True
        self.assertEqual(["user_quota_bytes_unknown", "user_quota_inodes_unknown"], self.plan()["unknown"])
        quota = {k: {"known": True, "status": "no_user_limit"} for k in ("bytes", "inodes")}
        self.assertTrue(self.plan({"/state": observation(quota=quota), "/out": observation(quota=quota)})["allowed"])

    def test_missing_readonly_inodes_and_legacy_reservations_fail_closed(self):
        sample = {"/state": observation(), "/out": observation(inodes=None)}
        sample["/out"]["readonly"] = True
        value = self.plan(sample, unresolved=["old-running"])
        self.assertFalse(value["allowed"])
        self.assertIn("filesystem_readonly", value["reasons"])
        self.assertIn("inodes_availability_unknown", value["unknown"])
        self.assertIn("running_storage_reservations_unknown", value["unknown"])
        sample["/out"] = {"filesystem_id": None}
        self.assertIn("filesystem_sample_unknown:/out", self.plan(sample)["unknown"])

    def test_config_and_task_request_are_strict_bounded_and_opt_in(self):
        self.assertFalse(storage.policy({})["enabled"])
        self.assertEqual({"bytes": 1, "inodes": 0}, storage.request({"resources": {"disk_gib": 1 / storage.GIB}}))
        for patch in ({"enabled": 1}, {"require_user_quota": "yes"}, {"reserve_gib": -1},
                      {"reserve_gib": float("nan")}, {"reserve_inodes": True}, {"other": 1}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                storage.policy({"storage_admission": patch})
        for value in (True, "1", -1, float("inf"), 2 ** 30 + 1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                storage.request({"resources": {"disk_gib": value}})
        for value in (True, 1.0, -1, 2 ** 63):
            with self.assertRaises(ValueError):
                storage.request({"resources": {"disk_inodes": value}})

    def test_linux_user_quota_units_flags_and_soft_ceiling(self):
        value = storage_probe.Quota(block_hard=100, block_soft=80, space_used=2048,
                                    inode_hard=50, inode_soft=40, inodes_used=10, valid=15)
        decoded = storage_probe.decode_quota(value)
        self.assertEqual(80 * 1024 - 2048, decoded["bytes"]["headroom"])
        self.assertEqual(30, decoded["inodes"]["headroom"])
        value.valid = 3
        decoded = storage_probe.decode_quota(value)
        self.assertTrue(decoded["bytes"]["known"])
        self.assertFalse(decoded["inodes"]["known"])
        value.block_hard = value.block_soft = 0
        self.assertEqual("no_user_limit", storage_probe.decode_quota(value)["bytes"]["status"])

    def test_timeout_retains_one_pending_helper_without_wait_or_spawn_storm(self):
        process = mock.Mock(returncode=None)
        process.communicate.side_effect = subprocess.TimeoutExpired("probe", 5)
        process.poll.return_value = None
        with mock.patch.object(storage, "_pending_probe", None), mock.patch.object(storage.subprocess, "Popen", return_value=process) as create:
            first = storage.probe({"/out": ["task"]})
            second = storage.probe({"/out": ["task"]})
            self.assertEqual({}, first["paths"])
            self.assertEqual({}, second["paths"])
            create.assert_called_once()
            process.kill.assert_called_once()
            process.wait.assert_not_called()


class StorageEvidenceTests(TempStateCase):
    batch = "batch-20260829-000000"

    def setUp(self):
        super().setUp()
        self.cfg["storage_admission"] = {"enabled": True, "reserve_gib": 0, "reserve_inodes": 0,
            "control_reserve_gib": 0, "control_reserve_inodes": 0}
        Path(self.config_path).write_text(json.dumps(self.cfg))
        self.job = self.seed_batch(job_status="pending")
        with state.connect() as conn:
            self.spec = json.loads(conn.execute("SELECT spec FROM tasks").fetchone()[0])

    def capture_observation(self):
        with state.connect() as conn:
            def sample(paths):
                return {"schema_version": 1, "observed_at": time.time(),
                        "paths": {p: observation() for p in paths}}
            with mock.patch.object(storage, "probe", side_effect=sample):
                report = storage.capture(conn, self.cfg, state.get_job(conn, self.job), self.spec)
        storage.publish({self.job: report})
        return report

    def query(self):
        rc, out, err = self.capture(cli.main, ["storage-explain", f"{self.batch}:task", "--json"])
        self.assertEqual(0, rc, err)
        return json.loads(out)

    def test_query_reuses_pure_decision_without_probes_migrations_or_writes(self):
        report = self.capture_observation()
        revision = self.batch_revision()
        with mock.patch.object(storage, "probe", side_effect=AssertionError("gateway probe")), \
             mock.patch.object(state, "init_db", side_effect=AssertionError("migration")), \
             mock.patch.object(Dispatcher, "__init__", side_effect=AssertionError("daemon")), \
             mock.patch.object(resources, "write_private_json", side_effect=AssertionError("write")):
            output = self.query()
        self.assertTrue(output["allowed"])
        self.assertEqual(report["filesystems"], output["filesystems"])
        self.assertEqual("sched-storage-explain-v1", output["contract"])
        self.assertEqual("none", output["effect"])
        self.assertFalse(output["admission_granted"])
        self.assertEqual(revision, self.batch_revision())

    def test_missing_stale_modified_decision_configuration_and_running_lag(self):
        self.assertIsNone(self.query()["allowed"])
        report = self.capture_observation()
        report["job_id"] = "different-job"
        storage.publish({self.job: report})
        self.assertIn("storage_identity_mismatch", self.query()["unknown"])
        report = self.capture_observation()
        report["observed_at"] -= 31
        storage.publish({self.job: report})
        self.assertIn("storage_observation_stale_or_future", self.query()["unknown"])
        report = self.capture_observation()
        report["filesystems"][0]["needed"]["bytes"] += 1
        storage.publish({self.job: report})
        self.assertIn("storage_observation_decision_mismatch", self.query()["unknown"])
        self.capture_observation()
        self.cfg["cpus_total"] = 12
        Path(self.config_path).write_text(json.dumps(self.cfg))
        self.assertIn("storage_configuration_or_spec_lag", self.query()["unknown"])
        self.capture_observation()
        self.seed_batch(batch_id="running", job_status="running")
        self.assertIn("storage_running_reservations_changed", self.query()["unknown"])

    def test_disabled_policy_does_not_read_any_evidence(self):
        with state.connect() as conn, mock.patch.object(resources, "read_private_json", side_effect=AssertionError("observation")):
            value = storage.explain(conn, {}, state.get_job(conn, self.job), self.spec)
        self.assertTrue(value["allowed"])
        self.assertFalse(value["enabled"])

    def test_old_schema_is_explicit_unknown_without_migration_or_probe(self):
        with state.connect() as conn:
            conn.execute("PRAGMA user_version=15")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migration")), \
             mock.patch.object(storage, "probe", side_effect=AssertionError("probe")):
            value = self.query()
        self.assertIsNone(value["allowed"])
        self.assertEqual(["storage_allocation_schema_unavailable"], value["unknown"])

    def test_tick_probe_budget_exhaustion_is_unknown_without_another_helper(self):
        with state.connect() as conn, mock.patch.object(storage, "probe", side_effect=AssertionError("budget exhausted")):
            value = storage.capture(conn, self.cfg, state.get_job(conn, self.job), self.spec,
                                    cache={"_deadline": time.monotonic() - 1})
        self.assertFalse(value["allowed"])
        self.assertTrue(value["unknown"])
        self.assertEqual("tick_probe_budget_exhausted", value["inputs"]["sample"]["reason"])

    def test_allocation_freezes_filesystems_and_old_positive_reservation_is_unknown(self):
        self.spec["resources"].update(disk_gib=1 / storage.GIB, disk_inodes=2)
        with state.connect() as conn:
            conn.execute("UPDATE tasks SET spec=?", (json.dumps(self.spec),))
            state.update_job(conn, self.job, status="running", pgid=None, rc=None)
            used, unknown = storage.running_reservations(conn)
            self.assertEqual([self.job], unknown)
            dispatcher = Dispatcher.__new__(Dispatcher)
            dispatcher.fake = True
            dispatcher._storage_launch_observation = {"allowed": True, "job_id": self.job,
                "spec_sha256": digest(self.spec), "filesystems": [{"filesystem_id": "1", "roles": ["task"]}]}
            conn.execute("BEGIN IMMEDIATE") if not conn.in_transaction else None
            allocation.reserve(conn, self.job, self.spec, dispatcher)
            used, unknown = storage.running_reservations(conn)
        self.assertEqual([], unknown)
        self.assertEqual({"1": {"bytes": 1, "inodes": 2}}, used)

    def launch_dispatcher(self):
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher._task_has_unresolved_launch_marker = mock.Mock(return_value=False)
        dispatcher._exact_dependencies_successful = mock.Mock(return_value=True)
        dispatcher._task_dependencies_successful = mock.Mock(return_value=True)
        dispatcher._load_task_launch_binding = mock.Mock(return_value=(self.spec, "p"))
        dispatcher._job_uses_native_exec = mock.Mock(return_value=False)
        dispatcher._read_gpu_policy = mock.Mock(return_value=self.cfg)
        dispatcher.log_line = mock.Mock()
        dispatcher._release_in_tx = mock.Mock()
        dispatcher._prepare_launch_marker = mock.Mock(side_effect=AssertionError("marker/cleanup reached"))
        return dispatcher

    def test_final_rejection_precedes_marker_cleanup_and_worker_launch(self):
        dispatcher = self.launch_dispatcher()
        with state.connect() as conn, mock.patch.object(storage, "capture", return_value={"allowed": False}), \
             mock.patch.object(storage, "publish"):
            self.assertFalse(dispatcher._launch_job(conn, state.get_job(conn, self.job), 0))
            self.assertEqual("pending", state.get_job(conn, self.job)["status"])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM allocations").fetchone()[0])
        dispatcher._release_in_tx.assert_called_once()

    def test_unreadable_hot_configuration_pauses_instead_of_failing_job(self):
        dispatcher = self.launch_dispatcher()
        dispatcher._read_gpu_policy.side_effect = RuntimeError("hot config unavailable")
        with state.connect() as conn, mock.patch.object(storage, "publish"), \
             mock.patch.object(storage, "capture", side_effect=AssertionError("invalid configuration")):
            self.assertFalse(dispatcher._launch_job(conn, state.get_job(conn, self.job), 0))
            self.assertEqual("pending", state.get_job(conn, self.job)["status"])
        dispatcher._release_in_tx.assert_called_once()

    def test_config_validation_and_path_bounds(self):
        invalid = copy.deepcopy(self.cfg)
        invalid["storage_admission"]["enabled"] = "true"
        with self.assertRaises(config.ConfigError):
            config._validate(invalid, Path(self.config_path))
        self.spec["artifacts"] = {str(i): {"path": f"d{i}/result"} for i in range(129)}
        with self.assertRaises(ValueError):
            storage.targets(self.cfg, self.spec, "p")
