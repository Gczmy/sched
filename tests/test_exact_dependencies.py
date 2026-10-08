"""Frozen dependency selectors on disposable fixtures; no real execution."""
import json
import os
from pathlib import Path
import sqlite3
from unittest import mock

from gsched import cli, dependencies, state
from gsched.dispatcher import Dispatcher
from gsched.integration import instance_id
from gsched.schema import SchemaError, validate_batch, validate_persisted_dependencies
from test_review_cli_state import TempStateCase
from test_batch_failure_policy import seed_attempt


class ExactDependencyTests(TempStateCase):
    def selector(self, batch="source-1", task="task", version=1):
        with state.connect() as conn:
            identity = instance_id(conn)
        return [{"instance_id": identity, "batch_id": batch,
                 "tasks": [{"task_id": task, "version": version}]}]

    def downstream(self, batch="child-1", *, exact=None, names=None):
        with state.connect() as conn:
            state.insert_batch(conn, batch, batch, "mix", names or [], None, self.tmp.name, {},
                               project="p", exact_dependencies=exact)
            spec = {"id": "task", "cmd": ["/bin/true"], "cwd_abs": self.tmp.name, "artifacts": {}, "resources": {"gpu": 0}}
            state.insert_task(conn, batch, "task", 1, spec, 0, "p")
            state.insert_job(conn, batch + "-task-v1", batch, "task", 1, "child-fp", None, "p")
        return batch

    def dispatcher(self):
        result = Dispatcher.__new__(Dispatcher)
        result.cfg = self.cfg
        result.host_dir = state.host_dir()
        result.log_line = mock.Mock()
        return result

    def query(self, batch="child-1", *args):
        code, out, err = self.capture(cli.main, ["batch-dependencies", batch, *args, "--json"])
        self.assertEqual(0, code, err)
        return json.loads(out)

    def batch_status(self, batch="child-1"):
        with state.connect() as conn:
            return state.get_batch(conn, batch)["status"]

    def test_input_requires_complete_explicit_selectors(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        good = self.selector()
        spec = {"name": "example", "project": "p", "depends_on_exact": good,
                "tasks": [{"id": "t", "cmd": ["/bin/true"], "resources": {"gpu": 0}}]}
        self.assertEqual(good, validate_batch(spec, self.cfg)["depends_on_exact"])
        invalid = [None, False, {}, ["source"], [{"batch_id": "source-1"}],
                   [dict(good[0], tasks=[])], [dict(good[0], instance_id="other")],
                   [dict(good[0], tasks=[{"task_id": "task", "version": "latest"}])],
                   [dict(good[0], tasks=[{"task_id": "task", "version": True}])],
                   [dict(good[0], tasks=good[0]["tasks"] * 2)], good * 2,
                   [dict(good[0], latest=True)]]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(SchemaError):
                validate_batch(dict(spec, depends_on_exact=value), self.cfg)

    def test_name_is_not_resolved_in_exact_mode_and_wrong_instance_fails(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        for selected in (self.selector("source"), self.selector(version=2), self.selector(task="absent"),
                         [dict(self.selector()[0], instance_id="0" * 32)]):
            with self.subTest(selected=selected), state.connect() as conn, self.assertRaises(SchemaError):
                validate_persisted_dependencies(conn, "child", [], selected)

    def test_frozen_binding_and_passive_query_preserve_revision_and_status_schema(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        self.downstream(exact=self.selector())
        before = self.batch_revision("child-1")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("read migrated")), \
             mock.patch("gsched.dispatcher.check_declared_artifacts", side_effect=AssertionError("read files")):
            facts = self.query()
        fact = facts["dependencies"][0]
        self.assertEqual("exact_task", fact["kind"])
        self.assertEqual("source-1-task-v1", fact["job_id"])
        self.assertEqual("fp-1", fact["fingerprint"])
        self.assertTrue(fact["binding_matches"] and fact["recorded_clear"])
        self.assertFalse(facts["external_artifacts_checked"])
        self.assertEqual(before, self.batch_revision("child-1"))
        output = self.status_json()
        child = next(b for b in output["batches"] if b["id"] == "child-1")
        self.assertEqual([], child["depends_on"])
        self.assertNotIn("depends_on_exact", child)
        with state.connect() as conn, self.assertRaises(sqlite3.IntegrityError):
            conn.execute("UPDATE batches SET depends_on_exact='[]' WHERE id='child-1'")

    def test_same_name_new_batch_success_cannot_release_failed_exact_source(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="failed")
        self.downstream(exact=self.selector())
        self.seed_batch(batch_id="source-2", name="source", job_status="done")
        self.downstream("legacy", names=["source"])
        self.dispatcher()._unlock_dependent_batches()
        self.assertEqual("queued", self.batch_status())
        self.assertEqual("active", self.batch_status("legacy"))
        self.assertEqual("source-1", self.query()["dependencies"][0]["batch_id"])
        self.assertEqual("source-2", self.query("legacy")["dependencies"][0]["resolved_batch_id"])

    def test_new_version_success_cannot_release_failed_selected_version(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="failed")
        self.downstream(exact=self.selector())
        with state.connect() as conn:
            old = conn.execute("SELECT spec FROM tasks WHERE batch_id='source-1' AND id='task' AND version=1").fetchone()
            state.insert_task(conn, "source-1", "task", 2, json.loads(old["spec"]), 0, "p")
            state.insert_job(conn, "source-1-task-v2", "source-1", "task", 2, "fp-2", None, "p")
            state.update_job(conn, "source-1-task-v2", status="done", rc=0)
        self.dispatcher()._unlock_dependent_batches()
        self.assertEqual("queued", self.batch_status())
        self.assertEqual(1, self.query()["dependencies"][0]["version"])

    def test_exact_subset_does_not_wait_for_an_unrelated_failed_task(self):
        self.seed_batch(batch_id="source-1", name="source", batch_status="blocked", job_status="done")
        with state.connect() as conn:
            spec = {"id": "other", "cmd": ["/bin/true"], "artifacts": {}}
            state.insert_task(conn, "source-1", "other", 1, spec, 1, "p")
            state.insert_job(conn, "other", "source-1", "other", 1, "other-fp", None, "p")
            state.update_job(conn, "other", status="failed")
        self.downstream(exact=self.selector())
        self.dispatcher()._unlock_dependent_batches()
        self.assertEqual("active", self.batch_status())

    def test_mixed_exact_and_latest_name_cycle_is_rejected(self):
        self.seed_batch(batch_id="old-A", name="A", job_status="done")
        self.downstream("B", names=["A"])
        with state.connect() as conn, self.assertRaisesRegex(SchemaError, "混合.*成环"):
            validate_persisted_dependencies(conn, "A", [], self.selector("B"))

    def test_changed_spec_or_fingerprint_cannot_match_frozen_source(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        self.downstream(exact=self.selector())
        with state.connect() as conn:
            conn.execute("UPDATE jobs SET fingerprint='other' WHERE batch_id='source-1'")
        self.dispatcher()._unlock_dependent_batches()
        self.assertEqual("queued", self.batch_status())
        self.assertFalse(self.query()["dependencies"][0]["binding_matches"])
        with state.connect() as conn:
            conn.execute("UPDATE jobs SET fingerprint='fp-1' WHERE batch_id='source-1'")
            conn.execute("UPDATE tasks SET spec='{}' WHERE batch_id='source-1'")
        self.dispatcher()._unlock_dependent_batches()
        self.assertEqual("queued", self.batch_status())

    def test_marker_reblocks_only_pending_and_recovers_without_new_version(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        self.downstream(exact=self.selector())
        dispatcher = self.dispatcher()
        dispatcher._unlock_dependent_batches()
        marker = Path(dispatcher._launch_marker_path({"id": "source-1-task-v1"}))
        marker.parent.mkdir()
        marker.write_text("unknown")
        dispatcher._unlock_dependent_batches()
        with state.connect() as conn:
            self.assertEqual("waiting_dep", state.get_job(conn, "child-1-task-v1")["status"])
        marker.unlink()
        dispatcher._unlock_dependent_batches()
        with state.connect() as conn:
            job = state.get_job(conn, "child-1-task-v1")
            self.assertEqual("pending", job["status"])
            self.assertEqual(1, job["version"])

    def test_unknown_source_attempt_blocks_without_inferring_wait(self):
        source = self.seed_batch(batch_id="source-1", name="source", job_status="done")
        self.downstream(exact=self.selector())
        with state.connect() as conn:
            seed_attempt(conn, source)
        self.dispatcher()._unlock_dependent_batches()
        self.assertEqual("queued", self.batch_status())
        self.assertFalse(self.query()["dependencies"][0]["recorded_clear"])

    def test_dispatch_rechecks_source_generation_started_earlier_in_same_tick(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='source-1'").fetchone()[0])
            state.insert_task(conn, "source-1", "task", 2, spec, 0, "p")
            state.insert_job(conn, "source-1-task-v2", "source-1", "task", 2, "fp-2", None, "p")
        self.downstream(exact=self.selector())
        dispatcher = self.dispatcher()
        dispatcher._unlock_dependent_batches()
        dispatcher._projects = self.cfg["projects"]
        dispatcher._project_quota_used = {}
        dispatcher._launch_inflight = {}
        dispatcher._launch_marker_alive = mock.Mock(return_value=False)
        dispatcher._cpu_in_use = mock.Mock(return_value=0)
        dispatcher.allocator = mock.Mock()
        dispatcher.executor = mock.Mock()
        dispatcher.executor.configured_owner.return_value = None
        def launch(conn, job, gpu):
            state.update_job(conn, job["id"], status="running")
            return True
        dispatcher._launch_job = mock.Mock(side_effect=launch)
        dispatcher._dispatch_ready_jobs()
        self.assertEqual(["source-1-task-v2"], [call.args[1]["id"] for call in dispatcher._launch_job.call_args_list])
        with state.connect() as conn:
            self.assertEqual("waiting_dep", state.get_job(conn, "child-1-task-v1")["status"])

    def test_direct_launch_guard_releases_assignment_and_never_executes_invalid_source(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="failed")
        self.downstream(exact=self.selector())
        dispatcher = self.dispatcher()
        dispatcher._release_in_tx = mock.Mock()
        dispatcher.executor = mock.Mock()
        with state.connect() as conn:
            self.assertFalse(dispatcher._launch_job(conn, state.get_job(conn, "child-1-task-v1"), 0))
        dispatcher._release_in_tx.assert_called_once()
        self.assertFalse(dispatcher.executor.mock_calls)

    def test_public_source_requires_original_wait_and_clean_group_matching_job(self):
        for index, (pid, rc, clean, ready) in enumerate(((123, 0, True, True),
                                                       (124, 0, True, False),
                                                       (123, 1, True, False),
                                                       (123, 0, False, False))):
            with self.subTest(index=index):
                batch, child = f"public-{index}", f"child-{index}"
                job = self.seed_batch(batch_id=batch, name=batch, job_status="done")
                with state.connect() as conn:
                    state.update_job(conn, job, pgid=123, rc=0)
                    conn.execute("INSERT INTO execution_attempts"
                                 " (attempt_id,job_id,job_version,backend_id,backend_config_sha256,phase,identity,observation,created_at)"
                                 " VALUES (?,?,?,?,?,?,?,?,?)",
                                 (job + "-attempt", job, 1, "linux_fd", "a" * 64, "exited", '{"binding":"immutable"}',
                                  json.dumps({"status": "exited", "group_clean": clean, "pid": pid, "returncode": rc}), state.now()))
                self.downstream(child, exact=self.selector(batch))
                self.dispatcher()._unlock_dependent_batches()
                self.assertEqual("active" if ready else "queued", self.batch_status(child))

    def test_retired_metadata_cannot_gain_release_authority_from_done_or_artifacts(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='source-1'").fetchone()[0])
            spec["_native_exec_profile_id"] = "retired"
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id='source-1'", (json.dumps(spec),))
        self.downstream(exact=self.selector())
        self.dispatcher()._unlock_dependent_batches()
        self.assertEqual("queued", self.batch_status())

    def test_invalid_source_after_activation_never_cancels_running_child(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        self.downstream(exact=self.selector())
        dispatcher = self.dispatcher()
        dispatcher._unlock_dependent_batches()
        with state.connect() as conn:
            state.update_job(conn, "child-1-task-v1", status="running", pgid=123)
            state.update_job(conn, "source-1-task-v1", status="failed")
        dispatcher._unlock_dependent_batches()
        with state.connect() as conn:
            child = state.get_job(conn, "child-1-task-v1")
            self.assertEqual("running", child["status"])
            self.assertEqual(123, child["pgid"])

    def test_query_paging_is_bounded_and_independent_of_status_fields(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        self.downstream(exact=self.selector(), names=["source"])
        first = self.query("child-1", "--limit", "1")
        self.assertTrue(first["truncated"])
        second = self.query("child-1", "--limit", "1", "--cursor", str(first["next_cursor"]))
        self.assertFalse(second["truncated"])
        self.assertEqual(first["binding_sha256"], second["binding_sha256"])
        self.assertEqual(["legacy_name", "exact_task"], [first["dependencies"][0]["kind"], second["dependencies"][0]["kind"]])

    def test_gateway_submit_binds_same_original_source_at_compute_acceptance(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="failed")
        path = Path(self.tmp.name) / "delivery.json"
        path.write_text(json.dumps({"name": "delivery", "project": "p", "depends_on_exact": self.selector(),
                                  "tasks": [{"id": "t", "cmd": ["/bin/true"], "resources": {"gpu": 0}}]}))
        with mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}), \
             mock.patch("socket.gethostname", return_value="gateway"), \
             mock.patch.object(state, "init_db", side_effect=AssertionError("gateway wrote DB")):
            code, out, err = self.capture(cli.main, ["submit", str(path), "--request-id", "delivery", "--json"])
        self.assertEqual(0, code, err)
        self.seed_batch(batch_id="source-2", name="source", job_status="done")
        dispatcher = self.dispatcher()
        dispatcher.executor = None
        dispatcher._drain_submit_inbox()
        dispatcher._process_control_requests()
        batch = json.loads(out)["batch_id"]
        self.assertEqual("source-1", self.query(batch)["dependencies"][0]["batch_id"])
        dispatcher._unlock_dependent_batches()
        self.assertEqual("queued", self.batch_status(batch))

    def test_schema13_passive_read_and_empty_migration_do_not_rebind_names(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        self.downstream(names=["source"])
        before = self.batch_revision("child-1")
        with state.connect() as conn:
            identity = instance_id(conn)
            conn.execute("DROP TRIGGER batch_exact_dependencies_immutable")
            conn.execute("ALTER TABLE batches DROP COLUMN depends_on_exact")
            conn.execute("PRAGMA user_version=13")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("query migrated")):
            result = self.query()
        self.assertEqual("legacy_only", result["source"])
        self.assertEqual("legacy_name", result["dependencies"][0]["kind"])
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertEqual(identity, instance_id(conn))
            self.assertEqual("[]", state.get_batch(conn, "child-1")["depends_on_exact"])
            self.assertEqual(before, state.get_batch(conn, "child-1")["revision"])

    def test_migration_failure_rolls_back_new_column_and_preserves_old_state(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="done")
        with state.connect() as conn:
            identity = instance_id(conn)
            revision = state.get_batch(conn, "source-1")["revision"]
            conn.execute("DROP TRIGGER batch_exact_dependencies_immutable")
            conn.execute("ALTER TABLE batches DROP COLUMN depends_on_exact")
            conn.execute("PRAGMA user_version=13")
        original = state.migrate_exact_dependencies
        def fail(conn):
            original(conn)
            raise RuntimeError("injected exact migration failure")
        with mock.patch.object(state, "migrate_exact_dependencies", side_effect=fail), self.assertRaises(RuntimeError):
            state.init_db()
        state.set_read_only(True)
        try:
            with state.connect() as conn:
                self.assertEqual(13, conn.execute("PRAGMA user_version").fetchone()[0])
                self.assertEqual(identity, instance_id(conn))
                row = state.get_batch(conn, "source-1")
                self.assertNotIn("depends_on_exact", row.keys())
                self.assertEqual(revision, row["revision"])
        finally:
            state.set_read_only(False)

    def test_modern_upgrade_preserves_wait_states_revisions_and_unknown_receipts(self):
        self.seed_batch(batch_id="source-1", name="source", job_status="waiting_quota")
        self.downstream(names=["source"])
        with state.connect() as conn:
            state.update_job(conn, "child-1-task-v1", status="waiting_dep")
            conn.execute("INSERT INTO operation_requests(request_id,argv,status,created_at) VALUES ('unknown','{}','started',?)", (state.now(),))
            jobs = [dict(row) for row in conn.execute("SELECT * FROM jobs ORDER BY id")]
            batches = [dict(row) for row in conn.execute("SELECT * FROM batches ORDER BY id")]
            receipt = dict(conn.execute("SELECT * FROM operation_requests WHERE request_id='unknown'").fetchone())
            conn.execute("DROP TRIGGER batch_exact_dependencies_immutable")
            conn.execute("ALTER TABLE batches DROP COLUMN depends_on_exact")
            conn.execute("PRAGMA user_version=13")
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(jobs, [dict(row) for row in conn.execute("SELECT * FROM jobs ORDER BY id")])
            self.assertEqual(batches, [dict(row) for row in conn.execute("SELECT * FROM batches ORDER BY id")])
            self.assertEqual(receipt, dict(conn.execute("SELECT * FROM operation_requests WHERE request_id='unknown'").fetchone()))
