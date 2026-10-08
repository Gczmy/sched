"""Disposable DAG/CAS fixtures; no real daemon, GPU or worker execution."""
import json
import sqlite3
from pathlib import Path
from unittest import mock

from gsched import cli, state, task_dependencies as dag
from gsched.dispatcher import Dispatcher
from gsched.integration import instance_id
from gsched.schema import SchemaError, validate_batch
from test_review_cli_state import TempStateCase
from test_batch_failure_policy import seed_attempt


class TaskDependencyTests(TempStateCase):
    def setUp(self):
        super().setUp()
        host = mock.patch("gsched.cli._is_foreign_host", return_value=False)
        host.start()
        self.addCleanup(host.stop)

    def seed(self, edges=None, *, batch="dag", policy="continue_independent"):
        edges = edges or {}
        with state.connect() as conn:
            state.insert_batch(conn, batch, batch, "mix", [], None, self.tmp.name, {}, project="p", failure_policy=policy)
            for index, name in enumerate(("a", "b", "c", "d")):
                spec = {"id": name, "cmd": ["/bin/true"], "cwd_abs": self.tmp.name,
                        "artifacts": {}, "resources": {"gpu": 0}}
                if name in edges:
                    spec["depends_on"] = [{"task_id": source, "version": 1} for source in edges[name]]
                state.insert_task(conn, batch, name, 1, spec, index, "p")
                state.insert_job(conn, f"{batch}-{name}-v1", batch, name, 1, "fp-" + name, None, "p")
            dag.bind_new_batch(conn, batch)
        return batch

    def selectors(self, task, version=1, batch="dag"):
        with state.connect() as conn:
            return [{"instance_id": instance_id(conn), "batch_id": batch, "tasks": [{"task_id": task, "version": version}]}]

    def dispatcher(self):
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.host_dir = state.host_dir()
        dispatcher.log_line = mock.Mock()
        return dispatcher

    def update(self, rid, task, selectors, *, revision=None, **flags):
        with state.connect() as conn:
            identity = instance_id(conn)
            row = state.get_batch(conn, "dag")
            job = conn.execute("SELECT * FROM jobs WHERE batch_id='dag' AND task_id=? ORDER BY version DESC LIMIT 1", (task,)).fetchone()
        args = ["request", rid, "--json", "--expect-kind", "task", "--expect-id", f"dag:{task}",
                "--expect-status", "pending", "--expect-version", str(job["version"]),
                "--expect-revision", str(row["revision"] if revision is None else revision),
                "--expect-instance", identity, "--", "dependency-update", f"dag:{task}",
                "--dependencies-json", json.dumps(selectors), "--yes"]
        if flags.get("reopen"):
            args.append("--reopen")
        return args

    def query(self, task, *extra):
        code, out, err = self.capture(cli.main, ["task-dependencies", f"dag:{task}", *extra, "--json"])
        self.assertEqual(0, code, err)
        return json.loads(out)

    def test_local_forward_reference_and_shape_cycles(self):
        spec = {"name": "example", "project": "p", "tasks": [
            {"id": "c", "cmd": ["/bin/true"], "depends_on": [{"task_id": "a", "version": 1}]},
            {"id": "a", "cmd": ["/bin/true"]}]}
        self.assertEqual("a", validate_batch(spec, self.cfg)["tasks"][0]["depends_on"][0]["task_id"])
        for dependency in (["a"], [{"task_id": "a", "version": True}], [{"task_id": "absent", "version": 1}], [{"task_id": "a", "version": 2}]):
            with self.subTest(dependency=dependency), self.assertRaises(SchemaError):
                validate_batch(dict(spec, tasks=[dict(spec["tasks"][0], depends_on=dependency), spec["tasks"][1]]), self.cfg)
        with self.assertRaisesRegex(SchemaError, "成环"):
            validate_batch(dict(spec, tasks=[spec["tasks"][0], dict(spec["tasks"][1], depends_on=[{"task_id": "c", "version": 1}])]), self.cfg)

    def test_failed_a_waits_c_independent_b_d_continue(self):
        self.seed({"c": ["a"], "d": ["b"]})
        with state.connect() as conn:
            state.update_job(conn, "dag-a-v1", status="blocked", rc=1)
            state.update_job(conn, "dag-b-v1", status="done", rc=0)
        dispatcher = self.dispatcher()
        dispatcher._unlock_dependent_batches()
        with state.connect() as conn:
            self.assertEqual("waiting_dep", state.get_job(conn, "dag-c-v1")["status"])
            self.assertEqual("pending", state.get_job(conn, "dag-d-v1")["status"])
            self.assertFalse(dispatcher._task_dependencies_successful(conn, state.get_job(conn, "dag-c-v1")))
            self.assertTrue(dispatcher._task_dependencies_successful(conn, state.get_job(conn, "dag-d-v1")))
        self.assertEqual("source_blocked", self.query("c")["blocking_paths"][0]["reason"])

    def test_dispatch_and_final_launch_guard_never_execute_waiting_child(self):
        self.seed({"c": ["a"]})
        dispatcher = self.dispatcher()
        dispatcher._release_in_tx = mock.Mock()
        dispatcher._task_has_unresolved_launch_marker = mock.Mock(return_value=False)
        with state.connect() as conn, mock.patch("gsched.dispatcher.subprocess.Popen", side_effect=AssertionError("executed")):
            self.assertFalse(dispatcher._launch_job(conn, state.get_job(conn, "dag-c-v1"), 0))
        dispatcher._release_in_tx.assert_called_once()

    def test_same_batch_v2_does_not_rebind_and_resubmit_inherits_overlay(self):
        self.seed({"c": ["a"]})
        with state.connect() as conn:
            for name in ("a", "c"):
                state.update_job(conn, f"dag-{name}-v1", status="blocked")
                old = state.get_job(conn, f"dag-{name}-v1")
                spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='dag' AND id=?", (name,)).fetchone()[0])
                state.insert_task(conn, "dag", name, 2, spec, 0, "p")
                state.insert_job(conn, f"dag-{name}-v2", "dag", name, 2, "fp-" + name, None, "p")
                dag.inherit(conn, old, state.get_job(conn, f"dag-{name}-v2"))
            state.update_job(conn, "dag-a-v2", status="done")
            dispatcher = self.dispatcher()
            self.assertFalse(dispatcher._task_dependencies_successful(conn, state.get_job(conn, "dag-c-v2")))
        self.assertEqual(1, self.query("c")["bindings"][0]["tasks"][0]["version"])

    def test_update_is_atomic_audited_replay_and_old_event_retained(self):
        self.seed({"c": ["a"]})
        before = self.query("c")
        args = self.update("replace", "c", self.selectors("b"))
        first = self.capture(cli.main, args)
        self.assertEqual(0, first[0], first[2])
        after = self.query("c")
        self.assertNotEqual(before["event_id"], after["event_id"])
        self.assertEqual(before["event_id"], after["previous_event_id"])
        self.assertEqual("b", after["bindings"][0]["tasks"][0]["task_id"])
        historical = self.query("c", "--event-id", before["event_id"])
        self.assertFalse(historical["effective"])
        self.assertEqual("a", historical["event"]["bindings"][0]["tasks"][0]["task_id"])
        revision = self.batch_revision("dag")
        self.assertEqual(0, self.capture(cli.main, args)[0])
        self.assertEqual(revision, self.batch_revision("dag"))
        self.assertEqual(65, self.capture(cli.main, self.update("stale", "c", [], revision=before["batch_revision"]))[0])
        with state.connect() as conn, self.assertRaises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM task_dependency_events")
        with state.connect() as conn, self.assertRaises(sqlite3.IntegrityError):
            conn.execute("UPDATE task_dependency_events SET bindings='[]'")

    def test_cycle_update_rolls_back_event_revision_and_command_effect(self):
        self.seed({"c": ["a"]})
        revision = self.batch_revision("dag")
        code, out, err = self.capture(cli.main, self.update("cycle", "a", self.selectors("c")))
        self.assertEqual(65, code, (out, err))
        self.assertIn("成环", err)
        self.assertEqual(revision, self.batch_revision("dag"))
        with state.connect() as conn:
            self.assertIsNone(conn.execute("SELECT 1 FROM task_dependency_events WHERE request_id='cycle'").fetchone())
            self.assertEqual(65, conn.execute("SELECT code FROM operation_requests WHERE request_id='cycle'").fetchone()[0])

    def test_unknown_attempt_marker_historical_start_and_foreign_host_refused(self):
        self.seed()
        with state.connect() as conn:
            seed_attempt(conn, "dag-c-v1")
        self.assertEqual(65, self.capture(cli.main, self.update("unknown", "c", []))[0])
        marker = Path(self.dispatcher()._launch_marker_path({"id": "dag-d-v1"}))
        marker.parent.mkdir()
        marker.write_text("unknown")
        self.assertEqual(65, self.capture(cli.main, self.update("marker", "d", []))[0])
        marker.unlink()
        with state.connect() as conn:
            state.update_job(conn, "dag-d-v1", started_at=state.now())
        self.assertEqual(65, self.capture(cli.main, self.update("started", "d", []))[0])
        with mock.patch("gsched.cli._is_foreign_host", return_value=True), mock.patch.dict("os.environ", SCHED_ALLOW_FOREIGN_WRITE="1"):
            self.assertEqual(65, self.capture(cli.main, self.update("foreign", "b", []))[0])

    def test_direct_missing_instance_mutable_input_and_unknown_receipt_rejected(self):
        self.seed()
        self.assertEqual(64, self.capture(cli.main, ["dependency-update", "dag:a", "--dependencies-json", "[]", "--yes"])[0])
        args = self.update("valid", "a", [])
        index = args.index("--expect-instance")
        self.assertEqual(64, self.capture(cli.main, args[:index] + args[index + 2:])[0])
        invalid = args.copy()
        invalid[invalid.index("--dependencies-json") + 1] = "/tmp/selectors.json"
        self.assertEqual(64, self.capture(cli.main, invalid)[0])
        envelope = cli._request_envelope(cli._build_parser().parse_args(args))
        binding = json.dumps({"command": envelope[1], "expect": envelope[2]}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with state.connect() as conn:
            conn.execute("INSERT INTO operation_requests(request_id,argv,status,created_at) VALUES ('valid',?,'started',?)", (binding, state.now()))
        self.assertEqual(75, self.capture(cli.main, args)[0])

    def test_passive_paths_paging_target_drift_and_no_migration(self):
        self.seed({"c": ["a", "b"], "d": ["c"]})
        revision = self.batch_revision("dag")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migrated")), mock.patch("os.stat", wraps=__import__("os").stat):
            first = self.query("d", "--limit", "1")
        self.assertTrue(first["truncated"])
        second = self.query("d", "--limit", "1", "--cursor", "1")
        self.assertEqual(2, len(second["blocking_paths"][0]["path"]))
        self.assertEqual(revision, self.batch_revision("dag"))
        self.assertFalse(first["external_artifacts_checked"])
        self.assertIsNone(first["dispatch_ready"])
        with state.connect() as conn:
            conn.execute("UPDATE tasks SET spec='{}' WHERE batch_id='dag' AND id='d' AND version=1")
            self.assertFalse(self.dispatcher()._task_dependencies_successful(conn, state.get_job(conn, "dag-d-v1")))

    def test_modern_migration_empty_table_preserves_old_waiting_and_receipt(self):
        self.seed()
        with state.connect() as conn:
            state.update_job(conn, "dag-c-v1", status="waiting_dep")
            for trigger in ("task_dependency_immutable", "task_dependency_retained", "revision_task_dependency"):
                conn.execute("DROP TRIGGER " + trigger)
            conn.execute("DROP TABLE task_dependency_events")
            conn.execute("PRAGMA user_version=14")
            revision = state.get_batch(conn, "dag")["revision"]
        self.assertFalse(self.query("c")["task_dag_supported"])
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(15, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT count(*) FROM task_dependency_events").fetchone()[0])
            self.assertEqual("waiting_dep", state.get_job(conn, "dag-c-v1")["status"])
            self.assertEqual(revision, state.get_batch(conn, "dag")["revision"])

    def test_direct_submit_and_inbox_preserve_forward_bindings_and_replay(self):
        path = Path(self.tmp.name) / "dag.json"
        path.write_text(json.dumps({"name": "submitted", "project": "p", "tasks": [
            {"id": "c", "cmd": ["/bin/true"], "resources": {"gpu": 0}, "depends_on": [{"task_id": "a", "version": 1}]},
            {"id": "a", "cmd": ["/bin/true"], "resources": {"gpu": 0}}]}))
        with mock.patch("gsched.cli._ensure_running_locked", return_value="not started"):
            code, out, err = self.capture(cli.main, ["submit", str(path), "--request-id", "direct", "--json"])
        self.assertEqual(0, code, err)
        batch = json.loads(out)["batch_id"]
        with state.connect() as conn:
            job = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id='c'", (batch,)).fetchone()
            self.assertEqual("a", dag.stored(conn, job)[0]["tasks"][0]["task_id"])
        other = json.loads(path.read_text())
        other["name"] = "delivered"
        path.write_text(json.dumps(other))
        with mock.patch("gsched.cli._is_foreign_host", return_value=True), mock.patch.dict("os.environ", SCHED_ALLOW_FOREIGN_WRITE=""), mock.patch.object(state, "init_db", side_effect=AssertionError("gateway wrote")):
            code, out, err = self.capture(cli.main, ["submit", str(path), "--request-id", "gateway", "--json"])
        self.assertEqual(0, code, err)
        batch = json.loads(out)["batch_id"]
        dispatcher = self.dispatcher()
        dispatcher.executor = None
        dispatcher._drain_submit_inbox()
        dispatcher._process_control_requests()
        with state.connect() as conn:
            job = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id='c'", (batch,)).fetchone()
            self.assertIsNotNone(job)
            original = dag.latest(conn, job)["event_id"]
            self.assertEqual("a", dag.stored(conn, job)[0]["tasks"][0]["task_id"])
        with mock.patch("gsched.cli._is_foreign_host", return_value=True), mock.patch.dict("os.environ", SCHED_ALLOW_FOREIGN_WRITE=""):
            self.assertEqual(0, self.capture(cli.main, ["submit", str(path), "--request-id", "gateway", "--json"])[0])
        dispatcher._drain_submit_inbox()
        dispatcher._process_control_requests()
        with state.connect() as conn:
            self.assertEqual(original, dag.latest(conn, job)["event_id"])

    def test_mixed_task_edge_and_legacy_batch_gate_cycle_rejected(self):
        self.seed({"c": ["a"]})
        with state.connect() as conn:
            state.insert_batch(conn, "external", "external", "mix", ["dag"], None, self.tmp.name, {}, project="p")
            spec = {"cmd": ["/bin/true"], "artifacts": {}}
            state.insert_task(conn, "external", "t", 1, spec, 0, "p")
            state.insert_job(conn, "external-t-v1", "external", "t", 1, "fp", None, "p")
        args = self.update("mixed-cycle", "a", [{"instance_id": self.selectors("a")[0]["instance_id"], "batch_id": "external", "tasks": [{"task_id": "t", "version": 1}]}])
        code, _, err = self.capture(cli.main, args)
        self.assertEqual(65, code)
        self.assertIn("成环", err)

    def test_changed_dependency_selection_cannot_reuse_old_success_fingerprint(self):
        self.seed({"c": ["a"]})
        with state.connect() as conn:
            state.update_job(conn, "dag-c-v1", status="done", finished_at=state.now(), rc=0)
            old = state.get_job(conn, "dag-c-v1")
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='dag' AND id='c'").fetchone()[0])
            state.insert_task(conn, "dag", "c", 2, spec, 0, "p")
            state.insert_job(conn, "dag-c-v2", "dag", "c", 2, "fp-c", None, "p")
            current = state.get_job(conn, "dag-c-v2")
            dag.inherit(conn, old, current)
            self.assertTrue(self.dispatcher()._fingerprint_matches(conn, spec, current, current_fingerprint="fp-c"))
        code, _, err = self.capture(cli.main, self.update("changed-source", "c", self.selectors("b")))
        self.assertEqual(0, code, err)
        with state.connect() as conn:
            self.assertFalse(self.dispatcher()._fingerprint_matches(conn, spec, state.get_job(conn, "dag-c-v2"), current_fingerprint="fp-c"))

    def test_fingerprint_snapshot_commit_cannot_bypass_final_dependency_guard(self):
        self.seed({"c": ["a"]})
        with state.connect() as conn:
            state.update_job(conn, "dag-a-v1", status="done", rc=0)
        dispatcher = self.dispatcher()
        dispatcher._unlock_dependent_batches()
        dispatcher._prepare_launch_marker = mock.Mock(return_value=False)
        dispatcher._task_has_unresolved_launch_marker = mock.Mock(return_value=False)
        dispatcher._release_in_tx = mock.Mock()
        def snapshot(*args):
            with state.connect() as other:
                state.update_job(other, "dag-a-v1", status="running")
            return "fp-c", {}, None
        dispatcher._snapshot_fingerprint = mock.Mock(side_effect=snapshot)
        with state.connect() as conn, mock.patch("gsched.dispatcher.subprocess.Popen", side_effect=AssertionError("executed")):
            self.assertFalse(dispatcher._launch_job(conn, state.get_job(conn, "dag-c-v1"), None))
            self.assertEqual("pending", state.get_job(conn, "dag-c-v1")["status"])

    def test_target_cache_fingerprint_change_keeps_frozen_spec_and_sources(self):
        self.seed({"c": ["a"]})
        with state.connect() as conn:
            state.update_job(conn, "dag-a-v1", status="done", rc=0)
        with state.connect() as conn:
            original = dag.stored(conn, state.get_job(conn, "dag-c-v1"))
            state.update_job(conn, "dag-c-v1", fingerprint=None)
            self.assertEqual(original, dag.stored(conn, state.get_job(conn, "dag-c-v1")))
            state.update_job(conn, "dag-c-v1", fingerprint="changed")
            self.assertEqual(original, dag.stored(conn, state.get_job(conn, "dag-c-v1")))

    def test_event_digest_rejects_unrecorded_binding_replacement(self):
        self.seed({"c": ["a"]})
        with state.connect() as conn:
            event = dict(dag.latest(conn, state.get_job(conn, "dag-c-v1")))
        event["bindings"] = "[]"
        with self.assertRaisesRegex(ValueError, "摘要不匹配"):
            dag.decode_event(event)
