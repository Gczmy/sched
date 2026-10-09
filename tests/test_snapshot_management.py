"""Private snapshot retention with real filesystem faults; no worker/Slurm."""
import json
import os
import time
from unittest import mock

from gsched import cli, maintenance, snapshot, snapshot_management as manager, state
from test_review_cli_state import TempStateCase


class SnapshotManagementTests(TempStateCase):
    def closed(self):
        identifier = snapshot.create(writers_quiesced=True)["snapshot_id"]
        snapshot.close(identifier)
        return identifier

    def plan(self, identifier):
        # Closed epoch can be fractional; a past as-of requires a past closure.
        with mock.patch.object(snapshot.time, "time", return_value=time.time() - 2):
            snapshot.close(identifier)
        return manager.prune(identifier, dry_run=True, retention_days=0, keep_last=0)

    def prepared(self):
        identifier = snapshot.create(writers_quiesced=True)["snapshot_id"]
        return identifier, self.plan(identifier)

    def apply(self, preview):
        return manager.prune(preview["snapshot_id"], retention_days=preview["retention_days"],
                             keep_last=preview["keep_last"], as_of=preview["as_of"],
                             expect_plan=preview["plan_sha256"])

    def test_catalog_is_bounded_passive_and_does_not_initialize(self):
        identifiers = sorted([self.closed(), self.closed()])
        with mock.patch.object(state, "init_db", side_effect=AssertionError("initialized")), \
                mock.patch.object(state, "connect", side_effect=AssertionError("DB opened")):
            first = manager.catalog(limit=1)
            self.assertEqual([identifiers[0]], [r["snapshot_id"] for r in first["snapshots"]])
            self.assertTrue(first["truncated"])
            second = manager.catalog(limit=1, cursor=first["next_cursor"])
            self.assertFalse(second["truncated"])
            self.assertEqual(identifiers[1], second["snapshots"][0]["snapshot_id"])
            self.assertEqual("none", second["effect"])

    def test_prune_retains_audit_close_and_current_database(self):
        identifier, preview = self.prepared()
        before = snapshot._file_fact(state.db_path(), private=True)
        close_path = os.path.join(maintenance.directory(), "closed-" + identifier + ".json")
        original_close = snapshot._read(close_path)
        manifest = snapshot._read(os.path.join(snapshot._point(identifier), "manifest.json"))
        result = self.apply(preview)
        self.assertEqual("pruned", result["phase"])
        self.assertEqual(before, snapshot._file_fact(state.db_path(), private=True))
        self.assertEqual(original_close, snapshot._read(close_path))
        self.assertEqual(manifest, snapshot._read(os.path.join(snapshot._point(identifier), "manifest.json")))
        self.assertFalse(os.path.exists(os.path.join(snapshot._point(identifier), "database.db")))
        self.assertEqual("pruned", manager.catalog()["snapshots"][0]["phase"])
        self.assertEqual(result, self.apply(preview))
        with self.assertRaises(FileNotFoundError):
            snapshot.rollback(identifier)

    def test_apply_requires_original_preview_and_no_intent_on_conflict(self):
        identifier, preview = self.prepared()
        with self.assertRaises(state.StateError):
            manager.prune(identifier)
        wrong = {**preview, "plan_sha256": "0" * 64}
        with self.assertRaises(state.StateError):
            self.apply(wrong)
        self.assertFalse(os.path.lexists(manager._journal(identifier)))
        self.assertTrue(snapshot.verify(identifier)["verified"])

    def test_open_window_retention_and_keep_last_refuse(self):
        identifier = snapshot.create(writers_quiesced=True)["snapshot_id"]
        open_preview = manager.prune(identifier, dry_run=True)
        self.assertFalse(open_preview["eligible"])
        self.assertIn("maintenance_open", open_preview["reasons"])
        snapshot.close(identifier)
        retained = manager.prune(identifier, dry_run=True)
        self.assertIn("retention_period_not_elapsed", retained["reasons"])
        self.assertIn("retained_by_keep_last", retained["reasons"])

    def test_old_naive_close_time_is_unknown_not_guessed(self):
        identifier = self.closed()
        path = os.path.join(maintenance.directory(), "closed-" + identifier + ".json")
        record = snapshot._json(path)
        record.pop("closed_at_epoch")
        snapshot._publish(path, record)  # Synthetic legacy record, never deployed state.
        preview = manager.prune(identifier, dry_run=True, retention_days=0, keep_last=0)
        self.assertFalse(preview["eligible"])
        self.assertIn("closed_time_not_recorded", preview["reasons"])

    def test_crash_after_unlink_resumes_original_intent(self):
        identifier, preview = self.prepared()
        real = os.unlink
        def crash(path, *args, **kwargs):
            real(path, *args, **kwargs)
            if path == "config.json":
                raise OSError("injected exit after unlink")
        with mock.patch.object(manager.os, "unlink", side_effect=crash):
            with self.assertRaises(OSError):
                self.apply(preview)
        journal = snapshot._json(manager._journal(identifier))
        self.assertEqual("pruning", journal["phase"])
        self.assertEqual(preview["plan_sha256"], journal["plan_sha256"])
        self.assertEqual("pruned", self.apply(preview)["phase"])

    def test_original_file_directory_or_unrecorded_content_drift_refuse(self):
        identifier, preview = self.prepared()
        path = os.path.join(snapshot._point(identifier), "config.json")
        with open(path, "ab") as stream:
            stream.write(b"drift")
        with self.assertRaises(state.StateError):
            self.apply(preview)
        self.assertFalse(os.path.lexists(manager._journal(identifier)))

    def test_link_and_inventory_bounds_refuse_before_intent(self):
        identifier, preview = self.prepared()
        path = os.path.join(snapshot._point(identifier), "unexpected")
        os.symlink(state.db_path(), path)
        with self.assertRaises(state.StateError):
            self.apply(preview)
        os.unlink(path)
        with mock.patch.object(manager, "MAX_BYTES", 0), self.assertRaises(state.StateError):
            self.apply(preview)
        self.assertFalse(os.path.lexists(manager._journal(identifier)))

    def test_inplace_change_between_digest_and_unlink_is_retained(self):
        identifier, preview = self.prepared()
        point = snapshot._point(identifier)
        payload = os.path.join(point, "config.json")
        original_open = os.open
        changed = False
        def replace_after_digest(path, flags, *args, **kwargs):
            nonlocal changed
            fd = original_open(path, flags, *args, **kwargs)
            if os.path.normpath(path) == point and flags & os.O_DIRECTORY and not changed:
                with open(payload, "ab") as stream:
                    stream.write(b"changed after final digest")
                changed = True
            return fd
        with mock.patch.object(manager.os, "open", side_effect=replace_after_digest):
            with self.assertRaises(state.StateError):
                self.apply(preview)
        self.assertTrue(changed)
        self.assertTrue(os.path.exists(payload))
        self.assertTrue(os.path.exists(os.path.join(point, "database.db")))
        self.assertEqual("pruning", snapshot._json(manager._journal(identifier))["phase"])

    def test_unrecorded_copy_content_or_empty_directory_is_not_pruned(self):
        identifier, preview = self.prepared()
        extra = os.path.join(snapshot._point(identifier), "files", "unknown-ticket")
        snapshot._write(extra, b"unknown")
        with self.assertRaises(state.StateError):
            self.apply(preview)
        self.assertTrue(os.path.exists(extra))
        os.unlink(extra)  # Synthetic fixture content only.
        extra_directory = os.path.join(snapshot._point(identifier), "extra")
        os.mkdir(extra_directory, 0o700)
        with self.assertRaises(state.StateError):
            self.apply(preview)
        self.assertFalse(os.path.lexists(manager._journal(identifier)))

    def test_a_new_window_refuses_previously_valid_plan(self):
        identifier, preview = self.prepared()
        other = snapshot.create(writers_quiesced=True)["snapshot_id"]
        with self.assertRaises(state.StateError):
            self.apply(preview)
        self.assertFalse(os.path.lexists(manager._journal(identifier)))
        self.assertTrue(os.path.exists(os.path.join(snapshot._point(identifier), "database.db")))
        snapshot.close(other)

    def test_completed_rollback_payload_is_pruned_but_journal_retained(self):
        identifier = snapshot.create(writers_quiesced=True)["snapshot_id"]
        snapshot.rollback(identifier)
        preview = self.plan(identifier)
        journal = snapshot._read(os.path.join(snapshot._point(identifier), "rollback.json"))
        self.assertEqual("pruned", self.apply(preview)["phase"])
        self.assertEqual(journal, snapshot._read(os.path.join(snapshot._point(identifier), "rollback.json")))
        self.assertFalse(os.path.exists(os.path.join(snapshot._point(identifier), "before-rollback")))

    def test_crash_with_all_payload_removed_can_resume_without_reverifying_image(self):
        identifier, preview = self.prepared()
        publish = snapshot._publish
        def fail_final(path, value):
            if path == manager._journal(identifier) and value.get("phase") == "pruned":
                raise OSError("injected final audit publication failure")
            return publish(path, value)
        with mock.patch.object(snapshot, "_publish", side_effect=fail_final):
            with self.assertRaises(OSError):
                self.apply(preview)
        self.assertFalse(os.path.exists(os.path.join(snapshot._point(identifier), "database.db")))
        self.assertEqual("pruned", self.apply(preview)["phase"])

    def test_resume_refuses_replaced_point_inode_and_audit_change(self):
        identifier, preview = self.prepared()
        with mock.patch.object(manager, "_remove", side_effect=OSError("injected before first unlink")):
            with self.assertRaises(OSError):
                self.apply(preview)
        point = snapshot._point(identifier)
        retained = point + "-fixture"
        os.rename(point, retained)
        os.mkdir(point, 0o700)
        with self.assertRaises(state.StateError):
            self.apply(preview)
        os.rmdir(point)
        os.rename(retained, point)
        manifest = os.path.join(point, "manifest.json")
        with open(manifest, "ab") as stream:
            stream.write(b" ")
        with self.assertRaises(state.StateError):
            self.apply(preview)
        self.assertTrue(os.path.exists(os.path.join(point, "database.db")))

    def test_catalog_without_control_directory_does_not_create_it(self):
        absent = os.path.join(self.tmp.name, "absent-control")
        with mock.patch.object(maintenance, "directory", return_value=absent):
            self.assertEqual([], manager.catalog()["snapshots"])
        self.assertFalse(os.path.lexists(absent))

    def test_gateway_preview_is_readonly_but_apply_still_requires_compute_host(self):
        identifier, preview = self.prepared()
        with mock.patch.object(cli, "_is_foreign_host", return_value=True):
            code, out, _ = self.capture(cli.main, ["snapshot", "prune", identifier, "--retention-days", "0",
                "--keep-last", "0", "--dry-run", "--json"])
            self.assertEqual(0, code)
            self.assertEqual("none", json.loads(out)["effect"])
            code, _, _ = self.capture(cli.main, ["snapshot", "prune", identifier, "--yes", "--json",
                "--as-of", str(preview["as_of"]), "--expect-plan", preview["plan_sha256"]])
            self.assertEqual(1, code)
        self.assertFalse(os.path.lexists(manager._journal(identifier)))

    def test_prune_preserves_current_unknown_request_and_pending_generation(self):
        job = self.seed_batch(job_status="pending")
        with state.connect() as conn:
            conn.execute("INSERT INTO operation_requests(request_id,argv,status,created_at) VALUES ('unknown','{}','started',?)", (state.now(),))
        identifier, preview = self.prepared()
        self.apply(preview)
        with state.connect() as conn:
            self.assertEqual("started", conn.execute("SELECT status FROM operation_requests WHERE request_id='unknown'").fetchone()[0])
            self.assertEqual("pending", state.get_job(conn, job)["status"])

    def test_catalog_bounds_and_retention_option_validation_are_explicit(self):
        identifier = self.closed()
        for value in (0, 101, True):
            with self.assertRaises(state.StateError):
                manager.catalog(limit=value)
        with mock.patch.object(manager, "MAX_ENTRIES", 0), self.assertRaises(state.StateError):
            manager.catalog()
        with mock.patch.object(manager, "MAX_METADATA_BYTES", 0), self.assertRaises(state.StateError):
            manager.catalog()
        with mock.patch.object(manager, "MAX_METADATA_BYTES", 0), self.assertRaises(state.StateError):
            manager.prune(identifier, dry_run=True)
        for options in ({"retention_days": -1}, {"keep_last": 101}, {"as_of": int(time.time()) + 100}):
            with self.assertRaises(state.StateError):
                manager.prune(identifier, dry_run=True, **options)

    def test_catalog_rejects_unbounded_identity_summary(self):
        identifier = self.closed()
        path = os.path.join(snapshot._point(identifier), "manifest.json")
        manifest = snapshot._json(path)
        manifest["database_facts"]["instance_id"] = "a" * 1000
        snapshot._publish(path, manifest)
        with self.assertRaises(state.StateError):
            manager.catalog()

    def test_failed_creation_closed_without_manifest_is_not_a_cleanup_candidate(self):
        with mock.patch.object(snapshot, "_source_image", side_effect=OSError("injected creation failure")):
            with self.assertRaises(OSError):
                snapshot.create(writers_quiesced=True)
        identifier = snapshot.status()["snapshot_id"]
        snapshot.close(identifier)
        preview = manager.prune(identifier, dry_run=True, retention_days=0, keep_last=0)
        self.assertFalse(preview["eligible"])
        self.assertIn("complete_closed_point_required", preview["reasons"])

    def test_cli_preview_without_yes_and_apply_confirmation(self):
        identifier, preview = self.prepared()
        code, out, _ = self.capture(cli.main, ["snapshot", "list", "--json"])
        self.assertEqual(0, code)
        self.assertEqual(manager.FORMAT, json.loads(out)["contract"])
        code, _, _ = self.capture(cli.main, ["snapshot", "prune", identifier, "--json"])
        self.assertEqual(1, code)
        code, out, _ = self.capture(cli.main, ["snapshot", "prune", identifier, "--retention-days", "0",
                                            "--keep-last", "0", "--dry-run", "--json"])
        self.assertEqual(0, code)
        self.assertEqual("none", json.loads(out)["effect"])
