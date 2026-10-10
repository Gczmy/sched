"""Synthetic private states; permission preparation never starts execution."""
import hashlib
import json
import os
from pathlib import Path
import stat
import time
from unittest import mock

from gsched import cli, maintenance, snapshot, snapshot_facts, snapshot_management, snapshot_permissions, state
from test_review_cli_state import TempStateCase
from test_snapshot import downgrade_to_ten


class SnapshotPermissionsTests(TempStateCase):
    def legacy_directory(self, name="old"):
        path = os.path.join(state.host_dir(), name)
        os.makedirs(path, exist_ok=True)
        os.chmod(path, 0o755)
        with open(os.path.join(path, "original.log"), "w") as stream:
            stream.write("original wait remains unknown\n")
        return path

    def test_preview_is_passive_and_apply_preserves_schema10_unknown_facts(self):
        path = self.legacy_directory()
        self.seed_batch(job_status="pending")
        downgrade_to_ten()
        with state._read_only_database(state.db_path()) as (database, _):
            original = hashlib.sha256(Path(database).read_bytes()).hexdigest()
        preview = snapshot_permissions.prepare(dry_run=True)
        self.assertEqual(1, preview["repair_count"])
        self.assertFalse(preview["permissions_changed"])
        self.assertEqual(0o755, stat.S_IMODE(os.lstat(path).st_mode))
        self.assertFalse(snapshot.status()["maintenance_open"])
        result = snapshot_permissions.prepare(writers_quiesced=True, expect_plan=preview["plan_sha256"])
        self.assertEqual("completed", result["phase"])
        self.assertEqual(0o700, stat.S_IMODE(os.lstat(path).st_mode))
        with state._read_only_database(state.db_path()) as (database, _):
            self.assertEqual(original, hashlib.sha256(Path(database).read_bytes()).hexdigest())
        self.assertEqual("original wait remains unknown\n", Path(path,"original.log").read_text())
        identifier = snapshot.create(writers_quiesced=True)["snapshot_id"]
        snapshot.migrate(identifier)
        snapshot.rollback(identifier)
        snapshot.close(identifier)

    def test_stale_plan_and_missing_confirmation_have_no_permission_effect(self):
        path = self.legacy_directory()
        preview = snapshot_permissions.prepare(dry_run=True)
        for kwargs in ({"expect_plan":preview["plan_sha256"]}, {"writers_quiesced":True},
                       {"writers_quiesced":True,"expect_plan":"0"*64}):
            with self.assertRaises(snapshot_facts.SnapshotConflict):
                snapshot_permissions.prepare(**kwargs)
        with open(os.path.join(path,"late"),"w") as stream:
            stream.write("new evidence")
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot_permissions.prepare(writers_quiesced=True,expect_plan=preview["plan_sha256"])
        self.assertEqual(0o755, stat.S_IMODE(os.lstat(path).st_mode))

    def test_owner_running_and_open_window_refuse_preparation(self):
        path = self.legacy_directory()
        owner = os.path.join(state.host_dir(),"daemon.pid")
        with open(owner,"w") as stream:
            stream.write("unknown owner")
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot_permissions.prepare(dry_run=True)
        os.unlink(owner)
        self.seed_batch(job_status="running")
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot_permissions.prepare(dry_run=True)
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.create(writers_quiesced=True)
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot_permissions.prepare(dry_run=True)
        self.assertEqual(0o755,stat.S_IMODE(os.lstat(path).st_mode))
        snapshot.close(snapshot.status()["snapshot_id"])

    def test_symlink_and_replaced_directory_never_chmod_the_target(self):
        path = self.legacy_directory()
        external = os.path.join(self.tmp.name,"external")
        os.mkdir(external); os.chmod(external,0o755)
        os.symlink(external,os.path.join(path,"link"))
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot_permissions.prepare(dry_run=True)
        os.unlink(os.path.join(path,"link"))
        preview = snapshot_permissions.prepare(dry_run=True)
        os.rename(path,path+".retired")
        os.symlink(external,path)
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot_permissions.prepare(writers_quiesced=True,expect_plan=preview["plan_sha256"])
        self.assertEqual(0o755,stat.S_IMODE(os.lstat(external).st_mode))

    def test_partial_failure_retains_tightening_and_persistent_audit(self):
        first = self.legacy_directory("a")
        second = self.legacy_directory("b")
        preview = snapshot_permissions.prepare(dry_run=True)
        real = snapshot_permissions._directory_fd
        calls = []
        def interrupt(*args):
            calls.append(args)
            if len(calls)==2: raise OSError("injected failure")
            return real(*args)
        with mock.patch.object(snapshot_permissions,"_directory_fd",side_effect=interrupt):
            with self.assertRaises(OSError):
                snapshot_permissions.prepare(writers_quiesced=True,expect_plan=preview["plan_sha256"])
        self.assertEqual(0o700,stat.S_IMODE(os.lstat(first).st_mode))
        self.assertEqual(0o755,stat.S_IMODE(os.lstat(second).st_mode))
        audits = [name for name in os.listdir(maintenance.directory()) if name.startswith("permissions-")]
        self.assertEqual(1,len(audits))
        with open(os.path.join(maintenance.directory(),audits[0])) as stream:
            self.assertEqual("incomplete",json.load(stream)["phase"])
        newer = snapshot_permissions.prepare(dry_run=True)
        self.assertEqual(1,newer["repair_count"])
        snapshot_permissions.prepare(writers_quiesced=True,expect_plan=newer["plan_sha256"])

    def test_cli_preview_confirmation_and_bounded_scan(self):
        path=self.legacy_directory()
        code,out,err=self.capture(cli.main,["snapshot","permissions","--dry-run","--json"])
        self.assertEqual(0,code,(out,err))
        preview=json.loads(out)
        code,out,err=self.capture(cli.main,["snapshot","permissions","--writers-quiesced","--expect-plan",preview["plan_sha256"],"--json"])
        self.assertEqual(1,code)
        self.assertEqual(0o755,stat.S_IMODE(os.lstat(path).st_mode))
        with mock.patch.object(snapshot,"MAX_FILES",1):
            with self.assertRaises(snapshot_facts.SnapshotConflict):
                snapshot_permissions.prepare(dry_run=True)
        code,out,err=self.capture(cli.main,["snapshot","permissions","--writers-quiesced","--expect-plan",preview["plan_sha256"],"--yes","--json"])
        self.assertEqual(0,code,(out,err))
        self.assertEqual(0o700,stat.S_IMODE(os.lstat(path).st_mode))

    def test_large_legacy_inventory_migrates_rolls_back_and_prunes_with_all_files(self):
        directory=os.path.join(state.host_dir(),"large")
        os.mkdir(directory,0o700)
        for index in range(10020):
            with open(os.path.join(directory,str(index)),"wb"):
                pass
        self.seed_batch(job_status="pending")
        downgrade_to_ten()
        preview=snapshot_permissions.prepare(dry_run=True)
        self.assertGreater(preview["entries"],10000)
        self.assertEqual(0,preview["repair_count"])
        identifier=snapshot.create(writers_quiesced=True)["snapshot_id"]
        self.assertTrue(snapshot.verify(identifier)["verified"])
        snapshot.migrate(identifier)
        snapshot.rollback(identifier)
        with mock.patch.object(snapshot.time,"time",return_value=time.time()-2):
            snapshot.close(identifier)
        plan=snapshot_management.prune(identifier,dry_run=True,retention_days=0,keep_last=0)
        self.assertTrue(plan["eligible"])
        snapshot_management.prune(identifier,retention_days=0,keep_last=0,
            as_of=plan["as_of"],expect_plan=plan["plan_sha256"])
        self.assertEqual(10020,len(os.listdir(directory)))
