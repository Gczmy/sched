from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gsched.execution_policy import (
    ExecutionPolicyError, INTERNAL_FIELD, normalize_execution, revalidate_binding,
    snapshot_file, validate_backends,
)
from gsched.schema import SchemaError, validate_batch


class ExecutionPolicyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = os.path.realpath(self.directory.name)
        self.cfg = {"projects": {"text": {"root": self.root}, "math": {"root": self.root}},
                    "execution_backends": {"worker": {
                        "kind": "linux_fd", "executable": os.path.join(self.root, "worker"),
                        "sha256": "1" * 64, "argv": ["worker", "--fixed"], "env": {},
                        "projects": ["text", "math"], "input_slots": {"3": {"max_bytes": 1024}},
                    }}}
        self.task = {"id": "work", "cmd": ["worker", "--fixed"], "stages": None,
                     "cwd_abs": self.root, "max_retry": 0, "env": {},
                     "execution": {"backend": "worker", "inputs": {
                         "3": {"path": "input.txt", "sha256": "2" * 64}}}}

    def test_two_projects_share_same_generic_contract_and_binding_is_cold(self):
        first = normalize_execution(self.task, self.cfg, "text", {})
        second = normalize_execution(self.task, self.cfg, "math", {})
        self.assertEqual(first, second)
        persisted = dict(self.task, **{INTERNAL_FIELD: first})
        self.assertEqual(self.cfg["execution_backends"]["worker"], revalidate_binding(persisted, self.cfg, "text", {}))
        changed = copy.deepcopy(self.cfg)
        changed["execution_backends"]["worker"]["env"] = {"CHANGED": "1"}
        with self.assertRaisesRegex(ExecutionPolicyError, "differs"):
            revalidate_binding(persisted, changed, "text", {})

    def test_rejects_application_callbacks_opaque_fields_and_reserved_identity_slot(self):
        for field in ("plugin", "metadata", "success_callback", "module"):
            cfg = copy.deepcopy(self.cfg)
            cfg["execution_backends"]["worker"][field] = "untrusted"
            with self.assertRaises(ExecutionPolicyError): validate_backends(cfg)
        cfg = copy.deepcopy(self.cfg)
        cfg["execution_backends"]["worker"]["input_slots"] = {"4": {"max_bytes": 1}}
        with self.assertRaisesRegex(ExecutionPolicyError, "FD4"):
            validate_backends(cfg)

    def test_rejects_environment_inheritance_nonadmin_argv_and_automatic_retry(self):
        for patch in ({"cmd": ["worker", "--different"]}, {"max_retry": 1},
                      {"env": {"LOCAL": "1"}}, {"runtime": "python"},
                      {INTERNAL_FIELD: {"forged": True}}):
            task = dict(self.task, **patch)
            with self.assertRaises(ExecutionPolicyError): normalize_execution(task, self.cfg, "text", {})
        with self.assertRaises(ExecutionPolicyError): normalize_execution(self.task, self.cfg, "text", {"BATCH": "1"})
        cfg = copy.deepcopy(self.cfg)
        cfg["execution_backends"]["worker"]["env"] = {"SCHED_RUN_ID": "forged"}
        with self.assertRaises(ExecutionPolicyError): validate_backends(cfg)

    def test_relative_input_paths_are_normalized_and_slots_are_exact(self):
        for path in ("../input.txt", "/input.txt", "./input.txt", "a//input.txt", "a\\input.txt", "a/../input.txt", "."):
            task = copy.deepcopy(self.task)
            task["execution"]["inputs"]["3"]["path"] = path
            with self.assertRaises(ExecutionPolicyError): normalize_execution(task, self.cfg, "text", {})
        task = copy.deepcopy(self.task)
        task["execution"]["inputs"]["5"] = task["execution"]["inputs"]["3"]
        with self.assertRaises(ExecutionPolicyError): normalize_execution(task, self.cfg, "text", {})

    def test_malformed_registry_and_retry_types_have_policy_errors(self):
        for projects in ([{}], [["text"]], ["text", "text"], [False]):
            cfg = copy.deepcopy(self.cfg)
            cfg["execution_backends"]["worker"]["projects"] = projects
            with self.assertRaises(ExecutionPolicyError):
                validate_backends(cfg)
        for retry in (False, 0.0, "0", None):
            with self.assertRaises(ExecutionPolicyError):
                normalize_execution(dict(self.task, max_retry=retry), self.cfg, "text", {})

    def test_null_execution_is_rejected_before_persistence(self):
        with self.assertRaises(ExecutionPolicyError):
            normalize_execution(dict(self.task, execution=None), self.cfg, "text", {})
        task = {key: value for key, value in self.task.items() if key != "cwd_abs"}
        task.update({"resources": {"gpu": 0}, "git": False, "execution": None})
        with self.assertRaises(SchemaError):
            validate_batch({"name": "null-execution", "project": "text", "tasks": [task]}, self.cfg)

    def test_schema_rejects_supplied_internal_binding_and_retired_strict(self):
        task = {key: value for key, value in self.task.items() if key != "cwd_abs"}
        task.update({"resources": {"gpu": 0}, "git": False, INTERNAL_FIELD: {"forged": True}})
        with self.assertRaises(SchemaError): validate_batch({"name": "forgery", "project": "text", "tasks": [task]}, self.cfg)
        task.pop(INTERNAL_FIELD)
        with self.assertRaisesRegex(SchemaError, "retired"):
            validate_batch({"name": "legacy", "project": "text", "mode": "strict", "tasks": [task]}, self.cfg)

    @unittest.skipUnless(sys.platform == "linux", "Linux sealed input snapshot")
    def test_snapshot_retains_immutable_bytes_after_source_changes(self):
        import fcntl
        path = Path(self.root) / "input.txt"
        original = b"stable bytes\n"
        path.write_bytes(original)
        fd = snapshot_file(str(path), hashlib.sha256(original).hexdigest(), 1024)
        self.addCleanup(os.close, fd)
        self.assertEqual(os.O_RDONLY, fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE)
        required = fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL
        self.assertEqual(required, fcntl.fcntl(fd, fcntl.F_GET_SEALS) & required)
        path.write_bytes(b"replaced")
        self.assertEqual(original, os.read(fd, 1024))
        with self.assertRaises(OSError): os.write(fd, b"changed")

    @unittest.skipUnless(sys.platform == "linux", "Linux no-symlink descriptor traversal")
    def test_snapshot_rejects_hash_drift_size_and_symlink_traversal(self):
        path = Path(self.root) / "input.txt"
        path.write_bytes(b"expected")
        sha = hashlib.sha256(b"expected").hexdigest()
        with self.assertRaisesRegex(ExecutionPolicyError, "SHA-256"):
            snapshot_file(str(path), "0" * 64, 1024)
        with self.assertRaises(ExecutionPolicyError): snapshot_file(str(path), sha, 1)
        link = Path(self.root) / "link.txt"
        link.symlink_to(path)
        with self.assertRaises(OSError): snapshot_file(str(link), sha, 1024)
        directory = Path(self.root) / "real"
        directory.mkdir(); (directory / "bytes").write_bytes(b"expected")
        (Path(self.root) / "alias").symlink_to(directory, target_is_directory=True)
        root_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        self.addCleanup(os.close, root_fd)
        with self.assertRaises(OSError): snapshot_file("alias/bytes", sha, 1024, root_fd=root_fd)

    @unittest.skipUnless(sys.platform == "linux", "Linux FIFO rejection")
    def test_fifo_without_a_writer_is_rejected_without_blocking(self):
        path = Path(self.root) / "input.fifo"
        os.mkfifo(path)
        code = '''
import os, sys
from gsched.execution_policy import ExecutionPolicyError, snapshot_file
root = os.open(sys.argv[2], os.O_RDONLY | os.O_DIRECTORY)
try:
    for path, root_fd in ((sys.argv[1], None), ("input.fifo", root)):
        try:
            snapshot_file(path, "0" * 64, 1024, root_fd=root_fd)
        except ExecutionPolicyError:
            pass
        else:
            raise AssertionError("FIFO was accepted")
finally:
    os.close(root)
'''
        subprocess.run([sys.executable, "-B", "-c", code, str(path), self.root],
                       check=True, capture_output=True, timeout=5)


if __name__ == "__main__":
    unittest.main()
