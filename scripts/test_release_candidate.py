"""Fault tests for candidate provenance, integrity and CI admission."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

from verify_release_candidate import ci_evidence, verify_candidate

COMMIT = "1" * 40


class CandidateChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="sched-candidate-check-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.source = self.directory / f"sched-{COMMIT[:7]}-source.zip"
        with zipfile.ZipFile(self.source, "w") as archive:
            archive.comment = COMMIT.encode("ascii")
            archive.writestr("gsched/__init__.py", '__version__ = "0.2.1"\n')
            archive.writestr("gsched/state.py", "DB_SCHEMA_VERSION = 7\n")
        (self.directory / "INSTALL.md").write_text("Candidate installation instructions", encoding="utf-8")
        self.manifest = {
            "schema_version": 1, "commit": COMMIT, "version": "0.2.1", "database_schema": 7,
            "candidate": True, "published": False, "deployed": False,
            "python": "3.10.12", "platform": "linux-x86_64", "native_abi": None,
            "independent_installations": [],
        }
        self.add_wheel(False)
        self.save()

    def add_wheel(self, native):
        tag = "cp310-cp310-linux_x86_64" if native else "py3-none-any"
        name = f"sched-0.2.1-{tag}.whl"
        with zipfile.ZipFile(self.directory / name, "w") as wheel:
            wheel.writestr("sched-0.2.1.dist-info/METADATA", "Name: sched\nVersion: 0.2.1\n")
            wheel.writestr("sched-0.2.1.dist-info/WHEEL", "Wheel-Version: 1.0\nTag: " + tag + "\n")
            if native:
                self.manifest["native_abi"] = "cpython-310-x86_64-linux-gnu"
                wheel.writestr("gsched/execution/_fdexec.cpython-310-x86_64-linux-gnu.so", b"binary fixture")
        self.manifest["independent_installations"].append({
            "wheel": name, "native": native, "independent_install": True,
            "cli_version": "sched 0.2.1", "original_owner_wait": native,
        })

    def save(self):
        self.manifest["files"] = {
            p.name: {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
            for p in self.directory.iterdir() if p.name != "manifest.json"
        }
        self.write_manifest()

    def write_manifest(self):
        (self.directory / "manifest.json").write_text(json.dumps(self.manifest), encoding="utf-8")

    def env(self):
        return {
            "GITHUB_ACTIONS": "true", "SCHED_CI_VALIDATED_SHA": COMMIT,
            "SCHED_CI_RESULTS": json.dumps({j: {"result": "success", "outputs": {}} for j in ("repository", "python", "native")}),
            "GITHUB_SHA": "2" * 40, "GITHUB_REPOSITORY": "example/sched", "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_SERVER_URL": "https://github.com",
            "PRIVATE_VALUE": "must-not-be-recorded",
        }

    def test_default_and_native_packets(self):
        verify_candidate(self.directory, COMMIT)
        self.add_wheel(True)
        self.save()
        verify_candidate(self.directory, COMMIT)

    def test_selected_commit_is_external(self):
        with self.assertRaisesRegex(ValueError, "source commit mismatch"):
            verify_candidate(self.directory, "2" * 40)

    def test_tampered_file(self):
        (self.directory / "INSTALL.md").write_text("tampered", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "size mismatch"):
            verify_candidate(self.directory, COMMIT)

    def test_same_size_corruption(self):
        path = self.directory / "INSTALL.md"
        path.write_bytes(b"x" * path.stat().st_size)
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            verify_candidate(self.directory, COMMIT)

    def test_source_commit_even_with_matching_hash(self):
        with zipfile.ZipFile(self.source, "a") as archive:
            archive.comment = ("2" * 40).encode("ascii")
        self.save()
        with self.assertRaisesRegex(ValueError, "archive commit mismatch"):
            verify_candidate(self.directory, COMMIT)

    def test_source_schema_even_with_matching_hash(self):
        self.manifest["database_schema"] = 6
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "database schema mismatch"):
            verify_candidate(self.directory, COMMIT)

    def test_path_traversal(self):
        self.manifest["files"]["../outside"] = {"bytes": 0, "sha256": "0" * 64}
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "unsafe candidate filename"):
            verify_candidate(self.directory, COMMIT)

    def test_symlink(self):
        original = self.directory / "INSTALL.md"
        outside = self.directory / "outside"
        original.rename(outside)
        try:
            original.symlink_to(outside)
        except OSError:
            self.skipTest("symlinks unavailable")
        with self.assertRaisesRegex(ValueError, "regular candidate file"):
            verify_candidate(self.directory, COMMIT)

    def test_unlisted_files(self):
        (self.directory / "unexpected.txt").write_text("extra", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unexpected candidate files"):
            verify_candidate(self.directory, COMMIT)

    def test_native_abi_mismatch(self):
        self.add_wheel(True)
        self.manifest["native_abi"] = "cpython-314-x86_64-linux-gnu"
        self.save()
        with self.assertRaisesRegex(ValueError, "Python/ABI mismatch"):
            verify_candidate(self.directory, COMMIT)

    def test_native_wait_missing(self):
        self.add_wheel(True)
        self.manifest["independent_installations"][-1]["original_owner_wait"] = False
        self.save()
        with self.assertRaisesRegex(ValueError, "owner wait evidence"):
            verify_candidate(self.directory, COMMIT)

    def replace_wheel_member(self, wheel_name, member, data):
        path = self.directory / wheel_name
        with zipfile.ZipFile(path) as wheel:
            members = {n: wheel.read(n) for n in wheel.namelist()}
        members[member] = data
        with zipfile.ZipFile(path, "w") as wheel:
            for name, value in members.items():
                wheel.writestr(name, value)
        self.save()

    def test_wheel_version_and_dependency_even_with_matching_hash(self):
        name = self.manifest["independent_installations"][0]["wheel"]
        member = "sched-0.2.1.dist-info/METADATA"
        self.replace_wheel_member(name, member, "Name: sched\nVersion: 0.2.0\n")
        with self.assertRaisesRegex(ValueError, "wheel package/version mismatch"):
            verify_candidate(self.directory, COMMIT)
        self.replace_wheel_member(name, member, "Name: sched\nVersion: 0.2.1\nRequires-Dist: unexpected\n")
        with self.assertRaisesRegex(ValueError, "runtime dependency"):
            verify_candidate(self.directory, COMMIT)

    def test_wheel_tag_even_with_matching_hash(self):
        self.add_wheel(True)
        name = self.manifest["independent_installations"][-1]["wheel"]
        self.replace_wheel_member(name, "sched-0.2.1.dist-info/WHEEL", "Tag: cp314-cp314-linux_x86_64\n")
        with self.assertRaisesRegex(ValueError, "native wheel tag mismatch"):
            verify_candidate(self.directory, COMMIT)

    def test_ci_required_and_filtered(self):
        with self.assertRaisesRegex(ValueError, "missing CI evidence"):
            verify_candidate(self.directory, COMMIT, require_ci=True)
        self.manifest["ci"] = ci_evidence(COMMIT, self.env(), COMMIT)
        self.assertNotIn("PRIVATE_VALUE", json.dumps(self.manifest["ci"]))
        self.write_manifest()
        verify_candidate(self.directory, COMMIT, require_ci=True)

    def test_ci_commit_and_prerequisites(self):
        env = self.env()
        with self.assertRaisesRegex(ValueError, "checkout/validated commit mismatch"):
            ci_evidence(COMMIT, env, "2" * 40)
        env["SCHED_CI_VALIDATED_SHA"] = "2" * 40
        with self.assertRaisesRegex(ValueError, "checkout/validated commit mismatch"):
            ci_evidence(COMMIT, env, COMMIT)
        env = self.env()
        for result in ("failure", "cancelled", "skipped"):
            env["SCHED_CI_RESULTS"] = json.dumps({j: {"result": result if j == "native" else "success"}
                                               for j in ("repository", "python", "native")})
            with self.assertRaisesRegex(ValueError, "did not all succeed"):
                ci_evidence(COMMIT, env, COMMIT)
        env["SCHED_CI_RESULTS"] = "{}"
        with self.assertRaisesRegex(ValueError, "missing CI prerequisite"):
            ci_evidence(COMMIT, env, COMMIT)

    def test_optimized_python_still_rejects_corruption(self):
        (self.directory / "INSTALL.md").write_text("bad", encoding="utf-8")
        result = subprocess.run([sys.executable, "-O", str(Path(__file__).with_name("verify_release_candidate.py")),
                                 str(self.directory), "--commit", COMMIT],
                                capture_output=True, text=True, env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
        self.assertEqual(result.returncode, 1)
        self.assertIn("size mismatch", result.stderr)


if __name__ == "__main__":
    unittest.main()
