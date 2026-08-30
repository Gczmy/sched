from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
HELPER = "tests/acceptance_cleanup.sh"


class AcceptanceCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp_path = Path(self.temp_dir.name)
        self.roots = self.temp_path / "roots"
        self.roots.mkdir()

    def run_helper(self, script: str, **environment: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(environment)
        env["TMPDIR"] = str(self.roots)
        return subprocess.run(
            ["bash", "-uc", script],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def make_successful_stop_cli(self) -> Path:
        fake_python = self.temp_path / "fake-sched-python"
        fake_python.write_text(
            """#!/bin/sh
case "$*" in
  *"daemon stop"*)
    exit 0
    ;;
  *"status"*"--json"*)
    printf '%s\n' "$SCHED_FAKE_STATUS_JSON"
    exit "${SCHED_FAKE_STATUS_RC:-0}"
    ;;
esac
exit 97
""",
            encoding="utf-8",
        )
        fake_python.chmod(0o755)
        return fake_python

    @staticmethod
    def canonical_status(
        *,
        jobs: list[dict] | None = None,
        batches: list[dict] | None = None,
        batches_truncated: bool = False,
        jobs_truncated: bool = False,
    ) -> dict:
        return {
            "schema_version": 1,
            "limit": 200,
            "batches": batches or [],
            "jobs": jobs or [],
            "gpus": [],
            "truncated": {
                "batches": batches_truncated,
                "jobs": jobs_truncated,
            },
            "next_cursor": None,
            "next_job_cursor": None,
            "daemon_health": {},
            "cpu": {"used": 0, "total": 0},
        }

    def test_daemon_stop_failure_preserves_root(self) -> None:
        failing_python = self.temp_path / "fail-python"
        failing_python.write_text("#!/bin/sh\nexit 23\n", encoding="utf-8")
        failing_python.chmod(0o755)

        result = self.run_helper(
            f"""
source {HELPER}
sched_accept_make_root TEST_ROOT sched-cleanup-stop-failure
printf '{{}}\n' > "$TEST_ROOT/config.json"
printf '%s\n' "$TEST_ROOT"
sched_accept_cleanup
""",
            PY=str(failing_python),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        root = Path(result.stdout.strip())
        self.assertTrue(root.is_dir(), "cleanup removed a root after daemon stop failed")
        self.assertIn("daemon stop failed", result.stderr)

    def test_daemon_stop_timeout_preserves_root(self) -> None:
        sleeping_python = self.temp_path / "sleep-python"
        sleeping_python.write_text(
            "#!/usr/bin/env python3\nimport time\ntime.sleep(5)\n",
            encoding="utf-8",
        )
        sleeping_python.chmod(0o755)

        result = self.run_helper(
            f"""
source {HELPER}
sched_accept_make_root TEST_ROOT sched-cleanup-stop-timeout
printf '{{}}\n' > "$TEST_ROOT/config.json"
printf '%s\n' "$TEST_ROOT"
sched_accept_cleanup
""",
            PY=str(sleeping_python),
            SCHED_ACCEPT_DAEMON_STOP_TIMEOUT="0.05",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        root = Path(result.stdout.strip())
        self.assertTrue(root.is_dir(), "cleanup removed a root after daemon stop timed out")
        self.assertIn("daemon stop failed or timed out", result.stderr)

    def test_successful_stop_with_running_status_preserves_root(self) -> None:
        fake_python = self.make_successful_stop_cli()
        status = self.canonical_status(
            batches=[
                {
                    "id": "active-batch",
                    "name": "active-batch",
                    "batch_id": "active-batch",
                    "batch_name": "active-batch",
                    "mode": "mix",
                    "status": "active",
                    "depends_on": [],
                    "progress": "0/1",
                    "project": "p",
                    "revision": 1,
                }
            ],
            jobs=[
                {
                    "id": "running-job",
                    "batch_id": "active-batch",
                    "batch_name": "active-batch",
                    "task": "task",
                    "status": "running",
                    "wait_reason": None,
                    "gpu": None,
                    "version": 1,
                    "resources": {"gpu": 0, "cpus": 1},
                    "retries": 0,
                    "failure": None,
                    "started_at": "2026-08-30 00:00:00",
                    "finished_at": None,
                    "progress": None,
                }
            ],
        )

        result = self.run_helper(
            f"""
source {HELPER}
sched_accept_make_root TEST_ROOT sched-cleanup-running
printf '{{"node":"review-node"}}\n' > "$TEST_ROOT/config.json"
printf '%s\n' "$TEST_ROOT"
sched_accept_cleanup
""",
            PY=str(fake_python),
            SCHED_FAKE_STATUS_JSON=json.dumps(status),
        )

        self.assertEqual(0, result.returncode, result.stderr)
        root = Path(result.stdout.strip())
        self.assertTrue(root.is_dir(), "cleanup removed a root with a running job")
        self.assertIn("running", result.stderr.lower())

    def test_successful_stop_with_untrustworthy_status_preserves_root(self) -> None:
        fake_python = self.make_successful_stop_cli()
        cases = (
            ("nonzero", json.dumps(self.canonical_status()), "9"),
            ("noncanonical", "{}", "0"),
            (
                "batches-truncated",
                json.dumps(self.canonical_status(batches_truncated=True)),
                "0",
            ),
            (
                "jobs-truncated",
                json.dumps(self.canonical_status(jobs_truncated=True)),
                "0",
            ),
        )

        for label, payload, status_rc in cases:
            with self.subTest(label=label):
                result = self.run_helper(
                    f"""
source {HELPER}
sched_accept_make_root TEST_ROOT sched-cleanup-status-{label}
printf '{{"node":"review-node"}}\n' > "$TEST_ROOT/config.json"
printf '%s\n' "$TEST_ROOT"
sched_accept_cleanup
""",
                    PY=str(fake_python),
                    SCHED_FAKE_STATUS_JSON=payload,
                    SCHED_FAKE_STATUS_RC=status_rc,
                )

                self.assertEqual(0, result.returncode, result.stderr)
                root = Path(result.stdout.strip())
                self.assertTrue(
                    root.is_dir(),
                    f"cleanup removed a root after {label} status",
                )
                self.assertTrue(result.stderr)

    def test_successful_stop_with_unresolved_launch_marker_preserves_root(
        self,
    ) -> None:
        fake_python = self.make_successful_stop_cli()
        status = self.canonical_status()

        result = self.run_helper(
            f"""
source {HELPER}
sched_accept_make_root TEST_ROOT sched-cleanup-launch-marker
printf '{{"node":"review-node"}}\n' > "$TEST_ROOT/config.json"
mkdir -p "$TEST_ROOT/review-node/launch"
printf '4242 proc:unresolved\n' > "$TEST_ROOT/review-node/launch/pending.launch"
printf '%s\n' "$TEST_ROOT"
sched_accept_cleanup
""",
            PY=str(fake_python),
            SCHED_FAKE_STATUS_JSON=json.dumps(status),
        )

        self.assertEqual(0, result.returncode, result.stderr)
        root = Path(result.stdout.strip())
        self.assertTrue(root.is_dir(), "cleanup removed a root with a pending launch marker")
        self.assertIn("launch", result.stderr.lower())

    def test_successful_stop_with_unsafe_launch_enumeration_preserves_root(
        self,
    ) -> None:
        fake_python = self.make_successful_stop_cli()
        outside = self.temp_path / "foreign-launch-directory"
        outside.mkdir()

        result = self.run_helper(
            f"""
source {HELPER}
sched_accept_make_root TEST_ROOT sched-cleanup-unsafe-launch
printf '{{"node":"review-node"}}\n' > "$TEST_ROOT/config.json"
mkdir -p "$TEST_ROOT/review-node"
ln -s {str(outside)!r} "$TEST_ROOT/review-node/launch"
printf '%s\n' "$TEST_ROOT"
sched_accept_cleanup
""",
            PY=str(fake_python),
            SCHED_FAKE_STATUS_JSON=json.dumps(self.canonical_status()),
        )

        self.assertEqual(0, result.returncode, result.stderr)
        root = Path(result.stdout.strip())
        self.assertTrue(
            root.is_dir(),
            "cleanup removed a root with unsafe launch enumeration",
        )
        self.assertIn("launch", result.stderr.lower())

    def test_recreated_root_with_copied_token_is_not_removed(self) -> None:
        result = self.run_helper(
            f"""
source {HELPER}
sched_accept_make_root TEST_ROOT sched-cleanup-replaced
owner_token=$(cat "$TEST_ROOT/$SCHED_ACCEPT_OWNER_MARKER")
rm -rf -- "$TEST_ROOT"
mkdir "$TEST_ROOT"
printf '%s\n' "$owner_token" > "$TEST_ROOT/$SCHED_ACCEPT_OWNER_MARKER"
printf '%s\n' "$TEST_ROOT"
sched_accept_cleanup
"""
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        root = Path(result.stdout.strip())
        self.assertTrue(root.is_dir(), "cleanup removed a replacement root with a copied token")
        self.assertIn("identity changed", result.stderr)

    def test_changed_token_is_not_removed(self) -> None:
        result = self.run_helper(
            f"""
source {HELPER}
sched_accept_make_root TEST_ROOT sched-cleanup-token
printf 'replacement-token\n' > "$TEST_ROOT/$SCHED_ACCEPT_OWNER_MARKER"
printf '%s\n' "$TEST_ROOT"
sched_accept_cleanup
"""
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        root = Path(result.stdout.strip())
        self.assertTrue(root.is_dir(), "cleanup removed a root after its token changed")
        self.assertIn("identity changed", result.stderr)

    def test_normal_cleanup_removes_quarantine_without_following_symlinks(self) -> None:
        outside = self.temp_path / "outside"
        outside.mkdir()
        victim = outside / "keep.txt"
        victim.write_text("keep", encoding="utf-8")

        result = self.run_helper(
            f"""
source {HELPER}
sched_accept_make_root TEST_ROOT sched-cleanup-normal
mkdir "$TEST_ROOT/nested"
printf data > "$TEST_ROOT/nested/data.txt"
ln -s {str(outside)!r} "$TEST_ROOT/outside-link"
printf '%s\n' "$TEST_ROOT"
sched_accept_cleanup
"""
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        root = Path(result.stdout.strip())
        self.assertFalse(root.exists(), "normal cleanup retained its owned root")
        self.assertEqual(victim.read_text(encoding="utf-8"), "keep")
        self.assertEqual(list(self.roots.iterdir()), [], "quarantine was not fully removed")
        self.assertEqual(result.stderr, "")

    def test_all_acceptance_scripts_keep_shared_source_and_traps(self) -> None:
        scripts = sorted((REPO_ROOT / "tests").glob("run_*_accept.sh"))
        self.assertEqual(len(scripts), 51)
        for script in scripts:
            text = script.read_text(encoding="utf-8")
            self.assertIn("source tests/acceptance_cleanup.sh", text, script.name)

        helper = (REPO_ROOT / HELPER).read_text(encoding="utf-8")
        self.assertIn("trap sched_accept_cleanup EXIT", helper)
        self.assertIn("trap 'exit 130' INT", helper)
        self.assertIn("trap 'exit 143' TERM", helper)

    def test_clean_versions_absolute_artifacts_opt_into_path_escape(self) -> None:
        script = (REPO_ROOT / "tests/run_clean_versions_accept.sh").read_text(encoding="utf-8")
        self.assertIn('"paths_escape": True', script)


if __name__ == "__main__":
    unittest.main()
