"""Synthetic Git fixtures; no real credentials, accounts or cluster access."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import check_repository as check


class RepositoryCheckTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="repo-check-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.git("init", "-q")
        self.git("config", "core.autocrlf", "false")

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.root), *args], check=True,
                              capture_output=True).stdout

    def write(self, path, content):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content.encode() if isinstance(content, str) else content)

    def add(self, path, content):
        self.write(path, content)
        self.git("add", "--", path)

    def commit(self):
        self.git("-c", "user.name=Test User", "-c", "user.email=test@example.invalid",
                 "commit", "-qm", "Fixture")

    def rules(self, staged=False):
        findings, _ = check.check(self.root, staged=staged)
        return {item.rule for item in findings}

    def test_public_examples_and_code_are_accepted(self):
        self.add("README.md", "[Guide](docs/guide.md#usage)\n[Site](https://example.invalid)\n")
        self.add("docs/guide.md", "/home/user/project /home/tester /Users/example C:/Users/user\n")
        self.add("source.py", 'password = os.environ["PASSWORD"]\n')
        self.assertEqual(set(), self.rules())
        self.assertEqual(set(), self.rules(staged=True))

    def test_personal_paths_cover_platforms_and_json_escaping(self):
        for prefix in ("/home/", "/Users/", "C:/Users/", "C:\\Users\\", "C:\\\\Users\\\\"):
            with self.subTest(prefix=prefix):
                self.add("source.txt", prefix + "synthetic-person/project\n")
                self.assertIn("personal-path", self.rules(staged=True))

    def test_known_credentials_and_runtime_sessions_are_detected(self):
        cases = {
            "private-key": "-----BEGIN " + "OPENSSH PRIVATE KEY-----",
            "github-token": "ghp_" + "A9" * 18,
            "aws-access-key": "AKIA" + "Z9" * 8,
            "service-token": "sk-proj-" + "B7" * 25,
            "credential-url": "https://example:" + "synthetic-secret" + "@example.invalid",
            "screen-session": "screen -d -r " + "123456.synthetic-session",
        }
        for rule, value in cases.items():
            with self.subTest(rule=rule):
                self.add("sample.txt", value + "\n")
                self.assertIn(rule, self.rules(staged=True))

    def test_binary_data_does_not_hide_ascii_key_header(self):
        self.add("sample.bin", b"\x00\xff-----BEGIN " + b"PRIVATE KEY-----\n")
        self.assertIn("private-key", self.rules(staged=True))

    def test_runtime_and_private_file_names_are_rejected(self):
        for path in (".env", ".env.production", ".local/notes.json", "logs/job.txt",
                     "docs/operations/report.md", "results/diagnostics/report.json", "secret.pem", "state.db-wal"):
            with self.subTest(path=path):
                self.assertTrue(check.private_filename(path))
        for path in (".env.example", ".env.template", "examples/profile.json", "tests/test_log.py"):
            with self.subTest(path=path):
                self.assertFalse(check.private_filename(path))

    def test_staged_secret_is_not_hidden_by_clean_working_copy(self):
        self.add("source.txt", "/home/" + "synthetic-person/work\n")
        self.write("source.txt", "safe\n")
        self.assertIn("personal-path", self.rules(staged=True))
        self.assertEqual(set(), self.rules())

    def test_staged_safe_content_does_not_read_working_secret(self):
        self.add("source.txt", "safe\n")
        self.write("source.txt", "/home/" + "synthetic-person/work\n")
        self.assertEqual(set(), self.rules(staged=True))
        self.assertIn("personal-path", self.rules())

    def test_type_change_to_symlink_scans_target_without_following_it(self):
        self.add("source.txt", "safe\n")
        self.commit()
        self.add("source.txt", "/home/" + "synthetic-person/private-file")
        blob = self.git("rev-parse", ":source.txt").decode().strip()
        self.git("update-index", "--cacheinfo", "120000," + blob + ",source.txt")
        self.assertIn("personal-path", self.rules(staged=True))

    def test_removed_target_checks_unchanged_markdown(self):
        self.add("README.md", "[Guide](guide.md)\n")
        self.add("guide.md", "Guide\n")
        self.commit()
        self.git("rm", "guide.md")
        self.assertIn("broken-doc-link", self.rules(staged=True))

    def test_renamed_target_and_link_are_checked_together(self):
        self.add("README.md", "[Guide](guide.md)\n")
        self.add("guide.md", "Guide\n")
        self.commit()
        self.git("mv", "guide.md", "new.md")
        self.add("README.md", "[Guide](new.md)\n")
        self.assertEqual(set(), self.rules(staged=True))

    def test_untracked_target_cannot_satisfy_staged_markdown(self):
        self.add("README.md", "[Guide](guide.md)\n")
        self.write("guide.md", "Guide\n")
        self.assertIn("broken-doc-link", self.rules(staged=True))

    def test_markdown_ignores_fences_and_supports_reference_links(self):
        self.add("README.md", "```md\n[Example](absent.md)\n```\n[guide]: docs/guide.md#usage\n")
        self.add("docs/guide.md", "[back](../README.md)\n")
        self.assertEqual(set(), self.rules(staged=True))
        self.add("AGENTS.md", "Use `docs/missing.md`.\n")
        self.assertIn("broken-doc-link", self.rules(staged=True))

    def test_exact_exception_cannot_hide_another_value_or_rule(self):
        line = "/home/" + "synthetic-person/project"
        self.add("fixture.txt", line + "\n")
        policy = {"schema_version": 1, "exceptions": [{"path": "fixture.txt",
            "rule": "personal-path", "line_sha256": hashlib.sha256(line.encode()).hexdigest(),
            "reason": "Synthetic fixture for a parser regression"}]}
        self.add(check.POLICY_PATH, json.dumps(policy) + "\n")
        self.assertEqual(set(), self.rules(staged=True))
        self.add("fixture.txt", line + "/different\n")
        self.assertIn("personal-path", self.rules(staged=True))
        self.write(check.POLICY_PATH, '{"schema_version":1,"exceptions":[]}\n')
        self.add("fixture.txt", line + "\n")
        self.assertEqual(set(), self.rules(staged=True))  # The index policy also stays authoritative.
        self.assertIn("personal-path", self.rules())

    def test_invalid_or_wildcard_exceptions_fail_closed(self):
        self.add("fixture.txt", "safe\n")
        for value in ({"schema_version": True, "exceptions": []},
                      {"schema_version": 1, "exceptions": [{"path": "*", "rule": "personal-path",
                        "line_sha256": "a" * 64, "reason": "A wildcard must not be accepted"}]}):
            self.add(check.POLICY_PATH, json.dumps(value))
            with self.assertRaises(check.CheckError):
                self.rules(staged=True)

    def test_whitespace_checks_index_and_committed_diff(self):
        self.add("source.txt", "safe\n")
        self.commit()
        base = self.git("rev-parse", "HEAD").decode().strip()
        self.add("source.txt", "bad space \n")
        self.assertIn("diff-whitespace", self.rules(staged=True))
        self.commit()
        findings, _ = check.check(self.root, staged=False, base=base)
        self.assertIn("diff-whitespace", {f.rule for f in findings})

    def test_cli_reports_location_without_disclosing_matched_content(self):
        secret = "ghp_" + "A9" * 18
        self.add("source.txt", secret + "\n")
        result = subprocess.run([sys.executable, str(Path(check.__file__).resolve()), "--staged"],
                                cwd=self.root, capture_output=True, text=True)
        self.assertEqual(1, result.returncode)
        self.assertIn('"source.txt":1: github-token', result.stdout)
        self.assertNotIn(secret, result.stdout + result.stderr)

    def test_oversize_file_fails_instead_of_being_silently_skipped(self):
        self.add("large.txt", b"x" * (check.MAX_FILE_BYTES + 1))
        self.assertIn("oversize-file", self.rules(staged=True))

    def test_precommit_hook_blocks_a_secret_without_printing_it(self):
        repository = Path(__file__).resolve().parents[1]
        self.write("scripts/check_repository.py", Path(check.__file__).read_bytes())
        self.write(".githooks/pre-commit", (repository / ".githooks/pre-commit").read_bytes())
        (self.root / ".githooks/pre-commit").chmod(0o755)
        self.git("config", "core.hooksPath", ".githooks")
        secret = "ghp_" + "B8" * 18
        self.add("source.txt", secret + "\n")
        result = subprocess.run(["git", "-C", str(self.root), "-c", "user.name=Test User",
            "-c", "user.email=test@example.invalid", "commit", "-qm", "Must be rejected"],
            capture_output=True, text=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("github-token", result.stdout + result.stderr)
        self.assertNotIn(secret, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
