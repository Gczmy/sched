"""Release provenance, immutable assets and unknown upload-result regressions."""
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
import zipfile

import prepare_release as release
import test_release_candidate as fixtures


class FakeGitHub:
    token = "fixture-token"
    def __init__(self, metadata):
        self.meta = copy.deepcopy(metadata)
        self.releases, self.calls = [], []
        self.interrupt_upload = False

    def metadata(self, run_id, tag):
        return copy.deepcopy(self.meta)

    def collection(self, route):
        return copy.deepcopy(self.releases)

    def request(self, method, route, data=None, **kwargs):
        self.calls.append((method, route))
        if method == "POST" and route == "/releases":
            value = {**data, "id": 1, "assets": []}
            self.releases.append(value)
            return copy.deepcopy(value)
        if method == "POST" and "/assets?" in route:
            name = parse_qs(urlsplit(route).query)["name"][0]
            value = {"name": name, "size": len(data), "state": "uploaded", "digest": "sha256:" + release.sha256(data)}
            self.releases[0]["assets"].append(value)
            if self.interrupt_upload:
                self.interrupt_upload = False
                raise RuntimeError("upload accepted but response lost")
            return copy.deepcopy(value)
        if method == "GET" and route == "/releases/1":
            return copy.deepcopy(self.releases[0])
        raise AssertionError("unexpected API mutation: " + method + " " + route)


class ReleasePreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo, self.commit, self.run_id = "example/sched", fixtures.COMMIT, 123
        self.meta = {"repository": self.repo, "main_sha": self.commit, "tag_sha": None,
                     "run": {"id": 123, "head_sha": self.commit, "head_branch": "main", "event": "push",
                             "path": ".github/workflows/ci.yml", "run_attempt": 1, "status": "completed",
                             "conclusion": "success", "repository": {"full_name": self.repo}},
                     "jobs": [{"id": i, "name": name, "status": "completed", "conclusion": "success"}
                              for i, name in enumerate(sorted(release.JOB_NAMES), 1)], "artifacts": [],
                     "private_value": "not-for-public-evidence"}
        self.data = {}
        for number, (python, target) in enumerate(release.PACKETS, 100):
            case = fixtures.CandidateChecks()
            case.setUp()
            self.addCleanup(case.doCleanups)
            case.add_wheel(True)
            if python == "3.14":
                old = case.directory / "sched-0.2.1-cp310-cp310-linux_x86_64.whl"
                new = case.directory / old.name.replace("310", "314")
                with zipfile.ZipFile(old) as source, zipfile.ZipFile(new, "w") as output:
                    for name in source.namelist():
                        value = source.read(name)
                        if name.endswith("/WHEEL"):
                            value = value.replace(b"310", b"314")
                        output.writestr(name.replace("310", "314"), value)
                old.unlink()
                case.manifest["native_abi"] = "cpython-314-x86_64-linux-gnu"
                case.manifest["independent_installations"][-1]["wheel"] = new.name
            case.manifest["python"] = python + ".0"
            case.manifest["libc"] = ["glibc", release.TARGETS[target]]
            env = case.env()
            env.update(GITHUB_SHA=self.commit, GITHUB_EVENT_NAME="push", SCHED_CI_BUILD_TARGET=target)
            case.manifest["ci"] = fixtures.ci_evidence(self.commit, env, self.commit)
            for item in case.manifest["independent_installations"]:
                enabled = "available" if item["native"] else "unavailable"
                item["capability_preflight"] = {"subprocess": "available", "linux_fd": enabled, "linux_fd_owner": enabled}
            (case.directory / "RELEASE_NOTES.md").write_text("Reviewed release notes\n", encoding="utf-8")
            case.save()
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
                for path in sorted(case.directory.iterdir()):
                    archive.writestr(path.name, path.read_bytes())
            self.data[number] = buffer.getvalue()
            self.meta["artifacts"].append({"id": number, "name": f"sched-candidate-{self.commit}-python-{python}-{target}-attempt-1",
                "size_in_bytes": len(self.data[number]), "digest": "sha256:" + release.sha256(self.data[number]), "expired": False,
                "workflow_run": {"id": 123, "head_sha": self.commit, "head_branch": "main"}})

    def prepare(self, *, online=False):
        return release.prepare_bundle(self.meta, self.repo, self.commit, self.run_id, "0.2.1", self.root / "release",
                                      lambda artifact: self.data[artifact["id"]], online=online)

    def change_zip(self, transform):
        artifact = self.meta["artifacts"][0]
        buffer = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(self.data[artifact["id"]])) as source, zipfile.ZipFile(buffer, "w") as target:
            transform(source, target)
        self.data[artifact["id"]] = buffer.getvalue()
        artifact.update(digest="sha256:" + release.sha256(buffer.getvalue()), size_in_bytes=len(buffer.getvalue()))

    def test_complete_bundle_preserves_zip_bytes_and_public_provenance(self):
        evidence, assets = self.prepare()
        self.assertEqual(7, len(assets))
        self.assertEqual(self.commit, evidence["source_commit"])
        self.assertEqual(4, len(evidence["artifacts"]))
        self.assertNotIn("not-for-public-evidence", json.dumps(evidence))
        for record in evidence["artifacts"]:
            self.assertEqual(self.data[record["github_artifact_id"]], assets[record["name"]])
        for line in assets["SHA256SUMS"].decode().splitlines():
            digest, name = line.split("  ")
            self.assertEqual(digest, release.sha256(assets[name]))
        self.assertEqual(assets, self.prepare()[1])

    def test_main_tag_event_workflow_and_run_bindings_fail_closed(self):
        variants = [("main_sha", "2" * 40), ("tag_sha", "2" * 40), ("repository", "other/sched")]
        for key, value in variants:
            with self.subTest(key=key):
                changed = copy.deepcopy(self.meta)
                changed[key] = value
                with self.assertRaises(ValueError):
                    release.validate_metadata(changed, self.repo, self.commit, self.run_id)
        for key, value in (("head_sha", "2" * 40), ("event", "pull_request"), ("head_branch", "topic"),
                           ("path", ".github/workflows/other.yml"), ("status", "in_progress"), ("conclusion", "failure")):
            with self.subTest(key=key):
                changed = copy.deepcopy(self.meta)
                changed["run"][key] = value
                with self.assertRaises(ValueError):
                    release.validate_metadata(changed, self.repo, self.commit, self.run_id)

    def test_missing_failed_or_duplicate_matrix_job_is_rejected(self):
        for change in (lambda jobs: jobs.pop(), lambda jobs: jobs[0].update(conclusion="skipped"),
                       lambda jobs: jobs.append(jobs[0])):
            changed = copy.deepcopy(self.meta)
            change(changed["jobs"])
            with self.assertRaises(ValueError):
                release.validate_metadata(changed, self.repo, self.commit, self.run_id)

    def test_artifact_expiry_duplicates_missing_and_cross_commit_are_rejected(self):
        for change in (lambda items: items.pop(), lambda items: items[0].update(expired=True),
                       lambda items: items[0]["workflow_run"].update(head_sha="2" * 40),
                       lambda items: items[0].update(id=items[1]["id"])):
            changed = copy.deepcopy(self.meta)
            change(changed["artifacts"])
            with self.assertRaises(ValueError):
                release.validate_metadata(changed, self.repo, self.commit, self.run_id)

    def test_outer_digest_mismatch_leaves_no_output(self):
        self.data[100] += b"changed"
        with self.assertRaisesRegex(ValueError, "digest/size"):
            self.prepare()
        self.assertFalse((self.root / "release").exists())

    def test_archive_traversal_with_matching_outer_digest_is_rejected(self):
        self.change_zip(lambda source, target: target.writestr("../escaped", b"payload"))
        with self.assertRaisesRegex(ValueError, "unsafe candidate"):
            self.prepare()
        self.assertFalse((self.root / "escaped").exists())

    def test_wrong_manifest_run_even_with_matching_outer_digest_is_rejected(self):
        def transform(source, target):
            for name in source.namelist():
                data = source.read(name)
                if name == "manifest.json":
                    value = json.loads(data)
                    value["ci"]["run_id"] = "456"
                    value["ci"]["run_url"] = "https://github.com/example/sched/actions/runs/456"
                    data = json.dumps(value).encode()
                target.writestr(name, data)
        self.change_zip(transform)
        with self.assertRaisesRegex(ValueError, "CI provenance"):
            self.prepare()

    def test_missing_installed_capability_evidence_is_rejected(self):
        def transform(source, target):
            for name in source.namelist():
                data = source.read(name)
                if name == "manifest.json":
                    value = json.loads(data)
                    value["independent_installations"][0].pop("capability_preflight")
                    data = json.dumps(value).encode()
                target.writestr(name, data)
        self.change_zip(transform)
        with self.assertRaisesRegex(ValueError, "capability evidence"):
            self.prepare()

    def test_existing_changed_asset_is_never_overwritten(self):
        self.prepare()
        notes = self.root / "release" / "RELEASE_NOTES.md"
        notes.write_bytes(b"external modification")
        with self.assertRaisesRegex(ValueError, "refusing overwrite"):
            self.prepare()
        self.assertEqual(b"external modification", notes.read_bytes())

    def test_offline_bundle_cannot_upload(self):
        evidence, assets = self.prepare()
        api = FakeGitHub(self.meta)
        with self.assertRaisesRegex(ValueError, "offline"):
            release.upload_draft(api, evidence, assets)
        self.assertEqual([], api.calls)

    def test_lost_asset_response_resumes_without_duplicate_or_publish(self):
        evidence, assets = self.prepare(online=True)
        api = FakeGitHub(self.meta)
        api.interrupt_upload = True
        with self.assertRaisesRegex(RuntimeError, "response lost"):
            release.upload_draft(api, evidence, assets)
        result = release.upload_draft(api, evidence, assets)
        self.assertTrue(result["draft"])
        self.assertEqual(7, len(api.releases[0]["assets"]))
        self.assertEqual(1, sum(method == "POST" and route == "/releases" for method, route in api.calls))
        self.assertEqual(7, sum(method == "POST" and "/assets?" in route for method, route in api.calls))
        self.assertFalse(any(method in ("PATCH", "DELETE") for method, route in api.calls))

    def test_published_and_conflicting_drafts_are_not_mutated(self):
        evidence, assets = self.prepare(online=True)
        api = FakeGitHub(self.meta)
        release.upload_draft(api, evidence, assets)
        for change in (lambda r: r.update(draft=False), lambda r: r.update(body="other draft"),
                       lambda r: r["assets"][0].update(digest="sha256:" + "0" * 64),
                       lambda r: r["assets"][0].update(state="starter")):
            changed = FakeGitHub(self.meta)
            changed.releases = copy.deepcopy(api.releases)
            change(changed.releases[0])
            with self.assertRaises(ValueError):
                release.upload_draft(changed, evidence, assets)
            self.assertFalse(any(method == "POST" for method, route in changed.calls))

    def test_new_main_or_artifact_identity_blocks_upload(self):
        evidence, assets = self.prepare(online=True)
        api = FakeGitHub(self.meta)
        api.meta["main_sha"] = "2" * 40
        with self.assertRaises(ValueError):
            release.upload_draft(api, evidence, assets)
        api.meta = copy.deepcopy(self.meta)
        api.meta["artifacts"][0]["digest"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(ValueError, "artifacts changed"):
            release.upload_draft(api, evidence, assets)
        self.assertEqual([], api.calls)

    def test_storage_redirect_receives_no_authorization(self):
        api = release.GitHub(self.repo, "private-token")
        first, second = mock.Mock(), mock.Mock()
        first.open.side_effect = HTTPError("https://api.github.com/fixture", 302, "redirect",
                                          {"Location": "https://example.blob.core.windows.net/artifact?signature=fixture"}, None)
        second.open.return_value = io.BytesIO(b"archive")
        with mock.patch.object(release, "build_opener", side_effect=[first, second]):
            self.assertEqual(b"archive", api.download({"id": 1}))
        self.assertEqual("Bearer private-token", first.open.call_args.args[0].get_header("Authorization"))
        self.assertIsNone(second.open.call_args.args[0].get_header("Authorization"))

    def test_untrusted_redirect_and_network_error_do_not_expose_secret(self):
        api = release.GitHub(self.repo, "private-token")
        opener = mock.Mock()
        opener.open.side_effect = HTTPError("https://api.github.com/fixture", 302, "redirect",
                                            {"Location": "https://untrusted.example/private-token"}, None)
        with mock.patch.object(release, "build_opener", return_value=opener):
            with self.assertRaises(ValueError) as error:
                api.download({"id": 1})
        self.assertNotIn("private-token", str(error.exception))
        opener.open.side_effect = URLError("private-token")
        with mock.patch.object(release, "build_opener", return_value=opener):
            with self.assertRaises(RuntimeError) as error:
                api.request("GET", "/releases")
        self.assertNotIn("private-token", str(error.exception))


if __name__ == "__main__":
    unittest.main()
