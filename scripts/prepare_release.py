"""Prepare immutable release assets from successful main CI; optionally upload a draft."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
import zipfile

from verify_release_candidate import COMMIT, require, verify_candidate

PYTHONS = ("3.10", "3.11", "3.12", "3.13", "3.14")
NATIVE_PYTHONS = ("3.10", "3.12", "3.14")
TARGETS = {"ubuntu-22.04": "2.35", "ubuntu-24.04": "2.39"}
PACKETS = tuple((python, target) for python in NATIVE_PYTHONS for target in TARGETS)
JOB_NAMES = {"Repository checks", *("Python " + p for p in PYTHONS),
             *(f"{kind} Python {p} / {t}" for kind in ("Native", "Candidate") for p, t in PACKETS)}
MAX_ARCHIVE_BYTES = 16 * 1024 * 1024


def sha256(data):
    return hashlib.sha256(data).hexdigest()


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GitHub:
    def __init__(self, repository, token=None):
        require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository), "invalid repository")
        self.repository, self.token = repository, token

    def request(self, method, route, data=None, *, upload=False, missing=False):
        require(route.startswith("/"), "invalid repository API route")
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2026-03-10",
                   "User-Agent": "sched-release-preparation"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if data is not None:
            headers["Content-Type"] = "application/octet-stream" if isinstance(data, bytes) else "application/json"
            if not isinstance(data, bytes):
                data = json.dumps(data).encode("utf-8")
        host = "uploads.github.com" if upload else "api.github.com"
        url = f"https://{host}/repos/{self.repository}{route}"
        try:
            with build_opener(NoRedirect()).open(Request(url, data=data, headers=headers, method=method), timeout=45) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
                require(len(raw) <= 4 * 1024 * 1024, "GitHub JSON response too large")
                return json.loads(raw)
        except HTTPError as error:
            error.close()
            if missing and method == "GET" and error.code == 404:
                return None
            raise RuntimeError(f"GitHub {method} failed with HTTP {error.code}; verify remote state before retrying") from None
        except URLError:
            raise RuntimeError("GitHub request unavailable; verify remote state before retrying") from None

    def collection(self, route, key=None):
        values = []
        for page in range(1, 21):
            payload = self.request("GET", route + ("&" if "?" in route else "?") + f"per_page=100&page={page}")
            items = payload[key] if key else payload
            require(isinstance(items, list), "invalid GitHub collection")
            values.extend(items)
            if len(items) < 100:
                return values
        raise ValueError("GitHub collection exceeds bounded pagination")

    def metadata(self, run_id, tag):
        run = self.request("GET", f"/actions/runs/{run_id}")
        main = self.request("GET", "/git/ref/heads/main")
        tag_ref = self.request("GET", "/git/ref/tags/" + quote(tag, safe=""), missing=True)
        return {"repository": self.repository, "run": run, "main_sha": main["object"]["sha"],
                "tag_sha": tag_ref["object"]["sha"] if tag_ref else None,
                "jobs": self.collection(f"/actions/runs/{run_id}/attempts/{run['run_attempt']}/jobs", "jobs"),
                "artifacts": self.collection(f"/actions/runs/{run_id}/artifacts", "artifacts")}

    def download(self, artifact):
        headers = {"User-Agent": "sched-release-preparation"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        url = f"https://api.github.com/repos/{self.repository}/actions/artifacts/{artifact['id']}/zip"
        try:
            try:
                response = build_opener(NoRedirect()).open(Request(url, headers=headers), timeout=45)
            except HTTPError as error:
                try:
                    require(error.code == 302, "artifact download did not provide a storage redirect")
                    location = error.headers.get("Location", "")
                    parsed = urlsplit(location)
                    require(parsed.scheme == "https" and not parsed.username and not parsed.password
                            and parsed.port in (None, 443) and parsed.hostname
                            and parsed.hostname.endswith((".blob.core.windows.net", ".actions.githubusercontent.com", ".githubusercontent.com")),
                            "unexpected artifact storage redirect")
                finally:
                    error.close()
                # Storage receives no GitHub authorization, including on redirects.
                response = build_opener(NoRedirect()).open(Request(location), timeout=45)
            with response:
                data = response.read(MAX_ARCHIVE_BYTES + 1)
            require(len(data) <= MAX_ARCHIVE_BYTES, "artifact archive too large")
            return data
        except (HTTPError, URLError) as error:
            if isinstance(error, HTTPError):
                error.close()
            raise RuntimeError("Artifact download failed; storage URL and credentials were not recorded") from None


def validate_metadata(meta, repository, commit, run_id):
    run = meta["run"]
    require(meta["repository"] == repository and run["repository"]["full_name"] == repository, "CI repository mismatch")
    require(run["id"] == run_id and run["head_sha"] == commit and meta["main_sha"] == commit, "CI/main commit binding mismatch")
    require(run["path"] == ".github/workflows/ci.yml" and run["head_branch"] == "main" and run["event"] == "push",
            "successful main push CI required")
    require(run["status"] == "completed" and run["conclusion"] == "success", "complete successful CI required")
    require(type(run["run_attempt"]) is int and run["run_attempt"] > 0, "invalid CI attempt")
    require(meta["tag_sha"] in (None, commit), "tag points to a different object; refusing replacement")
    jobs = meta["jobs"]
    require(len(jobs) == len(JOB_NAMES) and {j["name"] for j in jobs} == JOB_NAMES
            and all(j["status"] == "completed" and j["conclusion"] == "success" for j in jobs), "complete successful CI matrix required")
    expected = {f"sched-candidate-{commit}-python-{p}-{t}-attempt-{run['run_attempt']}" for p, t in PACKETS}
    artifacts = meta["artifacts"]
    require(len(artifacts) == len(PACKETS) and {a["name"] for a in artifacts} == expected
            and len({a["id"] for a in artifacts}) == len(PACKETS), "complete unique current-attempt candidate artifacts required")
    for artifact in artifacts:
        binding = artifact["workflow_run"]
        require(not artifact["expired"] and binding["id"] == run_id and binding["head_sha"] == commit
                and binding["head_branch"] == "main", "candidate artifact binding mismatch or expired")
    return run


def prepare_bundle(meta, repository, commit, run_id, version, output, read_artifact, *, online):
    require(COMMIT.fullmatch(commit), "independently reviewed full commit required")
    require(re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", version), "numeric package version required")
    run = validate_metadata(meta, repository, commit, run_id)
    output = Path(output)
    require(not output.is_symlink(), "release directory cannot be a symlink")
    output.parent.mkdir(parents=True, exist_ok=True)
    assets, records, notes = {}, [], None
    with tempfile.TemporaryDirectory(prefix="sched-release-verify-", dir=output.parent) as temporary:
        scratch = Path(temporary)
        for python, target in PACKETS:
            name = f"sched-candidate-{commit}-python-{python}-{target}-attempt-{run['run_attempt']}"
            artifact = next(a for a in meta["artifacts"] if a["name"] == name)
            data = read_artifact(artifact)
            require(len(data) == artifact["size_in_bytes"] and len(data) <= MAX_ARCHIVE_BYTES
                    and artifact["digest"] == "sha256:" + sha256(data), "artifact ZIP digest/size mismatch")
            packet = scratch / (python + "-" + target)
            packet.mkdir()
            zipped = scratch / (packet.name + ".zip")
            zipped.write_bytes(data)
            with zipfile.ZipFile(zipped) as archive:
                names = archive.namelist()
                require(1 <= len(names) <= 20 and len(names) == len(set(names))
                        and all(re.fullmatch(r"[A-Za-z0-9_.-]+", n) and n not in (".", "..") for n in names)
                        and sum(i.file_size for i in archive.infolist()) <= 32 * 1024 * 1024
                        and all(not i.flag_bits & 1 and not stat.S_ISLNK(i.external_attr >> 16) for i in archive.infolist()),
                        "unsafe candidate ZIP layout")
                archive.extractall(packet)
            manifest = verify_candidate(packet, commit, require_ci=True)
            require(manifest["version"] == version and manifest["platform"] == "linux-x86_64"
                    and manifest["python"].startswith(python + ".")
                    and manifest["libc"] == ["glibc", TARGETS[target]], "candidate version/platform/libc mismatch")
            ci = manifest["ci"]
            require(ci["repository"] == repository and ci["source_commit"] == ci["workflow_commit"] == commit
                    and ci["run_id"] == str(run_id) and ci["run_attempt"] == str(run["run_attempt"])
                    and ci["event"] == "push" and ci["build_target"] == target, "candidate CI provenance mismatch")
            require(manifest["native_abi"] == "cpython-" + python.replace(".", "") + "-x86_64-linux-gnu", "native ABI mismatch")
            for installation in manifest["independent_installations"]:
                enabled = "available" if installation["native"] else "unavailable"
                require(installation.get("capability_preflight") == {"subprocess": "available", "linux_fd": enabled,
                        "linux_fd_owner": enabled}, "independent installed capability evidence missing")
            require(len(manifest["independent_installations"]) == 2, "both independent wheel modes required")
            text = (packet / "RELEASE_NOTES.md").read_text(encoding="utf-8")
            require(notes is None or notes == text, "release notes differ between candidate packets")
            notes = text
            release_name = f"sched-{version}-python{python}-linux_x86_64-glibc{TARGETS[target]}.zip"
            assets[release_name] = data
            records.append({"name": release_name, "sha256": sha256(data), "bytes": len(data),
                            "github_artifact_id": artifact["id"], "github_artifact_name": artifact["name"],
                            "github_artifact_digest": artifact["digest"], "manifest_sha256": sha256((packet / "manifest.json").read_bytes()),
                            "python": manifest["python"], "target": target, "libc": manifest["libc"],
                            "native_abi": manifest["native_abi"], "database_schema": manifest["database_schema"],
                            "independent_installations": manifest["independent_installations"]})
        evidence = {"schema_version": 1, "repository": repository, "version": version, "tag": "v" + version,
                    "source_commit": commit, "metadata_source": "online" if online else "offline",
                    "ci_run": {"id": run_id, "attempt": run["run_attempt"], "head_sha": commit, "conclusion": "success",
                               "url": f"https://github.com/{repository}/actions/runs/{run_id}",
                               "jobs": [{k: j[k] for k in ("id", "name", "status", "conclusion")} for j in sorted(meta["jobs"], key=lambda j: j["name"])]},
                    "artifacts": records}
        assets["release-evidence.json"] = (json.dumps(evidence, indent=2) + "\n").encode("utf-8")
        prefix = (f"Prepared from `{commit}` and [complete CI](https://github.com/{repository}/actions/runs/{run_id}).\n\n"
                  "Original CI ZIP bytes and each packet's manifest are preserved. Check `sha256sum -c SHA256SUMS` before extraction.\n"
                  "Default wheels support Python >= 3.10 without runtime dependencies. Select native packets by Python ABI and recorded libc; no manylinux portability is claimed.\n"
                  f"Verify extracted packets with `python scripts/verify_release_candidate.py <directory> --commit {commit} --require-ci` from the reviewed source.\n"
                  "Manifest flags describe build time. Preparation creates no published Release or deployment; publication and production switching are separate.\n\n")
        assets["RELEASE_NOTES.md"] = (prefix + notes).encode("utf-8")
        assets["SHA256SUMS"] = "".join(sha256(assets[n]) + "  " + n + "\n" for n in sorted(assets)).encode("ascii")
        if output.exists():
            require(output.is_dir() and {p.name for p in output.iterdir()} == set(assets), "existing release directory differs")
            require(all(not (output / n).is_symlink() and (output / n).read_bytes() == data for n, data in assets.items()),
                    "existing release assets differ; refusing overwrite")
        else:
            ready = scratch / "ready"
            ready.mkdir()
            for name, data in assets.items():
                (ready / name).write_bytes(data)
            ready.rename(output)
    return evidence, assets


def upload_draft(api, evidence, assets):
    require(api.token, "GH_TOKEN required for draft upload")
    require(evidence["metadata_source"] == "online", "offline evidence cannot authorize draft upload")
    commit, run_id, tag = evidence["source_commit"], evidence["ci_run"]["id"], evidence["tag"]
    fresh = api.metadata(run_id, tag)
    validate_metadata(fresh, evidence["repository"], commit, run_id)
    require(fresh["run"]["run_attempt"] == evidence["ci_run"]["attempt"], "CI attempt changed before upload")
    require({a["id"]: a["digest"] for a in fresh["artifacts"]}
            == {a["github_artifact_id"]: a["github_artifact_digest"] for a in evidence["artifacts"]},
            "CI artifacts changed before draft upload")
    marker = f"<!-- sched-release-preparation/v1 {commit} {run_id} {sha256(assets['release-evidence.json'])} -->"
    body = marker + "\n\n" + assets["RELEASE_NOTES.md"].decode("utf-8")
    existing = [r for r in api.collection("/releases") if r["tag_name"] == tag]
    require(len(existing) <= 1, "ambiguous existing release tag")
    if existing:
        release = existing[0]
    else:
        release = api.request("POST", "/releases", {"tag_name": tag, "target_commitish": commit,
                              "name": "sched " + evidence["version"], "body": body, "draft": True, "prerelease": False})
    require(release["draft"] and not release["prerelease"] and release["target_commitish"] == commit
            and release["tag_name"] == tag and release["body"] == body, "release is published or draft binding changed; refusing overwrite")
    def check_assets(release, complete):
        existing_assets = {a["name"]: a for a in release["assets"]}
        require(len(existing_assets) == len(release["assets"]) and set(existing_assets) <= set(assets), "unexpected draft assets")
        if complete:
            require(set(existing_assets) == set(assets), "draft upload is incomplete")
        for name, asset in existing_assets.items():
            require(asset["state"] == "uploaded" and asset["digest"] == "sha256:" + sha256(assets[name])
                    and asset["size"] == len(assets[name]), "draft asset conflict or unknown upload result; refusing replacement")
        return existing_assets
    uploaded = check_assets(release, False)
    for name, data in assets.items():
        if name not in uploaded:
            api.request("POST", f"/releases/{release['id']}/assets?name=" + quote(name, safe=""), data, upload=True)
    final = api.request("GET", f"/releases/{release['id']}")
    require(final["draft"] and final["body"] == body and final["target_commitish"] == commit and final["tag_name"] == tag,
            "draft changed during upload")
    check_assets(final, True)
    return {"id": final["id"], "draft": True, "tag": tag, "assets": sorted(assets)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default="Gczmy/sched")
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run-id", required=True, type=int)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--offline-metadata", type=Path)
    parser.add_argument("--artifacts-directory", type=Path)
    parser.add_argument("--upload-draft", action="store_true")
    args = parser.parse_args()
    require(COMMIT.fullmatch(args.commit) and args.run_id > 0, "reviewed commit and positive CI run ID required")
    api = GitHub(args.repository, os.environ.get("GH_TOKEN"))
    if args.offline_metadata:
        require(args.artifacts_directory and not args.upload_draft, "offline preparation needs artifacts and cannot upload")
        meta = json.loads(args.offline_metadata.read_text(encoding="utf-8"))
        def read_artifact(artifact):
            path = args.artifacts_directory / (str(artifact["id"]) + ".zip")
            require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_ARCHIVE_BYTES, "missing bounded artifact file")
            return path.read_bytes()
    else:
        require(args.artifacts_directory is None, "artifact directory requires explicit offline metadata")
        meta, read_artifact = api.metadata(args.run_id, "v" + args.version), api.download
    evidence, assets = prepare_bundle(meta, args.repository, args.commit, args.run_id, args.version,
                                     args.output, read_artifact, online=args.offline_metadata is None)
    result = {"directory": str(args.output), "source_commit": args.commit, "published": False,
              "deployed": False, "assets": sorted(assets)}
    if args.upload_draft:
        result["release"] = upload_draft(api, evidence, assets)
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, OSError, KeyError, zipfile.BadZipFile) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
