"""Verify a downloaded candidate against an independently selected Git commit."""
from __future__ import annotations

import argparse
import ast
from email.parser import BytesParser
import hashlib
import json
from pathlib import Path
import re
import sys
import zipfile

COMMIT = re.compile(r"[0-9a-f]{40}")
VALIDATION_JOBS = {"repository", "python", "native"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check_ci(evidence, commit):
    require(isinstance(evidence, dict), "missing CI evidence")
    require(evidence.get("source_commit") == commit, "CI source commit mismatch")
    require(evidence.get("validation_jobs") == {j: "success" for j in VALIDATION_JOBS},
            "CI prerequisite checks did not all succeed")
    repository = evidence.get("repository", "")
    require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository), "invalid CI repository")
    for key in ("run_id", "run_attempt"):
        require(re.fullmatch(r"[1-9][0-9]*", str(evidence.get(key, ""))), "invalid CI " + key)
    require(evidence.get("run_url") ==
            f"https://github.com/{repository}/actions/runs/{evidence['run_id']}", "invalid CI run URL")
    require(COMMIT.fullmatch(evidence.get("workflow_commit", "")), "invalid CI workflow commit")
    require(evidence.get("event") in {"push", "pull_request", "workflow_dispatch"}, "invalid CI event")


def ci_evidence(commit, env, head):
    """Record only public CI identifiers, never the complete context or environment."""
    if env.get("GITHUB_ACTIONS") != "true":
        return None
    require(head == commit == env.get("SCHED_CI_VALIDATED_SHA"), "CI checkout/validated commit mismatch")
    jobs = json.loads(env.get("SCHED_CI_RESULTS", "{}"))
    require(set(jobs) == VALIDATION_JOBS, "missing CI prerequisite results")
    evidence = {
        "source_commit": commit,
        "workflow_commit": env.get("GITHUB_SHA", ""),
        "repository": env.get("GITHUB_REPOSITORY", ""),
        "event": env.get("GITHUB_EVENT_NAME", ""),
        "run_id": env.get("GITHUB_RUN_ID", ""),
        "run_attempt": env.get("GITHUB_RUN_ATTEMPT", ""),
        "validation_jobs": {j: jobs[j].get("result") for j in sorted(jobs)},
    }
    require(env.get("GITHUB_SERVER_URL") == "https://github.com", "unsupported CI server")
    evidence["run_url"] = f"https://github.com/{evidence['repository']}/actions/runs/{evidence['run_id']}"
    check_ci(evidence, commit)
    return evidence


def source_constant(archive, name, variable):
    tree = ast.parse(archive.read(name).decode("utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == variable for t in node.targets):
            return ast.literal_eval(node.value)
    raise ValueError("missing source constant: " + variable)


def verify_candidate(directory, commit, require_ci=False):
    require(COMMIT.fullmatch(commit), "expected commit must be a full lowercase Git commit")
    directory = Path(directory)
    require(not directory.is_symlink(), "candidate directory must not be a symlink")
    manifest_path = directory / "manifest.json"
    require(manifest_path.is_file() and not manifest_path.is_symlink(), "missing regular manifest")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    require(manifest.get("schema_version") == 1, "unsupported manifest schema")
    require(manifest.get("commit") == commit, "candidate source commit mismatch")
    require(manifest.get("candidate") is True and manifest.get("published") is False
            and manifest.get("deployed") is False, "invalid candidate status")
    files = manifest.get("files")
    require(isinstance(files, dict) and files, "missing file manifest")
    for name, record in files.items():
        require(re.fullmatch(r"[A-Za-z0-9_.-]+", name) and name not in {".", "..", "manifest.json"},
                "unsafe candidate filename")
        path = directory / name
        require(path.is_file() and not path.is_symlink(), "missing regular candidate file: " + name)
        data = path.read_bytes()
        require(record.get("bytes") == len(data), "file size mismatch: " + name)
        require(record.get("sha256") == hashlib.sha256(data).hexdigest(), "file hash mismatch: " + name)
    require({p.name for p in directory.iterdir()} == set(files) | {"manifest.json"}, "unexpected candidate files")
    source_name = f"sched-{commit[:7]}-source.zip"
    require(source_name in files and "INSTALL.md" in files, "missing source archive or installation notes")
    with zipfile.ZipFile(directory / source_name) as archive:
        require(archive.comment.decode("ascii") == commit, "source archive commit mismatch")
        require(source_constant(archive, "gsched/__init__.py", "__version__") == manifest.get("version"),
                "source package version mismatch")
        require(source_constant(archive, "gsched/state.py", "DB_SCHEMA_VERSION") == manifest.get("database_schema"),
                "source database schema mismatch")
    wheels = {name for name in files if name.endswith(".whl")}
    records = manifest.get("independent_installations", [])
    require(len(records) == len(wheels) and {r.get("wheel") for r in records} == wheels,
            "missing independent installation evidence")
    modes = []
    for record in records:
        native = record.get("native")
        require(type(native) is bool and record.get("independent_install") is True,
                "invalid installation evidence")
        require(record.get("cli_version") == "sched " + manifest["version"], "installed CLI version mismatch")
        if native:
            require(record.get("original_owner_wait") is True, "missing original owner wait evidence")
        modes.append(native)
        with zipfile.ZipFile(directory / record["wheel"]) as wheel:
            names = wheel.namelist()
            metadata_names = [n for n in names if n.endswith(".dist-info/METADATA")]
            wheel_names = [n for n in names if n.endswith(".dist-info/WHEEL")]
            require(len(metadata_names) == len(wheel_names) == 1, "invalid wheel metadata")
            metadata = BytesParser().parsebytes(wheel.read(metadata_names[0]))
            tags = BytesParser().parsebytes(wheel.read(wheel_names[0])).get_all("Tag", [])
            require(metadata["Name"] == "sched" and metadata["Version"] == manifest["version"],
                    "wheel package/version mismatch")
            require(not [v for v in metadata.get_all("Requires-Dist", []) if "extra ==" not in v],
                    "unexpected runtime dependency")
            binaries = [n for n in names if n.endswith((".so", ".pyd"))]
            if native:
                abi = manifest.get("native_abi") or ""
                match = re.fullmatch(r"cpython-([0-9]+)([a-z]*)-.+", abi)
                require(match is not None, "invalid native ABI")
                python_tag = "cp" + match[1]
                abi_tag = python_tag + match[2]
                require(".".join(manifest["python"].split(".")[:2]).replace(".", "") == match[1],
                        "native Python/ABI mismatch")
                expected_tag = f"{python_tag}-{abi_tag}-{manifest['platform'].replace('-', '_').replace('.', '_')}"
                require(tags == [expected_tag] and record["wheel"].endswith("-" + expected_tag + ".whl"),
                        "native wheel tag mismatch")
                require(binaries == [f"gsched/execution/_fdexec.{abi}.so"], "native extension ABI mismatch")
            else:
                require(tags == ["py3-none-any"] and not binaries, "default wheel contains native code or wrong tag")
                require(record["wheel"] == f"sched-{manifest['version']}-py3-none-any.whl", "default wheel filename mismatch")
    require(sorted(modes) == ([False, True] if manifest.get("native_abi") else [False]), "invalid wheel variants")
    if require_ci or "ci" in manifest:
        check_ci(manifest.get("ci"), commit)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--commit", required=True, help="full commit selected from the reviewed PR/main, not the manifest")
    parser.add_argument("--require-ci", action="store_true")
    args = parser.parse_args()
    try:
        manifest = verify_candidate(args.directory, args.commit, args.require_ci)
    except (ValueError, OSError, KeyError, TypeError, AttributeError, zipfile.BadZipFile) as error:
        print("FAIL:", error, file=sys.stderr)
        return 1
    print(f"PASS: candidate {manifest['version']} from {args.commit}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
