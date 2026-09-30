"""Build and verify wheel candidates from one explicit Git commit (Linux)."""
from __future__ import annotations

import argparse
from email.parser import BytesParser
import hashlib
from importlib.metadata import version as tool_version
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import zipfile

from verify_release_candidate import ci_evidence, require, verify_candidate

ROOT = Path(__file__).resolve().parents[1]


def run(args, cwd, env):
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise RuntimeError((args, result.stdout, result.stderr))
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True, help="full 40-character reviewed source commit")
    parser.add_argument("--native", action="store_true", help="also build the current Linux Python ABI wheel")
    args = parser.parse_args()
    if re.fullmatch("[0-9a-f]{40}", args.commit) is None:
        parser.error("--commit requires a full lowercase Git commit")
    if sys.platform != "linux":
        parser.error("candidate installation verification requires local Linux")
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")
    commit = run(["git", "rev-parse", args.commit + "^{commit}"], ROOT, env).strip()
    head = run(["git", "rev-parse", "HEAD"], ROOT, env).strip()
    ci = ci_evidence(commit, env, head)
    destination = ROOT / "dist" / ("candidate-" + commit[:7])
    destination.mkdir(parents=True, exist_ok=False)
    archive = destination / ("sched-" + commit[:7] + "-source.zip")
    run(["git", "archive", "--format=zip", "--output=" + str(archive), commit], ROOT, env)
    evidence = []
    package_version = None
    database_schema = None
    with tempfile.TemporaryDirectory(prefix="sched-release-candidate-") as temporary:
        root = Path(temporary)
        source, unrelated = root / "source", root / "unrelated"
        source.mkdir(); unrelated.mkdir()
        with zipfile.ZipFile(archive) as zipped:
            zipped.extractall(source)
        modes = [(False, "default")]
        if args.native:
            modes.append((True, "native"))
        for native, label in modes:
            build_env = dict(env, SCHED_BUILD_NATIVE="1" if native else "0")
            if not native:
                build_env["CC"] = "/nonexistent/compiler"
            dist = root / label
            run([sys.executable, "setup.py", "bdist_wheel", "--dist-dir", str(dist)], source, build_env)
            wheel = next(dist.glob("*.whl"))
            with zipfile.ZipFile(wheel) as zipped:
                files = zipped.namelist()
                require(bool([n for n in files if n.endswith((".so", ".pyd"))]) is native, "incorrect wheel variant")
                metadata = BytesParser().parsebytes(zipped.read(next(n for n in files if n.endswith("/METADATA"))))
                require(metadata["Name"] == "sched", "incorrect package name")
                require(not [v for v in metadata.get_all("Requires-Dist", []) if "extra ==" not in v], "runtime dependency")
                if package_version is not None:
                    require(package_version == metadata["Version"], "wheel version mismatch")
                package_version = metadata["Version"]
            shutil.copy2(wheel, destination / wheel.name)
            installed = root / (label + "-installed")
            run([sys.executable, "-m", "pip", "install", "--no-index", "--no-deps",
                 "--disable-pip-version-check", "--target", str(installed), str(wheel)], source, env)
            runtime = {k: v for k, v in env.items() if not k.startswith("SCHED_")}
            runtime["PYTHONPATH"] = str(installed)
            observed = run([str(installed / "bin/sched"), "--version"], unrelated, runtime).strip()
            require(observed == "sched " + package_version, "installed CLI version mismatch")
            run([str(installed / "bin/sched"), "--help"], unrelated, runtime)
            probe = "import json,gsched; from gsched import state; print(json.dumps({'version':gsched.__version__,'schema':state.DB_SCHEMA_VERSION,'file':gsched.__file__})); "
            probe += "from gsched.execution import LinuxFdBackend,BackendUnavailable; "
            probe += "LinuxFdBackend()" if native else "\ntry: LinuxFdBackend()\nexcept BackendUnavailable: pass\nelse: raise AssertionError('unexpected native')"
            observed = json.loads(run([sys.executable, "-c", probe], unrelated, runtime))
            require(observed["version"] == package_version and Path(observed["file"]).is_relative_to(installed),
                    "installation imported a different source tree")
            if database_schema is not None:
                require(database_schema == observed["schema"], "installed database schema mismatch")
            database_schema = observed["schema"]
            if native:
                run([sys.executable, "-c", """
import os,secrets
from gsched.execution import ExecutionEnvelope,PersistentLinuxFdBackend,PersistentOwner
executable=os.open('/bin/true',os.O_RDONLY|os.O_CLOEXEC)
cwd=os.open('.',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
try:
    prepared=PersistentLinuxFdBackend().prepare(ExecutionEnvelope(('true',)),
        executable_fd=executable,cwd_fd=cwd,fd_bindings={},
        identity={'schema':'sched_execution_identity/v1','attempt_id':secrets.token_hex(16)})
finally:
    os.close(executable);os.close(cwd)
owner=prepared.launch()
observation=PersistentOwner(owner.binding).wait(10)
if not (observation.returncode==0 and observation.group_clean is True and observation.rusage is not None):
    raise RuntimeError('original owner wait failed')
owner.close();prepared.close()
"""], unrelated, runtime)
            evidence.append({"wheel": wheel.name, "native": native, "independent_install": True,
                             "cli_version": "sched " + package_version, "original_owner_wait": native})
            print("PASS:", label, "candidate installs independently", flush=True)
        release_notes = source / "docs" / "releases" / (package_version + ".md")
        if release_notes.is_file():
            (destination / "RELEASE_NOTES.md").write_text(
                f"Source commit: `{commit}`.\n\n" + release_notes.read_text(encoding="utf-8"), encoding="utf-8")
    notes = destination / "INSTALL.md"
    notes.write_text(f"""# sched {package_version} candidate

Source commit: `{commit}`. Select the expected commit independently from the reviewed PR/main.
Use `scripts/verify_release_candidate.py <directory> --commit <full-commit>` from that source.
For CI artifacts add `--require-ci` and inspect the linked run's final result and artifact digest.
This directory is a candidate; no release tag, publication or deployment was performed.

The default wheel needs Python >= 3.10 and no compiler or runtime dependencies.
The native wheel, if present, matches the recorded build Python ABI and Linux platform.
It is not a manylinux portability claim. Use a matching ABI and check kernel capabilities.

```bash
python -m venv /opt/sched/{package_version}-{commit[:7]}
/opt/sched/{package_version}-{commit[:7]}/bin/python -m pip install --no-index --no-deps <wheel>
/opt/sched/{package_version}-{commit[:7]}/bin/sched --version
/opt/sched/{package_version}-{commit[:7]}/bin/sched --help
```

Follow `docs/execution-rollout.md` in the source archive before any live switch.
Use an authorized maintenance window, drain running work, retain the original installation
and an approved compatible-state recovery point, then use only CLI checks and mutations.
Writing uses database schema {database_schema}; old writers must not be assumed compatible.
Never replay unknown attempts or delete bindings during rollback. Do not remove an
installation used by live owners. `linux_fd_owner` needs explicit FD4 owner-wrapper support.
""", encoding="utf-8")
    files = {p.name: {"sha256": hashlib.sha256(p.read_bytes()).hexdigest(), "bytes": p.stat().st_size}
             for p in sorted(destination.iterdir()) if p.is_file()}
    manifest = {"schema_version": 1, "candidate": True, "published": False, "deployed": False,
                "version": package_version, "commit": commit, "database_schema": database_schema,
                "python": sys.version.split()[0], "platform": sysconfig.get_platform(),
                "libc": list(platform.libc_ver()), "native_abi": sysconfig.get_config_var("SOABI") if args.native else None,
                "files": files,
                "independent_installations": evidence,
                "build_tools": {n: tool_version(n) for n in ("setuptools", "wheel", "pip", "packaging")}}
    if ci is not None:
        manifest["ci"] = ci
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    verify_candidate(destination, commit, require_ci=ci is not None)
    print(json.dumps({"directory": str(destination), "commit": commit, "version": package_version}), flush=True)


if __name__ == "__main__":
    main()
