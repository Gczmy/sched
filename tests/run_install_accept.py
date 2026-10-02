"""Build/install sched alone, including a default build after native compilation."""
from __future__ import annotations

import argparse
from email.parser import BytesParser
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--native", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="sched-install-accept-") as temporary:
        root = Path(temporary)
        source, unrelated = root / "source", root / "unrelated"
        source.mkdir()
        unrelated.mkdir()
        for name in ("gsched", "native"):
            shutil.copytree(ROOT / name, source / name,
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.so", "*.pyd"))
        for name in ("setup.py", "pyproject.toml", "MANIFEST.in"):
            shutil.copy2(ROOT / name, source / name)
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")

        def run(command, *, cwd=source, environment=env):
            result = subprocess.run(command, cwd=cwd, env=environment,
                                    capture_output=True, text=True, timeout=120)
            if result.returncode:
                raise AssertionError((command, result.stdout, result.stderr))
            return result.stdout

        def install(native: bool, label: str):
            dist = root / label
            build_env = {**env, "SCHED_BUILD_NATIVE": "1" if native else "0"}
            if not native:
                build_env["CC"] = "/nonexistent/compiler"
            run([sys.executable, "setup.py", "bdist_wheel", "--dist-dir", str(dist)], environment=build_env)
            wheel = next(dist.glob("*.whl"))
            with zipfile.ZipFile(wheel) as archive:
                files = archive.namelist()
                binaries = [name for name in files if name.endswith((".so", ".pyd"))]
                assert bool(binaries) is native, binaries
                metadata = BytesParser().parsebytes(archive.read(next(n for n in files if n.endswith("/METADATA"))))
                assert metadata["Name"] == "sched"
                assert not [value for value in metadata.get_all("Requires-Dist", []) if "extra ==" not in value]
                entries = archive.read(next(n for n in files if n.endswith("/entry_points.txt"))).decode()
                assert "sched = gsched.cli:main" in entries
            target = dist / "installed"
            run([sys.executable, "-m", "pip", "install", "--no-index", "--no-deps",
                 "--disable-pip-version-check", "--target", str(target), str(wheel)])
            runtime_env = {key: value for key, value in env.items() if not key.startswith("SCHED_")}
            runtime_env["PYTHONPATH"] = str(target)
            observed_version = run([str(target / "bin/sched"), "--version"], cwd=unrelated, environment=runtime_env)
            assert observed_version.strip() == "sched " + metadata["Version"]
            run([str(target / "bin/sched"), "--help"], cwd=unrelated, environment=runtime_env)
            version_env = dict(runtime_env, SCHED_CONFIG=str(unrelated / "invalid-config.json"),
                               SCHED_STATE=str(unrelated / "untouched-state"))
            (unrelated / "invalid-config.json").write_text("invalid JSON", encoding="utf-8")
            version_info = json.loads(run([str(target / "bin/sched"), "version", "--json"],
                                          cwd=unrelated, environment=version_env))
            assert version_info["schema_version"] == 1 and version_info["query"] == "version"
            assert version_info["sched_version"] == metadata["Version"]
            assert not (unrelated / "untouched-state").exists()
            probe = (
                "import json,pathlib,gsched; from gsched.execution import LinuxFdBackend,BackendUnavailable; "
                "print(json.dumps({'file':gsched.__file__})); "
                + ("LinuxFdBackend()" if native else
                   "\ntry: LinuxFdBackend()\nexcept BackendUnavailable: pass\nelse: raise AssertionError('unexpected compiled backend')")
            )
            observed = run([sys.executable, "-c", probe], cwd=unrelated, environment=runtime_env)
            assert Path(json.loads(observed)["file"]).is_relative_to(target)
            if native:
                # The service uses isolated Python and must load native from the
                # installed wheel, even when no repository is on its path.
                owner_probe = """
import os, secrets
from gsched.execution import ExecutionEnvelope, PersistentLinuxFdBackend, PersistentOwner
executable = os.open('/bin/true', os.O_RDONLY | os.O_CLOEXEC)
cwd = os.open('.', os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
try:
    prepared = PersistentLinuxFdBackend().prepare(ExecutionEnvelope(('true',)),
        executable_fd=executable, cwd_fd=cwd, fd_bindings={},
        identity={'schema': 'sched_execution_identity/v1', 'attempt_id': secrets.token_hex(16)})
finally:
    os.close(cwd); os.close(executable)
owner = prepared.launch()
observation = PersistentOwner(owner.binding).wait(10)
assert observation.returncode == 0 and observation.group_clean is True
assert observation.rusage is not None
owner.close(); prepared.close()
"""
                run([sys.executable, "-c", owner_probe], cwd=unrelated, environment=runtime_env)
            print("PASS:", label, "installed public CLI works independently; native =", native, flush=True)

        install(False, "default-clean")
        if args.native:
            run([sys.executable, "setup.py", "build_ext", "--inplace"], environment={**env, "SCHED_BUILD_NATIVE": "1"})
            install(True, "native")
            install(False, "default-after-native")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
