"""Explicitly authorized CPU-only device BPF acceptance; no CUDA/ioctls.

Not part of automatic CI. The operator supplies an existing private delegation
and confirms permission to load/attach BPF in newly owned child scopes. This
script never prepares a parent, attaches there, requests a lease, or uses sudo.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

from run_cpu_cgroup_accept import CgroupAcceptance


def authorize(arguments, environment, platform):
    if arguments != ["--positive-cpu"]:
        raise ValueError("usage: run_device_positive_accept.py --positive-cpu")
    if platform != "linux":
        raise ValueError("Run on an authorized Linux compute node, never a gateway/laptop.")
    if environment.get("SCHED_TEST_DEVICE_ATTACH_AUTHORIZED") != "1":
        raise ValueError("Requires explicit BPF attach authorization: SCHED_TEST_DEVICE_ATTACH_AUTHORIZED=1")
    root = environment.get("SCHED_TEST_CPU_SCOPE_ROOT", "")
    if not root.startswith("/") or str(Path(root)) != root or root == "/" or ".." in Path(root).parts:
        raise ValueError("Supply a canonical absolute private SCHED_TEST_CPU_SCOPE_ROOT")
    # Kernel mount, private domain, original boundary, cpuset and ownership are
    # checked by daemon check/controller. This flag cannot grant kernel rights.
    return root


def probe_program(names):
    """No arbitrary path, device creation, ioctl, inherited GPU FD or CUDA."""
    if (type(names) is not list or not names or len(names) > 130
            or len(set(names)) != len(names)
            or any(type(name) is not str or not (name in {"nvidiactl", "nvidia-uvm"}
                or re.fullmatch(r"nvidia(?:0|[1-9][0-9]{0,2})", name) and int(name[6:]) <= 254) for name in names)):
        raise ValueError("Expected bounded exact NVIDIA device basenames")
    return ("import errno,json,os\n"
            "result={}\n"
            "for name in ['null','zero']+" + repr(names) + ":\n"
            " try:\n"
            "  fd=os.open('/dev/'+name,os.O_RDONLY|os.O_CLOEXEC|os.O_NOFOLLOW|os.O_NONBLOCK)\n"
            " except OSError as error: result[name]={'errno':error.errno}\n"
            " else:\n"
            "  os.close(fd)\n"
            "  result[name]={'opened':True}\n"
            "print(json.dumps(result,sort_keys=True))\n")


def verify_probe(result, names, *, denied):
    import errno
    expected = {name: {"opened": True} for name in ["null", "zero", *names]}
    if denied:
        expected.update({name: {"errno": errno.EPERM} for name in names})
    if result != expected:
        raise AssertionError(("exact open results differ; no kernel boundary proven", result, expected))


class DevicePositive(CgroupAcceptance):
    daemon_flags = ()  # Real read-only topology; CPU-only submissions, no fake.

    def __init__(self, root):
        super().__init__(root)
        self.env.pop("SCHED_FAKE_GPUS", None)
        self.device_snapshots = {}

    def save_config(self, delegated_root):
        super().save_config(delegated_root)
        self.cfg["device_isolation"] = {"mode": "nvidia"}
        self.config.write_text(json.dumps(self.cfg))

    def positive(self, delegated_root):
        self.save_config(delegated_root)
        inventory = self.data("device-inventory", "--with-mig-capability", "--json")["inventory"]
        self.names = sorted(name for name in inventory["nodes"] if name.startswith("nvidia"))
        self.probe = probe_program(self.names)
        # A parent/DAC denial cannot count as enforcement by our child policy.
        baseline = subprocess.run([sys.executable, "-I", "-c", self.probe],
            cwd=self.root, env=self.env, capture_output=True, text=True, timeout=15, check=True)
        verify_probe(json.loads(baseline.stdout), self.names, denied=False)
        super().positive(delegated_root)
        for identifier, frozen in self.device_snapshots.items():
            assert self.data("device-scopes", "--scope-id", identifier, "--json") == frozen
        print("PASS: CPU-only child BPF policies deny exact NVIDIA opens (EPERM), standard devices work, three backends/descendants inherit, original cancellation/removal/releases and stopped restart retain bindings; no CUDA/ioctl", flush=True)

    def scoped_worker(self, task, kind=None):
        worker = super().scoped_worker(task, kind)
        # Both the original worker and a fresh descendant perform new opens.
        # Each closes every successful FD before publishing the evidence.
        program = ("import json,subprocess,sys\nfrom pathlib import Path\n"
            "probe=" + repr(self.probe) + "\n"
            "import contextlib,io\noutput=io.StringIO()\n"
            "with contextlib.redirect_stdout(output): exec(probe,{})\n"
            "left=json.loads(output.getvalue())\n"
            "right=json.loads(subprocess.check_output([sys.executable,'-I','-c',probe],text=True,timeout=10))\n"
            "Path(" + repr(str(self.root / task / "devices.json")) + ").write_text(json.dumps([left,right]))\n")
        worker["cmd"][-1] = program + worker["cmd"][-1]
        if kind:
            self.cfg["execution_backends"][task]["argv"] = worker["cmd"]
        return worker

    def verify_scoped_worker(self, batch, task, scope, *, released):
        identifier = scope["scope_id"]
        result = self.data("device-scopes", "--scope-id", identifier, "--json")
        device = result["scopes"][0]
        assert device["allocation_id"] == scope["allocation_id"] and device["lease_id"] == scope["lease_id"]
        assert device["installation_consumed"] and device["launch_consumed"]
        assert device["original_program_binding"]["program_id"] > 0
        policy = device["intent"]["policy"]
        assert policy["default"] == "deny" and len(policy["rules"]) == 6
        assert {(r["kind"], r["major"], r["minor"], r["access"]) for r in policy["rules"]} == {
            ("char", 1, 3, 6), ("char", 1, 5, 6), ("char", 1, 7, 6),
            ("char", 1, 8, 6), ("char", 1, 9, 6), ("char", 5, 0, 6)}
        assert device["recorded_phase"] == ("released" if released else "launch_intent")
        assert device["release_recorded_ready"] is released
        observations = json.loads((self.root / task / "devices.json").read_text())
        assert len(observations) == 2
        for observation in observations:
            verify_probe(observation, self.names, denied=True)
        if released:
            assert device["events"][-1]["data"]["cpu_removed_event_id"] == scope["last_event_id"]
            self.device_snapshots[identifier] = result


def main():
    delegated_root = authorize(sys.argv[1:], os.environ, sys.platform)
    temporary = tempfile.mkdtemp(prefix="sched-device-positive-")
    acceptance = DevicePositive(Path(temporary))
    try:
        acceptance.positive(delegated_root)
    finally:
        for task in ("ordinary", "fd", "owner"):
            (acceptance.root / (task + "-release")).touch()
        acceptance.cli("daemon", "stop", timeout=150)
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private daemon not stopped; retained {temporary}")
    # On failure the exception skips removal: retain CLI-owned state/evidence.
    shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
