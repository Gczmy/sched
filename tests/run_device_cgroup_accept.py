"""Compute-only device config denial; never install/attach or probe real GPUs."""
import json
from pathlib import Path
import shutil
import sys
import tempfile

from run_cpu_cgroup_accept import CgroupAcceptance


class DeviceDenial(CgroupAcceptance):
    def save_config(self, delegated_root):
        super().save_config(delegated_root)
        self.cfg["device_isolation"] = {"mode": "nvidia"}
        self.config.write_text(json.dumps(self.cfg))

    def deny(self):
        super().deny()
        result = self.data("daemon", "check", "--fake", "--json", expect=None)
        assert any(c["id"] == "device_cgroup_preflight" and c["level"] == "fail" for c in result["checks"]), result
        assert self.data("device-scopes", "--json")["scopes"] == []
        assert self.data("device-inventory-bindings", "--json")["bindings"] == []
        print("PASS: explicit device mode/private invalid delegation/fake topology denied without BPF or GPU effects", flush=True)


def run():
    if sys.platform != "linux":
        raise RuntimeError("Run on an authorized compute node; never a gateway/laptop.")
    temporary = tempfile.mkdtemp(prefix="sched-device-denial-")
    acceptance = DeviceDenial(Path(temporary))
    try:
        acceptance.deny()
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError(f"Private daemon not stopped; retained {temporary}")
    shutil.rmtree(temporary)


if __name__ == "__main__":
    if sys.argv[1:]:
        raise SystemExit("usage: run_device_cgroup_accept.py (denial only; no positive attach authorization)")
    run()
