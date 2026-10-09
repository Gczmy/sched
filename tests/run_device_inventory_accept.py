"""Compute-only read-only inventory observation; no GPU work or BPF effects.

Uses a new private config outside any scheduler state. An unavailable real
inventory stays unavailable, never an empty successful map or isolation proof.
"""
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile

from run_feedback_accept import Acceptance


def run():
    if sys.platform != "linux":
        raise RuntimeError("Run on an authorized Linux compute lease, never the laptop/gateway.")
    temporary = tempfile.mkdtemp(prefix="sched-device-inventory-cli-")
    root = Path(temporary)
    fixture = Acceptance(root)
    before = hashlib.sha256(fixture.config.read_bytes()).hexdigest()
    try:
        result = fixture.cli("device-inventory", "--json", expect=None)
        assert result.returncode in (0, 1), result
        assert not (root / "state").exists(), "read-only inventory initialized scheduler state"
        assert hashlib.sha256(fixture.config.read_bytes()).hexdigest() == before
        if result.returncode == 0:
            report = json.loads(result.stdout)
            assert report["contract"] == "sched-device-inventory-v1" and report["node"] == fixture.cfg["node"]
            assert report["runtime_probed"] is True and report["effect"] == "read_only_probe"
            for key in ("admission_granted", "wait_authority_granted", "physical_boundary_verified"):
                assert report[key] is False
            print("OBSERVED: complete real read-only inventory; not installation/isolation acceptance", flush=True)
            print(json.dumps(report, sort_keys=True), flush=True)  # Keep output private.
        else:
            assert not result.stdout and "device-inventory 查询失败" in result.stderr
            print("OBSERVED: real device inventory unavailable; no map or hardware success claimed", flush=True)
            print(result.stderr.strip(), flush=True)
        print("PASS: actual diagnostic never creates state or changes config; failure is explicit", flush=True)

        # Test inputs, not scheduler config/state: no existing instance exists.
        fixture.cfg["node"] = "different-compute.example.invalid"
        fixture.config.write_text(json.dumps(fixture.cfg))
        fixture.env["SCHED_ALLOW_FOREIGN_WRITE"] = "1"
        result = fixture.cli("device-inventory", "--json", expect=1)
        assert not result.stdout and "必须在" in result.stderr and not (root / "state").exists()
        print("PASS: wrong-host probe rejected even with foreign-write override; no daemon/DB/BPF effects", flush=True)
    except BaseException:
        print("Fixture retained: " + temporary, file=sys.stderr, flush=True)
        raise
    if (root / "state").exists():
        raise RuntimeError("Unexpected scheduler state retained: " + temporary)
    shutil.rmtree(temporary)


if __name__ == "__main__":
    run()
