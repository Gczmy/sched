"""Existing real Slurm lease, independent CPU/CLI state; no lease mutation."""
import copy
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

from run_cluster_lease_accept import LeaseAcceptance


class AncestryAcceptance(LeaseAcceptance):
    def controller(self, state_name, *, unavailable=False):
        job = self.original_job
        raw = {"jobs": [{"job_id": int(job["job_id"]), "job_state": [state_name],
                "user_id": job["user_id"], "start_time": job["start_time"], "end_time": job["end_time"],
                "cpus": job["cpus"], "restart_cnt": job["restart_count"], "nodes": job["nodes"]}],
                "errors": [], "warnings": []}
        path = self.root / "controller-next.json"
        path.write_text(json.dumps({"unavailable": unavailable, "response": raw}))
        path.replace(self.root / "controller.json")

    def run(self):
        self.cfg.update(cpus_total="auto", cpus_auto_max=1,
                        lease_validation={"membership": "launch_ancestry", "interval_sec": 1})
        self.config.write_text(json.dumps(self.cfg))
        self.cli("daemon", "drain")
        self.cli("daemon", "start", "--fake")
        actual = self.wait(self.snapshot, lambda r: bool(r))[-1]
        self.wait(lambda: self.data("daemon", "status", "--json"), lambda r: r["health_state"] == "healthy")
        observed = actual["recorded_check"]["data"]
        assert observed["allocation_state"] == "valid", observed
        assert observed["protection_level"] == "launch_ancestry_affinity"
        assert observed["job_cgroup_verified"] is False and observed["hard_isolation"] is False
        assert observed["current_daemon_slurm_membership_verified"] is False
        original = actual["origin"]
        assert original["launch_ancestry"]["known"]
        assert observed["slurm_observation"]["launch_tracking"]["present"]
        capacity = self.data("cpu-capacity", "--json")
        assert capacity["available"] and capacity["effective_total"] == 1, capacity
        self.original_job = copy.deepcopy(observed["slurm_observation"]["job"])
        self.private_drain()
        print("PASS: actual original Slurm anchor/step tracking and affinity verified; CPU auto capped at one; no cgroup/hard-isolation claim", flush=True)

        binary = self.root / "bin"
        binary.mkdir()
        real_scontrol = shutil.which("scontrol")
        assert real_scontrol
        path = binary / "scontrol"
        path.write_text("#!" + sys.executable + "\nimport json,os,sys\nfrom pathlib import Path\n"
                        "if sys.argv[1:4] == ['--json','show','job']:\n"
                        " data=json.loads(Path(" + repr(str(self.root / "controller.json")) + ").read_text())\n"
                        " if data['unavailable']: raise SystemExit(1)\n"
                        " print(json.dumps(data['response']))\n"
                        "else:\n"
                        " assert sys.argv[1] in ('listpids','show')\n"
                        " os.execv(" + repr(real_scontrol) + ",[" + repr(real_scontrol) + ",*sys.argv[1:]])\n")
        path.chmod(0o700)
        self.env["PATH"] = str(binary) + os.pathsep + os.environ.get("PATH", "")
        self.controller("RUNNING")
        # submit may auto-start its daemon. Reset stop-when-idle before that
        # start; otherwise an incidental drained birth can exit immediately.
        self.cli("daemon", "drain")
        first = self.worker("first", "first")
        first["cmd"][-1] += ("\nimport time\nend=time.monotonic()+80\n"
                            "while not Path(" + repr(str(self.root / "release")) + ").exists():\n"
                            " if time.monotonic()>end: raise SystemExit(2)\n time.sleep(.05)\n")
        batch = self.submit("launch-ancestry", [first, self.worker("second", "second")], failure_policy="continue_independent")
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "first"), lambda r: r["jobs"][0]["status"] == "running" and (self.root / "first/runs.txt").exists())
        lease = next(r for r in self.snapshot() if r["current_owner_binding"])
        lease_id = lease["origin"]["lease_id"]
        birth = copy.deepcopy(lease["origin"])
        self.controller("RUNNING", unavailable=True)
        self.wait(lambda: self.snapshot(lease_id)[0], lambda r: r["recorded_check"]["data"]["allocation_state"] == "unknown")
        assert self.job(batch, "first")["jobs"][0]["status"] == "running"
        assert self.data("allocations", f"{batch}:second", "--json")["allocations"] == []
        self.controller("RUNNING")
        self.wait(lambda: self.snapshot(lease_id)[0], lambda r: r["recorded_check"]["data"]["allocation_state"] == "valid")
        assert self.snapshot(lease_id)[0]["origin"] == birth
        print("PASS: controller failure pauses new CPU dispatch; same original identity recovers from unknown without replacing birth or running worker", flush=True)
        self.controller("CANCELLED")
        self.wait(lambda: self.snapshot(lease_id)[0], lambda r: r["recorded_check"]["data"]["invalid_latched"])
        assert self.job(batch, "first")["jobs"][0]["status"] == "running"
        (self.root / "release").touch()
        self.wait(lambda: self.job(batch, "first"), lambda r: r["jobs"][0]["status"] == "done")
        self.controller("RUNNING")
        before = self.snapshot(lease_id)[0]["recorded_check"]["seq"]
        retained = self.wait(lambda: self.snapshot(lease_id)[0], lambda r: r["recorded_check"]["seq"] > before)
        assert retained["recorded_check"]["data"]["invalid_latched"]
        assert self.job(batch, "second")["jobs"][0]["status"] == "pending"
        assert self.data("allocations", f"{batch}:second", "--json")["allocations"] == []
        assert (self.root / "first/runs.txt").read_text() == "run\n"
        self.private_drain()
        self.cli("daemon", "resume")
        self.cli("daemon", "start", "--fake")
        self.wait(lambda: self.job(batch, "second"), lambda r: r["jobs"][0]["status"] == "done")
        assert (self.root / "second/runs.txt").read_text() == "run\n"
        self.private_drain()
        assert self.snapshot(lease_id)[0]["origin"] == birth
        print("PASS: fixture terminal job latches invalid, original worker settles once, later RUNNING cannot resume; explicit private restart alone runs second task once", flush=True)


def main():
    if sys.platform != "linux" or not os.environ.get("SLURM_JOB_ID") or not os.environ.get("SLURM_STEP_ID"):
        raise SystemExit("Run inside the authorized existing Linux Slurm compute lease; not gateway/standalone.")
    temporary = tempfile.mkdtemp(prefix="sched-launch-ancestry-")
    acceptance = AncestryAcceptance(Path(temporary))
    try:
        acceptance.run()
    except BaseException:
        print("FAILED: independent fixture retained at " + temporary, flush=True)
        raise
    finally:
        acceptance.cli("daemon", "stop")
        if acceptance.data("daemon", "status", "--json")["health_state"] != "stopped":
            raise RuntimeError("Private daemon not stopped; retained " + temporary)
    shutil.rmtree(temporary)


if __name__ == "__main__":
    main()
