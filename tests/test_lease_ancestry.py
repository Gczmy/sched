"""Mocked launch evidence and private DB; never spawns a Slurm/worker process."""
import copy
import unittest
from unittest import mock

from gsched import cluster_lease as lease, cpu_capacity, lease_ancestry as ancestry, lease_probe
from test_cluster_lease import LeaseEvidenceTests, context, origin, sample

BOOT = "00000000-0000-0000-0000-000000000001"


def processes():
    return {100: dict(pid=100, ppid=90, pgrp=100, session=100, start_ticks=123, uid=1234, comm="python"),
            90: dict(pid=90, ppid=80, pgrp=80, session=80, start_ticks=122, uid=1234, comm="python"),
            80: dict(pid=80, ppid=70, pgrp=80, session=80, start_ticks=121, uid=1234, comm="bash"),
            70: dict(pid=70, ppid=1, pgrp=70, session=70, start_ticks=120, uid=0, comm="slurmstepd")}


def compatible():
    initial = origin(membership="launch_ancestry")
    initial["cgroups"][0]["path"] = "/system.slice/slurmstepd.scope/system"
    current = {key: copy.deepcopy(initial[key]) for key in context()}
    chain = processes()
    anchor = dict(chain[80], affinity=[0, 1], cgroups=current["cgroups"])
    initial["launch_ancestry"] = {"known": True, "job_id": "42", "step_id": "0", "boot_id": BOOT,
                                   "chain": list(chain.values()), "anchor": anchor, "stepd": chain[70]}
    current["launch_anchor"] = {"known": True, "invalid": False, "boot_id": BOOT, "anchor": anchor, "stepd": chain[70]}
    observed = sample()
    observed["launch_tracking"] = {"known": True, "job_id": "42", "step_id": "0", "anchor_pid": 80, "present": True}
    return initial, current, observed


class AncestryCaptureTests(unittest.TestCase):
    def capture(self, changes=None, *, anchor_changes=None, reread=False):
        initial, current, _ = compatible()
        table = processes()
        table.update(changes or {})
        anchor = dict(table[80], affinity=[0, 1], cgroups=current["cgroups"], **(anchor_changes or {}))
        def read(pid):
            value = table[pid]
            if reread and pid == 90:
                value = dict(value, start_ticks=value["start_ticks"] + 1)
                table[90] = value
            return copy.deepcopy(value)
        with mock.patch.object(ancestry, "boot_id", return_value=BOOT), mock.patch.object(ancestry, "process", side_effect=read), \
                mock.patch.object(ancestry, "anchor_context", return_value=anchor):
            return ancestry.capture(current, initial["slurm_environment"])

    def test_capture_tracks_real_parent_edges_even_with_new_daemon_session(self):
        value = self.capture()
        self.assertTrue(value["known"])
        self.assertEqual(80, value["anchor"]["pid"])
        self.assertEqual([100, 90, 80, 70], [p["pid"] for p in value["chain"]])

    def test_env_or_process_name_without_root_slurm_parent_cannot_capture(self):
        for change in (dict(processes()[70], uid=1234), dict(processes()[70], comm="bash"), dict(processes()[90], ppid=1)):
            self.assertFalse(self.capture({change["pid"]: change})["known"])
        with mock.patch.object(ancestry, "process") as read:
            self.assertFalse(ancestry.capture(context(), {"SLURM_JOB_ID": "42"})["known"])
            read.assert_not_called()

    def test_raced_parent_or_wrong_anchor_identity_is_unknown(self):
        self.assertFalse(self.capture(reread=True)["known"])
        self.assertFalse(self.capture(anchor_changes={"uid": 99})["known"])
        self.assertFalse(self.capture(anchor_changes={"ppid": 5})["known"])

    def test_observe_preserves_original_anchor_and_detects_loss_reuse_and_boot(self):
        initial, current, _ = compatible()
        frozen = initial["launch_ancestry"]
        for boot, anchor, exception, expected in (
                (BOOT, current["launch_anchor"]["anchor"], None, False),
                (BOOT, dict(frozen["anchor"], start_ticks=999), None, True),
                ("00000000-0000-0000-0000-000000000002", None, None, True),
                (BOOT, None, FileNotFoundError(), True),
                (BOOT, None, PermissionError(), None)):
            with mock.patch.object(ancestry, "boot_id", return_value=boot), \
                    mock.patch.object(ancestry, "anchor_context", return_value=anchor, side_effect=exception), \
                    mock.patch.object(ancestry, "process", return_value=frozen["stepd"]):
                value = ancestry.observe(frozen)
            self.assertEqual(expected is not None, value["known"])
            if expected is not None:
                self.assertEqual(expected, value["invalid"])

    def test_proc_reader_handles_parentheses_and_bounds_without_commands(self):
        fields = ["S", "90", "100", "100"] + ["0"] * 15 + ["123"]
        raw = "100 (python (test)) " + " ".join(fields)
        with mock.patch.object(ancestry, "text", return_value=raw), mock.patch.object(ancestry.Path, "stat", return_value=mock.Mock(st_uid=1234)):
            self.assertEqual("python (test)", ancestry.process(100)["comm"])
        for raw in ("99 (python) " + " ".join(fields), "100 (python) S 1", "100 (python) " + " ".join(["Z", *fields[1:]])):
            with mock.patch.object(ancestry, "text", return_value=raw), self.assertRaises(ValueError):
                ancestry.process(100)

    def test_missing_boot_file_is_unknown_not_confirmed_anchor_death(self):
        initial, _, _ = compatible()
        with mock.patch.object(ancestry, "boot_id", side_effect=FileNotFoundError()):
            self.assertFalse(ancestry.observe(initial["launch_ancestry"])["known"])


class AncestryDecisionTests(unittest.TestCase):
    def test_explicit_mode_validates_lineage_not_job_cgroup_or_hard_isolation(self):
        initial, current, observed = compatible()
        value = lease.decide(initial, current, observed)
        self.assertEqual("valid", value["allocation_state"])
        self.assertTrue(value["dispatch_allowed"])
        for key in ("hard_isolation", "job_cgroup_verified", "current_daemon_slurm_membership_verified"):
            self.assertFalse(value[key])
        initial["policy"] = lease.policy({})
        self.assertFalse(lease.decide(initial, current, observed)["dispatch_allowed"])

    def test_unknown_tracking_or_anchor_never_uses_allow_and_does_not_latch(self):
        for where, key, value in (("current", "launch_anchor", {"known": False}),
                                  ("sample", "launch_tracking", {"known": False}),
                                  ("origin", "launch_ancestry", {"known": False})):
            initial, current, observed = compatible()
            {"origin": initial, "current": current, "sample": observed}[where][key] = value
            decision = lease.decide(initial, current, observed)
            self.assertEqual("unknown", decision["allocation_state"])
            self.assertFalse(decision["dispatch_allowed"])
            self.assertFalse(decision["invalid_latched"])
        for settings in ({"unknown_policy": "allow"}, {"mode": "observe"}, {"membership": True}, {"membership": []}):
            with self.assertRaises(ValueError):
                lease.policy({"lease_validation": {"membership": "launch_ancestry", **settings}})

    def test_anchor_loss_or_controller_invalidation_is_latched(self):
        for change in ("anchor_absent", "tracking_absent", "cancelled", "resize", "restart", "affinity", "boot"):
            initial, current, observed = compatible()
            if change == "anchor_absent":
                current["launch_anchor"] = {"known": True, "invalid": True, "reason": "original_launch_anchor_absent"}
            elif change == "tracking_absent":
                observed["launch_tracking"]["present"] = False
            elif change == "cancelled":
                observed["job"]["states"] = ["CANCELLED"]
            elif change in ("resize", "restart"):
                observed["job"]["cpus" if change == "resize" else "restart_count"] += 1
            elif change == "affinity":
                current["affinity"] = [0]
            else:
                current["launch_anchor"]["boot_id"] = "new-boot"
            bound = lease.binding(sample()["job"])
            # Match a frozen timestamp instead of creating two independently timed samples.
            bound.update(start_time=observed["job"]["start_time"])
            decision = lease.decide(initial, current, observed, frozen_binding=bound)
            self.assertTrue(decision["invalid_latched"], change)
            initial, current, observed = compatible()
            self.assertFalse(lease.decide(initial, current, observed, invalid_latched=True)["dispatch_allowed"])

    def test_foreign_job_step_anchor_and_malformed_tracking_are_not_proof(self):
        for key, value in (("job_id", "99"), ("step_id", "1"), ("anchor_pid", 999), ("present", 1)):
            initial, current, observed = compatible()
            observed["launch_tracking"][key] = value
            self.assertFalse(lease.decide(initial, current, observed)["dispatch_allowed"])
        initial, current, observed = compatible()
        initial["slurm_environment"]["SLURM_STEP_ID"] = "1"
        self.assertFalse(lease.decide(initial, current, observed)["dispatch_allowed"])

    def test_auto_capacity_uses_same_verified_ancestry_and_pauses_on_unknown(self):
        initial, current, observed = compatible()
        value = cpu_capacity.resolve({"cpus_total": "auto"}, origin=initial, current=current, slurm=observed)
        self.assertTrue(value["available"])
        self.assertEqual(2, value["effective_total"])
        observed["launch_tracking"] = {"known": False}
        self.assertFalse(cpu_capacity.resolve({"cpus_total": "auto"}, origin=initial, current=current, slurm=observed)["available"])

    def test_parser_accepts_only_exact_local_tracking_table(self):
        header = "PID JOBID STEPID LOCALID GLOBALID\n"
        for output, known, present in ((header + "80 42 0 0 0\n", True, True), (header, True, False),
                                     (header + "80 99 0 0 0\n", False, None), ("no processes", False, None),
                                     (header + "80 42 0 0 0\n80 42 0 0 0\n", False, None)):
            with mock.patch.object(lease_probe, "command", return_value=output) as command:
                value = lease_probe.launch_tracking("42", "0", 80)
            self.assertEqual(known, value["known"])
            if known:
                self.assertEqual(present, value["present"])
            command.assert_called_once_with(["listpids", "42.0"])


class AncestryEvidenceTests(LeaseEvidenceTests):
    def test_original_capture_is_not_replaced_after_cli_exit_or_unknown_recovery(self):
        initial, current, observed = compatible()
        self.current = {key: current[key] for key in context()}
        self.observed = observed
        original_sample = copy.deepcopy(observed)
        with mock.patch.object(ancestry, "capture", return_value=initial["launch_ancestry"]), \
                mock.patch.object(ancestry, "observe", return_value=current["launch_anchor"]), \
                mock.patch.object(lease, "probe", side_effect=lambda jid, **kwargs: copy.deepcopy(self.observed)):
            monitor = self.monitor(membership="launch_ancestry")
            original = copy.deepcopy(monitor.origin)
            self.assertTrue(monitor.decision["dispatch_allowed"])
            self.observed["launch_tracking"] = {"known": False}
            self.assertFalse(monitor.update(force=True))
            self.observed = copy.deepcopy(original_sample)
            self.assertTrue(monitor.update(force=True))
            self.assertEqual(original, monitor.origin)
            monitor.finish("fixture_finished")
        self.assertEqual(original, self.report(lease_id=self.owner["lease_id"])["leases"][0]["origin"])

    def test_missing_birth_ancestry_cannot_be_adopted_later(self):
        initial, current, observed = compatible()
        self.observed = observed
        with mock.patch.object(ancestry, "capture", return_value={"known": False}), \
                mock.patch.object(ancestry, "observe", return_value=current["launch_anchor"]):
            monitor = self.monitor(membership="launch_ancestry")
            self.assertFalse(monitor.decision["dispatch_allowed"])
            self.assertFalse(monitor.update(force=True))
            self.assertEqual({"known": False}, monitor.origin["launch_ancestry"])
