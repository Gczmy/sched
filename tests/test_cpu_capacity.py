"""Pure CPU decisions and passive synthetic state; no worker or Slurm execution."""
import argparse
import copy
import json
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from gsched import allocation, cli, config, cpu_capacity as cpu, daemon, resources, state
from gsched.dispatcher import Dispatcher
from gsched.execution_policy import digest
from test_cluster_lease import context, origin, sample
from test_review_cli_state import TempStateCase


class CpuDecisionTests(unittest.TestCase):
    def resolve(self, cfg=None, initial=None, current=None, observed=None, **kwargs):
        return cpu.resolve(cfg or {"cpus_total": "auto"}, origin=initial or origin(),
                           current=current or context(), slurm=sample() if observed is None else observed, **kwargs)

    def test_verified_minimum_and_optional_cap(self):
        initial = origin()
        initial["slurm_environment"].update(SLURM_CPUS_ON_NODE="4", SLURM_CPUS_PER_TASK="1", SLURM_NTASKS="8")
        value = self.resolve(initial=initial)
        self.assertEqual(1, value["effective_total"])
        self.assertTrue(value["available"])
        self.assertIn("cpu_capacity_sources_disagree", value["warnings"])
        self.assertNotIn("SLURM_NTASKS", [s["name"] for s in value["sources"]])
        self.assertEqual(1, self.resolve({"cpus_total": "auto", "cpus_auto_max": 1})["effective_total"])
        self.assertFalse(value["hard_isolation"])
        self.assertFalse(value["admission_granted"])

    def test_multi_node_total_is_not_divided_or_taken_as_node_capacity(self):
        observed = sample()
        observed["job"].update(hosts=["compute-a", "compute-b"], nodes="compute-[a-b]", cpus=8)
        self.assertFalse(self.resolve(observed=observed)["available"])
        initial = origin()
        initial["slurm_environment"]["SLURM_CPUS_ON_NODE"] = "4"
        value = self.resolve(initial=initial, observed=observed)
        self.assertTrue(value["available"])
        self.assertEqual(2, value["effective_total"])
        self.assertIsNone(value["sources"][1]["cpus"])

    def test_invalid_declarations_fail_closed(self):
        for number in ("0", "-1", "lots", None, True, "1048577"):
            initial = origin()
            initial["slurm_environment"]["SLURM_CPUS_ON_NODE"] = number
            with self.subTest(number=number):
                self.assertFalse(self.resolve(initial=initial)["available"])

    def test_unknown_controller_even_observe_allow_does_not_become_unlimited(self):
        for settings in ({"mode": "observe"}, {"unknown_policy": "allow"}):
            for observed in ({"known": False}, {**sample(), "observed_at": time.time() - 46}):
                value = self.resolve(initial=origin(**settings), observed=observed)
                self.assertFalse(value["available"])
                self.assertFalse(cpu.permits({"cpus_total": "auto"}, value, cpus=1, used=0, cpu_only=False, cpu_jobs=0))

    def test_confirmed_invalid_and_kernel_drift_even_observe_cannot_grant_auto(self):
        observed = sample()
        observed["job"]["states"] = ["CANCELLED"]
        self.assertFalse(self.resolve(initial=origin(mode="observe"), observed=observed)["available"])
        for key, value in (("pid", 101), ("affinity", [0]), ("cgroups", [])):
            current = context()
            current[key] = value
            self.assertFalse(self.resolve(initial=origin(mode="observe"), current=current)["available"])
        self.assertFalse(self.resolve(lease_decision={"invalid_latched": True})["available"])

    def test_cgroup_uncertainty_requires_explicit_lease_override(self):
        initial = origin()
        initial["cgroups"][0]["path"] = "/system.slice/slurmstepd.scope/system"
        current = {key: copy.deepcopy(initial[key]) for key in context()}
        self.assertFalse(self.resolve(initial=initial, current=current)["available"])
        initial["policy"]["unknown_policy"] = "allow"
        value = self.resolve(initial=initial, current=current)
        self.assertTrue(value["available"])
        self.assertIn("job_cgroup_membership_not_verified", value["warnings"])

    def test_standalone_uses_affinity_and_invalid_affinity_is_not_zero(self):
        initial = origin()
        initial["slurm_environment"] = {}
        self.assertTrue(self.resolve(initial=initial, observed={"known": False})["available"])
        for cpus in ([], None, [0, 0], [True], ["0"], [-1]):
            current = context()
            current["affinity"] = cpus
            self.assertFalse(self.resolve(initial=initial, current=current, observed={"known": False})["available"])

    def test_fixed_oversize_warns_without_silent_clamping(self):
        value = self.resolve({"cpus_total": 120})
        self.assertEqual(120, value["effective_total"])
        self.assertTrue(value["available"])
        self.assertIn("fixed_cpu_total_exceeds_observed_capacity", value["warnings"])

    def test_zero_and_none_keep_unlimited_gpu_and_cpu_only_concurrency(self):
        for number in (0, None):
            cfg = {"cpus_total": number, "max_cpu_jobs": 1}
            self.assertTrue(cpu.permits(cfg, None, cpus=999, used=999, cpu_only=False, cpu_jobs=1))
            self.assertFalse(cpu.permits(cfg, None, cpus=1, used=0, cpu_only=True, cpu_jobs=1))
            self.assertTrue(cpu.permits(cfg, None, cpus=999, used=999, cpu_only=True, cpu_jobs=0))

    def test_policy_rejects_ambiguous_values_and_cap_outside_auto(self):
        for patch in ({"cpus_total": True}, {"cpus_total": "120"}, {"cpus_total": -1},
                      {"cpus_total": "AUTO"}, {"cpus_total": 0, "cpus_auto_max": 1},
                      {"cpus_total": "auto", "cpus_auto_max": 0}, {"cpus_total": "auto", "cpus_auto_max": True}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                cpu.policy(patch)

    def test_both_cpu_and_gpu_reservations_use_same_auto_total(self):
        cfg = {"cpus_total": "auto"}
        value = self.resolve()
        for cpu_only in (False, True):
            self.assertFalse(cpu.permits(cfg, value, cpus=1, used=2, cpu_only=cpu_only, cpu_jobs=0))
            self.assertTrue(cpu.permits(cfg, value, cpus=1, used=1, cpu_only=cpu_only, cpu_jobs=99))
        self.assertFalse(cpu.permits(cfg, value, cpus=1, used=None, cpu_only=False, cpu_jobs=0))


class CpuObservationTests(TempStateCase):
    def setUp(self):
        super().setUp()
        self.cfg["cpus_total"] = "auto"
        self.owner = {"schema_version": 1, "lease_id": "a" * 32, "pid": 100,
                      "start_token": "proc:123", "physical_host": "compute-a"}
        self.health = {"query_host": "compute-a", "process_state": "running"}
        self.monitor = SimpleNamespace(origin={**origin(), "instance_id": None}, current_context=context(),
            sample=sample(), frozen_binding=None, decision={"invalid_latched": False}, owner=self.owner)
        from gsched.integration import instance_id
        with state.connect() as conn:
            self.monitor.origin["instance_id"] = instance_id(conn)
        self.dispatcher = SimpleNamespace(cfg=self.cfg, _cluster_lease=self.monitor, log_line=lambda line: None)
        for target, value in (("_read_lease_owner", lambda: copy.deepcopy(self.owner)),
                              ("health_snapshot", lambda: copy.deepcopy(self.health))):
            patcher = mock.patch.object(daemon, target, side_effect=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def report(self):
        with state.connect() as conn:
            return cpu.recorded(conn, self.cfg)

    def rewrite(self, fn):
        body = resources.read_private_json("daemon.cpu-capacity.json")
        body.pop("sha256")
        fn(body)
        resources.write_private_json("daemon.cpu-capacity.json", {**body, "sha256": digest(body)})

    def test_valid_passive_observation_never_probes_query_host(self):
        cpu.capture(self.dispatcher)
        with mock.patch("os.sched_getaffinity", create=True, side_effect=AssertionError("query affinity")), \
                mock.patch("gsched.cluster_lease.probe", side_effect=AssertionError("query Slurm")):
            value = self.report()
        self.assertTrue(value["available"])
        self.assertEqual(2, value["effective_total"])
        self.assertEqual(self.owner["lease_id"], value["lease_id"])

    def test_missing_observation_auto_unknown_not_unlimited(self):
        value = self.report()
        self.assertFalse(value["available"])
        self.assertIsNone(value["effective_total"])
        self.assertEqual("cpu_observation_missing", value["observation"]["error"])

    def test_stale_identity_owner_config_and_malformed_observations_rejected(self):
        mutations = [lambda b: b.update(captured_at=time.time() - 46),
                     lambda b: b.update(captured_at=time.time() + 60),
                     lambda b: b.update(instance_id="foreign"), lambda b: b.update(node="foreign"),
                     lambda b: b["owner"].update(start_token="reused"),
                     lambda b: b.update(policy_sha256="old"),
                     lambda b: b["capacity"].update(effective_total="auto"),
                     lambda b: b["capacity"].update(unknown="not-list"),
                     lambda b: b["capacity"].update(sources=[{"cpus": True}])]
        for mutate in mutations:
            cpu.capture(self.dispatcher)
            self.rewrite(mutate)
            value = self.report()
            self.assertFalse(value["available"])
            self.assertIsNone(value["effective_total"])
        cpu.capture(self.dispatcher)
        self.health["process_state"] = "stopped"
        self.assertFalse(self.report()["available"])

    def test_fixed_and_zero_remain_declarations_without_live_observation(self):
        for number in (0, 120):
            self.cfg["cpus_total"] = number
            value = self.report()
            self.assertEqual(number, value["effective_total"])
            self.assertTrue(value["available"])
            self.assertEqual("cpu_observation_missing", value["observation"]["error"])

    def test_status_default_numeric_or_omitted_and_explicit_query_contract(self):
        self.assertNotIn("cpu", self.status_json())
        cpu.capture(self.dispatcher)
        self.assertEqual({"used": 0, "total": 2}, self.status_json()["cpu"])
        with mock.patch.object(cli, "load_config", return_value=self.cfg):
            rc, stdout, stderr = self.capture(cli.cmd_cpu_capacity, argparse.Namespace(json=True))
        self.assertEqual(0, rc, stderr)
        result = json.loads(stdout)
        self.assertEqual("sched-cpu-capacity-v1", result["contract"])
        self.assertEqual("auto", result["configured"])
        self.assertEqual(0, result["used"])
        self.assertFalse(result["admission_granted"])

    def test_legacy_gpu_without_explicit_cpus_unknown_auto_fixed_still_compatible(self):
        identifier = self.seed_batch(job_status="running")
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks").fetchone()[0])
            spec["resources"] = {"gpu": 1}
            conn.execute("UPDATE tasks SET spec=?", (json.dumps(spec),))
        with state.connect() as conn, self.assertRaises(cpu.CpuReservationUnknown):
            cpu.reserved(conn, self.cfg, lambda spec: 8)
        with state.connect() as conn:
            self.assertEqual(8, cpu.reserved(conn, {"cpus_total": 0}, lambda spec: 8))
        cpu.capture(self.dispatcher)
        self.assertNotIn("cpu", self.status_json())

    def test_running_allocation_default_remains_frozen_after_hot_default_shrink(self):
        job = self.seed_batch(job_status="running")
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = {"gpu_job_cpus": 8}
        dispatcher.fake = True
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks").fetchone()[0])
            spec["resources"] = {"gpu": 1}
            conn.execute("UPDATE tasks SET spec=?", (json.dumps(spec),))
            state.update_job(conn, job, pgid=None)
            allocation.reserve(conn, job, spec, dispatcher)
        dispatcher.cfg = {"cpus_total": "auto", "gpu_job_cpus": 1}
        with state.connect() as conn:
            self.assertEqual(8, dispatcher._cpu_in_use(conn))

    def test_initial_cpu_rejection_precedes_old_marker_cleanup_or_launch_claim(self):
        job = self.seed_batch(job_status="pending")
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = {"cpus_total": 2}
        dispatcher._read_gpu_policy = mock.Mock(return_value={"cpus_total": "auto"})
        dispatcher._task_has_unresolved_launch_marker = mock.Mock(return_value=False)
        dispatcher._exact_dependencies_successful = mock.Mock(return_value=True)
        dispatcher._task_dependencies_successful = mock.Mock(return_value=True)
        dispatcher._job_uses_native_exec = mock.Mock(return_value=False)
        dispatcher._prepare_launch_marker = mock.Mock(side_effect=AssertionError("marker cleanup"))
        dispatcher._release_in_tx = mock.Mock()
        dispatcher.log_line = mock.Mock()
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks").fetchone()[0])
            dispatcher._load_task_launch_binding = mock.Mock(return_value=(spec, "p"))
            self.assertFalse(dispatcher._launch_job(conn, state.get_job(conn, job), 0))
            self.assertEqual("pending", state.get_job(conn, job)["status"])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM allocations").fetchone()[0])
        dispatcher._prepare_launch_marker.assert_not_called()
        dispatcher._release_in_tx.assert_called_once()

    def test_final_hot_shrink_uses_new_policy_not_dispatch_cached_capacity(self):
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = {"cpus_total": 2}
        dispatcher._read_gpu_policy = mock.Mock(return_value={"cpus_total": 1})
        with state.connect() as conn:
            self.assertFalse(dispatcher._cpu_launch_allowed(conn, {"resources": {"gpu": 1, "cpus": 2}}))
        dispatcher._read_gpu_policy.side_effect = RuntimeError("half-written config")
        with state.connect() as conn:
            self.assertFalse(dispatcher._cpu_launch_allowed(conn, {"resources": {"gpu": 0, "cpus": 1}}))


if __name__ == "__main__":
    unittest.main()
