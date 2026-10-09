"""Pure shared resource plans on synthetic state; no child execution."""
import json
import os
import time
from pathlib import Path
from unittest import mock

from gsched import admission, cli, gpu_admission, state, resources
from gsched.dispatcher import Dispatcher
from test_review_cli_state import TempStateCase


class AdmissionPlanTests(TempStateCase):
    def setUp(self):
        super().setUp()
        fake = mock.patch.dict(os.environ, SCHED_FAKE_GPUS="0:16,1:24", SCHED_FAKE_COMPUTE_APPS="")
        fake.start()
        self.addCleanup(fake.stop)
        self.cfg.update(gpus=[0, 1], co_locate=True, co_locate_safety=0.85, co_locate_max_jobs=4)
        self.cfg["projects"]["p"].update(gpu_quota=0)
        Path(self.config_path).write_text(json.dumps(self.cfg))
        self.dispatcher = Dispatcher(self.cfg, fake=True)
        self.addCleanup(self.dispatcher.log.close)
        with state.connect() as conn:
            state.init_gpus(conn, [0, 1])
        self.dispatcher.allocator.probe_capacity()

    def seed(self, conn, name, idx, peak, *, project="p"):
        state.insert_batch(conn, name, name, "mix", [], None, self.tmp.name, {}, project=project)
        state.insert_task(conn, name, "task", 1, {"resources": {"gpu": 1, "vram_gib": peak, "gpu_share": True}}, 0, project)
        state.insert_job(conn, name, name, "task", 1, None, None, project)
        state.update_job(conn, name, status="running", gpu=idx, pgid=90000 + idx)
        conn.execute("UPDATE gpus SET status='assigned',job_id=? WHERE idx=?", (name, idx))
        conn.execute("INSERT INTO gpu_jobs(gpu_id,job_id,vram_gib,updated_at) VALUES(?,?,?,?)", (idx, name, peak, state.now()))

    def plan(self, conn, spec):
        return admission.gpu_plan(conn, self.dispatcher, "candidate", spec, "p")

    def test_post_placement_load_and_plan_are_identical_to_actual_allocation(self):
        spec = {"resources": {"gpu_share": True, "vram_gib": 8}}
        with state.connect() as conn:
            self.seed(conn, "a", 0, 4)
            self.seed(conn, "b", 1, 8)
            before = [dict(row) for row in conn.execute("SELECT * FROM gpu_jobs ORDER BY job_id")]
            plan = self.plan(conn, spec)
            self.assertEqual(1, plan["selected"])
            self.assertEqual([0.75, 2 / 3], [card["load_after"] for card in plan["candidates"]])
            self.assertEqual(before, [dict(row) for row in conn.execute("SELECT * FROM gpu_jobs ORDER BY job_id")])
            self.assertEqual(plan["selected"], self.dispatcher._assign_in_tx(conn, "candidate", spec, "p"))
            self.assertEqual(8, conn.execute("SELECT vram_gib FROM gpu_jobs WHERE job_id='candidate'").fetchone()[0])

    def test_all_card_reasons_are_retained_without_signals_or_allocations(self):
        with state.connect() as conn:
            self.seed(conn, "a", 0, None)
            conn.execute("UPDATE gpus SET quarantined=1 WHERE idx=0")
            self.dispatcher._frozen_gpus.add(0)
            self.dispatcher._gpu_max_jobs[0] = 1
            self.cfg["projects"]["p"]["max_jobs"] = 1
            with mock.patch("os.killpg", side_effect=AssertionError("signal")):
                plan = self.plan(conn, {"resources": {"gpu_share": True, "vram_gib": 30}})
            reasons = plan["candidates"][0]["reasons"]
            for reason in ("quarantined", "frozen", "exclusive_assignment_present", "global_or_card_pack_limit", "project_pack_limit", "packing_vram_budget"):
                self.assertIn(reason, reasons)
            self.assertIsNone(plan["selected"])
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM gpu_jobs").fetchone()[0])

    def test_affinity_and_profile_peak_share_the_exact_selection(self):
        self.cfg["projects"]["p"].update(gpu_affinity=[0], gpu_affinity_hard=True)
        with state.connect() as conn:
            self.seed(conn, "a", 0, 12)
            spec = {"resources": {"gpu_share": True, "vram_gib": 8}}
            self.assertIsNone(self.plan(conn, spec)["selected"])
            self.cfg["projects"]["p"]["gpu_affinity_hard"] = False
            self.assertEqual(1, self.plan(conn, spec)["selected"])
            from gsched.dispatcher import _profile_cache_key
            conn.execute("INSERT INTO profile_cache(profile_key,peak_gib) VALUES (?,?)", (_profile_cache_key("p", "peak"), 25))
            spec["resources"]["profile_key"] = "peak"
            plan = self.plan(conn, spec)
            self.assertEqual(25, plan["cached_peak_gib"])
            self.assertIsNone(plan["selected"])

    def test_free_unknown_capacity_legacy_rule_is_not_silently_hardened(self):
        self.dispatcher.allocator._mem_cache = {0: 0.0, 1: 0.0}
        with state.connect() as conn:
            self.assertEqual(0, self.plan(conn, {"resources": {"vram_gib": 40}})["selected"])
            self.assertEqual(0, self.plan(conn, {"resources": {"gpu_share": True, "vram_gib": 40}})["selected"])
            self.seed(conn, "a", 0, 1)
            plan = self.plan(conn, {"resources": {"gpu_share": True, "vram_gib": 1}})
            self.assertIn("capacity_unknown_for_existing_pack", plan["candidates"][0]["reasons"])

    def test_fresh_decision_keeps_every_rejection_and_actual_permits_matches(self):
        self.cfg["projects"]["p"]["gpu_admission"] = {"min_free_gib": 20}
        self.dispatcher._gpu_admission_samples = gpu_admission.sample(self.dispatcher.allocator)
        with state.connect() as conn:
            conn.execute("UPDATE gpus SET quarantined=1,ignore_until='manual' WHERE idx=0")
            spec = {"resources": {"gpu": 1}}
            plan = self.plan(conn, spec)
            first = plan["candidates"][0]
            for reason in ("quarantined", "ignored", "reserved_vram_exceeds_capacity", "fresh_free_vram_headroom"):
                self.assertIn(reason, first["reasons"])
            self.assertEqual(1, plan["selected"])
            for card in plan["candidates"]:
                self.assertEqual(card["allowed"], gpu_admission.permits(conn, self.cfg, card["idx"], "candidate", spec, "p", self.dispatcher._gpu_admission_samples))
            self.assertEqual(plan["selected"], self.dispatcher._assign_in_tx(conn, "candidate", spec, "p"))

    def test_unknown_or_stale_fresh_observation_never_becomes_capacity(self):
        self.cfg["projects"]["p"]["gpu_admission"] = {}
        with state.connect() as conn:
            plan = self.plan(conn, {"resources": {"gpu": 1}})
            self.assertIsNone(plan["selected"])
            self.assertTrue(all("fresh_vram_sample" in card["unknown"] for card in plan["candidates"]))
            samples = gpu_admission.sample(self.dispatcher.allocator)
            for sample in samples.values():
                sample["sampled_at"] -= 10
            self.dispatcher._gpu_admission_samples = samples
            self.assertIsNone(self.plan(conn, {"resources": {"gpu": 1}})["selected"])

    def query(self):
        code, out, err = self.capture(cli.main, ["admission-explain", "explain:task", "--json"])
        self.assertEqual(0, code, err)
        return json.loads(out)

    def seed_explain(self):
        self.seed_batch(batch_id="explain", name="display", job_status="pending")
        with state.connect() as conn:
            raw = conn.execute("SELECT spec FROM tasks WHERE batch_id='explain'").fetchone()[0]
            spec = json.loads(raw)
            spec["resources"]["gpu"] = 1
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id='explain'", (json.dumps(spec),))

    def observation(self):
        with state.connect() as conn:
            admission.publish_runtime(admission.capture_runtime(self.dispatcher, conn, self.cfg, {"MemTotal": 128, "MemAvailable": 120}, 0, time.time()))

    def test_cli_query_is_passive_and_independent_of_strict_status(self):
        self.seed_explain()
        self.observation()
        revision = self.batch_revision("explain")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migrated")), mock.patch.object(resources, "host_memory", side_effect=AssertionError("gateway memory probe")), mock.patch.object(gpu_admission, "sample", side_effect=AssertionError("gateway gpu probe")), mock.patch.object(Dispatcher, "__init__", side_effect=AssertionError("daemon constructor")), mock.patch("os.killpg", side_effect=AssertionError("signal")):
            output = self.query()
        self.assertEqual("sched-admission-explain-v1", output["contract"])
        self.assertEqual("none", output["effect"])
        self.assertFalse(output["admission_granted"] or output["hard_isolation"])
        self.assertTrue(output["resource_fit"])
        self.assertEqual([], output["unknown"])
        self.assertTrue(output["unchecked_dispatch_gates"])
        self.assertEqual(1, output["order"]["rank_before_gates"])
        self.assertEqual(revision, self.batch_revision("explain"))
        with state.connect() as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM gpu_jobs").fetchone()[0])

    def test_missing_stale_and_config_lagging_runtime_are_explicit_unknown(self):
        self.seed_explain()
        output = self.query()
        self.assertIn("runtime_observation_missing", output["unknown"])
        self.assertIsNone(output["resource_fit"])
        self.observation()
        raw = resources.read_private_json("daemon.admission.json")
        raw["captured_at"] -= 100
        resources.write_private_json("daemon.admission.json", raw)
        self.assertIn("runtime_observation_stale_or_future", self.query()["unknown"])
        self.observation()
        self.cfg["cpus_total"] = 123
        Path(self.config_path).write_text(json.dumps(self.cfg))
        self.assertIn("runtime_configuration_lag", self.query()["unknown"])

    def test_changed_running_identity_cannot_reuse_physical_memory_headroom(self):
        self.cfg.update(host_mem_total_gib=96, host_mem_reserve_gib=16)
        Path(self.config_path).write_text(json.dumps(self.cfg))
        self.seed_explain()
        self.observation()
        self.seed_batch(batch_id="new-running", job_status="running")
        output = self.query()
        self.assertIn("running_reservations_changed_since_memory_sample", output["unknown"])
        self.assertIsNone(output["resource_fit"])
        self.assertIsNone(output["budget"]["checks"]["host_memory"]["sample"])

    def test_explanation_bounds_fail_or_mark_order_incomplete_not_grant(self):
        self.seed_explain()
        self.observation()
        with mock.patch.object(admission, "MAX_READY", 0):
            output = self.query()
        self.assertTrue(output["order"]["truncated"])
        self.assertIsNone(output["order"]["rank_before_gates"])
        with state.connect() as conn:
            self.seed(conn, "running", 0, 1)
        with mock.patch.object(admission, "MAX_RUNNING", 0):
            self.assertEqual(1, self.capture(cli.main, ["admission-explain", "explain:task", "--json"])[0])

    def test_legacy_parallel_conversion_matches_dispatch_and_invalid_resources_fail_readonly(self):
        self.seed_explain()
        self.observation()
        with state.connect() as conn:
            raw = conn.execute("SELECT spec FROM tasks WHERE batch_id='explain'").fetchone()[0]
            spec = json.loads(raw)
            spec["max_parallel"] = "2"
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id='explain'", (json.dumps(spec),))
        self.assertEqual(2, self.query()["budget"]["checks"]["batch_parallel"]["limit"])
        revision = self.batch_revision("explain")
        for value in (True, 2, "1"):
            spec["resources"]["gpu"] = value
            with state.connect() as conn:
                conn.execute("UPDATE tasks SET spec=? WHERE batch_id='explain'", (json.dumps(spec),))
            self.assertEqual(1, self.capture(cli.main, ["admission-explain", "explain:task", "--json"])[0])
        self.assertEqual(revision, self.batch_revision("explain"))


class AdmissionBudgetTests(TempStateCase):
    def budget(self, **changes):
        values = dict(cpu_only=False, cpus=8, memory=8, parallel=1, used_cpu=8, cpu_jobs=0,
                      used_memory=8, memory_sample=None, outstanding_memory=0, launched_memory=0,
                      batch_running=1, project_running=1)
        values.update(changes)
        return admission.budgets(self.cfg, self.cfg, "p", **values)

    def test_every_simultaneous_budget_reason_is_returned(self):
        self.cfg.update(cpus_total=8, host_mem_total_gib=8)
        self.cfg["projects"]["p"].update(gpu_enabled=False, gpu_quota=1)
        plan = self.budget()
        self.assertEqual(["project_gpu_disabled", "quota", "parallel", "cpu", "host_memory"], plan["reasons"])
        self.assertEqual(["host_memory_sample"], plan["unknown"])
        self.assertFalse(plan["checks"]["cpu_reservation"]["hard_isolation"])

    def test_cpu_only_and_zero_budget_semantics_do_not_change(self):
        self.cfg.update(cpus_total=0, max_cpu_jobs=2)
        self.cfg["projects"]["p"].update(gpu_enabled=False, gpu_quota=1)
        self.assertTrue(self.budget(cpu_only=True, parallel=None)["allowed"])
        result = self.budget(cpu_only=True, parallel=None, cpu_jobs=2)
        self.assertEqual(["cpu"], result["reasons"])
        self.assertFalse(result["checks"]["cpu_reservation"]["applies"])

    def test_reservation_and_observed_headroom_both_restrict_same_tick(self):
        self.cfg.update(cpus_total=64, host_mem_total_gib=96, host_mem_reserve_gib=16)
        result = self.budget(parallel=None, memory=32, memory_sample={"MemTotal": 128, "MemAvailable": 70},
                             launched_memory=16, outstanding_memory=16)
        self.assertFalse(result["allowed"])
        self.assertEqual(22, result["checks"]["host_memory"]["physical_headroom_gib"])
        self.assertEqual(96, result["checks"]["host_memory"]["effective_budget_gib"])
