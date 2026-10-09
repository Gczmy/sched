"""Scheduler-owned CPU scope effects; original identity, no recovery starts.

Filesystem effects happen outside SQLite transactions. Durable intents consume
each effect before it happens; original execution cleanup and original scope
removal are independent requirements for releasing CPU/GPU reservations.
"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import PurePosixPath
import re
import time
import uuid

from . import cluster_lease, cpu_scope_state as ledger, device_scope_controller as devices, state
from .execution import CpuScopeBinding, CpuScopeIntent, DelegatedCpuScopes, ScopeUnavailable
from .execution_policy import digest

MAX_ACTIVE = 256


class _RecordedRootChanged(ValueError):
    pass


def _path(value):
    if (type(value) is not str or not value.startswith("/") or value.startswith("//")
            or len(value) > 4096 or "\0" in value or str(PurePosixPath(value)) != value
            or ".." in PurePosixPath(value).parts):
        raise ValueError("noncanonical cgroup authority path")
    return value


def _below(value, root):
    return value == root or value.startswith(root.rstrip("/") + "/")


def mounts_from_text(text):
    if type(text) is not str or len(text.encode()) > 1024 * 1024:
        raise ValueError("cgroup mount evidence exceeds bound")
    rows = text.splitlines()
    if len(rows) > 8192:
        raise ValueError("cgroup mount count exceeds bound")
    mounts = []
    for line in rows:
        fields = line.split()
        if "-" not in fields:
            raise ValueError("invalid mount evidence")
        marker = fields.index("-")
        if marker < 6 or len(fields) < marker + 4:
            raise ValueError("invalid mount evidence")
        if fields[marker + 1] == "cgroup2":
            unescape = lambda value: re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), value)
            mounts.append({"root": _path(unescape(fields[3])), "mountpoint": _path(unescape(fields[4]))})
    return mounts


def authority_from_facts(parent_path, origin, current, decision, mounts):
    """Pure mapping to original kernel hierarchy; Slurm unknown never allows it."""
    keys = ("pid", "start_token", "physical_host", "uid", "cgroups", "affinity")
    if any(origin.get(k) is None or current.get(k) != origin[k] for k in keys) or decision.get("invalid_latched"):
        raise ValueError("original cgroup kernel context changed or unknown")
    groups = [g for g in current["cgroups"] if g.get("hierarchy") == "0" and g.get("controllers") == ""]
    candidates = [m for m in mounts if _below(parent_path, m["mountpoint"])]
    if len(groups) != 1 or len(candidates) != 1 or candidates[0]["root"] != "/":
        raise ValueError("unified cgroup mount/namespace mapping not verified")
    parent = _path("/" + parent_path[len(candidates[0]["mountpoint"]):].lstrip("/"))
    current_group = _path(groups[0]["path"])
    env = origin.get("slurm_environment", {})
    job = env.get("SLURM_JOB_ID")
    if job:
        if decision.get("allocation_state") != "valid" or not decision.get("dispatch_allowed"):
            raise ValueError("cgroup mode requires a verified valid original Slurm allocation")
        matched = re.search(r"(?:^|/)job_" + re.escape(job) + r"(?=/|$)", current_group)
        if matched is None:
            raise ValueError("original Slurm job cgroup not verified")
        boundary = current_group[:matched.end()]
        step = env.get("SLURM_STEP_ID")
        if step:
            matched = re.search(r"/step_" + re.escape(step) + r"(?=/|$)", current_group[len(boundary):])
            if matched is None:
                raise ValueError("original Slurm step cgroup not verified")
            boundary = current_group[:len(boundary) + matched.end()]
        if not _below(parent, boundary):
            raise ValueError("delegated root is outside original Slurm job/step")
    else:
        if env or not (_below(parent, current_group) or _below(current_group, parent)):
            raise ValueError("non-Slurm delegation is outside original daemon hierarchy")
        boundary = current_group
    return {"schema_version": 1, "mountpoint": candidates[0]["mountpoint"], "mount_root": "/",
            "parent_cgroup": parent, "original_daemon_cgroup": current_group,
            "original_boundary": boundary, "slurm_job_id": job,
            "kernel_context_sha256": digest({k: origin[k] for k in keys})}


def _mounts():
    return mounts_from_text(cluster_lease._text("/proc/self/mountinfo", 1024 * 1024))


def active(conn):
    if conn.execute("PRAGMA user_version").fetchone()[0] < 19:
        return []
    return conn.execute("SELECT scope_id FROM cpu_scopes WHERE COALESCE((SELECT kind FROM cpu_scope_events e WHERE e.scope_id=cpu_scopes.scope_id ORDER BY seq DESC LIMIT 1),'missing') NOT IN ('removed','abandoned') ORDER BY scope_id LIMIT ?", (MAX_ACTIVE + 1,)).fetchall()


def _advance(identifier, kind, data=None):
    with state.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        _, events = ledger.load(conn, identifier)
        return ledger.advance(conn, identifier, events[-1]["event_id"], kind, data)


def _unknown(identifier, reason):
    with state.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        _, events = ledger.load(conn, identifier)
        if events[-1]["kind"] not in ledger.TERMINAL | {"unknown"}:
            ledger.advance(conn, identifier, events[-1]["event_id"], "unknown", {"reason": reason})


class Controller:
    def __init__(self, dispatcher):
        from .cpu_isolation import policy
        self.dispatcher = dispatcher
        self.policy = policy(dispatcher.cfg)
        self.device_policy = devices.policy(dispatcher.cfg)
        if self.policy["mode"] != "cgroup":
            raise ValueError("CPU scope controller requires explicit cgroup mode")
        if self.device_policy["mode"] != "off" and getattr(dispatcher, "fake", False):
            raise ValueError("device isolation cannot run with fake GPU topology")
        self.manager = DelegatedCpuScopes(self.policy["delegated_root"])
        self.handles = {}
        self.device_handles = {}
        self.job_handles = {}
        self.healthy = True
        self.sampled_at = None
        self.authority = None
        try:
            self.preflight()
        except BaseException:
            self.close()
            raise

    def preflight(self):
        from . import scope_health
        observed_at = time.time()
        started = time.monotonic()
        try:
            return self._preflight(observed_at, started)
        except _RecordedRootChanged:
            self.sampled_at = None
            raise
        except (OSError, ValueError, RuntimeError, state.StateError):
            self.sampled_at = None
            scope_health.record(self, reason="root_preflight_unavailable", observed_at=time.time())
            raise

    def _preflight(self, observed_at, started):
        from . import scope_health
        monitor = self.dispatcher._cluster_lease
        current = cluster_lease.kernel_context()
        authority = authority_from_facts(self.manager.parent.path, monitor.origin, current, monitor.decision, _mounts())
        if self.authority is not None and self.authority != authority:
            raise ValueError("original delegated cgroup authority changed")
        cpus, mems = self.manager._verify()
        if hasattr(self, "cpus") and (cpus != self.cpus or mems != self.mems):
            raise ValueError("original delegated CPU/NUMA parent capacity changed")
        if not set(current["affinity"]) & set(cpus):
            raise ValueError("delegated CPU parent has no CPU in original pool")
        device_query = devices.preflight(self.manager) if self.device_policy["mode"] != "off" else None
        after = cluster_lease.kernel_context()
        if (self.manager._verify() != (cpus, mems)
                or authority_from_facts(self.manager.parent.path, monitor.origin, after, monitor.decision, _mounts()) != authority
                or (self.device_policy["mode"] != "off" and devices.preflight(self.manager) != device_query)):
            raise ValueError("delegated root observation changed during sampling")
        facts = {"policies": {"cpu": self.policy, "device": self.device_policy},
                 "parent": asdict(self.manager.parent), "authority": authority,
                 "cpus": list(cpus), "mems": list(mems), "device_query": device_query}
        if not 0 <= time.monotonic() - started <= 5:
            raise state.StateError("original root observation exceeded launch freshness")
        if not scope_health.record(self, facts=facts, reason=None if self.healthy else "active_scopes_unresolved", observed_at=observed_at):
            raise _RecordedRootChanged("original delegated root evidence changed")
        if not 0 <= time.monotonic() - started <= 5:
            raise state.StateError("original root observation expired during publication")
        self.authority, self.mems, self.cpus = authority, mems, cpus
        self.sampled_at = started
        return self.admission_current()

    def refresh_health(self):
        """Tick observation also runs while drained; no scope/BPF mutations."""
        from .cpu_isolation import policy
        from . import scope_health
        probing = False
        try:
            current = self.dispatcher._read_gpu_policy()
            if policy(current) != self.policy or devices.policy(current) != self.device_policy:
                self.sampled_at = None
                scope_health.record(self, reason="cold_policy_changed", observed_at=time.time())
                return False
            probing = True
            return self.preflight()
        except (OSError, ValueError, RuntimeError, state.StateError) as error:
            self.sampled_at = None
            if not probing:
                try:
                    scope_health.record(self, reason="root_preflight_unavailable", observed_at=time.time())
                except (OSError, ValueError, RuntimeError, state.StateError):
                    pass  # Failed publication never mints a fresh positive result.
            self.dispatcher.log_line(f"CPU root health unavailable: {error}")
            return False

    def admission_current(self):
        return self.healthy and self.sampled_at is not None and 0 <= time.monotonic() - self.sampled_at <= 5

    def binding_fields(self):
        fields = {"scope_parent": asdict(self.manager.parent), "scope_mems": list(self.mems),
                  "scope_authority": self.authority, "scope_pool": list(self.cpus)}
        if self.device_policy["mode"] != "off":
            fields["device_isolation"] = "nvidia"
        return fields

    def reserve(self, conn, job):
        from .cpu_isolation import allocation_binding
        if not self.admission_current() or len(active(conn)) >= MAX_ACTIVE:
            raise state.StateError("original CPU scope admission stale or scope bound exhausted")
        value, cpu = allocation_binding(conn, job["allocation_id"], job["id"])
        if cpu["mode"] != "cgroup" or any(cpu.get(k) != v for k, v in self.binding_fields().items()):
            raise state.StateError("CPU scope allocation differs from original delegation")
        intent = CpuScopeIntent(self.manager.parent, uuid.uuid4().hex, tuple(cpu["cpus"]), self.mems, digest(value))
        ledger.reserve(conn, job["id"], intent)
        return intent

    def prepare(self, conn, job):
        from .cpu_isolation import policy
        if conn.in_transaction:
            raise state.StateError("CPU scope create/configure must occur outside DB writer")
        row = conn.execute("SELECT scope_id FROM cpu_scopes WHERE allocation_id=? AND job_id=?", (job["allocation_id"], job["id"])).fetchone()
        if row is None:
            raise state.StateError("CPU scope creation intent missing")
        value, events = ledger.load(conn, row[0])
        if events[-1]["kind"] != "reserved":
            raise state.StateError("CPU scope creation consumed; recovery cannot restart")
        _, binding = ledger._allocation(conn, job["allocation_id"], job["id"], active=True)
        from .device_scope_state import for_allocation
        current = self.dispatcher._read_gpu_policy()
        if (binding.get("device_isolation", "off") != self.device_policy["mode"]
                or policy(current) != self.policy or devices.policy(current) != self.device_policy
                or for_allocation(conn, job["allocation_id"], job["id"]) is not None):
            raise state.StateError("original device requirement changed or preparation consumed")
        intent = CpuScopeIntent.from_dict(value["intent"])
        _advance(intent.scope_id, "create_intent")
        scope = None
        try:
            scope = self.manager.create(intent)
            _advance(intent.scope_id, "inode_bound", {"binding": scope.binding.to_dict()})
            _advance(intent.scope_id, "configure_intent")
            observation = scope.configure()
            _advance(intent.scope_id, "configured", {"observation": observation})
            constraints = (devices.prepare(self, conn, job, scope)
                           if self.device_policy["mode"] != "off" else scope.constraints())
            self.handles[job["allocation_id"]] = scope
            self.job_handles[job["id"]] = job["allocation_id"]
            return constraints
        except BaseException:
            if scope is not None:
                scope.close()
            try:
                _unknown(intent.scope_id, "scope_preparation_result_unknown")
            except Exception:
                pass  # Consumed durable intent remains unresolved even if DB fails.
            raise

    def launch_check(self, conn, job):
        from .cpu_isolation import policy
        if conn.in_transaction:
            raise state.StateError("CPU scope launch probes must occur outside DB writer")
        from .device_scope_state import for_allocation
        device_policy = getattr(self, "device_policy", {"mode": "off"})
        if device_policy["mode"] == "off" and for_allocation(conn, job["allocation_id"], job["id"]) is not None:
            raise state.StateError("device scope requires original retained device handle; CPU-only launch refused")
        current = self.dispatcher._read_gpu_policy()
        _, binding = ledger._allocation(conn, job["allocation_id"], job["id"], active=True)
        if (policy(current) != self.policy or devices.policy(current) != device_policy
                or binding.get("device_isolation", "off") != device_policy["mode"]
                or not self.dispatcher._cluster_lease.update()):
            raise state.StateError("CPU scope cold policy or original lease changed")
        if not self.preflight():
            raise state.StateError("CPU scope health unresolved")
        scope = self.handles.get(job["allocation_id"])
        if scope is None:
            raise state.StateError("recovered CPU scope cannot grant a new launch")
        observation = scope.observe()
        ledger._observation(observation, scope.binding.intent, configured=True)
        _, events = ledger.load(conn, scope.binding.intent.scope_id)
        if events[-1]["kind"] != "configured":
            raise state.StateError("CPU scope launch intent consumed or cleanup requested")
        checked = scope.binding.intent.scope_id, events[-1]["event_id"]
        if device_policy["mode"] != "off":
            checked += devices.launch_check(self, conn, job, scope)
        return checked

    @staticmethod
    def launch_intent(conn, checked):
        if len(checked) == 4:
            devices.launch_intent(conn, checked[0], checked[2], checked[3])
        return ledger.advance(conn, checked[0], checked[1], "launch_intent")

    def finish_launch(self, job_id):
        identifier = self.job_handles.pop(job_id, None)
        self.device_handles.pop(identifier, None)
        scope = self.handles.pop(identifier, None)
        if scope is not None:
            scope.close()  # Never remove on close or transfer/reissue a capability.

    def close(self):
        for scope in self.handles.values():
            scope.close()
        self.handles.clear()
        self.device_handles.clear()
        self.job_handles.clear()
        self.manager.close()


def preflight_check(cfg):
    """Compute-only passive deployment check, not lease/launch authorization."""
    from .cpu_isolation import policy
    with_manager = DelegatedCpuScopes(policy(cfg)["delegated_root"])
    try:
        current = cluster_lease.kernel_context()
        origin = {**current, "slurm_environment": cluster_lease.slurm_environment(), "started_at": time.time(),
                  "policy": cluster_lease.policy(cfg)}
        job = origin["slurm_environment"].get("SLURM_JOB_ID")
        sample = cluster_lease.probe(job) if job else {"known": False}
        decision = cluster_lease.decide(origin, current, sample)
        authority_from_facts(with_manager.parent.path, origin, current, decision, _mounts())
        if devices.policy(cfg)["mode"] != "off":
            devices.preflight(with_manager)
    finally:
        with_manager.close()


def maintain(dispatcher):
    """Restore only original inode for observations/removal, never create/start."""
    if state._bound_connection.get() is not None:
        return False  # A request owns the commit; never act before its commit.
    controller = getattr(dispatcher, "_cpu_scopes", None)
    healthy = True
    with state.connect() as conn:
        rows = active(conn)
        if len(rows) > MAX_ACTIVE:
            healthy = False
        entries = [ledger.load(conn, row[0]) for row in rows[:MAX_ACTIVE]]
    for value, events in entries:
        identifier = value["scope_id"]
        last = events[-1]
        binding = next((e["data"]["binding"] for e in events if e["kind"] == "inode_bound"), None)
        manager, scope = None, None
        try:
            live = controller is not None and value["allocation_id"] in controller.handles
            if not live and last["kind"] in ("reserved", "create_intent", "inode_bound", "configure_intent", "configured"):
                with state.connect() as conn:
                    job = dict(state.get_job(conn, value["job_id"]))
                    job["allocation_id"] = value["allocation_id"]
                    ledger.request_cleanup(conn, job, cleanup_source="configured_not_started")
                with state.connect() as conn:
                    value, events = ledger.load(conn, identifier)
                    last = events[-1]
            if last["kind"] in ledger.TERMINAL:
                continue
            if binding is None or last["kind"] == "unknown":
                healthy = False
                continue
            if last["kind"] == "cleanup_intent":
                _unknown(identifier, "scope_removal_result_unknown")
                healthy = False
                continue
            bound = CpuScopeBinding.from_dict(binding)
            manager = DelegatedCpuScopes(bound.intent.parent.path)
            scope = manager.restore(bound)
            observation = scope.observe()
            if last["kind"] == "cleanup_ready":
                if observation["populated"] or observation["direct_process_count"]:
                    continue  # Group cleanup never proves descendant scope cleanup.
                _advance(identifier, "cleanup_intent", {"observation": observation, "cleanup_source": last["data"]["cleanup_source"]})
                result = scope.remove()
                _advance(identifier, "removed", result)
            elif not observation["scope_configured"]:
                healthy = False  # No resizing or killing a running application.
                _unknown(identifier, "scope_configuration_drift")
            elif not live:
                try:
                    with state.connect() as conn:
                        devices.observe_restored(conn, scope, value["allocation_id"], value["job_id"])
                except (OSError, ValueError, RuntimeError, state.StateError):
                    try:
                        devices.unknown(identifier, "original_device_observation_unknown")
                    except Exception:
                        pass
                    raise
        except (OSError, ValueError, RuntimeError, state.StateError, ScopeUnavailable) as error:
            healthy = False
            dispatcher.log_line(f"CPU scope {identifier} retained; original identity/cleanup unavailable: {error}")
        finally:
            if scope is not None:
                scope.close()
            if manager is not None:
                manager.close()
    if controller is not None:
        controller.healthy = healthy
    return healthy
