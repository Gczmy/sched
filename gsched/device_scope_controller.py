"""Original-scope device effects; no recovery installation or new launch rights."""
from __future__ import annotations

import time

from . import cpu_scope_state as cpu, device_inventory as inventory, device_inventory_state as frozen, device_scope_state as ledger, state
from .execution import DeviceBinding, DeviceIntent, DeviceScope
from .execution_policy import digest


def policy(cfg):
    raw = cfg.get("device_isolation", {})
    if (type(raw) is not dict or set(raw) - {"mode"}
            or type(raw.get("mode", "off")) is not str or raw.get("mode", "off") not in ("off", "nvidia")):
        raise ValueError("device_isolation 仅接受 mode=off|nvidia")
    if raw.get("mode") == "nvidia":
        from .cpu_isolation import policy as cpu_policy
        if cpu_policy(cfg)["mode"] != "cgroup":
            raise ValueError("nvidia 设备隔离必须显式使用 CPU cgroup/delegated_root")
    return {"mode": raw.get("mode", "off")}


def preflight(manager):
    """Only native/query prerequisites on the retained parent; never load/attach."""
    from .execution.devices import _native
    result = _native().device_program_query(manager._fd)
    if (type(result) is not dict or set(result) != {"program_ids", "attach_flags"}
            or type(result["program_ids"]) is not list or len(result["program_ids"]) > 64
            or any(type(n) is not int or not 0 < n <= 0xffffffff for n in result["program_ids"])
            or len(set(result["program_ids"])) != len(result["program_ids"])
            or type(result["attach_flags"]) is not int or not 0 <= result["attach_flags"] <= 0xffffffff):
        raise state.StateError("device parent query is incomplete or exceeds bound")


def unknown(scope_id, reason):
    with state.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        _, events = ledger.load(conn, scope_id)
        if events[-1]["kind"] not in ledger.TERMINAL | {"unknown"}:
            ledger.advance(conn, scope_id, events[-1]["event_id"], "unknown", {"reason": reason})


def _sample():
    result = inventory.capture()
    return result, time.time()


def prepare(controller, conn, job, scope):
    if conn.in_transaction:
        raise state.StateError("device installation must occur outside writer")
    original, binding = cpu._allocation(conn, job["allocation_id"], job["id"], active=True)
    if binding.get("device_isolation") != "nvidia" or policy(controller.dispatcher._read_gpu_policy()) != controller.device_policy:
        raise state.StateError("original device requirement or cold policy changed")
    if ledger.for_allocation(conn, job["allocation_id"], job["id"]) is not None:
        raise state.StateError("device preparation consumed; recovery cannot install again")
    sampled, observed_at = _sample()
    intent = DeviceIntent(scope.binding, inventory.select_policy(sampled, original["gpu_reservations"]))
    scope_id = scope.binding.intent.scope_id
    # Atomic reservation + mapping, followed by a separately committed effect CAS.
    with state.connect() as writer:
        writer.execute("BEGIN IMMEDIATE")
        reserved = ledger.reserve(writer, job["id"], intent)
        frozen.freeze(writer, scope_id, sampled, observed_at)
    try:
        device = DeviceScope(scope, intent)  # Immediately disables plain CPU launch.
        fresh, fresh_at = _sample()
        evidence = frozen.verify_current(conn, scope_id, fresh, fresh_at)
        if not controller.preflight() or not controller.dispatcher._cluster_lease.update():
            raise state.StateError("original device lease/delegation unavailable before install")
        with state.connect() as writer:
            writer.execute("BEGIN IMMEDIATE")
            _checked(writer, scope_id, {**evidence, "observed_at": fresh_at})
            consumed = ledger.advance(writer, scope_id, reserved["event_id"], "install_intent")
        installed = device.install()  # Exact original inode; one load/attach, no replace.
        observation = device.observe()
        with state.connect() as writer:
            writer.execute("BEGIN IMMEDIATE")
            ledger.advance(writer, scope_id, consumed["event_id"], "installed", {
                "binding": installed.to_dict(), "observation": observation})
        # No constraints before the original installed binding is durable.
        fresh, fresh_at = _sample()
        frozen.verify_current(conn, scope_id, fresh, fresh_at)
        constraints = device.constraints()
        controller.device_handles[job["allocation_id"]] = device
        return constraints
    except BaseException:
        try:
            unknown(scope_id, "device_preparation_result_unknown")
        except Exception:
            pass  # Durable consumed intent still retains original reservations.
        raise


def launch_check(controller, conn, job, scope):
    device = controller.device_handles.get(job["allocation_id"])
    if (device is None or device.scope is not scope or device.intent.scope != scope.binding
            or device._phase != "launch_capability_issued"):
        raise state.StateError("device launch requires its original retained one-shot handle")
    scope_id = scope.binding.intent.scope_id
    sampled, observed_at = _sample()
    evidence = frozen.verify_current(conn, scope_id, sampled, observed_at)
    value, events = ledger.load(conn, scope_id)
    installed = next((e["data"]["binding"] for e in events if e["kind"] == "installed"), None)
    if events[-1]["kind"] != "installed" or installed != device.binding.to_dict() or value["intent"] != device.intent.to_dict():
        raise state.StateError("original installed binding absent, consumed or changed")
    device.observe()  # Actual original attachment, not installed record inference.
    return events[-1]["event_id"], {**evidence, "observed_at": observed_at}


def _checked(conn, scope_id, evidence):
    now = time.time()
    if (type(evidence) is not dict or type(evidence.get("observed_at")) not in (int, float)
            or not 0 <= now - evidence["observed_at"] <= inventory.MAX_AGE
            or digest(frozen.load(conn, scope_id)) != evidence.get("inventory_binding_sha256")):
        raise state.StateError("device launch check expired or original binding changed")
    # Recheck original claims under the same writer as both launch intents.
    _, _, original, intent = frozen._original(conn, scope_id, active=True)
    if inventory.select_policy(frozen.load(conn, scope_id)["inventory"], original["gpu_reservations"], now=now) != intent.policy:
        raise state.StateError("original GPU topology expired before device effect CAS")


def launch_intent(conn, scope_id, event_id, evidence):
    _checked(conn, scope_id, evidence)
    return ledger.advance(conn, scope_id, event_id, "launch_intent")


def observe_restored(conn, scope, allocation_id, job_id):
    """Original binding only; no install/constraints even after daemon restart."""
    entry = ledger.for_allocation(conn, allocation_id, job_id)
    if entry is None:
        _, binding = cpu._allocation(conn, allocation_id, job_id)
        if binding.get("device_isolation") == "nvidia":
            raise state.StateError("required original device binding missing")
        return
    value, events = entry
    installed = next((e["data"]["binding"] for e in events if e["kind"] == "installed"), None)
    if installed is None or events[-1]["kind"] == "unknown":
        raise state.StateError("original device installation not confirmed")
    original = DeviceBinding.from_dict(installed)
    if original.intent != DeviceIntent.from_dict(value["intent"]):
        raise state.StateError("restored device intent differs")
    frozen.load(conn, value["scope_id"])
    DeviceScope(scope, original.intent, binding=original).observe()
