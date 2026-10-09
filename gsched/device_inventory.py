"""Read-only NVIDIA mapping; an inventory never grants install or launch rights.

Only exact primary/control/UVM devices are understood. MIG enabled or unknown
cards cannot grant a full-GPU policy; capability/DRM device guesses are forbidden.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import math
import os
import re
import selectors
import stat
import subprocess
import sys
import threading
import time

from .execution import DevicePolicy, DeviceRule

VERSION = "sched-device-inventory/v1"
VERSION_MIG = "sched-device-inventory/v2"
MAX_GPUS = 128
MAX_TEXT = 64 * 1024
MAX_AGE = 5
CPU_DEVICES = (("null", 1, 3), ("zero", 1, 5), ("full", 1, 7),
               ("random", 1, 8), ("urandom", 1, 9), ("tty", 5, 0))
SHARED_DEVICES = ("nvidiactl", "nvidia-uvm")
UUID = re.compile(r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")
_query_lock = threading.Lock()
_pending_query = None


def _number(value, maximum):
    if type(value) is not str or re.fullmatch(r"0|[1-9][0-9]{0,9}", value) is None or int(value) > maximum:
        raise ValueError("invalid NVIDIA device number")
    return int(value)


def parse_smi(text):
    if type(text) is not str or len(text.encode()) > MAX_TEXT:
        raise ValueError("NVIDIA inventory exceeds text bound")
    rows, seen = [], [set(), set(), set()]
    lines = text.splitlines()
    if not 1 <= len(lines) <= MAX_GPUS:
        raise ValueError("NVIDIA inventory missing or exceeds GPU bound")
    modes = {"Disabled": "disabled", "Enabled": "enabled", "N/A": "unknown", "[N/A]": "unknown"}
    for line in lines:
        fields = [v.strip() for v in line.split(",")]
        if len(fields) != 5 or UUID.fullmatch(fields[1]) is None or fields[3] not in modes or fields[4] not in modes:
            raise ValueError("NVIDIA inventory is incomplete or unsupported")
        index = _number(fields[0], 65535)
        bus = re.fullmatch(r"([0-9a-fA-F]{4,8}):([0-9a-fA-F]{2}):([0-9a-fA-F]{2})\.([0-7])", fields[2])
        if bus is None or int(bus[1], 16) > 65535 or int(bus[3], 16) > 31:
            raise ValueError("invalid NVIDIA PCI binding")
        pci = f"{int(bus[1], 16):04x}:{int(bus[2], 16):02x}:{int(bus[3], 16):02x}.{bus[4]}"
        identities = (index, fields[1], pci)
        if any(v in used for v, used in zip(identities, seen)):
            raise ValueError("ambiguous NVIDIA index/UUID/PCI binding")
        for v, used in zip(identities, seen):
            used.add(v)
        rows.append({"index": index, "uuid": fields[1], "pci": pci,
                     "mig_current": modes[fields[3]], "mig_pending": modes[fields[4]]})
    return sorted(rows, key=lambda r: r["index"])


def parse_driver_majors(text):
    if type(text) is not str or len(text.encode()) > MAX_TEXT:
        raise ValueError("driver major evidence exceeds bound")
    character, found = False, {}
    for line in text.splitlines():
        if line == "Character devices:":
            character = True
        elif line == "Block devices:":
            character = False
        elif character:
            fields = line.split()
            if len(fields) == 2 and fields[1] in {"nvidia", "nvidia-frontend", "nvidia-uvm"}:
                key = "primary" if fields[1] != "nvidia-uvm" else "uvm"
                if key in found:
                    raise ValueError("ambiguous NVIDIA driver major")
                found[key] = _number(fields[0], 0xffffffff)
    if set(found) != {"primary", "uvm"} or found["primary"] == found["uvm"]:
        raise ValueError("NVIDIA primary/UVM driver major missing or ambiguous")
    return found


def verify_information(text, card):
    if type(text) is not str or len(text.encode()) > MAX_TEXT:
        raise ValueError("NVIDIA information exceeds bound")
    values = {}
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        if separator and key.strip() in {"GPU UUID", "Device Minor"}:
            if key.strip() in values:
                raise ValueError("ambiguous NVIDIA information")
            values[key.strip()] = value.strip()
    if set(values) != {"GPU UUID", "Device Minor"} or values["GPU UUID"] != card["uuid"]:
        raise ValueError("driver information differs from NVIDIA UUID/minor")
    return _number(values["Device Minor"], 254)


def _with_minors(cards, information):
    if type(information) is not dict or set(information) != {c["pci"] for c in cards}:
        raise ValueError("NVIDIA driver information incomplete")
    result = [{**c, "minor": verify_information(information[c["pci"]], c)} for c in cards]
    if len({c["minor"] for c in result}) != len(result):
        raise ValueError("NVIDIA driver minor is ambiguous")
    return result


@dataclass(frozen=True)
class DeviceNode:
    name: str
    device: int
    inode: int
    major: int
    minor: int

    def __post_init__(self):
        if (type(self.name) is not str or re.fullmatch(r"[a-z0-9-]{1,64}", self.name) is None
                or any(type(v) is not int or v < 0 for v in (self.device, self.inode, self.major, self.minor))
                or self.inode == 0 or max(self.device, self.inode) > 0xffffffffffffffff
                or max(self.major, self.minor) > 0xffffffff):
            raise ValueError("invalid exact device node identity")

    def to_dict(self):
        return dict(name=self.name, device=self.device, inode=self.inode, major=self.major, minor=self.minor)

    def rule(self):
        return DeviceRule("char", self.major, self.minor, 6)  # read/write, never mknod


def from_facts(before, after, majors, nodes, information, context, *, mig_capabilities=None):
    """Pure, bounded bracket verification; no kernel effects or admission."""
    cards, later = parse_smi(before), parse_smi(after)
    if cards != later:
        raise ValueError("NVIDIA topology changed during device capture")
    cards = _with_minors(cards, information)
    drivers = parse_driver_majors(majors)
    expected = {name: (major, minor) for name, major, minor in CPU_DEVICES}
    expected.update(nvidiactl=(drivers["primary"], 255), **{"nvidia-uvm": (drivers["uvm"], 0)})
    expected.update({"nvidia" + str(card["minor"]): (drivers["primary"], card["minor"]) for card in cards})
    if (type(nodes) is not dict or set(nodes) != set(expected)
            or type(information) is not dict or set(information) != {c["pci"] for c in cards}
            or type(context) is not dict or set(context) != {"boot_id", "mount_namespace", "dev_device", "dev_inode"}
            or type(context["boot_id"]) is not str or re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", context["boot_id"]) is None
            or type(context["mount_namespace"]) is not str or re.fullmatch(r"mnt:\[[1-9][0-9]*\]", context["mount_namespace"]) is None
            or type(context["dev_device"]) is not int or context["dev_device"] < 0
            or type(context["dev_inode"]) is not int or context["dev_inode"] <= 0):
        raise ValueError("device capture identity incomplete")
    seen = set()
    for name, (major, minor) in expected.items():
        node = nodes[name]
        if type(node) is not DeviceNode or node.name != name or (node.major, node.minor) != (major, minor) or (major, minor) in seen:
            raise ValueError("device node does not match driver/baseline or aliases another rule")
        seen.add((major, minor))
    result = {"interface_version": VERSION, "context": dict(context), "cards": cards,
              "nodes": {name: nodes[name].to_dict() for name in sorted(nodes)}}
    if mig_capabilities is not None:
        from .mig_capability import reconcile
        reconcile(mig_capabilities, cards)
        result.update(interface_version=VERSION_MIG, mig_capabilities=json.loads(json.dumps(mig_capabilities)))
    return result


def select_policy(inventory, reservations, *, now=None):
    """Freeze only the original exact GPU reservation, never index==minor."""
    modern = type(inventory) is dict and inventory.get("interface_version") == VERSION_MIG
    keys = {"interface_version", "context", "cards", "nodes"} | ({"mig_capabilities"} if modern else set())
    if (type(inventory) is not dict or set(inventory) != keys
            or inventory.get("interface_version") not in {VERSION, VERSION_MIG}
            or type(inventory["nodes"]) is not dict or len(inventory["nodes"]) > len(CPU_DEVICES) + len(SHARED_DEVICES) + MAX_GPUS
            or type(reservations) is not list or len(reservations) > MAX_GPUS):
        raise ValueError("invalid device policy selection")
    # Reconstruct and verify every node even for CPU-only policy selection.
    cards = inventory["cards"]
    if type(cards) is not list or not 1 <= len(cards) <= MAX_GPUS:
        raise ValueError("device inventory cards missing or exceeds bound")
    modes = {"disabled": "Disabled", "enabled": "Enabled", "unknown": "N/A"}
    csv = "\n".join(",".join((str(c["index"]), c["uuid"], c["pci"],
        modes[c["mig_current"]], modes[c["mig_pending"]])) for c in cards)
    nodes = {name: DeviceNode(**value) for name, value in inventory["nodes"].items()}
    major_text = f"Character devices:\n{nodes['nvidiactl'].major} nvidia\n{nodes['nvidia-uvm'].major} nvidia-uvm\nBlock devices:\n"
    infos = {c["pci"]: f"GPU UUID: {c['uuid']}\nDevice Minor: {c['minor']}\n" for c in cards}
    checked = from_facts(csv, csv, major_text, nodes, infos, inventory["context"],
                         mig_capabilities=inventory.get("mig_capabilities"))
    if checked != inventory:
        raise ValueError("device inventory is not canonical")
    now = time.time() if now is None else now
    if type(now) not in (int, float) or not math.isfinite(now):
        raise ValueError("invalid device selection time")
    selected, used = [], set()
    from .mig_capability import reconcile
    capabilities = reconcile(inventory["mig_capabilities"], cards) if modern else None
    for reservation in reservations:
        if (type(reservation) is not dict or type(reservation.get("gpu_id")) is not int
                or reservation.get("simulated") is not False or reservation.get("topology_status") != "recorded_sample"
                or type(reservation.get("topology_observed_at")) not in (int, float)
                or not 0 <= now - reservation["topology_observed_at"] <= MAX_AGE
                or reservation["gpu_id"] in used):
            raise ValueError("original GPU reservation missing, simulated, stale or ambiguous")
        card = next((c for c in cards if c["index"] == reservation["gpu_id"]), None)
        if card is None or card["uuid"] != reservation.get("gpu_uuid"):
            raise ValueError("original GPU index/UUID differs from device inventory")
        unsupported = capabilities is not None and capabilities[card["uuid"]] == "not_supported"
        if (not unsupported and (card["mig_current"] != "disabled" or card["mig_pending"] != "disabled")
                or capabilities is not None and capabilities[card["uuid"]] == "unknown"):
            raise ValueError("MIG enabled or unknown cannot grant a full-GPU device policy")
        used.add(card["index"])
        selected.append("nvidia" + str(card["minor"]))
    names = [name for name, _, _ in CPU_DEVICES] + (list(SHARED_DEVICES) + selected if reservations else [])
    return DevicePolicy(tuple(sorted(nodes[name].rule() for name in names)))


def _read(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            data = stream.read(MAX_TEXT + 1)
        if len(data) > MAX_TEXT:
            raise ValueError("device text probe exceeds bound")
        return data.decode("utf-8")
    finally:
        os.close(descriptor)


def _query(deadline, *, uuids=None):
    global _pending_query
    if not _query_lock.acquire(blocking=False):
        raise ValueError("device topology query already in progress")
    try:
        return _query_locked(deadline, uuids=uuids)
    finally:
        _query_lock.release()


def _query_locked(deadline, *, uuids=None):
    global _pending_query
    if _pending_query is not None:
        if _pending_query.poll() is None:
            raise TimeoutError("previous device topology child remains unreaped")
        _pending_query.wait(timeout=0)
        _pending_query.stdout.close()
        _pending_query = None
    if deadline <= time.monotonic():
        raise TimeoutError("device topology query deadline already expired")
    command = ["nvidia-smi", "--query-gpu=index,uuid,pci.bus_id,mig.mode.current,mig.mode.pending", "--format=csv,noheader,nounits"]
    if uuids is not None:
        from .mig_capability import ENTRY
        if (type(uuids) is not list or not 1 <= len(uuids) <= MAX_GPUS
                or any(type(u) is not str or UUID.fullmatch(u) is None for u in uuids)
                or len(set(uuids)) != len(uuids)):
            raise ValueError("invalid original MIG probe UUID targets")
        command = [sys.executable, "-I", "-S", "-c", ENTRY, json.dumps(uuids)]
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, close_fds=True)
    _pending_query = process
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            data = bytearray()
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise TimeoutError("device topology query exceeded deadline")
                chunk = os.read(process.stdout.fileno(), min(16384, MAX_TEXT + 1 - len(data)))
                if not chunk:
                    break
                data.extend(chunk)
                if len(data) > MAX_TEXT:
                    raise ValueError("device topology query exceeds output bound")
        if process.wait(timeout=max(.001, deadline - time.monotonic())) != 0:
            raise ValueError("device topology query failed")
        return bytes(data).decode("utf-8")
    finally:
        if process.poll() is None:
            process.kill()
        try:
            process.wait(timeout=1)
        finally:
            process.stdout.close()
        _pending_query = None  # Remains bound if actual wait/kill failed.


def capture(*, include_mig=False):
    """Compute-only caller; O_PATH identifies nodes without opening a device."""
    if not hasattr(os, "O_PATH"):
        raise ValueError("device inventory requires Linux O_PATH")
    if type(include_mig) is not bool:
        raise ValueError("include_mig must be explicit boolean")
    deadline = time.monotonic() + MAX_AGE
    root = os.open("/dev", os.O_PATH | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        before = _query(deadline)
        cards = parse_smi(before)
        mig_before = json.loads(_query(deadline, uuids=[c["uuid"] for c in cards])) if include_mig else None
        context = {"boot_id": _read("/proc/sys/kernel/random/boot_id").strip(),
                   "mount_namespace": os.readlink("/proc/self/ns/mnt"),
                   "dev_device": os.fstat(root).st_dev, "dev_inode": os.fstat(root).st_ino}
        majors = _read("/proc/devices")
        information = {c["pci"]: _read("/proc/driver/nvidia/gpus/" + c["pci"] + "/information") for c in cards}
        cards = _with_minors(cards, information)
        names = [n for n, _, _ in CPU_DEVICES] + list(SHARED_DEVICES) + ["nvidia" + str(c["minor"]) for c in cards]
        nodes = {}
        for name in names:
            descriptor = os.open(name, os.O_PATH | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=root)
            try:
                info = os.fstat(descriptor)
                if not stat.S_ISCHR(info.st_mode):
                    raise ValueError("device inventory path is not a direct character node")
                nodes[name] = DeviceNode(name, info.st_dev, info.st_ino, os.major(info.st_rdev), os.minor(info.st_rdev))
            finally:
                os.close(descriptor)
        after = _query(deadline)
        mig_after = json.loads(_query(deadline, uuids=[c["uuid"] for c in cards])) if include_mig else None
        if mig_after != mig_before:
            raise ValueError("original MIG capability changed during device capture")
        if include_mig and mig_before is None:
            raise ValueError("original MIG capability missing")
        # Detect node/root/namespace replacement during the same bracket.
        root_now = os.stat("/dev", follow_symlinks=False)
        if context != {"boot_id": _read("/proc/sys/kernel/random/boot_id").strip(),
                       "mount_namespace": os.readlink("/proc/self/ns/mnt"),
                       "dev_device": root_now.st_dev, "dev_inode": root_now.st_ino}:
            raise ValueError("device inventory kernel namespace/root changed")
        if majors != _read("/proc/devices") or any(information[c["pci"]] != _read(
                "/proc/driver/nvidia/gpus/" + c["pci"] + "/information") for c in cards):
            raise ValueError("NVIDIA driver information changed during capture")
        for name, node in nodes.items():
            info = os.stat(name, dir_fd=root, follow_symlinks=False)
            if (not stat.S_ISCHR(info.st_mode) or (info.st_dev, info.st_ino, os.major(info.st_rdev), os.minor(info.st_rdev))
                    != (node.device, node.inode, node.major, node.minor)):
                raise ValueError("device inventory node changed")
        if time.monotonic() > deadline:
            raise TimeoutError("device inventory bracket expired")
        return from_facts(before, after, majors, nodes, information, context, mig_capabilities=mig_before)
    finally:
        os.close(root)
