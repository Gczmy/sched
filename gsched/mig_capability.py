"""Bounded original-UUID NVML observations; no MIG/device mutation APIs."""
from __future__ import annotations

import re

VERSION = "sched-mig-capability/v1"
MAX_GPUS = 128
UUID = re.compile(r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")

# Executed in an isolated, externally bounded child, never in the daemon.
# Only init/shutdown and read-only getters; no customer module or vendor Python.
ENTRY = r'''
import ctypes as C,json,re,sys
targets=json.loads(sys.argv[1])
pattern=r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
if type(targets) is not list or not 1<=len(targets)<=128 or any(type(u) is not str or re.fullmatch(pattern,u) is None for u in targets) or len(set(targets))!=len(targets):
    raise ValueError("invalid original UUID targets")
def row(u,phase,code=None,verified=False,current=None,pending=None):
    return dict(uuid=u,phase=phase,code=code,identity_verified=verified,current=current,pending=pending)
result=dict(interface_version="sched-mig-capability/v1",driver_version=None,library_version=None,entries=[])
phase="library"
initialized=False
try:
    lib=C.CDLL("libnvidia-ml.so.1")
    def function(name,args):
        f=getattr(lib,name); f.argtypes=args; f.restype=C.c_int; return f
    phase="api"
    init=function("nvmlInit_v2",[])
    shutdown=function("nvmlShutdown",[])
    driver=function("nvmlSystemGetDriverVersion",[C.c_char_p,C.c_uint])
    version=function("nvmlSystemGetNVMLVersion",[C.c_char_p,C.c_uint])
    handle=function("nvmlDeviceGetHandleByUUID",[C.c_char_p,C.POINTER(C.c_void_p)])
    uuid=function("nvmlDeviceGetUUID",[C.c_void_p,C.c_char_p,C.c_uint])
    mig=function("nvmlDeviceGetMigMode",[C.c_void_p,C.POINTER(C.c_uint),C.POINTER(C.c_uint)])
    phase="initialization"
    rc=init()
    if rc:
        result["entries"]=[row(u,phase,rc) for u in targets]
    else:
        initialized=True
        phase="version"
        values=[]
        for getter in (driver,version):
            buf=C.create_string_buffer(256)
            rc=getter(buf,256)
            if rc: break
            values.append(buf.value.decode("ascii"))
        if rc:
            result["entries"]=[row(u,phase,rc) for u in targets]
        else:
            result["driver_version"],result["library_version"]=values
            for target in targets:
                phase="handle"
                dev=C.c_void_p()
                rc=handle(target.encode("ascii"),C.byref(dev))
                if rc or not dev.value:
                    result["entries"].append(row(target,phase,rc)); continue
                phase="uuid_before"
                first=C.create_string_buffer(256)
                rc=uuid(dev,first,256)
                if rc or first.value.decode("ascii")!=target:
                    result["entries"].append(row(target,phase,rc)); continue
                phase="mig"
                current,pending=C.c_uint(0xffffffff),C.c_uint(0xffffffff)
                rc=mig(dev,C.byref(current),C.byref(pending))
                phase="uuid_after"
                last=C.create_string_buffer(256)
                checked=uuid(dev,last,256)
                if checked or last.value.decode("ascii")!=target:
                    result["entries"].append(row(target,phase,checked)); continue
                result["entries"].append(row(target,"mig",rc,True,current.value if rc==0 else None,pending.value if rc==0 else None))
except (OSError,AttributeError,ValueError,UnicodeError):
    result["entries"]=[row(u,"api" if phase=="mig" else phase) for u in targets]
finally:
    if initialized:
        try:
            if shutdown()!=0: result["entries"]=[row(u,"shutdown") for u in targets]
        except (OSError,ValueError):
            result["entries"]=[row(u,"shutdown") for u in targets]
print(json.dumps(result,sort_keys=True,separators=(",",":")))
'''


def validate(value, cards):
    """Strict facts, not caller-supplied classifications or a model whitelist."""
    if (type(value) is not dict or set(value) != {"interface_version", "driver_version", "library_version", "entries"}
            or value["interface_version"] != VERSION or type(value["entries"]) is not list
            or not 1 <= len(value["entries"]) <= MAX_GPUS):
        raise ValueError("MIG capability observation incomplete or exceeds bound")
    versions = (value["driver_version"], value["library_version"])
    if any(v is not None and (type(v) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._()-]{0,127}", v) is None) for v in versions):
        raise ValueError("invalid MIG driver/library version")
    expected = {c["uuid"] for c in cards}
    entries = {}
    for entry in value["entries"]:
        if (type(entry) is not dict or set(entry) != {"uuid", "phase", "code", "identity_verified", "current", "pending"}
                or type(entry["uuid"]) is not str or UUID.fullmatch(entry["uuid"]) is None
                or entry["uuid"] not in expected or entry["uuid"] in entries
                or type(entry["phase"]) is not str or entry["phase"] not in
                {"library", "api", "initialization", "version", "handle", "uuid_before", "mig", "uuid_after", "shutdown"}
                or type(entry["identity_verified"]) is not bool
                or entry["code"] is not None and (type(entry["code"]) is not int or not 0 <= entry["code"] <= 0xffffffff)
                or any(n is not None and (type(n) is not int or not 0 <= n <= 0xffffffff) for n in (entry["current"], entry["pending"]))):
            raise ValueError("invalid original MIG observation")
        verified = entry["phase"] == "mig" and entry["identity_verified"] and all(v is not None for v in versions)
        if (entry["phase"] == "mig" and (not verified or entry["code"] is None)
                or entry["identity_verified"] != (entry["phase"] == "mig") or ((entry["current"] is not None or entry["pending"] is not None)
                and not (verified and entry["code"] == 0))):
            raise ValueError("MIG observation identity/modes not verified")
        entries[entry["uuid"]] = entry
    if set(entries) != expected or len(expected) != len(cards):
        raise ValueError("MIG capability original UUID set differs")
    return entries


def classification(entry):
    if entry["phase"] != "mig" or not entry["identity_verified"]:
        return "unknown"
    if entry["code"] == 3 and entry["current"] is None and entry["pending"] is None:
        return "not_supported"
    if (entry["code"] == 0 and type(entry["current"]) is int and entry["current"] in (0, 1)
            and type(entry["pending"]) is int and entry["pending"] in (0, 1)):
        return "supported"
    return "unknown"


def reconcile(value, cards):
    entries = validate(value, cards)
    status = {}
    for card in cards:
        entry = entries[card["uuid"]]
        status[card["uuid"]] = classification(entry)
        if status[card["uuid"]] == "not_supported" and (card["mig_current"] != "unknown" or card["mig_pending"] != "unknown"):
            raise ValueError("NVML MIG not-supported contradicts CSV mode")
        if status[card["uuid"]] == "supported":
            expected = tuple("enabled" if entry[k] else "disabled" for k in ("current", "pending"))
            if expected != (card["mig_current"], card["mig_pending"]):
                raise ValueError("NVML MIG mode contradicts CSV mode")
    return status
