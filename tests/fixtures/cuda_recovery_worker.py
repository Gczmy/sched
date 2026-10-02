"""Manual GPU acceptance fixture: CUDA driver calls, bounded progress, no framework."""
import ctypes as C
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

GIB = 1024**3

class CudaOOM(MemoryError):
    pass

class Cuda:
    def __init__(self, ordinal=0):
        self.lib = C.CDLL("libcuda.so.1")
        signatures = {
            "cuInit": [C.c_uint], "cuDeviceGet": [C.POINTER(C.c_int), C.c_int],
            "cuCtxCreate_v2": [C.POINTER(C.c_void_p), C.c_uint, C.c_int],
            "cuCtxDestroy_v2": [C.c_void_p],
            "cuMemAlloc_v2": [C.POINTER(C.c_uint64), C.c_size_t],
            "cuMemFree_v2": [C.c_uint64],
            "cuMemGetInfo_v2": [C.POINTER(C.c_size_t), C.POINTER(C.c_size_t)],
            "cuModuleLoadData": [C.POINTER(C.c_void_p), C.c_void_p],
            "cuModuleGetFunction": [C.POINTER(C.c_void_p), C.c_void_p, C.c_char_p],
            "cuLaunchKernel": [C.c_void_p] + [C.c_uint]*7 + [C.c_void_p, C.POINTER(C.c_void_p), C.POINTER(C.c_void_p)],
            "cuCtxSynchronize": [],
            "cuMemcpyDtoH_v2": [C.c_void_p, C.c_uint64, C.c_size_t],
        }
        for name, arguments in signatures.items():
            getattr(self.lib, name).argtypes = arguments
        self.call("cuInit", 0)
        device = C.c_int()
        self.call("cuDeviceGet", C.byref(device), ordinal)
        self.context = C.c_void_p()
        self.call("cuCtxCreate_v2", C.byref(self.context), 0, device)
        self.allocations = []
        ptx = C.create_string_buffer(b""".version 6.0
.target sm_50
.address_size 64
.visible .entry bump(.param .u64 pointer, .param .u32 value) {
.reg .b64 %rd1;
.reg .b32 %r1;
ld.param.u64 %rd1, [pointer];
ld.param.u32 %r1, [value];
add.u32 %r1, %r1, 1;
st.global.u32 [%rd1], %r1;
ret;
}
""")
        module = C.c_void_p()
        self.call("cuModuleLoadData", C.byref(module), C.cast(ptx, C.c_void_p))
        self.kernel = C.c_void_p()
        self.call("cuModuleGetFunction", C.byref(self.kernel), module, b"bump")
        self.word = self.allocate(4)

    def call(self, name, *args):
        rc = getattr(self.lib, name)(*args)
        if rc == 2:
            raise CudaOOM("CUDA driver allocation returned OUT_OF_MEMORY")
        if rc:
            raise RuntimeError(f"{name} failed with CUDA error {rc}")

    def allocate(self, size):
        pointer = C.c_uint64()
        self.call("cuMemAlloc_v2", C.byref(pointer), size)
        self.allocations.append(pointer)
        return pointer

    def free_bytes(self):
        free, total = C.c_size_t(), C.c_size_t()
        self.call("cuMemGetInfo_v2", C.byref(free), C.byref(total))
        return free.value

    def compute(self, value):
        argument = C.c_uint(value)
        parameters = (C.c_void_p*2)(C.cast(C.byref(self.word), C.c_void_p), C.cast(C.byref(argument), C.c_void_p))
        self.call("cuLaunchKernel", self.kernel, 1, 1, 1, 1, 1, 1, 0, None, parameters, None)
        self.call("cuCtxSynchronize")
        result = C.c_uint()
        self.call("cuMemcpyDtoH_v2", C.byref(result), self.word, 4)
        assert result.value == value+1
        return result.value

    def fail_allocation(self):
        # Failed allocation never consumes all device memory or kills neighbors.
        try:
            self.allocate(self.free_bytes()+256*1024**2)
        except CudaOOM:
            return
        raise AssertionError("expected a real CUDA allocation OOM")

    def close(self):
        for pointer in reversed(self.allocations):
            self.call("cuMemFree_v2", pointer)
        self.call("cuCtxDestroy_v2", self.context)


def main():
    if sys.argv[1] == "--occupy":
        gpu, target = int(sys.argv[2]), float(sys.argv[3])
        assert 10 <= target <= 20
        cuda = Cuda(gpu)
        try:
            free = float(subprocess.check_output(["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits", "-i", str(gpu)], text=True).strip())*1024**2
            amount = int(free-target*GIB)
            assert amount > 0
            cuda.allocate(amount)
            cuda.compute(0)
            stopping = False
            def stop(signum, frame):
                nonlocal stopping
                stopping = True
            signal.signal(signal.SIGTERM, stop)
            signal.signal(signal.SIGINT, stop)
            print(json.dumps({"ready": True, "allocated_gib": amount/GIB}), flush=True)
            deadline = time.monotonic()+600
            while not stopping and time.monotonic()<deadline:
                time.sleep(.1)
        finally:
            cuda.close()
        return 0
    cuda = Cuda()
    try:
        if sys.argv[1] == "--ordinary":
            Path("ordinary-result.json").write_text(json.dumps({"cuda_result": cuda.compute(0)}))
            return 0
        from gsched.recovery import CheckpointStore
        store = CheckpointStore.from_environment()
        group = sys.argv[1]
        settings = json.loads(Path("settings.json").read_text())
        settings.update(settings.get("groups", {}).get(group, {}))
        if store.value["mode"] == "smoke":
            store.save({"next": 1, "results": [cuda.compute(0)]})
            cuda.fail_allocation()
            assert store.load()["results"] == [1]
            store.report("smoke_ok")
            return 0
        cuda.allocate(64*1024**2)
        with Path("starts.jsonl").open("a") as stream:
            stream.write(json.dumps({"group": group, "job_id": store.value["job_id"], "pid": os.getpid()})+"\n")
        progress = store.load() or {"next": 0, "results": []}
        for step in range(progress["next"], settings.get("total", 5)):
            progress["results"].append(cuda.compute(step))
            progress["next"] = step+1
            store.save(progress)
            if progress["next"] == settings.get("hold_step"):
                deadline = time.monotonic()+120
                while not Path("release").exists() and time.monotonic()<deadline:
                    time.sleep(.05)
            if progress["next"] in settings.get("oom_at", []):
                cuda.fail_allocation()
                store.report("oom")
                return 42
            time.sleep(.1)
        Path(f"result-{group}.json").write_text(json.dumps(progress))
        return 0
    finally:
        cuda.close()

if __name__ == "__main__":
    raise SystemExit(main())
