/* Optional parent-side cgroup-v2 device BPF. No detach, replace, pin or helpers. */
#include <linux/bpf.h>
#include <stdint.h>

#define DEVICE_INSTRUCTION_LIMIT 4096
#define DEVICE_PROGRAM_LIMIT 64

static int device_descriptor(PyObject *argument) {
    if (!PyLong_CheckExact(argument)) {
        PyErr_SetString(PyExc_TypeError, "device descriptor must be an integer"); return -1;
    }
    long value = PyLong_AsLong(argument);
    if (PyErr_Occurred()) return -1;
    if (value < 0 || value > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "device descriptor out of range"); return -1;
    }
    return (int)value;
}

static PyObject *device_return_fd(int descriptor) {
    PyObject *result = PyLong_FromLong(descriptor);
    if (!result) close(descriptor);
    return result;
}

static int device_syscall(enum bpf_cmd command, union bpf_attr *attribute) {
    return (int)syscall(SYS_bpf, command, attribute, sizeof(*attribute));
}

static int device_scope_fd(int descriptor) {
    struct stat identity;
    struct statfs filesystem;
    if (fstat(descriptor, &identity) < 0 || fstatfs(descriptor, &filesystem) < 0) {
        PyErr_SetFromErrno(PyExc_OSError); return -1;
    }
    if (!S_ISDIR(identity.st_mode) || filesystem.f_type != 0x63677270) {
        PyErr_SetString(PyExc_ValueError, "requires original cgroup-v2 directory FD"); return -1;
    }
    return 0;
}

static int device_info(int descriptor, struct bpf_prog_info *info) {
    union bpf_attr attribute;
    memset(&attribute, 0, sizeof(attribute));
    memset(info, 0, sizeof(*info));
    attribute.info.bpf_fd = descriptor;
    attribute.info.info_len = sizeof(*info);
    attribute.info.info = (uint64_t)(uintptr_t)info;
    if (device_syscall(BPF_OBJ_GET_INFO_BY_FD, &attribute) < 0) {
        PyErr_SetFromErrno(PyExc_OSError); return -1;
    }
    if (info->type != BPF_PROG_TYPE_CGROUP_DEVICE || !info->id) {
        PyErr_SetString(PyExc_ValueError, "requires cgroup-device program"); return -1;
    }
    return 0;
}

static PyObject *device_program_load(PyObject *module, PyObject *argument) {
    (void)module;
    if (!PyBytes_CheckExact(argument)) {
        PyErr_SetString(PyExc_TypeError, "device bytecode must be bytes"); return NULL;
    }
    Py_ssize_t length = PyBytes_GET_SIZE(argument);
    if (length < 16 || length % sizeof(struct bpf_insn) || length > DEVICE_INSTRUCTION_LIMIT * (Py_ssize_t)sizeof(struct bpf_insn)) {
        PyErr_SetString(PyExc_ValueError, "device bytecode exceeds bounds"); return NULL;
    }
    union bpf_attr attribute;
    memset(&attribute, 0, sizeof(attribute));
    attribute.prog_type = BPF_PROG_TYPE_CGROUP_DEVICE;
    attribute.expected_attach_type = BPF_CGROUP_DEVICE;
    attribute.insn_cnt = (uint32_t)(length / sizeof(struct bpf_insn));
    attribute.insns = (uint64_t)(uintptr_t)PyBytes_AS_STRING(argument);
    /* Repository MIT license; emitted programs use no GPL-only helpers. */
    attribute.license = (uint64_t)(uintptr_t)"MIT";
    int descriptor = device_syscall(BPF_PROG_LOAD, &attribute);
    if (descriptor < 0) return PyErr_SetFromErrno(PyExc_OSError);
    /* Kernel-created BPF FDs are CLOEXEC; verify rather than assume. */
    int flags = fcntl(descriptor, F_GETFD);
    if (flags < 0 || !(flags & FD_CLOEXEC)) {
        int saved = flags < 0 ? errno : EINVAL; close(descriptor); errno = saved;
        return PyErr_SetFromErrno(PyExc_OSError);
    }
    return device_return_fd(descriptor);
}

static PyObject *device_program_info(PyObject *module, PyObject *argument) {
    (void)module;
    int descriptor = device_descriptor(argument);
    if (descriptor < 0) return NULL;
    struct bpf_prog_info info;
    if (device_info(descriptor, &info) < 0) return NULL;
    char tag[BPF_TAG_SIZE * 2 + 1];
    for (unsigned int i = 0; i < BPF_TAG_SIZE; ++i) snprintf(tag + i * 2, 3, "%02x", info.tag[i]);
    return Py_BuildValue("{s:I,s:s}", "program_id", info.id, "program_tag", tag);
}

static PyObject *device_program_fd(PyObject *module, PyObject *argument) {
    (void)module;
    if (!PyLong_CheckExact(argument)) {
        PyErr_SetString(PyExc_TypeError, "device program ID must be an integer"); return NULL;
    }
    unsigned long number = PyLong_AsUnsignedLong(argument);
    if (PyErr_Occurred()) return NULL;
    if (!number || number > UINT32_MAX) {
        PyErr_SetString(PyExc_ValueError, "invalid device program ID"); return NULL;
    }
    union bpf_attr attribute;
    memset(&attribute, 0, sizeof(attribute));
    attribute.prog_id = (uint32_t)number;
    int descriptor = device_syscall(BPF_PROG_GET_FD_BY_ID, &attribute);
    if (descriptor < 0) return PyErr_SetFromErrno(PyExc_OSError);
    struct bpf_prog_info info;
    if (device_info(descriptor, &info) < 0) { close(descriptor); return NULL; }
    return device_return_fd(descriptor);
}

static PyObject *device_program_query(PyObject *module, PyObject *argument) {
    (void)module;
    int descriptor = device_descriptor(argument);
    if (descriptor < 0 || device_scope_fd(descriptor) < 0) return NULL;
    uint32_t ids[DEVICE_PROGRAM_LIMIT];
    union bpf_attr attribute;
    memset(&attribute, 0, sizeof(attribute));
    attribute.query.target_fd = descriptor;
    attribute.query.attach_type = BPF_CGROUP_DEVICE;
    attribute.query.prog_cnt = DEVICE_PROGRAM_LIMIT;
    attribute.query.prog_ids = (uint64_t)(uintptr_t)ids;
    if (device_syscall(BPF_PROG_QUERY, &attribute) < 0) return PyErr_SetFromErrno(PyExc_OSError);
    if (attribute.query.prog_cnt > DEVICE_PROGRAM_LIMIT) {
        PyErr_SetString(PyExc_ValueError, "device attachment query exceeds bounds"); return NULL;
    }
    PyObject *items = PyList_New(attribute.query.prog_cnt);
    if (!items) return NULL;
    for (uint32_t i = 0; i < attribute.query.prog_cnt; ++i) {
        PyObject *number = PyLong_FromUnsignedLong(ids[i]);
        if (!number) { Py_DECREF(items); return NULL; }
        PyList_SET_ITEM(items, i, number);
    }
    return Py_BuildValue("{s:N,s:I}", "program_ids", items, "attach_flags", attribute.query.attach_flags);
}

static PyObject *device_program_attach(PyObject *module, PyObject *arguments) {
    (void)module;
    PyObject *directory_value, *program_value;
    if (!PyArg_ParseTuple(arguments, "OO", &directory_value, &program_value)) return NULL;
    int directory = device_descriptor(directory_value);
    if (directory < 0) return NULL;
    int program = device_descriptor(program_value);
    if (program < 0) return NULL;
    struct bpf_prog_info info;
    if (device_scope_fd(directory) < 0 || device_info(program, &info) < 0) return NULL;
    union bpf_attr attribute;
    memset(&attribute, 0, sizeof(attribute));
    attribute.target_fd = directory;
    attribute.attach_bpf_fd = program;
    attribute.attach_type = BPF_CGROUP_DEVICE;
    /* NONE and OVERRIDE can replace existing policies. MULTI only appends;
     * restrictive ancestor policies are preserved, never detached or changed. */
    attribute.attach_flags = BPF_F_ALLOW_MULTI;
    if (device_syscall(BPF_PROG_ATTACH, &attribute) < 0) return PyErr_SetFromErrno(PyExc_OSError);
    Py_RETURN_NONE;
}
