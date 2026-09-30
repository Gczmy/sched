/* Project-independent Linux direct-child FD execution.
 * All allocations and descriptor duplication happen before fork.  The child
 * uses only async-signal-safe operations and raw Linux syscalls until exec.
 */
#define _GNU_SOURCE 1
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define BINDING_LIMIT 128
#define OWNED_LIMIT (BINDING_LIMIT + 5)

typedef struct {
    PyObject_HEAD
    pid_t creator_pid, creator_tid, pid;
    int state, consumed, wait_status, launch_error, group_clean;
    int executable_fd, cwd_fd, error_fd;
    int owned[OWNED_LIMIT], owned_count;
    int source[BINDING_LIMIT + 3], target[BINDING_LIMIT + 3], binding_count;
    char **argv, **envp;
    struct rusage usage;
} child_owner;

static PyTypeObject owner_type;

static void close_owned(child_owner *o) {
    for (int i = 0; i < o->owned_count; ++i) close(o->owned[i]);
    o->owned_count = 0;
}

static int check_owner(child_owner *o) {
    if (getpid() != o->creator_pid ||
        (pid_t)syscall(SYS_gettid) != o->creator_tid) {
        PyErr_SetString(PyExc_RuntimeError, "owner requires original process and thread");
        return -1;
    }
    if (o->state == 4) {
        PyErr_SetString(PyExc_RuntimeError, "native owner is closed");
        return -1;
    }
    return 0;
}

static void free_strings(char **items) {
    if (!items) return;
    for (size_t i = 0; items[i]; ++i) PyMem_Free(items[i]);
    PyMem_Free(items);
}

static char **copy_strings(PyObject *items) {
    if (!PyTuple_Check(items)) {
        PyErr_SetString(PyExc_TypeError, "native argv/environment must be tuples");
        return NULL;
    }
    Py_ssize_t count = PyTuple_GET_SIZE(items);
    char **copied = PyMem_Calloc((size_t)count + 1, sizeof(char *));
    if (!copied) { PyErr_NoMemory(); return NULL; }
    for (Py_ssize_t i = 0; i < count; ++i) {
        Py_ssize_t length;
        const char *value = PyUnicode_AsUTF8AndSize(PyTuple_GET_ITEM(items, i), &length);
        if (!value) { free_strings(copied); return NULL; }
        if (memchr(value, 0, (size_t)length)) {
            free_strings(copied);
            PyErr_SetString(PyExc_ValueError, "NUL in native argv/environment");
            return NULL;
        }
        copied[i] = PyMem_Malloc((size_t)length + 1);
        if (!copied[i]) { free_strings(copied); PyErr_NoMemory(); return NULL; }
        memcpy(copied[i], value, (size_t)length + 1);
    }
    return copied;
}

static int retain_fd(child_owner *o, int source, int minimum) {
    int retained = fcntl(source, F_DUPFD_CLOEXEC, minimum);
    if (retained < 0) return -1;
    if (o->owned_count >= OWNED_LIMIT) {
        close(retained); errno = E2BIG; return -1;
    }
    o->owned[o->owned_count++] = retained;
    return retained;
}

static void owner_dealloc(child_owner *o) {
    /* Python registers a strong reference before start; no implicit signal or
     * fabricated wait is performed by garbage collection. */
    close_owned(o);
    if (o->error_fd >= 0) close(o->error_fd);
    free_strings(o->argv);
    free_strings(o->envp);
    Py_TYPE(o)->tp_free((PyObject *)o);
}

static void child_failure(int descriptor, int error_number) {
    ssize_t written;
    do { written = write(descriptor, &error_number, sizeof(error_number)); }
    while (written < 0 && errno == EINTR);
    _exit(127);
}

static int compare_int(const void *left, const void *right) {
    int a = *(const int *)left, b = *(const int *)right;
    return (a > b) - (a < b);
}

static PyObject *owner_start(child_owner *o, PyObject *unused) {
    (void)unused;
    if (check_owner(o) < 0) return NULL;
    if (o->consumed) {
        PyErr_SetString(PyExc_RuntimeError, "native launch already attempted"); return NULL;
    }
    o->consumed = 1;
    int pipes[2];
    if (pipe2(pipes, O_CLOEXEC | O_NONBLOCK) < 0) {
        o->state = 2; o->launch_error = errno; o->group_clean = 1;
        close_owned(o); return PyErr_SetFromErrno(PyExc_OSError);
    }
    int minimum = 3;
    for (int i = 0; i < o->binding_count; ++i)
        if (o->target[i] >= minimum) minimum = o->target[i] + 1;
    int error_write = fcntl(pipes[1], F_DUPFD_CLOEXEC, minimum);
    int saved = errno;
    close(pipes[1]);
    if (error_write < 0) {
        close(pipes[0]); errno = saved;
        o->state = 2; o->launch_error = saved; o->group_clean = 1;
        close_owned(o); return PyErr_SetFromErrno(PyExc_OSError);
    }
    int keep[BINDING_LIMIT + 5], keep_count = 0;
    for (int i = 0; i < o->binding_count; ++i) keep[keep_count++] = o->target[i];
    keep[keep_count++] = o->executable_fd;
    keep[keep_count++] = error_write;
    qsort(keep, (size_t)keep_count, sizeof(int), compare_int);
    sigset_t empty;
    sigemptyset(&empty);
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = SIG_DFL;
    sigemptyset(&action.sa_mask);
    pid_t pid = fork();
    saved = errno;
    if (pid == 0) {
        if (setsid() < 0) child_failure(error_write, errno);
        if (sigprocmask(SIG_SETMASK, &empty, NULL) < 0) child_failure(error_write, errno);
        for (int number = 1; number < NSIG; ++number)
            if (number != SIGKILL && number != SIGSTOP) sigaction(number, &action, NULL);
        if (o->cwd_fd >= 0 && fchdir(o->cwd_fd) < 0) child_failure(error_write, errno);
        for (int i = 0; i < o->binding_count; ++i)
            if (dup2(o->source[i], o->target[i]) < 0) child_failure(error_write, errno);
        unsigned int first = 0;
        for (int i = 0; i < keep_count; ++i) {
            unsigned int retained = (unsigned int)keep[i];
            if (retained > first && syscall(SYS_close_range, first, retained - 1, 0) < 0)
                child_failure(error_write, errno);
            first = retained + 1;
        }
        if (syscall(SYS_close_range, first, UINT_MAX, 0) < 0)
            child_failure(error_write, errno);
        syscall(SYS_execveat, o->executable_fd, "", o->argv, o->envp, AT_EMPTY_PATH);
        child_failure(error_write, errno);
    }
    close(error_write);
    if (pid < 0) {
        close(pipes[0]); errno = saved;
        o->state = 2; o->launch_error = saved; o->group_clean = 1;
        close_owned(o); return PyErr_SetFromErrno(PyExc_OSError);
    }
    /* Persist native wait authority before any return to the Python boundary. */
    o->pid = pid; o->state = 1; o->error_fd = pipes[0];
    close_owned(o);
    Py_RETURN_NONE;
}

static void inspect_launch_error(child_owner *o) {
    if (o->error_fd >= 0) {
        int reported = 0;
        ssize_t count = read(o->error_fd, &reported, sizeof(reported));
        if (count == (ssize_t)sizeof(reported)) o->launch_error = reported;
        if (count == 0 || count == (ssize_t)sizeof(reported)) {
            close(o->error_fd); o->error_fd = -1;
        }
        /* EOF is intentionally not exposed as proof of successful exec. */
    }
}

static void inspect_child(child_owner *o) {
    inspect_launch_error(o);
    if (o->state == 1) {
        pid_t observed;
        do { observed = wait4(o->pid, &o->wait_status, WNOHANG, &o->usage); }
        while (observed < 0 && errno == EINTR);
        if (observed == o->pid) o->state = 2;
        else if (observed < 0) o->state = 3;
    }
    /* Once wait4 has returned, the child cannot still be writing its error
     * report. Collect it before exposing a terminal observation. */
    if (o->state == 2) inspect_launch_error(o);
    if (o->state == 2 && !o->group_clean) {
        if (kill(-o->pid, 0) < 0 && errno == ESRCH) o->group_clean = 1;
    }
}

static PyObject *owner_poll(child_owner *o, PyObject *unused) {
    (void)unused;
    if (check_owner(o) < 0) return NULL;
    inspect_child(o);
    const char *status = o->state == 0 ? "prepared" : o->state == 1 ? "running" :
        o->state == 3 ? "authority_lost" : o->pid <= 0 ? "not_started" :
        o->group_clean ? "exited" : "cleanup_pending";
    PyObject *result = Py_BuildValue("{s:s,s:O,s:O,s:O,s:O,s:O}",
        "status", status, "pid", Py_None, "returncode", Py_None,
        "rusage", Py_None, "launch_error", Py_None,
        "group_clean", o->group_clean ? Py_True : Py_False);
    if (!result) return NULL;
    if (o->pid > 0) {
        PyObject *pid = PyLong_FromLong(o->pid);
        if (!pid || PyDict_SetItemString(result, "pid", pid) < 0) { Py_XDECREF(pid); Py_DECREF(result); return NULL; }
        Py_DECREF(pid);
    }
    if (o->state == 2 && o->pid > 0) {
        int code = WIFEXITED(o->wait_status) ? WEXITSTATUS(o->wait_status) : -WTERMSIG(o->wait_status);
        PyObject *returncode = PyLong_FromLong(code);
        PyObject *usage = Py_BuildValue("{s:d,s:d,s:l,s:l,s:l}",
            "user_seconds", (double)o->usage.ru_utime.tv_sec + o->usage.ru_utime.tv_usec / 1000000.0,
            "system_seconds", (double)o->usage.ru_stime.tv_sec + o->usage.ru_stime.tv_usec / 1000000.0,
            "max_rss_kib", o->usage.ru_maxrss, "input_blocks", o->usage.ru_inblock,
            "output_blocks", o->usage.ru_oublock);
        if (!returncode || !usage || PyDict_SetItemString(result, "returncode", returncode) < 0 ||
            PyDict_SetItemString(result, "rusage", usage) < 0) {
            Py_XDECREF(returncode); Py_XDECREF(usage); Py_DECREF(result); return NULL;
        }
        Py_DECREF(returncode); Py_DECREF(usage);
    }
    if (o->launch_error) {
        PyObject *error = PyLong_FromLong(o->launch_error);
        if (!error || PyDict_SetItemString(result, "launch_error", error) < 0) { Py_XDECREF(error); Py_DECREF(result); return NULL; }
        Py_DECREF(error);
    }
    return result;
}

static PyObject *owner_signal(child_owner *o, PyObject *arg) {
    if (check_owner(o) < 0) return NULL;
    long number = PyLong_AsLong(arg);
    if (PyErr_Occurred()) return NULL;
    if (number != SIGTERM && number != SIGKILL) {
        PyErr_SetString(PyExc_ValueError, "only TERM/KILL lifecycle signals are supported"); return NULL;
    }
    inspect_child(o);
    if (o->state == 3) {
        PyErr_SetString(PyExc_RuntimeError, "original wait authority lost"); return NULL;
    }
    if (o->state == 0 || o->group_clean) Py_RETURN_NONE;
    if (kill(-o->pid, (int)number) < 0) {
        if (errno != ESRCH) return PyErr_SetFromErrno(PyExc_OSError);
        /* Before setsid, the original child remains protected by wait ownership. */
        if (o->state == 1 && kill(o->pid, (int)number) < 0 && errno != ESRCH)
            return PyErr_SetFromErrno(PyExc_OSError);
    }
    Py_RETURN_NONE;
}

static PyObject *owner_close(child_owner *o, PyObject *unused) {
    (void)unused;
    if (check_owner(o) < 0) return NULL;
    inspect_child(o);
    if (o->state != 0 && !(o->state == 2 && o->group_clean)) {
        PyErr_SetString(PyExc_RuntimeError, "cannot close active or unresolved process group"); return NULL;
    }
    close_owned(o);
    if (o->error_fd >= 0) { close(o->error_fd); o->error_fd = -1; }
    o->state = 4;
    Py_RETURN_NONE;
}

static PyObject *owner_pid(child_owner *o, PyObject *unused) {
    (void)unused;
    if (check_owner(o) < 0) return NULL;
    if (o->pid <= 0) Py_RETURN_NONE;
    return PyLong_FromLong(o->pid);
}

static PyMethodDef owner_methods[] = {
    {"start", (PyCFunction)owner_start, METH_NOARGS, NULL},
    {"poll", (PyCFunction)owner_poll, METH_NOARGS, NULL},
    {"send_signal", (PyCFunction)owner_signal, METH_O, NULL},
    {"close", (PyCFunction)owner_close, METH_NOARGS, NULL},
    {"pid", (PyCFunction)owner_pid, METH_NOARGS, NULL},
    {NULL, NULL, 0, NULL}
};

static PyTypeObject owner_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "gsched.execution._fdexec.ChildOwner",
    .tp_basicsize = sizeof(child_owner),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)owner_dealloc,
    .tp_methods = owner_methods,
};

static PyObject *prepare(PyObject *module, PyObject *args) {
    (void)module;
    int executable, cwd;
    PyObject *argv, *envp, *bindings;
    if (!PyArg_ParseTuple(args, "iOOiO", &executable, &argv, &envp, &cwd, &bindings)) return NULL;
    if (!PyTuple_Check(bindings) || PyTuple_GET_SIZE(bindings) > BINDING_LIMIT) {
        PyErr_SetString(PyExc_ValueError, "too many or invalid descriptor bindings"); return NULL;
    }
    struct stat identity;
    char header[4];
    if (fstat(executable, &identity) < 0)
        return PyErr_SetFromErrno(PyExc_OSError);
    ssize_t header_bytes = pread(executable, header, sizeof(header), 0);
    if (header_bytes < 0) return PyErr_SetFromErrno(PyExc_OSError);
    if (!S_ISREG(identity.st_mode) || !(identity.st_mode & 0111) || header_bytes != 4 || memcmp(header, "\177ELF", 4)) {
        PyErr_SetString(PyExc_ValueError, "FD execution requires an executable ELF file"); return NULL;
    }
    child_owner *o = (child_owner *)owner_type.tp_alloc(&owner_type, 0);
    if (!o) return NULL;
    o->creator_pid = getpid(); o->creator_tid = (pid_t)syscall(SYS_gettid);
    o->executable_fd = o->cwd_fd = o->error_fd = -1;
    o->argv = copy_strings(argv); o->envp = copy_strings(envp);
    if (!o->argv || !o->envp || !o->argv[0]) goto fail;
    int targets[BINDING_LIMIT], sources[BINDING_LIMIT], minimum = 3;
    int count = (int)PyTuple_GET_SIZE(bindings);
    for (int i = 0; i < count; ++i) {
        if (!PyArg_ParseTuple(PyTuple_GET_ITEM(bindings, i), "ii", &targets[i], &sources[i])) goto fail;
        if (targets[i] < 0 || targets[i] > 65535 || sources[i] < 0) {
            PyErr_SetString(PyExc_ValueError, "invalid descriptor binding"); goto fail;
        }
        for (int j = 0; j < i; ++j) if (targets[j] == targets[i]) {
            PyErr_SetString(PyExc_ValueError, "duplicate target descriptor"); goto fail;
        }
        if (targets[i] >= minimum) minimum = targets[i] + 1;
    }
    o->executable_fd = retain_fd(o, executable, minimum);
    if (o->executable_fd < 0) goto os_fail;
    if (cwd >= 0) {
        if (fstat(cwd, &identity) < 0) goto os_fail;
        if (!S_ISDIR(identity.st_mode)) { PyErr_SetString(PyExc_ValueError, "cwd_fd must retain a directory"); goto fail; }
        o->cwd_fd = retain_fd(o, cwd, minimum);
        if (o->cwd_fd < 0) goto os_fail;
    }
    for (int i = 0; i < count; ++i) {
        int source = retain_fd(o, sources[i], minimum);
        if (source < 0) goto os_fail;
        o->target[o->binding_count] = targets[i]; o->source[o->binding_count++] = source;
    }
    for (int target = 0; target < 3; ++target) {
        int bound = 0;
        for (int i = 0; i < count; ++i) if (targets[i] == target) bound = 1;
        if (!bound) {
            int null_fd = open("/dev/null", O_RDWR | O_CLOEXEC);
            if (null_fd < 0) goto os_fail;
            int source = retain_fd(o, null_fd, minimum);
            int saved = errno; close(null_fd); errno = saved;
            if (source < 0) goto os_fail;
            o->target[o->binding_count] = target; o->source[o->binding_count++] = source;
        }
    }
    return (PyObject *)o;
os_fail:
    PyErr_SetFromErrno(PyExc_OSError);
fail:
    if (!PyErr_Occurred()) PyErr_SetString(PyExc_ValueError, "argv must be nonempty");
    Py_DECREF(o); return NULL;
}

static PyObject *check_capabilities(PyObject *module, PyObject *unused) {
    (void)module; (void)unused;
    if (syscall(SYS_close_range, UINT_MAX, UINT_MAX, 0) < 0)
        return PyErr_SetFromErrno(PyExc_OSError);
    char *const empty[] = {NULL};
    syscall(SYS_execveat, -1, "", empty, empty, AT_EMPTY_PATH);
    if (errno != EBADF) return PyErr_SetFromErrno(PyExc_OSError);
    Py_RETURN_NONE;
}

static PyMethodDef module_methods[] = {
    {"prepare", prepare, METH_VARARGS, NULL},
    {"check_capabilities", check_capabilities, METH_NOARGS, NULL},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT, "_fdexec", "Original-parent Linux FD exec owner.", -1,
    module_methods, NULL, NULL, NULL, NULL
};

PyMODINIT_FUNC PyInit__fdexec(void) {
    if (PyType_Ready(&owner_type) < 0) return NULL;
    PyObject *result = PyModule_Create(&module);
    if (!result) return NULL;
    if (PyModule_AddStringConstant(result, "interface_version", "sched-execution/v1") < 0) {
        Py_DECREF(result); return NULL;
    }
    return result;
}
