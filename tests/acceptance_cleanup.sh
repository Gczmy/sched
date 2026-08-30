#!/bin/bash

# Acceptance scripts source this file after changing to the repository root.
# Cleanup claims bind each private token to an external hard-link anchor so
# directory inode reuse on NFS cannot authenticate a replacement root.
# Daemons are stopped through the CLI before any root is quarantined; cleanup
# never edits scheduler state or signals processes directly.
SCHED_ACCEPT_CLEANUP_DONE=0
SCHED_ACCEPT_CLEANUP_ROOTS=()
SCHED_ACCEPT_CLEANUP_TOKENS=()
SCHED_ACCEPT_CLEANUP_DEVS=()
SCHED_ACCEPT_CLEANUP_INOS=()
SCHED_ACCEPT_CLEANUP_ANCHORS=()
SCHED_ACCEPT_CLEANUP_MARKER_DEVS=()
SCHED_ACCEPT_CLEANUP_MARKER_INOS=()
SCHED_ACCEPT_REPO_ROOT=$PWD
SCHED_ACCEPT_OWNER_MARKER=".sched-accept-owned"
SCHED_ACCEPT_SYSTEM_PYTHON=${SCHED_ACCEPT_SYSTEM_PYTHON:-python3}
SCHED_ACCEPT_DAEMON_STOP_TIMEOUT=${SCHED_ACCEPT_DAEMON_STOP_TIMEOUT:-20}

sched_accept_warn() {
  printf 'acceptance cleanup: warning: %s\n' "$*" >&2
}

sched_accept_root_owned() {
  local root=$1
  local token=$2
  local expected_dev=$3
  local expected_ino=$4
  local anchor=$5
  local expected_marker_dev=$6
  local expected_marker_ino=$7
  "$SCHED_ACCEPT_SYSTEM_PYTHON" - "$root" "$SCHED_ACCEPT_OWNER_MARKER" \
    "$token" "$expected_dev" "$expected_ino" "$anchor" \
    "$expected_marker_dev" "$expected_marker_ino" <<'PY'
import os
import stat
import sys

(
    root,
    marker,
    token,
    expected_dev,
    expected_ino,
    anchor,
    expected_marker_dev,
    expected_marker_ino,
) = sys.argv[1:]
expected_root_identity = (int(expected_dev), int(expected_ino))
expected_marker_identity = (int(expected_marker_dev), int(expected_marker_ino))
payload = (token + "\n").encode()
parent = os.path.dirname(root)
name = os.path.basename(root)
anchor_parent = os.path.dirname(anchor)
anchor_name = os.path.basename(anchor)
current_uid = os.getuid()
parent_fd = None
root_fd = None
marker_fd = None
anchor_fd = None


def read_from_start(fd):
    os.lseek(fd, 0, os.SEEK_SET)
    return os.read(fd, 4097)


def validate_private_file(opened, identity, label):
    observed = os.fstat(opened)
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != current_uid
        or stat.S_IMODE(observed.st_mode) != 0o600
        or (observed.st_dev, observed.st_ino) != identity
        or read_from_start(opened) != payload
    ):
        raise ValueError(f"{label} identity changed")
    return observed


try:
    if (
        not os.path.isabs(root)
        or not os.path.isabs(anchor)
        or not name
        or not anchor_name.startswith(".sched-accept-claim-")
        or parent != anchor_parent
        or name == anchor_name
    ):
        raise ValueError("cleanup claim paths are invalid")
    parent_flags = (
        os.O_RDONLY
        | os.O_CLOEXEC
        | os.O_DIRECTORY
    )
    directory_flags = parent_flags | os.O_NOFOLLOW
    file_flags = (
        os.O_RDONLY
        | os.O_CLOEXEC
        | os.O_NOFOLLOW
        | os.O_NONBLOCK
    )
    parent_fd = os.open(parent, parent_flags)
    root_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if (
        not stat.S_ISDIR(root_stat.st_mode)
        or stat.S_ISLNK(root_stat.st_mode)
        or root_stat.st_uid != current_uid
        or stat.S_IMODE(root_stat.st_mode) != 0o700
        or (root_stat.st_dev, root_stat.st_ino) != expected_root_identity
    ):
        raise ValueError("root identity changed")
    root_fd = os.open(name, directory_flags, dir_fd=parent_fd)
    opened_root = os.fstat(root_fd)
    if (
        not stat.S_ISDIR(opened_root.st_mode)
        or opened_root.st_uid != current_uid
        or stat.S_IMODE(opened_root.st_mode) != 0o700
        or (opened_root.st_dev, opened_root.st_ino) != expected_root_identity
    ):
        raise ValueError("root changed while opening it")

    marker_stat = os.stat(marker, dir_fd=root_fd, follow_symlinks=False)
    if (
        not stat.S_ISREG(marker_stat.st_mode)
        or marker_stat.st_uid != current_uid
        or stat.S_IMODE(marker_stat.st_mode) != 0o600
        or (marker_stat.st_dev, marker_stat.st_ino) != expected_marker_identity
    ):
        raise ValueError("ownership marker identity changed")
    marker_fd = os.open(marker, file_flags, dir_fd=root_fd)
    validate_private_file(
        marker_fd,
        expected_marker_identity,
        "ownership marker",
    )

    anchor_stat = os.stat(anchor_name, dir_fd=parent_fd, follow_symlinks=False)
    if (
        not stat.S_ISREG(anchor_stat.st_mode)
        or anchor_stat.st_uid != current_uid
        or stat.S_IMODE(anchor_stat.st_mode) != 0o600
        or (anchor_stat.st_dev, anchor_stat.st_ino) != expected_marker_identity
    ):
        raise ValueError("ownership anchor identity changed")
    anchor_fd = os.open(anchor_name, file_flags, dir_fd=parent_fd)
    validate_private_file(
        anchor_fd,
        expected_marker_identity,
        "ownership anchor",
    )
    opened_marker = os.fstat(marker_fd)
    opened_anchor = os.fstat(anchor_fd)
    if (opened_marker.st_dev, opened_marker.st_ino) != (
        opened_anchor.st_dev,
        opened_anchor.st_ino,
    ):
        raise ValueError("ownership marker detached from anchor")
except (OSError, ValueError):
    raise SystemExit(1)
finally:
    if anchor_fd is not None:
        os.close(anchor_fd)
    if marker_fd is not None:
        os.close(marker_fd)
    if root_fd is not None:
        os.close(root_fd)
    if parent_fd is not None:
        os.close(parent_fd)
PY
}

sched_accept_make_root() {
  local variable=$1
  local label=${2:-sched-accept}
  local created_root
  local claim
  local token
  local root_dev
  local root_ino
  local anchor
  local marker_dev
  local marker_ino

  created_root=$(mktemp -d "${TMPDIR:-/tmp}/${label}.XXXXXX") || return 1
  case $created_root in
    /*) ;;
    *) created_root=$PWD/$created_root ;;
  esac
  claim=$(
    "$SCHED_ACCEPT_SYSTEM_PYTHON" - "$created_root" "$SCHED_ACCEPT_OWNER_MARKER" <<'PY'
import os
import secrets
import stat
import sys

root, marker = sys.argv[1:]
parent = os.path.dirname(root)
name = os.path.basename(root)
current_uid = os.getuid()
token = secrets.token_hex(32)
payload = (token + "\n").encode()
parent_flags = (
    os.O_RDONLY
    | os.O_CLOEXEC
    | os.O_DIRECTORY
)
directory_flags = parent_flags | os.O_NOFOLLOW
read_flags = (
    os.O_RDONLY
    | os.O_CLOEXEC
    | os.O_NOFOLLOW
    | os.O_NONBLOCK
)
parent_fd = os.open(parent, parent_flags)
root_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
if (
    not stat.S_ISDIR(root_stat.st_mode)
    or stat.S_ISLNK(root_stat.st_mode)
    or root_stat.st_uid != current_uid
    or stat.S_IMODE(root_stat.st_mode) != 0o700
):
    raise SystemExit("temporary root is not a private directory")
root_fd = os.open(name, directory_flags, dir_fd=parent_fd)
marker_fd = None
anchor_fd = None
anchor_name = None
anchor = None


def candidate_marker_link_state(candidate):
    candidate_fd = None
    try:
        marker_observed = os.fstat(marker_fd)
        if (
            not stat.S_ISREG(marker_observed.st_mode)
            or marker_observed.st_uid != current_uid
            or stat.S_IMODE(marker_observed.st_mode) != 0o600
        ):
            return None
        observed = os.stat(
            candidate,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        marker_identity = (marker_observed.st_dev, marker_observed.st_ino)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != current_uid
            or stat.S_IMODE(observed.st_mode) != 0o600
            or (observed.st_dev, observed.st_ino) != marker_identity
        ):
            return False
        candidate_fd = os.open(candidate, read_flags, dir_fd=parent_fd)
        opened = os.fstat(candidate_fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != current_uid
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (opened.st_dev, opened.st_ino) != marker_identity
            or (opened.st_dev, opened.st_ino)
            != (observed.st_dev, observed.st_ino)
            or os.read(candidate_fd, 4097) != payload
        ):
            return None
        return True
    except OSError:
        return None
    finally:
        if candidate_fd is not None:
            os.close(candidate_fd)


try:
    opened_root = os.fstat(root_fd)
    if (
        not stat.S_ISDIR(opened_root.st_mode)
        or opened_root.st_uid != current_uid
        or stat.S_IMODE(opened_root.st_mode) != 0o700
        or (opened_root.st_dev, opened_root.st_ino)
        != (root_stat.st_dev, root_stat.st_ino)
    ):
        raise RuntimeError("temporary root changed while opening it")
    write_flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | os.O_CLOEXEC
        | os.O_NOFOLLOW
    )
    marker_fd = os.open(marker, write_flags, 0o600, dir_fd=root_fd)
    written = 0
    while written < len(payload):
        written += os.write(marker_fd, payload[written:])
    os.fsync(marker_fd)
    for _ in range(100):
        candidate = f".sched-accept-claim-{secrets.token_hex(32)}"
        anchor_name = candidate
        link_error = None
        try:
            os.link(
                marker,
                candidate,
                src_dir_fd=root_fd,
                dst_dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except OSError as error:
            link_error = error
        candidate_state = candidate_marker_link_state(candidate)
        if candidate_state is True:
            anchor = os.path.join(parent, candidate)
            break
        if isinstance(link_error, FileExistsError) and candidate_state is False:
            anchor_name = None
            continue
        if link_error is not None:
            if candidate_state is None:
                raise RuntimeError(
                    f"could not verify ownership anchor {candidate!r}"
                ) from link_error
            raise link_error
        raise RuntimeError("ownership anchor changed after link")
    if anchor is None:
        raise RuntimeError("could not allocate ownership anchor")
    marker_stat = os.fstat(marker_fd)
    marker_identity = (marker_stat.st_dev, marker_stat.st_ino)
    anchor_stat = os.stat(
        anchor_name,
        dir_fd=parent_fd,
        follow_symlinks=False,
    )
    if (
        not stat.S_ISREG(marker_stat.st_mode)
        or marker_stat.st_uid != current_uid
        or stat.S_IMODE(marker_stat.st_mode) != 0o600
        or not stat.S_ISREG(anchor_stat.st_mode)
        or anchor_stat.st_uid != current_uid
        or stat.S_IMODE(anchor_stat.st_mode) != 0o600
        or (anchor_stat.st_dev, anchor_stat.st_ino) != marker_identity
    ):
        raise RuntimeError("ownership anchor was not linked to marker")
    anchor_fd = os.open(anchor_name, read_flags, dir_fd=parent_fd)
    opened_anchor = os.fstat(anchor_fd)
    if (
        not stat.S_ISREG(opened_anchor.st_mode)
        or opened_anchor.st_uid != current_uid
        or stat.S_IMODE(opened_anchor.st_mode) != 0o600
        or (opened_anchor.st_dev, opened_anchor.st_ino) != marker_identity
        or os.read(anchor_fd, 4097) != payload
    ):
        raise RuntimeError("ownership anchor changed while opening it")
    confirmed = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if (confirmed.st_dev, confirmed.st_ino) != (
        opened_root.st_dev,
        opened_root.st_ino,
    ):
        raise RuntimeError("temporary root identity changed while claiming it")
    print(
        f"{token}\t{root_stat.st_dev}\t{root_stat.st_ino}\t"
        f"{anchor}\t{marker_stat.st_dev}\t{marker_stat.st_ino}"
    )
except BaseException:
    owned_anchor = False
    if anchor_name is not None:
        owned_anchor = candidate_marker_link_state(anchor_name) is True
    if anchor_fd is not None:
        os.close(anchor_fd)
        anchor_fd = None
    if owned_anchor:
        try:
            observed = os.stat(
                anchor_name,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            marker_observed = os.fstat(marker_fd)
            if (observed.st_dev, observed.st_ino) == (
                marker_observed.st_dev,
                marker_observed.st_ino,
            ):
                os.unlink(anchor_name, dir_fd=parent_fd)
        except OSError:
            pass
    raise
finally:
    if anchor_fd is not None:
        os.close(anchor_fd)
    if marker_fd is not None:
        os.close(marker_fd)
    os.close(root_fd)
    os.close(parent_fd)
PY
  ) || {
    sched_accept_warn "could not create cleanup claim for $created_root; preserving it"
    return 1
  }
  IFS=$'\t' read -r token root_dev root_ino anchor marker_dev marker_ino <<< "$claim"
  if [ -z "$token" ] || [ -z "$root_dev" ] || [ -z "$root_ino" ] \
    || [ -z "$anchor" ] || [ -z "$marker_dev" ] || [ -z "$marker_ino" ]; then
    sched_accept_warn "cleanup claim for $created_root was incomplete; preserving it"
    return 1
  fi

  SCHED_ACCEPT_CLEANUP_ROOTS+=("$created_root")
  SCHED_ACCEPT_CLEANUP_TOKENS+=("$token")
  SCHED_ACCEPT_CLEANUP_DEVS+=("$root_dev")
  SCHED_ACCEPT_CLEANUP_INOS+=("$root_ino")
  SCHED_ACCEPT_CLEANUP_ANCHORS+=("$anchor")
  SCHED_ACCEPT_CLEANUP_MARKER_DEVS+=("$marker_dev")
  SCHED_ACCEPT_CLEANUP_MARKER_INOS+=("$marker_ino")
  printf -v "$variable" '%s' "$created_root"
}

sched_accept_stop_daemon() {
  local root=$1
  local python=${PY:-python3}
  local inherited_pythonpath=${PYTHONPATH-}
  env SCHED_STATE="$root" SCHED_CONFIG="$root/config.json" \
    PYTHONPATH="$SCHED_ACCEPT_REPO_ROOT${inherited_pythonpath:+:$inherited_pythonpath}" \
    "$SCHED_ACCEPT_SYSTEM_PYTHON" - "$python" "$SCHED_ACCEPT_DAEMON_STOP_TIMEOUT" <<'PY'
import math
import subprocess
import sys

python, timeout_text = sys.argv[1:]
try:
    timeout = float(timeout_text)
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError
except ValueError:
    raise SystemExit(125)
try:
    completed = subprocess.run(
        [python, "-m", "gsched.cli", "daemon", "stop"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout,
        check=False,
    )
except (OSError, subprocess.TimeoutExpired):
    raise SystemExit(124)
raise SystemExit(completed.returncode)
PY
}

sched_accept_runtime_absent() {
  local root=$1
  "$SCHED_ACCEPT_SYSTEM_PYTHON" - "$root" <<'PY'
import os
import stat
import sys

root = sys.argv[1]
directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY
runtime_names = {
    "daemon.heartbeat",
    "daemon.log",
    "daemon.pid",
    "dispatcher.lock",
    "dispatcher.lock.guard",
    "launch",
}


def contains_runtime(dir_fd, tree_dev):
    with os.scandir(dir_fd) as entries:
        names = [entry.name for entry in entries]
    for name in names:
        observed = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        if (
            name in runtime_names
            or name.startswith("state.db")
            or name.endswith(".launch")
        ):
            return True
        if stat.S_ISDIR(observed.st_mode) and not stat.S_ISLNK(observed.st_mode):
            if observed.st_dev != tree_dev:
                return True
            child_fd = os.open(name, directory_flags, dir_fd=dir_fd)
            try:
                opened = os.fstat(child_fd)
                if (
                    not stat.S_ISDIR(opened.st_mode)
                    or (opened.st_dev, opened.st_ino)
                    != (observed.st_dev, observed.st_ino)
                ):
                    return True
                if contains_runtime(child_fd, tree_dev):
                    return True
            finally:
                os.close(child_fd)
    return False


root_fd = None
try:
    observed = os.lstat(root)
    if not stat.S_ISDIR(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
        raise RuntimeError("cleanup root is not a physical directory")
    root_fd = os.open(root, directory_flags)
    opened = os.fstat(root_fd)
    if (opened.st_dev, opened.st_ino) != (observed.st_dev, observed.st_ino):
        raise RuntimeError("cleanup root changed while opening")
    raise SystemExit(1 if contains_runtime(root_fd, opened.st_dev) else 0)
except (OSError, RecursionError, RuntimeError):
    raise SystemExit(1)
finally:
    if root_fd is not None:
        os.close(root_fd)
PY
}

sched_accept_verify_quiescent() {
  local root=$1
  local python=${PY:-python3}
  local inherited_pythonpath=${PYTHONPATH-}
  env SCHED_STATE="$root" SCHED_CONFIG="$root/config.json" \
    PYTHONPATH="$SCHED_ACCEPT_REPO_ROOT${inherited_pythonpath:+:$inherited_pythonpath}" \
    "$SCHED_ACCEPT_SYSTEM_PYTHON" - "$python" "$SCHED_ACCEPT_DAEMON_STOP_TIMEOUT" \
    "$root" <<'PY'
import json
import math
import os
import stat
import subprocess
import sys

python, timeout_text, root = sys.argv[1:]


def reject(reason):
    print(f"acceptance cleanup: unsafe after daemon stop: {reason}", file=sys.stderr)
    raise SystemExit(1)


try:
    timeout = float(timeout_text)
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError
except ValueError:
    reject("invalid status timeout")

try:
    completed = subprocess.run(
        [python, "-m", "gsched.cli", "status", "--json"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        check=False,
    )
except (OSError, subprocess.TimeoutExpired) as error:
    reject(f"status --json failed or timed out: {error}")
if completed.returncode != 0:
    reject(f"status --json returned {completed.returncode}")
try:
    status_payload = json.loads(completed.stdout)
except (TypeError, json.JSONDecodeError, RecursionError) as error:
    reject(f"status --json was not canonical JSON: {error}")

required_fields = {
    "schema_version",
    "limit",
    "batches",
    "jobs",
    "gpus",
    "truncated",
    "next_cursor",
    "next_job_cursor",
    "daemon_health",
    "cpu",
}
if not isinstance(status_payload, dict) or not required_fields.issubset(
    status_payload
):
    reject("status --json did not contain the canonical status envelope")
if (
    type(status_payload["schema_version"]) is not int
    or status_payload["schema_version"] != 1
    or type(status_payload["limit"]) is not int
    or status_payload["limit"] <= 0
    or not isinstance(status_payload["batches"], list)
    or not isinstance(status_payload["jobs"], list)
    or not isinstance(status_payload["gpus"], list)
    or not isinstance(status_payload["daemon_health"], dict)
    or not isinstance(status_payload["cpu"], dict)
    or not {"used", "total"}.issubset(status_payload["cpu"])
):
    reject("status --json schema was not canonical")
truncated = status_payload["truncated"]
if (
    not isinstance(truncated, dict)
    or type(truncated.get("batches")) is not bool
    or type(truncated.get("jobs")) is not bool
):
    reject("status --json truncation schema was not canonical")
if truncated["batches"] or truncated["jobs"]:
    reject("status --json batches or jobs were truncated")
if (
    status_payload["next_cursor"] is not None
    or status_payload["next_job_cursor"] is not None
):
    reject("untruncated status --json unexpectedly contained a cursor")

batch_fields = {
    "id",
    "name",
    "batch_id",
    "batch_name",
    "mode",
    "status",
    "depends_on",
    "progress",
    "project",
    "revision",
}
job_fields = {
    "id",
    "batch_id",
    "batch_name",
    "task",
    "status",
    "wait_reason",
    "gpu",
    "version",
    "resources",
    "retries",
    "failure",
    "started_at",
    "finished_at",
    "progress",
}
task_refs = set()
for label, rows, fields in (
    ("batch", status_payload["batches"], batch_fields),
    ("job", status_payload["jobs"], job_fields),
):
    for row in rows:
        if (
            not isinstance(row, dict)
            or not fields.issubset(row)
            or not isinstance(row["status"], str)
            or not row["status"]
        ):
            reject(f"status --json contained a noncanonical {label}")
        if row["status"] == "running":
            reject(f"status --json still reported a running {label}")
        if label == "job":
            batch_id = row.get("batch_id")
            task_id = row.get("task")
            if (
                not isinstance(batch_id, str)
                or not batch_id
                or not isinstance(task_id, str)
                or not task_id
            ):
                reject("status --json job reference was invalid")
            task_refs.add((batch_id, task_id))

# status --json intentionally exposes only each task's latest generation.
# Query the public task timeline for every visible task as well, otherwise an
# older running version could be hidden behind a newer terminal version and
# cleanup could erase the only state needed to stop that process safely.
for batch_id, task_id in sorted(task_refs):
    try:
        completed = subprocess.run(
            [
                python,
                "-m",
                "gsched.cli",
                "task",
                f"{batch_id}:{task_id}",
                "--json",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        reject(f"task --json failed or timed out: {error}")
    if completed.returncode != 0:
        reject(
            f"task --json returned {completed.returncode} for "
            f"{batch_id}:{task_id}"
        )
    try:
        task_payload = json.loads(completed.stdout)
    except (TypeError, json.JSONDecodeError, RecursionError) as error:
        reject(f"task --json was not canonical JSON: {error}")
    versions = task_payload.get("jobs") if isinstance(task_payload, dict) else None
    if (
        not isinstance(task_payload, dict)
        or task_payload.get("schema_version") != 1
        or task_payload.get("batch_id") != batch_id
        or task_payload.get("task") != task_id
        or not isinstance(versions, list)
        or not versions
    ):
        reject(f"task --json envelope was invalid for {batch_id}:{task_id}")
    for version in versions:
        if (
            not isinstance(version, dict)
            or not isinstance(version.get("status"), str)
            or not version["status"]
        ):
            reject(f"task --json contained a noncanonical job for {batch_id}:{task_id}")
        if version["status"] == "running":
            reject(f"task --json still reported a running version for {batch_id}:{task_id}")

directory_flags = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)


def open_verified_directory(parent_fd, name, observed):
    fd = os.open(name, directory_flags, dir_fd=parent_fd)
    opened = os.fstat(fd)
    if (
        not stat.S_ISDIR(opened.st_mode)
        or (opened.st_dev, opened.st_ino) != (observed.st_dev, observed.st_ino)
    ):
        os.close(fd)
        raise RuntimeError(f"directory identity changed: {name!r}")
    return fd


def inspect_launch_directory(launch_fd, display_path):
    with os.scandir(launch_fd) as launch_entries:
        launch_names = [entry.name for entry in launch_entries]
    for launch_name in launch_names:
        marker = os.stat(
            launch_name,
            dir_fd=launch_fd,
            follow_symlinks=False,
        )
        marker_path = f"{display_path}/{launch_name}"
        if launch_name.endswith(".launch"):
            raise RuntimeError(f"unresolved launch marker: {marker_path}")
        if stat.S_ISLNK(marker.st_mode):
            raise RuntimeError(f"symlink in launch directory: {marker_path}")
        if not stat.S_ISREG(marker.st_mode):
            raise RuntimeError(f"unexpected launch entry: {marker_path}")


def inspect_for_launch(parent_fd, display_path):
    with os.scandir(parent_fd) as entries:
        names = [entry.name for entry in entries]
    for name in names:
        observed = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        entry_path = f"{display_path}/{name}" if display_path else name
        if stat.S_ISLNK(observed.st_mode):
            raise RuntimeError(
                f"symlink prevents safe launch enumeration: {entry_path}"
            )
        if not stat.S_ISDIR(observed.st_mode):
            continue
        child_fd = open_verified_directory(parent_fd, name, observed)
        try:
            if name == "launch":
                inspect_launch_directory(child_fd, entry_path)
            else:
                inspect_for_launch(child_fd, entry_path)
        finally:
            os.close(child_fd)


root_fd = None
try:
    root_observed = os.lstat(root)
    if (
        not stat.S_ISDIR(root_observed.st_mode)
        or stat.S_ISLNK(root_observed.st_mode)
    ):
        raise RuntimeError("cleanup root is not a physical directory")
    root_fd = os.open(root, directory_flags)
    root_opened = os.fstat(root_fd)
    if (root_opened.st_dev, root_opened.st_ino) != (
        root_observed.st_dev,
        root_observed.st_ino,
    ):
        raise RuntimeError("cleanup root changed while opening")
    inspect_for_launch(root_fd, "")
except (OSError, RecursionError, RuntimeError) as error:
    reject(f"launch enumeration was unsafe: {error}")
finally:
    if root_fd is not None:
        os.close(root_fd)
PY
}

sched_accept_remove_claimed_root() {
  local root=$1
  local token=$2
  local expected_dev=$3
  local expected_ino=$4
  local anchor=$5
  local expected_marker_dev=$6
  local expected_marker_ino=$7
  "$SCHED_ACCEPT_SYSTEM_PYTHON" - "$root" "$SCHED_ACCEPT_OWNER_MARKER" \
    "$token" "$expected_dev" "$expected_ino" "$anchor" \
    "$expected_marker_dev" "$expected_marker_ino" <<'PY'
import os
import secrets
import stat
import sys

(
    root,
    marker,
    token,
    expected_dev,
    expected_ino,
    anchor,
    expected_marker_dev,
    expected_marker_ino,
) = sys.argv[1:]
expected_root_identity = (int(expected_dev), int(expected_ino))
expected_marker_identity = (int(expected_marker_dev), int(expected_marker_ino))
payload = (token + "\n").encode()
parent = os.path.dirname(root)
name = os.path.basename(root)
anchor_parent = os.path.dirname(anchor)
anchor_name = os.path.basename(anchor)
current_uid = os.getuid()
quarantine_name = None
renamed = False
root_removed = False
anchor_removed = False
outcome_unknown = None
parent_fd = None
root_fd = None
marker_fd = None
anchor_fd = None

directory_flags = (
    os.O_RDONLY
    | os.O_CLOEXEC
    | os.O_NOFOLLOW
    | os.O_DIRECTORY
)
parent_flags = (
    os.O_RDONLY
    | os.O_CLOEXEC
    | os.O_DIRECTORY
)
file_flags = (
    os.O_RDONLY
    | os.O_CLOEXEC
    | os.O_NOFOLLOW
    | os.O_NONBLOCK
)


def identity(observed):
    return (observed.st_dev, observed.st_ino)


def read_from_start(fd):
    os.lseek(fd, 0, os.SEEK_SET)
    return os.read(fd, 4097)


def validate_root_stat(observed, label):
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != current_uid
        or stat.S_IMODE(observed.st_mode) != 0o700
        or identity(observed) != expected_root_identity
    ):
        raise RuntimeError(f"{label} identity changed")


def validate_private_stat(observed, label):
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != current_uid
        or stat.S_IMODE(observed.st_mode) != 0o600
        or identity(observed) != expected_marker_identity
    ):
        raise RuntimeError(f"{label} identity changed")


def validate_private_fd(fd, label):
    observed = os.fstat(fd)
    validate_private_stat(observed, label)
    if read_from_start(fd) != payload:
        raise RuntimeError(f"{label} token changed")
    return observed


def open_private_at(dir_fd, entry_name, label):
    observed = os.stat(entry_name, dir_fd=dir_fd, follow_symlinks=False)
    validate_private_stat(observed, label)
    opened_fd = os.open(entry_name, file_flags, dir_fd=dir_fd)
    try:
        opened = validate_private_fd(opened_fd, label)
        if identity(opened) != identity(observed):
            raise RuntimeError(f"{label} changed while opening it")
    except BaseException:
        os.close(opened_fd)
        raise
    return opened_fd


def validate_anchor_path():
    observed = os.stat(
        anchor_name,
        dir_fd=parent_fd,
        follow_symlinks=False,
    )
    validate_private_stat(observed, "ownership anchor")
    opened = validate_private_fd(anchor_fd, "opened ownership anchor")
    if identity(observed) != identity(opened):
        raise RuntimeError("ownership anchor path changed")


def validate_root_path(entry_name):
    observed = os.stat(entry_name, dir_fd=parent_fd, follow_symlinks=False)
    validate_root_stat(observed, "cleanup root")
    opened = os.fstat(root_fd)
    validate_root_stat(opened, "opened cleanup root")
    if identity(observed) != identity(opened):
        raise RuntimeError("cleanup root path changed")


def validate_marker_path():
    observed = os.stat(marker, dir_fd=root_fd, follow_symlinks=False)
    validate_private_stat(observed, "ownership marker")
    opened = validate_private_fd(marker_fd, "opened ownership marker")
    anchored = validate_private_fd(anchor_fd, "opened ownership anchor")
    if identity(observed) != identity(opened):
        raise RuntimeError("ownership marker path changed")
    if identity(opened) != identity(anchored):
        raise RuntimeError("ownership marker detached from anchor")


def open_root_claim(entry_name):
    observed = os.stat(entry_name, dir_fd=parent_fd, follow_symlinks=False)
    validate_root_stat(observed, "cleanup root")
    opened_root_fd = os.open(entry_name, directory_flags, dir_fd=parent_fd)
    opened_marker_fd = None
    try:
        opened = os.fstat(opened_root_fd)
        validate_root_stat(opened, "opened cleanup root")
        if identity(opened) != identity(observed):
            raise RuntimeError("cleanup root changed while opening it")
        opened_marker_fd = open_private_at(
            opened_root_fd,
            marker,
            "ownership marker",
        )
    except BaseException:
        if opened_marker_fd is not None:
            os.close(opened_marker_fd)
        os.close(opened_root_fd)
        raise
    return opened_root_fd, opened_marker_fd


def entry_points_to_open_root(entry_name):
    candidate_fd = None
    try:
        observed = os.stat(
            entry_name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return False
    except OSError:
        return None
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != current_uid
        or stat.S_IMODE(observed.st_mode) != 0o700
        or identity(observed) != expected_root_identity
    ):
        return False
    try:
        candidate_fd = os.open(
            entry_name,
            directory_flags,
            dir_fd=parent_fd,
        )
        opened = os.fstat(candidate_fd)
        pinned = os.fstat(root_fd)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or identity(opened) != identity(observed)
            or identity(opened) != identity(pinned)
        ):
            return None
        return True
    except OSError:
        return None
    finally:
        if candidate_fd is not None:
            os.close(candidate_fd)


def validate_quarantine_entry():
    validate_root_path(quarantine_name)
    validate_marker_path()
    validate_anchor_path()
    reopened_root_fd = os.open(
        quarantine_name,
        directory_flags,
        dir_fd=parent_fd,
    )
    reopened_marker_fd = None
    try:
        reopened_root = os.fstat(reopened_root_fd)
        validate_root_stat(reopened_root, "reopened quarantine")
        if identity(reopened_root) != identity(os.fstat(root_fd)):
            raise RuntimeError("quarantine does not refer to opened root")
        reopened_marker_fd = open_private_at(
            reopened_root_fd,
            marker,
            "reopened ownership marker",
        )
        reopened_marker = os.fstat(reopened_marker_fd)
        if identity(reopened_marker) != identity(os.fstat(marker_fd)):
            raise RuntimeError("quarantine marker changed after rename")
        if identity(reopened_marker) != identity(os.fstat(anchor_fd)):
            raise RuntimeError("quarantine marker detached from anchor")
    finally:
        if reopened_marker_fd is not None:
            os.close(reopened_marker_fd)
        os.close(reopened_root_fd)


def clear_directory(dir_fd, tree_dev, preserve_name=None):
    with os.scandir(dir_fd) as entries:
        names = [entry.name for entry in entries]
    for child_name in names:
        if child_name == preserve_name:
            continue
        child_stat = os.stat(child_name, dir_fd=dir_fd, follow_symlinks=False)
        if stat.S_ISDIR(child_stat.st_mode) and not stat.S_ISLNK(child_stat.st_mode):
            if child_stat.st_dev != tree_dev:
                raise RuntimeError(f"refusing to cross filesystem: {child_name!r}")
            child_fd = os.open(child_name, directory_flags, dir_fd=dir_fd)
            try:
                opened_stat = os.fstat(child_fd)
                child_identity = identity(child_stat)
                if (
                    not stat.S_ISDIR(opened_stat.st_mode)
                    or identity(opened_stat) != child_identity
                ):
                    raise RuntimeError(f"child changed while opening: {child_name!r}")
                clear_directory(child_fd, tree_dev)
                current = os.stat(child_name, dir_fd=dir_fd, follow_symlinks=False)
                if identity(current) != child_identity:
                    raise RuntimeError(f"child identity changed: {child_name!r}")
            finally:
                os.close(child_fd)
            os.rmdir(child_name, dir_fd=dir_fd)
        else:
            current = os.stat(child_name, dir_fd=dir_fd, follow_symlinks=False)
            if identity(current) != identity(child_stat):
                raise RuntimeError(f"child identity changed: {child_name!r}")
            os.unlink(child_name, dir_fd=dir_fd)


try:
    if (
        not os.path.isabs(root)
        or not os.path.isabs(anchor)
        or not name
        or not anchor_name.startswith(".sched-accept-claim-")
        or parent != anchor_parent
        or name == anchor_name
    ):
        raise RuntimeError("cleanup claim paths are invalid")
    parent_fd = os.open(parent, parent_flags)
    anchor_fd = open_private_at(parent_fd, anchor_name, "ownership anchor")
    root_fd, marker_fd = open_root_claim(name)
    validate_root_path(name)
    validate_marker_path()
    validate_anchor_path()

    for _ in range(100):
        candidate = f".{name}.sched-accept-quarantine-{secrets.token_hex(16)}"
        try:
            os.stat(candidate, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            quarantine_name = candidate
            break
    if quarantine_name is None:
        raise RuntimeError("could not allocate a quarantine name")

    try:
        os.rename(
            name,
            quarantine_name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
    except OSError:
        quarantine_state = entry_points_to_open_root(quarantine_name)
        original_state = entry_points_to_open_root(name)
        if quarantine_state is True:
            renamed = True
        elif original_state is True:
            raise
        else:
            outcome_unknown = "rename"
            raise
    else:
        renamed = True
    validate_quarantine_entry()

    clear_directory(root_fd, expected_root_identity[0], preserve_name=marker)
    with os.scandir(root_fd) as entries:
        remaining = [entry.name for entry in entries]
    if remaining != [marker]:
        raise RuntimeError("quarantine changed while clearing it")
    validate_quarantine_entry()

    opened_marker = validate_private_fd(marker_fd, "final ownership marker")
    opened_anchor = validate_private_fd(anchor_fd, "final ownership anchor")
    if identity(opened_marker) != identity(opened_anchor):
        raise RuntimeError("final ownership marker detached from anchor")
    os.close(marker_fd)
    marker_fd = None
    marker_path = os.stat(marker, dir_fd=root_fd, follow_symlinks=False)
    validate_private_stat(marker_path, "final ownership marker path")
    if identity(marker_path) != identity(opened_anchor):
        raise RuntimeError("ownership marker changed before unlink")
    try:
        os.unlink(marker, dir_fd=root_fd)
    except OSError:
        outcome_unknown = "marker unlink"
        raise
    try:
        os.stat(marker, dir_fd=root_fd, follow_symlinks=False)
    except FileNotFoundError:
        pass
    else:
        raise RuntimeError("ownership marker remained after unlink")
    validate_anchor_path()

    with os.scandir(root_fd) as entries:
        if any(True for _ in entries):
            raise RuntimeError("quarantine was not empty after marker removal")
    validate_root_path(quarantine_name)
    try:
        os.rmdir(quarantine_name, dir_fd=parent_fd)
    except OSError:
        outcome_unknown = "quarantine rmdir"
        raise
    root_removed = True

    validate_anchor_path()
    os.close(anchor_fd)
    anchor_fd = None
    release_fd = open_private_at(
        parent_fd,
        anchor_name,
        "ownership anchor before release",
    )
    os.close(release_fd)
    release_observed = os.stat(
        anchor_name,
        dir_fd=parent_fd,
        follow_symlinks=False,
    )
    validate_private_stat(release_observed, "ownership anchor before unlink")
    try:
        os.unlink(anchor_name, dir_fd=parent_fd)
    except OSError:
        outcome_unknown = "anchor unlink"
        raise
    anchor_removed = True
    try:
        os.stat(anchor_name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        pass
    else:
        raise RuntimeError("ownership anchor remained after unlink")
except BaseException as error:
    if outcome_unknown == "rename":
        retained = (
            f"{root} or {os.path.join(parent, quarantine_name)}, "
            f"and claim anchor {anchor} (rename outcome unknown)"
        )
    elif outcome_unknown is not None:
        if root_removed:
            retained = f"claim anchor {anchor} (root already removed)"
        elif renamed:
            retained = f"{os.path.join(parent, quarantine_name)} and claim anchor {anchor}"
        else:
            retained = f"{root} and claim anchor {anchor}"
        retained = f"{retained} ({outcome_unknown} outcome unknown)"
    elif root_removed:
        retained = f"claim anchor {anchor} (root already removed)"
    elif renamed:
        retained = f"{os.path.join(parent, quarantine_name)} and claim anchor {anchor}"
    else:
        retained = f"{root} and claim anchor {anchor}"
    print(
        f"acceptance cleanup: warning: preserving {retained}: {error}",
        file=sys.stderr,
    )
    raise SystemExit(1)
finally:
    if anchor_fd is not None:
        os.close(anchor_fd)
    if marker_fd is not None:
        os.close(marker_fd)
    if root_fd is not None:
        os.close(root_fd)
    if parent_fd is not None:
        os.close(parent_fd)
PY
}

sched_accept_cleanup() {
  [ "$SCHED_ACCEPT_CLEANUP_DONE" = "0" ] || return 0
  SCHED_ACCEPT_CLEANUP_DONE=1
  local index
  local root
  local token
  local root_dev
  local root_ino
  local anchor
  local marker_dev
  local marker_ino
  local -a cleanup_ready=()
  local cleanup_safe=1

  # A caller may be running inside one of its claimed project roots.  Move to
  # the stable repository before quarantine/removal so later CLI/Python calls
  # never inherit a deleted working directory.
  if ! cd -- "$SCHED_ACCEPT_REPO_ROOT"; then
    sched_accept_warn \
      "preserving all claimed roots: could not leave the current working directory"
    return 0
  fi

  for ((index = 0; index < ${#SCHED_ACCEPT_CLEANUP_ROOTS[@]}; index++)); do
    root=${SCHED_ACCEPT_CLEANUP_ROOTS[$index]}
    token=${SCHED_ACCEPT_CLEANUP_TOKENS[$index]}
    root_dev=${SCHED_ACCEPT_CLEANUP_DEVS[$index]}
    root_ino=${SCHED_ACCEPT_CLEANUP_INOS[$index]}
    anchor=${SCHED_ACCEPT_CLEANUP_ANCHORS[$index]}
    marker_dev=${SCHED_ACCEPT_CLEANUP_MARKER_DEVS[$index]}
    marker_ino=${SCHED_ACCEPT_CLEANUP_MARKER_INOS[$index]}
    cleanup_ready[$index]=0
    if ! sched_accept_root_owned \
      "$root" "$token" "$root_dev" "$root_ino" \
      "$anchor" "$marker_dev" "$marker_ino"; then
      sched_accept_warn "preserving $root: identity changed before cleanup"
      cleanup_safe=0
      continue
    fi
    if { [ -e "$root/config.json" ] || [ -L "$root/config.json" ]; } \
      && ! sched_accept_runtime_absent "$root"; then
      if ! sched_accept_stop_daemon "$root"; then
        sched_accept_warn "preserving $root: daemon stop failed or timed out"
        cleanup_safe=0
        continue
      fi
      if ! sched_accept_verify_quiescent "$root"; then
        sched_accept_warn "preserving $root: post-stop safety check failed"
        cleanup_safe=0
        continue
      fi
    fi
    if ! sched_accept_root_owned \
      "$root" "$token" "$root_dev" "$root_ino" \
      "$anchor" "$marker_dev" "$marker_ino"; then
      sched_accept_warn "preserving $root: identity changed during cleanup checks"
      cleanup_safe=0
      continue
    fi
    cleanup_ready[$index]=1
  done

  if [ "$cleanup_safe" != "1" ]; then
    sched_accept_warn "preserving all claimed roots: cleanup safety was not proven"
    return 0
  fi

  for ((index = 0; index < ${#SCHED_ACCEPT_CLEANUP_ROOTS[@]}; index++)); do
    [ "${cleanup_ready[$index]}" = "1" ] || continue
    root=${SCHED_ACCEPT_CLEANUP_ROOTS[$index]}
    token=${SCHED_ACCEPT_CLEANUP_TOKENS[$index]}
    root_dev=${SCHED_ACCEPT_CLEANUP_DEVS[$index]}
    root_ino=${SCHED_ACCEPT_CLEANUP_INOS[$index]}
    anchor=${SCHED_ACCEPT_CLEANUP_ANCHORS[$index]}
    marker_dev=${SCHED_ACCEPT_CLEANUP_MARKER_DEVS[$index]}
    marker_ino=${SCHED_ACCEPT_CLEANUP_MARKER_INOS[$index]}
    sched_accept_remove_claimed_root \
      "$root" "$token" "$root_dev" "$root_ino" \
      "$anchor" "$marker_dev" "$marker_ino" || true
  done
}

trap sched_accept_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
