#!/bin/bash

# Acceptance scripts source this file after changing to the repository root.
# Cleanup claims are private tokens bound to the device/inode created here.
# Daemons are stopped through the CLI before any root is quarantined; cleanup
# never edits scheduler state or signals processes directly.
SCHED_ACCEPT_CLEANUP_DONE=0
SCHED_ACCEPT_CLEANUP_ROOTS=()
SCHED_ACCEPT_CLEANUP_TOKENS=()
SCHED_ACCEPT_CLEANUP_DEVS=()
SCHED_ACCEPT_CLEANUP_INOS=()
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
  "$SCHED_ACCEPT_SYSTEM_PYTHON" - "$root" "$SCHED_ACCEPT_OWNER_MARKER" \
    "$token" "$expected_dev" "$expected_ino" <<'PY'
import os
import stat
import sys

root, marker, token, expected_dev, expected_ino = sys.argv[1:]
root_fd = None
try:
    root_stat = os.lstat(root)
    if (
        not stat.S_ISDIR(root_stat.st_mode)
        or stat.S_ISLNK(root_stat.st_mode)
        or root_stat.st_dev != int(expected_dev)
        or root_stat.st_ino != int(expected_ino)
    ):
        raise ValueError("root identity changed")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    root_fd = os.open(root, flags | getattr(os, "O_DIRECTORY", 0))
    opened_root = os.fstat(root_fd)
    if (opened_root.st_dev, opened_root.st_ino) != (
        root_stat.st_dev,
        root_stat.st_ino,
    ):
        raise ValueError("root changed while opening it")
    marker_fd = os.open(marker, flags, dir_fd=root_fd)
    try:
        marker_stat = os.fstat(marker_fd)
        if not stat.S_ISREG(marker_stat.st_mode):
            raise ValueError("ownership marker is not a regular file")
        observed = os.read(marker_fd, 4097)
        if observed != (token + "\n").encode():
            raise ValueError("ownership token changed")
    finally:
        os.close(marker_fd)
except (OSError, ValueError):
    raise SystemExit(1)
finally:
    if root_fd is not None:
        os.close(root_fd)
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

  created_root=$(mktemp -d "${TMPDIR:-/tmp}/${label}.XXXXXX") || return 1
  claim=$(
    "$SCHED_ACCEPT_SYSTEM_PYTHON" - "$created_root" "$SCHED_ACCEPT_OWNER_MARKER" <<'PY'
import os
import secrets
import stat
import sys

root, marker = sys.argv[1:]
root_stat = os.lstat(root)
if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
    raise SystemExit("temporary root is not a directory")
token = secrets.token_hex(32)
flags = (
    os.O_WRONLY
    | os.O_CREAT
    | os.O_EXCL
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
marker_fd = os.open(os.path.join(root, marker), flags, 0o600)
try:
    os.write(marker_fd, (token + "\n").encode())
finally:
    os.close(marker_fd)
confirmed = os.lstat(root)
if (confirmed.st_dev, confirmed.st_ino) != (root_stat.st_dev, root_stat.st_ino):
    raise SystemExit("temporary root identity changed while claiming it")
print(f"{token}\t{root_stat.st_dev}\t{root_stat.st_ino}")
PY
  ) || {
    sched_accept_warn "could not create cleanup claim for $created_root; preserving it"
    return 1
  }
  IFS=$'\t' read -r token root_dev root_ino <<< "$claim"
  if [ -z "$token" ] || [ -z "$root_dev" ] || [ -z "$root_ino" ]; then
    sched_accept_warn "cleanup claim for $created_root was incomplete; preserving it"
    return 1
  fi

  SCHED_ACCEPT_CLEANUP_ROOTS+=("$created_root")
  SCHED_ACCEPT_CLEANUP_TOKENS+=("$token")
  SCHED_ACCEPT_CLEANUP_DEVS+=("$root_dev")
  SCHED_ACCEPT_CLEANUP_INOS+=("$root_ino")
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
  "$SCHED_ACCEPT_SYSTEM_PYTHON" - "$root" "$SCHED_ACCEPT_OWNER_MARKER" \
    "$token" "$expected_dev" "$expected_ino" <<'PY'
import os
import secrets
import stat
import sys

root, marker, token, expected_dev, expected_ino = sys.argv[1:]
expected_identity = (int(expected_dev), int(expected_ino))
parent = os.path.dirname(root)
name = os.path.basename(root)
quarantine_name = None
parent_fd = None
root_fd = None


def validate_root_at(dir_fd, entry_name):
    root_stat = os.stat(entry_name, dir_fd=dir_fd, follow_symlinks=False)
    if (
        not stat.S_ISDIR(root_stat.st_mode)
        or stat.S_ISLNK(root_stat.st_mode)
        or (root_stat.st_dev, root_stat.st_ino) != expected_identity
    ):
        raise RuntimeError("root identity changed")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    opened_root_fd = os.open(
        entry_name,
        flags | getattr(os, "O_DIRECTORY", 0),
        dir_fd=dir_fd,
    )
    opened_stat = os.fstat(opened_root_fd)
    if (opened_stat.st_dev, opened_stat.st_ino) != expected_identity:
        os.close(opened_root_fd)
        raise RuntimeError("root changed while opening it")
    try:
        marker_fd = os.open(marker, flags, dir_fd=opened_root_fd)
        try:
            marker_stat = os.fstat(marker_fd)
            if not stat.S_ISREG(marker_stat.st_mode):
                raise RuntimeError("ownership marker is not a regular file")
            observed = os.read(marker_fd, 4097)
            if observed != (token + "\n").encode():
                raise RuntimeError("ownership token changed")
        finally:
            os.close(marker_fd)
    except BaseException:
        os.close(opened_root_fd)
        raise
    return opened_root_fd


def clear_directory(dir_fd):
    with os.scandir(dir_fd) as entries:
        names = [entry.name for entry in entries]
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )
    for child_name in names:
        child_stat = os.stat(child_name, dir_fd=dir_fd, follow_symlinks=False)
        if stat.S_ISDIR(child_stat.st_mode) and not stat.S_ISLNK(child_stat.st_mode):
            child_fd = os.open(child_name, flags, dir_fd=dir_fd)
            try:
                opened_stat = os.fstat(child_fd)
                child_identity = (child_stat.st_dev, child_stat.st_ino)
                if (opened_stat.st_dev, opened_stat.st_ino) != child_identity:
                    raise RuntimeError(f"child changed while opening: {child_name!r}")
                clear_directory(child_fd)
                current = os.stat(child_name, dir_fd=dir_fd, follow_symlinks=False)
                if (current.st_dev, current.st_ino) != child_identity:
                    raise RuntimeError(f"child identity changed: {child_name!r}")
            finally:
                os.close(child_fd)
            os.rmdir(child_name, dir_fd=dir_fd)
        else:
            os.unlink(child_name, dir_fd=dir_fd)


try:
    if not os.path.isabs(root) or not name or parent == root:
        raise RuntimeError("root path is not a removable absolute path")
    parent_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
    parent_fd = os.open(parent, parent_flags)
    preflight_fd = validate_root_at(parent_fd, name)
    os.close(preflight_fd)

    for _ in range(100):
        candidate = f".{name}.sched-accept-quarantine-{secrets.token_hex(16)}"
        try:
            os.stat(candidate, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            quarantine_name = candidate
            break
    if quarantine_name is None:
        raise RuntimeError("could not allocate a quarantine name")

    os.rename(name, quarantine_name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    root_fd = validate_root_at(parent_fd, quarantine_name)
    clear_directory(root_fd)
    current = os.stat(quarantine_name, dir_fd=parent_fd, follow_symlinks=False)
    if (current.st_dev, current.st_ino) != expected_identity:
        raise RuntimeError("quarantine identity changed before removal")
    os.close(root_fd)
    root_fd = None
    os.rmdir(quarantine_name, dir_fd=parent_fd)
except BaseException as error:
    retained = root if quarantine_name is None else os.path.join(parent, quarantine_name)
    print(
        f"acceptance cleanup: warning: preserving {retained}: {error}",
        file=sys.stderr,
    )
    raise SystemExit(1)
finally:
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
  local -a cleanup_ready=()
  local cleanup_safe=1

  for ((index = 0; index < ${#SCHED_ACCEPT_CLEANUP_ROOTS[@]}; index++)); do
    root=${SCHED_ACCEPT_CLEANUP_ROOTS[$index]}
    token=${SCHED_ACCEPT_CLEANUP_TOKENS[$index]}
    root_dev=${SCHED_ACCEPT_CLEANUP_DEVS[$index]}
    root_ino=${SCHED_ACCEPT_CLEANUP_INOS[$index]}
    cleanup_ready[$index]=0
    if ! sched_accept_root_owned "$root" "$token" "$root_dev" "$root_ino"; then
      sched_accept_warn "preserving $root: identity changed before cleanup"
      cleanup_safe=0
      continue
    fi
    if [ -e "$root/config.json" ] || [ -L "$root/config.json" ]; then
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
    cleanup_ready[$index]=1
  done

  if [ "$cleanup_safe" != "1" ]; then
    sched_accept_warn "preserving all claimed roots: daemon quiescence was not proven"
    return 0
  fi

  for ((index = 0; index < ${#SCHED_ACCEPT_CLEANUP_ROOTS[@]}; index++)); do
    [ "${cleanup_ready[$index]}" = "1" ] || continue
    root=${SCHED_ACCEPT_CLEANUP_ROOTS[$index]}
    token=${SCHED_ACCEPT_CLEANUP_TOKENS[$index]}
    root_dev=${SCHED_ACCEPT_CLEANUP_DEVS[$index]}
    root_ino=${SCHED_ACCEPT_CLEANUP_INOS[$index]}
    sched_accept_remove_claimed_root "$root" "$token" "$root_dev" "$root_ino" || true
  done
}

trap sched_accept_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
