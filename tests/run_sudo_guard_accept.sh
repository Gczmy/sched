#!/bin/bash
# L13: privilege command checks must cover absolute paths and shell -c nesting.
set -u
cd "$(dirname "$0")/.."
export SCHED_STATE="$(mktemp -d)"
NODE="$(uname -n)"
cat > "$SCHED_STATE/config.json" <<EOF
{
  "schema_version": 1, "user": "t", "node": "$NODE", "state_dir": "$SCHED_STATE",
  "default_project": "p", "gpus": [], "venvs": {"k": "/bin"},
  "projects": {"p": {"root": "/tmp", "git": false}}
}
EOF
python3 - <<'PY'
from gsched.schema import SchemaError, parse_shell_cmd, validate_batch
from gsched.templates import expand_cmd
cfg = {"default_project": "p", "venvs": {"k": "/bin"}, "projects": {"p": {"root": "/tmp", "git": False}}}
base = {"name": "sudo", "project": "p", "tasks": [{"id": "t1", "cmd": ["echo", "ok"]}]}

def rejected(cmd):
    spec = {**base, "tasks": [{"id": "t1", "cmd": cmd}]}
    try:
        validate_batch(spec, cfg)
    except SchemaError:
        return
    raise AssertionError(f"privileged command accepted: {cmd}")

def accepted(cmd):
    spec = {**base, "tasks": [{"id": "t1", "cmd": cmd}]}
    validate_batch(spec, cfg)

rejected(["/usr/bin/sudo", "id"])
rejected(["{ROOT}/sudo", "id"])
rejected(["bash", "-c", "sudo id"])
rejected(["bash", "--noprofile", "-c", "sudo id"])
rejected(["bash", "-ilc", "sudo id"])
rejected(["bash", "-csudo", "id"])
rejected(["bash", "+c", "sudo id"])
rejected(["bash", "-ci", "sudo id"])
rejected(["bash", "+ci", "sudo id"])
rejected(["bash", "-c", "sudo${IFS}id"])
rejected(["bash", "-csu", "id"])
rejected(["bash", "-crunuser", "id"])
rejected(["bash", "-Ocheckwinsize", "-c", "sudo id"])
rejected(["zsh", "-ocshnullglob", "-c", "sudo id"])
rejected(["fish", "-C", "sudo id"])
rejected(["fish", "-NC", "sudo id"])
rejected(["fish", "--init-command=sudo id"])
rejected(["fish", "--init-cmd=sudo id"])
rejected(["bash", "-c", "sudo; id"])
rejected(["bash", "-c", "echo $(sudo id)"])
rejected(["bash", "-c", "echo `sudo id`"])
rejected(["bash", "-c", "echo \"$(sudo id)\""])
rejected(["bash", "-c", "bash -c 'bash -c \"bash -c \\\"sudo id\\\"\"'"])
rejected(["bash", "-c", "--", "sudo id"])
rejected(["bash", "-lc", "--", "sudo id"])
rejected(["env", "bash", "-c", "sudo id"])
rejected(["command", "bash", "-c", "sudo id"])
rejected(["bash", "-c", "s$'udo' id"])
rejected(["bash", "-c", "${SUDO} id"])
rejected(["bash", "-c", "echo $(sudo id)"])
rejected(["env", "-S", "bash -c sudo id"])
rejected(["env", "--split-string=bash -c sudo id"])
rejected(["nohup", "--", "bash", "-c", "sudo id"])
rejected(["exec", "--", "bash", "-c", "sudo id"])
rejected(["command", "-p", "bash", "-c", "sudo id"])
rejected(["command", "--", "bash", "-c", "sudo id"])
rejected(["bash", "-c", "eval $x"])
rejected(["exec", "/bin/true"])
rejected(["find", ".", "-exec", "sh", "-c", "sudo id", ";"])
rejected(["xargs", "sh", "-c", "sudo id"])
rejected(["busybox", "sh", "-c", "sudo id"])
rejected(["bash", "-Oexpand_aliases", "-c", "alias x=sudo; x id"])
rejected(["bash", "-c", "builtin eval \"$x\""])
rejected(["nice", "-n", "10", "/usr/bin/sudo", "id"])
rejected(["timeout", "5", "/usr/bin/sudo", "id"])
rejected(["command", "-p", "/usr/bin/sudo", "id"])
rejected(["nohup", "--", "/usr/bin/sudo", "id"])
try:
    parse_shell_cmd("bash -lc 'runuser -u root id'", "sched run")
except SchemaError:
    pass
else:
    raise AssertionError("nested runuser accepted")

loop_cfg = {
    "default_project": "p",
    "venvs": {"k": "/bin"},
    "projects": {"p": {"root": "bash -c {ROOT}", "git": False}},
}

growth_cfg = {
    "default_project": "p",
    "venvs": {"k": "/bin"},
    "projects": {"p": {"root": "{ROOT}/bash -c {ROOT}/bash", "git": False}},
}
try:
    validate_batch(
        {**base, "tasks": [{"id": "growth", "cmd": ["{ROOT}/bash", "-c", "{ROOT}/bash"]}]},
        growth_cfg,
    )
except SchemaError:
    pass
else:
    raise AssertionError("growing template cycle accepted")

accepted(["grep", "/usr/bin/sudo"])
accepted(["echo", "foo;sudo"])
accepted(["echo", "bash", "-c", "sudo id"])
accepted(["bash", "-c", "echo", "--command", "sudo id"])
validate_batch(
    {**base, "tasks": [{"id": "loop", "cmd": ["bash", "-c", "{ROOT}"]}]},
    loop_cfg,
)

try:
    expand_cmd(
        ["{stage0_sudo}"],
        cfg,
        {0: {"sudo": {"path": "/usr/bin/sudo"}}},
        "/tmp",
    )
except SchemaError:
    pass
else:
    raise AssertionError("expanded stage sudo accepted")
print("L13 absolute and nested privilege commands are rejected")
PY
