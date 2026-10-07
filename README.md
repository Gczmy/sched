# sched

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-%E2%89%A53.10-blue)

**English** | [中文](README.zh-CN.md)

**sched** (package `gsched`) is a node-level GPU/CPU batch scheduler for multi-GPU compute nodes. It replaces the "nohup everything inside a screen" workflow with a re-entrant, resumable, self-diagnosing batch execution layer — powered by a single daemon and driven entirely through a CLI.

The default background daemon runs in an independent POSIX session (`start_new_session=True`); `sched daemon foreground [--supervise]` also supports foreground waiting and optional automatic restarts. sched does not invoke systemd, tmux, or screen to manage it and does not request a new cluster allocation. Newly started daemons, execution owners and tasks inherit their launching process's cgroup, cpuset, and device permissions, so enter the intended compute allocation before starting them. screen/tmux are operator access paths only. If that external allocation ends, a surviving daemon does not rebind itself to a later allocation; reconnecting to an existing persistent owner does not migrate its tasks' resource context either.

Zero third-party dependencies: pure Python standard library (`>= 3.10`).

## Features

- **Batch semantics** — one `batch.json` submits N tasks; any task entering a failed terminal state blocks the batch for human review instead of silently swallowing errors.
- **Artifact fingerprinting & SKIP** — tasks declare artifact paths with validation rules (e.g. `min_bytes`); job-private stage checkpoints are reused only when their producer fingerprint and artifacts still match. A successful stage validates its artifacts before committing a checkpoint, and final job settlement validates task plus stage artifacts again. `force_rerun` bypasses both task and stage skips.
- **One job per GPU by default** — with declarative `gpu_share` + `vram_gib`/CPU quotas for co-location packing when you want higher utilization; recognizes externally occupied GPUs (`unmanaged`), debounces PID-free utilization noise, and automatically quarantines repeated health-probe failures until `gpu-ok` recovery.
- **Multi-project mode (B11c)** — per-project GPU-job quotas, priorities, and GPU affinity on a single daemon. Hard affinity restricts where that project's jobs may run; it does not reserve those GPUs against other projects. Exclusive separation requires all competing projects to use non-overlapping hard affinities. Submissions without a registered `project` are rejected outright.
- **Sweep matrices** — declare a batch-level `sweep.matrix`; its cartesian product expands the task templates, optionally capped by `max_parallel`.
- **Failure diagnostics** — `sched diag <batch-id-or-name>:<task>` prints status + full command + git rev + log tail in one step.
- **Notifications** — batch terminal events to file inbox / email / user scripts; the webhook channel is reserved but not implemented.
- **Git fingerprints** — fingerprints include HEAD plus tracked staged/unstaged content, while excluding arbitrary untracked outputs; every task records `git_rev`, and retry/resubmit warns on code drift.

## Public integration

Independent clients can negotiate [CLI contracts](docs/integration-contract.md)
for persistent instance identity, same-snapshot task ownership and durable request
receipts. `submit --request-id ... --json` preserves one original batch across
gateway delivery and response loss. A delivered envelope is not database acceptance;
unknown results retain the original request.

## Installation

Published source and wheel packets are available in the
[GitHub Releases](https://github.com/Gczmy/sched/releases).
Verify `SHA256SUMS` before extraction, then select the native wheel by Python ABI and
recorded libc. See the [release notes](docs/releases/0.3.1.md) for the validated matrix;
publication does not imply a production deployment.

```bash
# Option A: pip install (console script)
pip install .

# Option B: wrapper script + PYTHONPATH (production-friendly, no env pollution)
#   ~/bin/sched:
#!/bin/bash
# export SCHED_STATE="/shared/sched"  # optional explicit override of config.state_dir
export PYTHONPATH="/path/to/sched-repo${PYTHONPATH:+:$PYTHONPATH}"
exec python3 -m gsched.cli "$@"
```

Initialize config (the interactive wizard defaults to `~/.sched/config.json`;
`--config`, `SCHED_CONFIG`, or `SCHED_STATE` can select another bootstrap path):

```bash
sched init
```

Minimal example config:

```json
{
  "schema_version": 1,
  "node": "compute-01",
  "user": "user",
  "default_project": "vision",
  "state_dir": "/home/user/.sched",
  "gpus": [0, 1, 2, 3],
  "venvs": {"dl": "/home/user/venvs/dl/bin/python"},
  "co_locate": true,
  "co_locate_safety": 0.7,
  "notify": {"on": ["batch_done", "batch_blocked"], "file": {"enabled": true}},
  "projects": {
    "vision": {
      "root": "/home/user/repos/vision", "git": true,
      "gpu_affinity": [0, 1], "gpu_quota": 2,
      "priority": 10, "gpu_affinity_hard": true
    },
    "nlp": {
      "root": "/home/user/repos/nlp", "git": true,
      "gpu_affinity": [2, 3], "gpu_quota": 2,
      "priority": 5, "gpu_affinity_hard": true
    }
  }
}
```

State-root precedence is `SCHED_STATE` > loaded `config.state_dir` > `~/.sched`.

Every batch JSON must contain `"project": "<name>"` registered in `config.projects`
(`sched run` uses `--project`). `projects.<name>.gpu_quota` is a non-negative
integer: omission or `0` means **unlimited**, not “no GPUs.” A positive value caps
concurrent running GPU jobs, not distinct physical cards; co-located jobs each consume
one quota unit, while CPU-only jobs consume none.

`projects.<name>.gpu_enabled` is a boolean, defaulting to `true`. Setting it to
`false` rejects new GPU submissions and manual GPU retry/resubmit operations,
holds queued GPU work, and leaves running jobs and CPU-only work unaffected.
Re-enabling resumes the existing queued versions. Inspect the effective policy
with `sched project list --json`; see [project GPU access](docs/project-gpu-access.md).

Project and batch priorities default to `0` and may be any integer. Ready jobs are
considered by project priority descending, then batch priority descending, then FIFO
insertion order. Priority is non-preemptive: a running lower-priority job is never
evicted, and an unlaunchable higher-priority candidate may be skipped so later jobs
can use otherwise idle capacity.

## Quick Start

```bash
# 1. Preview (read-only; ready-artifact tasks are marked SKIP)
sched submit batch.json --dry-run

# 2. Ensure the daemon is healthy (run these ON the compute node)
sched daemon check
sched daemon start

# 3. Submit (preferred from a login/gateway node; the daemon ingests its inbox)
sched submit batch.json
sched verify BATCH_ID   # after the next tick; retry if ingestion is still pending

# 4. Observe (queries work from login nodes)
sched status                # three views: batches / tasks / GPUs+CPUs
sched status --project nlp  # filter by project
sched log <batch-id-or-name>:<task> -f # exact batch ID first; otherwise newest matching name

# 5. Diagnose failures (start here when something breaks)
sched diag <batch-id-or-name>:<task>

# 6. Unlock / re-run
sched retry <batch-id-or-name> # unlock all failed-terminal tasks (same spec)
sched resubmit <batch-id-or-name>:<task> # queue a new version (same spec)
```

### One-off single command (compute node only)

```bash
sched run --project vision --gpus 1 -- python train.py --seed 42   # occupies 1 GPU
sched run --project vision --cpu-only -- python prep_data.py       # CPU-only task
```

`sched run` is a direct compute-node mutation and does not use the gateway submission
inbox. It currently supports exactly one GPU: omit `--gpus` or use `--gpus 1`; use
`--cpu-only` for a zero-GPU task. Other GPU counts and combining `--gpus` with
`--cpu-only` are rejected. Explicit CPU counts and durations must be positive integers.
Its batch priority is always the
default `0`. Unless overridden, it uses the first configured venv and `{ROOT}` (the
`default_project` root) as its working directory; `--project` does not change `{ROOT}`.
For another project root, pass `--cwd '{PROJECT:nlp}'` explicitly (replace `nlp`
with the selected project). Both `run --dry-run` and real submission validate project
membership. Commands preserve argv quoting and the configured venv PATH through a
non-login Bash; use explicit `bash -c` for shell pipelines. Unlike stateless `submit --dry-run`, the run
preview currently requires an existing readable state database.

## batch.json Format

```jsonc
{
  "name": "my_batch",                       // safe ASCII identifier, 1..128 chars
  "project": "vision",                      // required; must exist in config.projects
  "mode": "mix",                            // default; legacy strict is not admitted
  "priority": 5,                            // larger integer is considered first; default 0
  "cwd": "{PROJECT:vision}",                // supports {PROJECT:name}/{ROOT}/{VENV:key}
  "env": {"NN_NO_CUDNN": "1"},              // string-valued batch environment (optional)
  "depends_on": ["other_batch_name"],       // safe identifiers; waits until upstream is done
  "sweep": {                                // batch-level cartesian expansion
    "matrix": {"seed": [42, 43, 44]},
    "max_parallel": 2
  },
  "tasks": [
    {
      "id": "train_s{seed}",
      "runtime": {"venv_alias": "dl"},       // exactly one resolvable runtime channel
      "stages": [                           // ordered stages; first failure fails the task
        {
          "cmd": ["python", "train.py", "--seed", "{seed}"],
          "artifacts": {                    // min_bytes validates the artifact size
            "ckpt": {"path": "out/checkpoint-{seed}.pt", "min_bytes": 100000}
          }
        },
        {"cmd": ["python", "infer.py"]}
      ],
      "duration_min": 50,                   // positive, finite hard timeout in minutes
      "max_retry": 1,                       // non-negative; 0 = fail is terminal
      "resources": {
        "gpu": 1, "gpu_share": true, "vram_gib": 8.0
      }
    }
  ]
}
```

Template variables: `{ROOT}` = `default_project` root; `{PROJECT:<name>}` = explicit project root; `{VENV:<key>}` = interpreter from config `venvs`.

Batch/task/dependency identifiers must match `[A-Za-z0-9][A-Za-z0-9._-]*` (not `.` or `..`). A `runtime` selects exactly one of `venv_alias`, `conda_env`, or `prefix`, and must resolve at submission. Unsupported batch `gpus`, task/stage `retry_transform`, and stage-level `probes` are rejected; put GPU demand in `resources.gpu` and probes at task level.

The public execution interface registers a fixed executable, argument list,
environment and allowed input FDs through administrator-owned cold configuration.
The scheduler owns resources, launch and cancellation intents, child wait and cleanup;
the external program owns its application protocol and result meaning.
See [execution API](docs/execution-api.md) and [repository boundaries](docs/execution-boundary.md).

The default subprocess installation requires no compiler or other repository.
The optional Linux native backend is built entirely from this repository with
`SCHED_BUILD_NATIVE=1`. Project programs run as child processes; the daemon does
not import project adapters, dynamically load project Python modules or accept
extensions injected into the `gsched` namespace.

New `mode: "strict"` submissions and old native input fields are rejected.
Historical persisted records retain conservative compatibility guards; unresolved
launches are not replayed. See [legacy compatibility](docs/native-integration.md).
Customer research contracts and scientific acceptance belong to the customer repository.

## State Machine

```text
task:   pending → running → done / failed / blocked / cancelled / timed_out / interrupted
        pending → skip                                    (artifact fingerprint hit; equivalent to done)
batch:  queued → active → done / blocked
        blocked → active                                  (retry or resubmit)
        done → active                                     (resubmit, or clean when latest skip jobs requeue)
        blocked / queued → discarded                      (operator retirement)
GPU:    free → assigned → releasing → free; external use → unmanaged; repeated health failure → quarantined
```

Physical occupancy probing is fail-closed. A detected compute PID, or an indeterminate
compute/topology/utilization probe, moves a free GPU to `unmanaged` immediately. Only a positive
utilization sample with a complete, empty compute-process list is debounced: it must
persist for three consecutive daemon ticks before the state changes. During confirmation
the registry can still display `free`, but the GPU is excluded from every dispatch path;
any clean sample resets the streak. An `unmanaged` GPU automatically returns to `free`
after two consecutive clean samples, while `quarantined` requires `sched gpu-ok`.

## Automation Tips

The CLI is designed for reliable scripting:

- Parse output with `--json` (`status --json`, `history --json`, `submit --dry-run --json`). `status` has `schema_version: 1`; its limit defaults to 200 (clamped to 1..1000), selects bounded current/newest batches first, and includes latest-version jobs only for returned batch IDs with separate `truncated.batches`/`truncated.jobs`. Jobs use `batch_id` + `batch_name` and canonical `status` plus `wait_reason`. `history` has `schema_version: 1`, defaults to 50 entries (limit clamped to 1..200), and has its own `truncated` flag.
- Task-targeting commands require `<batch-id-or-name>:<task>`: an exact full batch ID wins; a name selects its newest instance. Bare task IDs are invalid.
- Destructive commands require `--yes` (`cancel`, `discard`, `clean`, `config set`, `gpu-free`); exit code 1 without it means "unconfirmed", not "failed".
- DB-backed query connections use a stable private DB/WAL snapshot rather than opening the live NFS source. On `config.node`, a preflight may first open the source as a writer only when initialization, migration, or repair is necessary; the actual query still uses `mode=ro` and `query_only` on the snapshot. On another host the CLI never initializes or migrates the source. Snapshot acquisition is bounded and fails closed if a stable, supported WAL snapshot cannot be obtained. File-only `markers`, `notify-inbox`, and `daemon status`, plus `config get`, do not open the database.
- On a host other than `config.node`, `sched submit` durably writes a `submit_inbox` payload for the daemon; “delivered” is not yet “queued,” so confirm it with `sched verify`. Other mutations—including `sched run`—are rejected by default. Daemon `start`, `stop`, and `check` are compute-node-only. `SCHED_ALLOW_FOREIGN_WRITE=1` is an explicit safety override, not the normal gateway workflow.
- Don't edit `state.db` directly — it uses a WAL concurrency protocol; ad-hoc SQL is an anti-pattern.

## Project GPU Access

Apply a patch such as `{"projects":{"my-project":{"gpu_enabled":false}}}` through
`sched config set -f <patch.json> --yes` on the configured compute node. This setting
supports hot updates; queued GPU jobs remain `pending` with
`wait_reason: "project_gpu_disabled"`. Update the paired `dsh-node-sched` plugin before
using this feature because older strict status validators reject the new wait reason.
Gateway delivery rejected after a policy change is diagnosed with
`sched verify <full-batch-id>`. See the [behavior and validation record](docs/project-gpu-access.md)
and [development backlog](docs/next-development.md).

## Tests

See [contributing and repository checks](CONTRIBUTING.md) for the public Linux CI,
Python regressions, privacy checks and optional pre-commit hook.

```bash
bash tests/run_project_gpu_enabled_accept.sh # project GPU access, hot updates, CPU-only
bash tests/run_probes_accept.sh          # probe semantics
bash tests/run_colocate_accept.sh        # co-location packing
bash tests/run_gpu_mem_accept.sh         # memory management
bash tests/run_cancel_forward_accept.sh  # cancel forwards kill signals
bash tests/run_daemon_heartbeat_accept.sh
bash tests/run_notify_accept.sh          # notification channels
bash tests/run_ux_accept.sh              # CLI UX
# ... see tests/ for the full suite
```

## License

MIT — see [LICENSE](LICENSE).
