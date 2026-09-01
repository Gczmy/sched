# sched

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-%E2%89%A53.10-blue)

**English** | [中文](README.zh-CN.md)

**sched** (package `gsched`) is a node-level GPU/CPU batch scheduler for multi-GPU compute nodes. It replaces the "nohup everything inside a screen" workflow with a re-entrant, resumable, self-diagnosing batch execution layer — powered by a single daemon and driven entirely through a CLI.

Zero third-party dependencies: pure Python standard library (`>= 3.10`).

## Features

- **Batch semantics** — one `batch.json` submits N tasks; any task entering a failed terminal state blocks the batch for human review instead of silently swallowing errors.
- **Artifact fingerprinting & SKIP** — tasks declare artifact paths with validation rules (e.g. `min_bytes`); job-private stage checkpoints are reused only when their producer fingerprint and artifacts still match. A successful stage validates its artifacts before committing a checkpoint, and final job settlement validates task plus stage artifacts again. `force_rerun` bypasses both task and stage skips.
- **One job per GPU by default** — with declarative `gpu_share` + `vram_gib`/CPU quotas for co-location packing when you want higher utilization; recognizes externally occupied GPUs (`unmanaged`) and supports manual quarantine.
- **Multi-project mode (B11c)** — per-project GPU quotas, priorities, and hard GPU affinity isolation on a single daemon. Hard affinity means a project's jobs only land on its assigned GPUs — no borrowing, no OOM from neighbors. Submissions without a registered `project` are rejected outright.
- **Sweep matrices** — declare a batch-level `sweep.matrix`; its cartesian product expands the task templates, optionally capped by `max_parallel`.
- **Failure diagnostics** — `sched diag <batch-id-or-name>:<task>` prints status + full command + git rev + log tail in one step.
- **Notifications** — batch terminal events to file inbox / email / user scripts (webhook-ready).
- **Git fingerprints** — fingerprints include HEAD plus tracked staged/unstaged content, while excluding arbitrary untracked outputs; every task records `git_rev`, and retry/resubmit warns on code drift.

## Installation

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

Initialize config (interactive wizard writes `~/.sched/config.json`):

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

With `projects` configured, every batch JSON must contain `"project": "<name>"` (`sched run` uses `--project`); scheduling order is `(project priority desc, batch priority desc)` and each project never exceeds its quota.

## Quick Start

```bash
# 1. Preview (read-only; ready-artifact tasks are marked SKIP)
sched submit batch.json --dry-run

# 2. Submit
sched submit batch.json

# 3. Start the daemon (must run ON the compute node; queries work from login nodes too)
sched daemon start

# 4. Observe
sched status                # three views: batches / tasks / GPUs+CPUs
sched status --project nlp  # filter by project
sched log <batch-id-or-name>:<task> -f # exact batch ID first; otherwise newest matching name

# 5. Diagnose failures (start here when something breaks)
sched diag <batch-id-or-name>:<task>

# 6. Unlock / re-run
sched retry <batch-id-or-name> # unlock all failed-terminal tasks (same spec)
sched resubmit <batch-id-or-name>:<task> # queue a new version (same spec)
```

### One-off single command

```bash
sched run --project vision --gpus 1 -- python train.py --seed 42   # occupies 1 GPU
sched run --project vision --cpu-only -- python prep_data.py       # CPU-only task
```

## batch.json Format

```jsonc
{
  "name": "my_batch",                       // safe ASCII identifier, 1..128 chars
  "project": "vision",                      // required; must exist in config.projects
  "mode": "mix",                            // default; strict is admin-profile-only
  "priority": 5,                            // batch priority within its project (optional)
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

Legacy V1 `mode: "strict"` is a deployment-only direct-exec path. It is accepted only
when one cold `config.native_exec_profiles` entry exactly matches
`(mode, project, batch_name, task_id, submitted_argv)`. The task must contain
one `cmd`, use a normalized absolute executable, run from the configured
project root, use exactly CPU-only `resources: {"gpu":0,"cpus":1}`, have empty
batch/task `env`, omit `stages`, `sweep`, and `runtime`, and explicitly set
`git: false` and `max_retry: 0`; scheduler artifact skip/cleanup rules and log
probes are forbidden. Requiring `git: false` prevents a PATH-resolved Git
subprocess from running before the reviewed native verifier; code-byte binding
belongs to that verifier instead.
The profile reserves its batch name from every mix path, including `sched run`,
and consumes it permanently on the first durable strict insert, regardless of
terminal status. A fresh submit, retry, resubmit, or automatic restart replay
is rejected. An inbox redelivery with the same batch id is idempotent only when
the durable immutable batch/task/job binding is an exact match; a same-name
preclaim or persisted drift is rejected.

The scheduler persists both the profile digest and a canonical project-root
device/inode digest, includes both in the fingerprint, and re-attests them
before claiming the job. Referenced project roots and the profile registry are
cold; hot changes are rejected. Native launch starts from an empty inherited
environment, adds scheduler-owned identity/control keys and an empty
`CUDA_VISIBLE_DEVICES`, and invokes the argv directly without the Bash/RC
supervisor. Only an exit code from the exact `Popen` retained by the current
daemon can complete successfully; after daemon authority is lost, the consumed
job blocks instead of trusting an RC sidecar or replaying.

This path is deliberately only a launcher foundation. It does **not** make
pathname execution byte/FD exact or close the re-attestation-to-`Popen` TOCTOU
window. The external retained-FD verifier/monitor and seven-field attestation
are still required before formal execution. The current strict schema also
rejects all user env, so the dedicated seven-field poison-overwrite probe is not
yet runnable; its exact cold-profile exception belongs with that verifier.

The explicitly versioned `sched_native_exec_profile_v2` is only that frozen
batch compatibility validator. It exact-binds the public root/task keysets and
values, including raw `{PROJECT:...}` cwd, empty dependencies, `_protocol`,
non-empty batch env, empty task env, prefix runtime, integer duration, zero
retry, raw CPU resources, empty artifacts, absent public task `git`, and the
unchanged logical argv. A matching V2 batch is rejected by both local submit
and daemon inbox handling before dependency/fingerprint work, durable batch or
name consumption, running claim, or process creation. V2 currently persists
nothing, has no launch-time re-attestation, and does not authorize `Popen`;
those properties remain work for the retained/bootstrap launcher step.

The next interface is present only as a fail-closed plan foundation. A private
one-shot `NativeLaunchPlan` owns revalidated copies of a retained launcher FD,
fully sealed request memfd, connected AF_UNIX stream control FD, retained
project-root directory FD, and append-only log FD. The request copy has an
independent zero offset; the root must be `O_RDONLY` and not `O_PATH`; and the
single-link log FD must match a symlink-free relative path below the retained
root's `logs/` directory. Its actual entry contract is fixed to argv
`("m2b-exec-monitor[native-entry-v1]", "--native-entry-v1")`, an empty
environment, and child request/control/root descriptors 3/4/5; the logical
submitted argv is evidence inside the plan and is never appended to or used as
the actual argv. The request body remains opaque and non-authoritative. The
private generated/no-data `NativeStep5DNoDataLaunchOwner` adapter now creates
the AF_UNIX socketpair itself, supplies only the native-side endpoint to the
plan factory, immediately closes that source endpoint, and privately retains
the peer endpoint in the creating process without a raw-FD or transfer API. It
fixes the intended endpoint-construction shape but does not authenticate a
scheduler role or claim that exclusive ownership can be inferred by the native
peer. The plan names the SHA-256 of the complete sealed frame exclusively as
`request_frame_sha256` and separately names the SHA-256 of its opaque body as
`request_body_sha256`; the ambiguous `request_sha256` alias does not exist.
`Executor.launch_native(plan)` currently closes the plan and fails before
process creation because the Linux FD-exec backend is not yet connected. V2
submission remains blocked as described above. The adapter has no launch,
control-protocol, nonce, publication, or daemon route; atomic final
validate/map/FD-exec and the reviewed real direct-parent lifecycle remain hard
prerequisites for connecting that backend.

`gsched.native_step5d_alignment.foundation_alignment_projection()` exposes a
read-only, JSON-compatible
`digest_and_direct_parent_endpoint_foundation_only` declaration derived from
production launch constants, plan/owner slots, private factory signatures,
and authority flags. It explicitly records canonical request construction,
owner-lifetime nonce handling, the control protocol, and isolated runtime as
`unimplemented`, with `step5d_complete=false`. The declaration imports no
project protocol, creates no endpoint, and adds no launch, send, or daemon API;
the main-repository validator independently reconstructs and traces the
production foundation before accepting equality. This is an alignment aid,
not completion evidence.

## State Machine

```text
task:   pending → running → done / failed / blocked / cancelled / timed_out / interrupted
        pending → skip                                    (artifact fingerprint hit; equivalent to done)
batch:  active → done / blocked
        done / blocked → active                              (terminal-task resubmit)
GPU:    free ⇄ assigned ⇄ releasing;  external use → unmanaged;  needs attention → quarantined
```

## Automation Tips

The CLI is designed for reliable scripting:

- Parse output with `--json` (`status --json`, `history --json`, `submit --dry-run --json`). `status` has `schema_version: 1`; its limit defaults to 200 (clamped to 1..1000), selects bounded current/newest batches first, and includes latest-version jobs only for returned batch IDs with separate `truncated.batches`/`truncated.jobs`. Jobs use `batch_id` + `batch_name` and canonical `status` plus `wait_reason`. `history` has `schema_version: 1`, defaults to 50 entries (limit clamped to 1..200), and has its own `truncated` flag.
- Task-targeting commands require `<batch-id-or-name>:<task>`: an exact full batch ID wins; a name selects its newest instance. Bare task IDs are invalid.
- Destructive commands require `--yes` (`cancel`, `gpu-free`); exit code 1 without it means "unconfirmed", not "failed".
- On a host other than `config.node`, queries open no writable SQLite connection: they always copy a stable private DB plus any WAL and open that snapshot read-only; they never mark a still-live source DB immutable. Daemon `start`, `stop`, and `check` are compute-node-only unless `SCHED_ALLOW_FOREIGN_WRITE=1` is explicitly set; `daemon status` remains read-only.
- Don't edit `state.db` directly — it uses a WAL concurrency protocol; ad-hoc SQL is an anti-pattern.

## Tests

```bash
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
