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
  "mode": "mix",                            // the only implemented batch mode
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
