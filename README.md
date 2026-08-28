# sched

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-%E2%89%A53.10-blue)

**English** | [中文](README.zh-CN.md)

**sched** (package `gsched`) is a node-level GPU/CPU batch scheduler for multi-GPU compute nodes. It replaces the "nohup everything inside a screen" workflow with a re-entrant, resumable, self-diagnosing batch execution layer — powered by a single daemon and driven entirely through a CLI.

Zero third-party dependencies: pure Python standard library (`>= 3.10`).

## Features

- **Batch semantics** — one `batch.json` submits N tasks; any task entering a failed terminal state blocks the batch for human review instead of silently swallowing errors.
- **Artifact fingerprinting & SKIP** — tasks declare artifact paths with validation rules (e.g. `min_bytes`); on resubmission, tasks whose artifacts are already valid are skipped automatically. Resume long pipelines without re-running completed work.
- **One job per GPU by default** — with declarative co-location packing via `vram`/CPU quotas when you want higher utilization; recognizes externally-occupied GPUs (`unmanaged`) and supports manual quarantine.
- **Multi-project mode (B11c)** — per-project GPU quotas, priorities, and hard GPU affinity isolation on a single daemon. Hard affinity means a project's jobs only land on its assigned GPUs — no borrowing, no OOM from neighbors. Submissions without a registered `project` are rejected outright.
- **Sweep matrices** — declare a `sweep` of parameter arrays in a task; the cartesian product is expanded into individual jobs, optionally capped by `max_parallel`.
- **Failure diagnostics** — `sched diag <batch>:<task>` prints status + full command + git rev + log tail in one shot.
- **Notifications** — batch terminal events to file inbox / email / user scripts (webhook-ready).
- **Git fingerprints** — every task records the `git_rev` at submission; retry/resubmit warns on code drift.

## Installation

```bash
# Option A: pip install (console script)
pip install .

# Option B: wrapper script + PYTHONPATH (production-friendly, no env pollution)
#   ~/bin/sched:
#!/bin/bash
export SCHED_STATE="${SCHED_STATE:-$HOME/.sched}"
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

With `projects` configured, every submission must carry `--project <name>`; scheduling order is `(project priority desc, batch priority desc)` and each project never exceeds its quota.

## Quick Start

```bash
# 1. Preview (read-only; ready-artifact tasks are marked SKIP)
sched submit batch.json --dry-run --project vision

# 2. Submit
sched submit batch.json --project vision

# 3. Start the daemon (must run ON the compute node; queries work from login nodes too)
sched daemon start

# 4. Observe
sched status                # three views: batches / tasks / GPUs+CPUs
sched status --project nlp  # filter by project
sched log <batch>:<task> -f # follow a task's log

# 5. Diagnose failures (start here when something breaks)
sched diag <batch>:<task>

# 6. Unlock / re-run
sched retry <batch>           # unlock all failed-terminal tasks (same spec)
sched resubmit <batch>:<task> # queue a new version (same spec)
```

### One-off single command

```bash
sched run --project vision --gpus 1 -- python train.py --seed 42   # occupies 1 GPU
sched run --project vision --cpu-only -- python prep_data.py       # CPU-only task
```

## batch.json Format

```jsonc
{
  "name": "my_batch",                       // batch name (a timestamp is appended automatically)
  "mode": "mix",                            // 目前唯一实现的批次模式
  "priority": 5,                            // batch priority within its project (optional)
  "cwd": "{ROOT}",                          // working dir; supports {ROOT}/{VENV:key} templates
  "env": {"NN_NO_CUDNN": "1"},              // batch-level env vars (optional)
  "depends_on": ["other_batch_name"],       // upstream dependency; waits until it's done (optional)
  "tasks": [
    {
      "id": "train_s42",
      "stages": [                           // ordered stages; first failure fails the task
        {
          "cmd": ["{VENV:dl}", "train.py", "--seed", "42"],
          "artifacts": {                    // stage artifact fingerprints (hit => SKIP)
            "ckpt": {"path": "{ROOT}/out/checkpoint.pt", "rule": "min_bytes", "min_bytes": 100000}
          }
        },
        {"cmd": ["{VENV:dl}", "infer.py"]}
      ],
      "sweep": {                            // optional: expand into multiple jobs
        "over": {"seed": [42, 43, 44]},
        "max_parallel": 2                   // cap concurrent expanded jobs
      },
      "duration_min": 50,                   // ETA (for timeout warnings)
      "max_retry": 1,                       // automatic retries (0 = fail is terminal)
      "resources": {"gpu": 1, "vram": 8.0}  // vram declaration feeds co-location packing
    }
  ]
}
```

Template variables: `{ROOT}` = project root; `{VENV:<key>}` = interpreter from config `venvs`.

## State Machine

```text
task:   pending → running → done / failed / blocked / cancelled / timed_out / interrupted
        pending → skip                                    (artifact fingerprint hit; equivalent to done)
batch:  active → done          (all terminal-successful)
        active → blocked       (any failed-terminal; awaits retry/resubmit)
GPU:    free ⇄ assigned ⇄ releasing;  external use → unmanaged;  needs attention → quarantined
```

## Automation Tips

The CLI is designed for reliable scripting:

- Parse output with `--json` (`status --json`, `submit --dry-run --json`).
- Destructive commands require `--yes` (`cancel`, `gpu-free`); exit code 1 without it means "unconfirmed", not "failed".
- Daemon lifecycle commands belong on the compute node; queries also work from login nodes (shared state directory).
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
