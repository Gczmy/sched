# Changelog

## 0.6.0 (2026-10-09)

Published as [v0.6.0](https://github.com/Gczmy/sched/releases/tag/v0.6.0) from
`8f412cc2c2d4828d97dd370df61e0cb72a210c02`; complete main CI passed 14/14.
All seven original assets were downloaded publicly and compared byte for byte.
Production deployment and real Slurm lease-end acceptance remain separate.

- Add bounded passive closed-snapshot browsing and fenced retention pruning,
  preserving permanent close records and audit without reviving rollback authority.
- Complete existing-lease launch-ancestry and affinity acceptance with a fixed
  120-CPU accounting budget, private faults and original owner crash reconnection.
  Affinity still does not provide cpuset/BPF hard isolation.
- Refresh the original launch anchor before CPU claim selection; reopen an unlinked
  same-identity publication inode once without accepting foreign or insecure records.
- Keep writer schema 25, complete read range 1–25 and existing default query JSON.

## 0.5.0 (2026-10-09)

Published as [v0.5.0](https://github.com/Gczmy/sched/releases/tag/v0.5.0) from
`7471c1cfdff5b8290799fceecabf20c8d230bf6f`; complete CI and seven original assets
were verified independently. Production deployment is separate from publication.

- Add explicit GPU pool/hard-affinity validation, structured artifact diagnostics
  and `json_equals`, immutable initial validation and audited artifact-only
  revalidation/settlement without reconstructing missing original waits.
- Add opt-in independent-task failure policy, exact dependencies and task DAGs,
  bounded task facts, atomic pending-only cancellation, allocation event chains,
  admission explanations and opt-in storage admission.
- Record and continuously validate original daemon leases; explicit launch-ancestry
  compatibility keeps unknown paused without claiming Slurm membership or isolation.
  Add conservative CPU auto capacity while preserving zero as an unlimited budget.
- Provide opt-in CPU affinity and explicitly delegated CPU/device scope candidates,
  original NVIDIA/MIG bindings and passive root health. Positive cpuset/BPF/device
  isolation acceptance and production rollout remain incomplete; defaults stay off.
- Write schema 25 and read complete schemas 1–25 without query migration. Older
  writers cannot reopen this database; verified pre-upgrade recovery is required.

## 0.2.2 (2026-10-01)

Published as [v0.2.2](https://github.com/Gczmy/sched/releases/tag/v0.2.2) from
`8aa2559dc64e74acd2cec6bcb2f5481d1d9fbc1d`; the complete 14-job CI and seven
original release assets are recorded in the Release evidence. Production deployment
is separate from publication.

- Check each administrator execution backend's executable and project roots before
  deployment, with bounded streaming SHA-256, ELF checks and stable failure codes.
  Checks never reserve attempts, launch children or replace startup validation.
- Validate default wheels on Python 3.10–3.14, native wheels on Python 3.10/3.14
  and Ubuntu 22.04/24.04, and actual isolated syscall denials before admitting
  all four immutable CI candidate packets.
- Prepare release evidence and checksums from all four original successful-main
  artifacts. A manual workflow can resume matching draft uploads without replacing
  assets or publishing a Release; offline verification cannot upload a draft.

## 0.2.1

- Add passive cross-task execution browsing with project/batch/backend/phase/owner
  filters, bounded live keyset pagination and explicit completeness semantics.
- Add local execution capability JSON and structured daemon preflight checks;
  declared capabilities never imply verified availability, and host guards remain.
- Replace full owner-history scans and an unbounded acknowledgement cache with
  an indexed durable queue: at most eight historical acknowledgements per tick,
  a soft two-second start budget and ten-second retry backoff.
- Add bounded, cold `linux_fd_owner` preparation and terminal retention settings.
- Add recorded owner connection and acknowledgement health to `sched execution`.
  Queries remain passive; acknowledgement metadata never changes original wait facts.
- Write database schema 7 and read schemas 1–7 without query migration. Migrate
  existing bindings once without replaying attempts or deleting their history.
- Add a fixed-commit candidate builder with independent default/native installs,
  source and wheel hashes, ABI evidence and installation/rollback notes.
- Save immutable CI candidate artifacts for Python 3.10 and 3.14 after all prerequisite
  checks pass. Bind checks and builds to the same source commit, record public CI
  evidence, and verify downloaded packets against an independently selected commit.

## 0.2.0 (candidate)

- Add `sched --version` for installation and release verification without opening state.
- Add the opt-in `linux_fd_owner` backend. An independent original child owner
  survives daemon crashes, authenticates reconnects and reports its actual wait,
  rusage and process-group cleanup. Recovery never launches a replacement child.
- Drive accepted cancellation escalation and duration limits in the owner service.
  Unreachable owners retain task/resource guards; lost authority never implies success.
- Add immutable owner bindings in database schema 6. Read-only queries accept
  schemas 1–6 without migrating old state. Public execution diagnostics omit secrets.
- Keep ordinary subprocess, `linux_fd` FD4 v1, legacy history/replay guards and
  status/task/history schema 1 unchanged. The new backend requires explicit support
  for the `sched_execution_owner_identity/v1` FD4 wrapper.

0.2.0 remained an unpublished candidate. The published source and assets for 0.2.1
are recorded in [GitHub Release](https://github.com/Gczmy/sched/releases/tag/v0.2.1).
Installation and rollback constraints are documented in [execution-rollout.md](docs/execution-rollout.md).
