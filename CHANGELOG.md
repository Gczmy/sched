# Changelog

## 0.2.2 (unreleased)

- Check each administrator execution backend's executable and project roots before
  deployment, with bounded streaming SHA-256, ELF checks and stable failure codes.
  Checks never reserve attempts, launch children or replace startup validation.
- Validate default wheels on Python 3.10–3.14, native wheels on Python 3.10/3.14
  and Ubuntu 22.04/24.04, and actual isolated syscall denials before admitting
  all four immutable CI candidate packets.

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
