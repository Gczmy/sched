# Changelog

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

This candidate is not a production deployment or a published release. Installation
and rollback constraints are documented in [execution-rollout.md](docs/execution-rollout.md).
