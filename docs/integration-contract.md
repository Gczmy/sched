# sched integration contract v1

This contract covers independent consumers of the public CLI. sched owns
execution and physical allocation; consumers own experiments, scientific
acceptance and tracking. No ledger, W&B or customer module enters the daemon.

## Versions and compatibility

`version --json` reports local code, database compatibility and named contracts
without opening state. Public JSON remains schema_version 1. New task fields
are additive; consumers must negotiate named contracts and tolerate unknown
fields. Legacy task replies remain readable through their existing validation.
Database schema 10 adds persistent identity and structured operation receipts.
Read-only identity/receipt queries never initialize or migrate a database.

## Identity and task ownership

`identity --json` returns instance_id, configured node and query_host.
An unavailable/legacy identity is explicit, never synthesized from a hostname,
PID, allocation or path. Writer initialization generates a random identity once.
Restart and recovery preserve it. A copied production recovery state is the same
instance and must never run as an independent writer. A separate installation
starts with new state; no CLI resets an existing identity or rewrites history.

`task --json` includes project and instance_id in the same SQLite snapshot as
batch_revision and the version timeline. Old schemas return null instance_id.
New request expectations bind instance/project inside the mutation transaction;
old requests retain byte-identical bindings when these options are absent.
Configuration and SSH compute-node checks remain required.

## Operation receipts

`request-status ID --json` is read-only. A done receipt reports the recorded code
and structured effect metadata where available. A started request is unknown,
never an invitation to repeat an external action. not_found is absence of a
record in the queried instance, not proof that no action occurred elsewhere.
Replies omit raw command, output, configuration and private paths. Output
compaction keeps the binding, code and structured result. Legacy receipts may
have null result; no effect is guessed from text or current task state.

## Idempotent submission

`submit FILE --request-id ID --json` binds canonical finite JSON content,
resolved project and optional --expect-instance/--expect-project. File names do
not define payload identity. Dry-run and nesting inside request are rejected.
The same exact binding retains one batch_id. A changed binding returns 64.
Gateway delivery writes only private, durable intent and inbox files; the
daemon publishes its structured receipt and batch in one local DB transaction.
Accepted and rejected receipts retain their original binding indefinitely.

The response separates phase delivered (persisted=false) from phase done.
request-status reads the authoritative DB receipt when present, otherwise the
original delivery ticket. An incomplete intent or uncertain delivery returns
75 and remains unknown. It is not automatically redelivered. A later atomic
daemon receipt can settle a lost gateway response. No batch name matching is
used. Regular submit without a request ID retains its existing behavior.

## Consumer compatibility

| Consumer | Default/legacy mode | Explicit integration mode |
| --- | --- | --- |
| lab-ledger | Existing ledger instance IDs and old request payloads remain unchanged | Optional cli_instance_id negotiates all four contracts; original submit ID, exact batch binding and reservations are persisted |
| dsh-node-sched | Existing writer request format is retained | mutationExpectedInstance binds writes, negotiates submit/identity/receipt support and submits without a nested request |

Consumers do not infer execution identity from batch names. Historical unresolved
submissions without an exact receipt require reconciliation. The optional
contracts add no runtime dependency between repositories. They do not enable
tracking upload or alter experiment acceptance.

## Release and deployment gates

Before release run the independent Linux suite, consumer contracts, installation
and ABI CI. Publish fixed artifacts, then deploy eligible machines using drain;
keep running tasks, queued work and old recovery identities intact.
Query caching/event streams and additional diagnostics require measured need,
and are separate from this required integration delivery work.
