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

### Candidate diagnostic extensions (source, not a published/deployed release)

The original four contracts and phase values remain unchanged. `version --json`
also advertises `sched-request-status-many-v1`, `sched-request-validation-v1`,
`sched-request-result-v1`, `sched-artifact-check-v1` and
`sched-artifact-rules-v2`. Negotiate these capabilities on the actual receiving
CLI/daemon deployment before using new commands/rules. These diagnostic extensions
alone use schema 10; the subsequent failure-isolation candidate uses schema 11.
The subsequent immutable artifact-validation candidate writes schema 12 and reads
complete schemas 1–12. Negotiate `sched-artifact-validations-v1`; see
[artifact validation](artifact-validation.md). `artifact-validations TASK --json`
returns bounded frozen summaries, not fresh file checks. `--validation-id ID`
reads one exact task/version record and verifies full payload/header digests.
Live ID keyset pages are not a complete snapshot. Queries never migrate old
schemas or reconstruct original waits; migration appends an empty table only.
Existing status/task/history field sets stay unchanged. These records grant no
settlement authority. The later artifact-only revalidation candidate writes
schema 13 (complete read range 1–13) and advertises `sched-artifact-revalidations-v1`.
`artifact-revalidate` requires task CAS/instance via request; default only appends
an immutable event, explicit --settle requires original wait/rules/file evidence.
Code 0 means the event committed, not artifact or job success. Inspect effect
revalidation_id, artifact_rules_passed, settled and reason. Same RID replays without
file reads; unknown remains 75. No worker launch, deletion or version creation.
`artifact-revalidations` is a separate passive, bounded event query. Existing
status/task/history fields remain unchanged. See [revalidation](artifact-revalidation.md).
The subsequent exact-dependency candidate writes schema 14 (complete reads 1–14)
and advertises `sched-batch-dependencies-v1`. `depends_on_exact` requires explicit
instance/batch/task/version selectors; acceptance freezes job/spec/fingerprint
bindings in the submission transaction. No name or new version silently replaces
the selected source. Existing names retain dynamic latest-name semantics and are
not rebound during migration. `batch-dependencies` is a passive bounded query;
recorded facts are not artifact/marker checks or dispatch permission. Existing
status/task/history fields remain unchanged: status.batches.depends_on contains
legacy names only, not the complete dependency set. Consumers must negotiate the
new query to see exact bindings. See [reference](reference.md).

The subsequent task DAG candidate writes schema 15 (complete reads 1–15) and adds
`sched-task-dependencies-v1`. Local task/version and external exact selectors are
frozen only on acceptance, never rebound by resubmit. `dependency-update` accepts
bounded inline JSON through task/instance CAS request, updates only a never-started
current pending version and appends an immutable linked event. Graph cycle checks,
event, revision and receipt are atomic; replay/unknown rules are unchanged.
Recorded blocking paths do not probe artifacts/markers or grant dispatch authority;
batch gates are queried separately. Existing status/task/history field sets stay
unchanged, including raw task status versus displayed status.wait_reason. Consumers
must negotiate the independent contract before claiming full task DAG visibility.

The subsequent bounded task-facts candidate advertises `sched-task-facts-v1`
without changing writer schema 15. `task-facts` selects 1–100 distinct task IDs
and explicit versions under one full batch ID from one private read snapshot.
It returns immutable-input bindings and all recorded generations, not permission
to cancel: `cancel_ready:null`, launch markers and external files are unchecked.
Limits are 64 KiB inline input, 1000 generations per task, 10000 total records and
4 MiB canonical evidence; exceeding a bound fails, never silently truncates.

`cancel-pending` requires one batch/instance CAS request with the complete frozen
binding list. Every member must still be latest pending and all generations must
prove no start; unknown intents, original execution/validation/recovery facts and
local startup files are checked before any update. One conflict rolls back the
whole group. The durable request result includes original cancelled_tasks,
task_binding_sha256, count and false running_tasks_touched/signals_sent/artifacts_deleted.
Same RID recovers that result; unknown remains 75. No signals, artifact deletion,
resource release or new versions occur. Ordinary cancel and strict status/task/history
contracts are unchanged. Client replacement plans/reservations/lineage remain outside
the daemon; this is not cross-system atomic pending-replace. See [reference](reference.md).

### Candidate allocation observations

The later source candidate advertises `sched-allocations-v1`, with independent
`allocations` summaries and exact-ID layered evidence. Schema 16 adds immutable
allocation/event tables and a nullable current job pointer; migration never
backfills historical allocation, wait or worker identities. Ordinary retries
using the same job/version receive independent random allocation IDs, also bound
in new artifact-validation completion keys. Existing keys are unchanged.

Resource intent, original supervisor/backend-child waits, monitor claims,
scheduler classification and artifact references remain separate. Persistent
owner service PIDs never stand in for workers. Actual SIGKILL Popen waits are
retained only in a bounded local pid/start-token cache; missing/restarted or
mismatched evidence remains unknown, never a synthesized successful exit.
The passive query uses a private DB/WAL snapshot, does not probe processes,
hardware, owners or current artifacts, and grants no settlement/execution authority.
Truncation, byte bounds, compatibility and CPU/fake-GPU acceptance are specified
in [allocation evidence](allocation-evidence.md). Strict status/task/history
fields and wait_reason remain unchanged; consumers must negotiate this contract.

### Candidate resource explanation

The source resource-explanation candidate advertises `sched-admission-explain-v1`
without changing schema 15 or strict status/task/history fields. Its independent
query shares budget and GPU-selection decisions with dispatch, returns simultaneous
resource rejections and per-card packing reasons, and never probes gateway hardware.
Current DB facts and separately recorded compute-node observations are not one
atomic snapshot. Observations expire after 90 seconds; fresh VRAM keeps its 5-second
limit. Identity/config/usage changes, missing evidence and stale timestamps are explicit
unknowns, not inferred capacity. Resource reservations are not per-job hard isolation.
resource_fit is null when relevant evidence is unknown; admission_granted remains false.
Final dependency/artifact/marker/owner/recovery/launch checks are not granted by this
diagnostic. Bounds and legacy-capacity compatibility are documented in [reference](reference.md).

### Candidate storage admission

The source candidate advertises `sched-storage-explain-v1`, with opt-in
storage_admission (disabled by default) and nonnegative disk_gib/disk_inodes task
reservations. Only compute dispatch probes output/control-plane filesystems and
locally readable current-UID quota; unsupported/remote/group/project quotas are
not inferred unlimited. Known insufficient quota always rejects; optional unknown
user quota is explicitly reported, and require_user_quota makes it a blocking unknown.
Independent storage-explain and admission-explain's nested storage use retained,
identity/config/spec/running-bound observations, expire after 30 seconds, and share
the actual pure decision. Queries never probe gateway filesystems or grant launch
authority. Strict status/task/history and wait_reason remain unchanged; no schema
migration or historical backfill is added. Optional allocation filesystem bindings
are reservations, not hard isolation or physical ownership. See [storage admission](storage-admission.md).

### Candidate CPU capacity

The schema 17 candidate advertises `sched-cpu-capacity-v1`. Explicit cpus_total="auto"
uses conservative original-lease/affinity CPU counts and optional cpus_auto_max;
zero remains unlimited reservations with CPU-only concurrency fallback. Unknown
original capacity blocks new auto dispatch; running allocation reservations stay
frozen despite default changes. Passive `cpu-capacity --json` and opt-in
`status --json --include-cpu-capacity` describe configuration, recorded capacity,
source and lease status, without gateway probes or admission authority. Default
optional status.cpu remains two integers; unknown auto omits it, never emits a
string/zero to imply unlimited capacity. The companion plugin accepts this absence
and the unchanged cpu wait_reason; it must negotiate before requesting the nested
extension. No further schema bump or per-job isolation is implied. See
[CPU capacity](cpu-capacity.md).

### Candidate batch failure policy

The schema 17 candidate also advertises `sched-daemon-lease-v1`: immutable
whitelisted daemon birth, recorded controller/kernel checks and exit facts.
Default health/status/task/history field sets are unchanged; only explicit
`daemon status --json --include-lease` adds the nested private-snapshot query.
`daemon-lease --json` never probes Slurm or grants execution authority. Auto mode
validates Slurm origins, pauses unknown by default and latches confirmed invalid
without cancelling running jobs or migrating to another lease. See
[daemon lease](daemon-lease.md) for bounds, policy, notification and schema rollback.

The source candidate additionally advertises `sched-batch-policy-v1`. Its writer
introduced schema 11; the later artifact-validation candidate writes 12 (read
range 1–12); the subsequent revalidation candidate writes 13 (reads 1–13).
The later exact-dependency candidate writes 14 (reads 1–14); the subsequent task
DAG candidate writes 15 (reads 1–15). The later allocation candidate writes 16
(complete reads 1–16); the subsequent daemon-lease candidate writes 17 (complete
reads 1–17); earlier writers cannot reopen that state.
Released 0.4.0 cannot open these new writer states.
Migration adds `batches.failure_policy` with default `freeze` and a monotonic
revision trigger, without rewriting status, job versions, execution identity,
unknown attempts, receipts or instance identity. Migration is atomic.

New submissions may opt into `failure_policy:continue_independent`. Default
freeze semantics remain unchanged. Independent dispatch continues while latest
unfinished tasks, old running generations or unresolved launch/attempt facts
remain; final partial failure still settles blocked. This policy alone creates no
task DAG; the subsequent schema 15 contract above adds explicit frozen task edges.
Old blocked batches are never automatically reopened.

`batch-policy BATCH --json` returns a private read-only snapshot with
schema_version, query=batch_policy, contract, instance_id, batch_id, project,
batch_revision, status, failure_policy, source, effect and task_dag_supported
(true only if the task dependency event table is available in this snapshot).
Old schemas return freeze/source=legacy_default without migration. Current
status/task/history schemas and strict field sets remain unchanged.

Policy updates require `request` with batch kind, full ID, current status and
revision, then `batch-policy ID --failure-policy POLICY --yes`. Instance/project
expectations remain available. The policy and receipt commit in one transaction;
policy changes increment revision, same-value updates do not. The receipt effect
for this command also reports failure_policy. CAS 65 and same-RID replay retain
the original result. Direct writes are rejected with 64 before state access.
Changing policy alone never reopens blocked. Explicit `--reopen` requires
continue_independent, blocked and a latest pending/waiting/running task; it does
not retry failed tasks, create versions, cancel running or infer unknown waits.
Done/discarded and historical strict policy updates are rejected; queued updates do not unlock dependencies.

Receipt queries add `receipt_source` (database/ticket/null),
`receipt_persisted`, nullable `delivery_confirmed` and `batch_persisted`,
`created_at`, `finished_at`, `observed_at` and `reason_code`.
A persisted intent ticket is **not** a database acceptance receipt.
`delivery_confirmed:null` means unknown, not confirmed non-delivery. `done`
means the request settled; rejected submissions can also be done with nonzero
code and `batch_persisted:false`. It does not mean training completed.

`request-status ID --wait-sec N --expect-instance INSTANCE --json` waits up to
0..60 finite seconds, querying fresh private snapshots and closing each before
sleeping. It never submits, changes RID, or writes a receipt. Query exit code 0
means the query succeeded, not that the receipt code is 0. At the deadline,
`wait_timed_out:true` preserves the last phase; it is not a rejection. If a
previously observed receipt disappears, prior evidence is retained with
`observation_incomplete:true`, `query_observed_at` and an explicit reason;
this is not a fresh complete result. Instance/binding changes fail the query.
Read failure returns nonzero with `query:request_status_error`, never not_found.

`request-status-many ID... --json` supports 1..100 distinct IDs in input order,
using one DB snapshot per poll. Its named contract returns `requests` and
`ticket_fallback_atomic:false`: separate delivery tickets are not a single
atomic filesystem snapshot. It supports the same wait and expected-instance
options; waiting ends when all requested receipts settle or time expires.
Longer client waits should resume the same IDs, not redeliver them.

`request-validate` accepts the same complete command/preconditions as `request`.
It only validates envelope and exact nested CLI syntax; it reads no config/DB,
does not reserve the RID, and reports `state_checked:false`. File contents,
confirmation flags, current state, node guards and CAS still apply at execution.

`request ... --json -- MUTATION` returns an opt-in structured result. Without
the option, original stdout/stderr replay is retained; the option does not enter
the immutable binding. Missing envelope fields or invalid nested syntax return
64 before state initialization or request insertion. Errors identify
`reason_code`, `stage`, `missing_fields`, target kind and facts about **this
invocation**. They cannot negate an older effect of the same RID. CAS conflict
still returns 65, now retaining structured error metadata in the existing
result_json; prior started requests still return 75 without dispatch. Generic
transport, argument-parser and infrastructure errors must not be interpreted
as a confirmed negative receipt.

`artifact-check BATCH:TASK --version N --json` reads one frozen task spec and
the current declared files on the configured compute node. It never initializes
or migrates state, changes task status, deletes files, retries training, or
reconstructs the first failure. `passed` is only the current artifact predicate,
not execution authority or scientific acceptance. Job completion observations
are logged per rule in daemon.log; stage observations are retained in task logs.
Log observations are diagnostic, not immutable validation/settlement records.
Detailed errors distinguish no-match, regex timeout/child failure, invalid JSON,
missing keys, unequal typed JSON values and filesystem failures. File SHA-256
is available only when bounded content checks read bytes; existence/min-size
checks do not hash potentially large checkpoint files.

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
