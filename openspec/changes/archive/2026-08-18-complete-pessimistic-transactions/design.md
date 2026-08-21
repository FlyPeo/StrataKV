## Context

See `proposal.md` for motivation and `specs/pessimistic-transactions/spec.md` for the behavior contract.

The current coordinator implements optimistic Percolator-style 2PC over Region-local Raft groups. A transaction is identified by its globally allocated `startTs`; ordinary reads use that fixed timestamp and only mutation keys participate in Prewrite conflict checks. `Transaction` keeps mutations and known pessimistic locks in memory, while durable MVCC locks live in Region RocksDB.

Several existing details shape the design:

- `TimestampOracle` exposes `Next/Peek/Observe` as `uint64_t`; the production TSO is a three-member Raft group backed by a segment-reserving persistent allocator.
- `NodeServer` owns all local `RegionPeer` instances and one shared `NodeTxnScheduler`. Each Region remains an independent Raft group and owns its MVCC/RocksDB state.
- The scheduler holds short-lived per-key latches only until the corresponding Raft command applies. These latches are not transaction locks.
- Current lock scanning and GC managers are instantiated by each coordinator process. Their `steady_clock` values are not portable across processes, and their lifetime is tied to SDK/Gateway processes.
- Gateway Fibers may wait for HTTP sockets, but synchronous RPC, TSO, 2PC, recovery, and GC work must execute on bounded pthread workers.
- Proto3 RPC messages can be extended with new tags. Durable MVCC lock values use appendable length-prefixed fields. New Raft Apply semantics require a command-version gate.

## Goals / Non-Goals

**Goals:**

- Add exact-key locking reads that turn an application-declared invariant into a durable write/write conflict.
- Keep ordinary reads on Snapshot Isolation and make the non-Serializable boundary explicit.
- Make every acknowledged lock, transaction-status decision, and resolution action durable through Region Raft Apply.
- Give TSO, lock expiry, transaction age, and GC one monotonic physical/logical time domain.
- Make partial acquisition, unknown commit outcomes, process crashes, and expired locks converge without guessing.
- Preserve old optimistic clients and legacy lock decoding while gating new traffic until the cluster supports the new protocol.

**Non-Goals:**

- Lock waiting, wait-for graphs, deadlock detection, primary-lock heartbeat, transparent transaction replay, range/gap/predicate locks, SSI, or Serializable isolation.
- Using `NodeLatchManager` as a transaction lock manager.
- Persisting Gateway session handles or coordinator callback state.
- Replacing synchronous MPRPC or changing Region/Raft ownership boundaries.

## Decisions

### 1. Encode HLC in the existing uint64 timestamp type

Use 18 low bits for the logical counter and the remaining high bits for Unix epoch milliseconds. `HlcPhysical(ts)`, `HlcLogical(ts)`, and `ComposeHlc(ms, logical)` live in the transaction timestamp module so TSO, transaction, recovery, and GC code use one implementation.

`Next()` computes `physical = max(system_clock_ms, physical(last))`; it increments the logical component when physical time did not advance and starts logical at zero when it did. Logical overflow advances the physical component by one millisecond. `Observe(x)` moves the local high water to at least `x` without allowing later allocation to regress. `Peek()` remains a non-allocating high-water read; components needing authoritative current physical time call `Next()`.

The persistent allocator changes from an unversioned decimal high water to a versioned HLC record. A decimal-only file is treated as legacy v1, and migration selects a new HLC high water greater than both the legacy value and current wall-clock HLC. The first v2 reservation is published with the existing temp/fsync/rename/directory-fsync sequence. Unknown versions fail startup. TSO Raft followers continue applying `Observe(committedTs)`, and a new leader cannot allocate until committed state is applied.

Alternative rejected: compare Region or coordinator wall clocks. It cannot safely survive host changes, NTP steps, or persisted locks. Alternative rejected: interpret differences between old logical TSO values as milliseconds; they have no time meaning.

### 2. Keep three independent timeout domains

- `transactionTimeoutMs` defaults to 60,000 and is evaluated from the HLC physical part of `startTs`.
- `lockTtlMs` defaults to 120,000 and is persisted as an absolute HLC physical expiry. Configuration validation requires lock TTL to exceed transaction timeout.
- Each blocking RPC/TSO/cleanup operation receives a local `steady_clock` budget. Only remaining milliseconds cross an RPC boundary; no absolute steady-clock tick is serialized.

Every public transaction operation first checks the hard transaction age using a recently allocated HLC. Expiry transitions the transaction to abort-only and starts cleanup. There is no heartbeat in this phase.

### 3. Use an explicit coordinator transaction state machine

`Transaction` gains `maxForUpdateTs`, a fixed `primaryKey`, known and uncertain pessimistic-lock key sets, and a state enum:

`Active -> AbortOnly -> CleanupPending -> Finished`

`Active -> ResultUnknown -> Finished`

The first successfully locked or mutated key becomes Primary and never changes. A conflict makes the transaction abort-only. An ambiguous lock response records the key in the uncertain set before cleanup. A primary commit timeout enters `ResultUnknown`; it never triggers blind rollback. Only an authoritative status can transition it to committed or rolled back. Public errors carry retryability plus `startTs` and Primary when status lookup is required.

Transparent replay is rejected because callbacks may contain external side effects. Callers restart the whole transaction after a retryable conflict.

### 4. Locking reads are one HLC allocation plus ordered Raft-applied point locks

`GetForUpdate` allocates one `forUpdateTs`. `BatchGetForUpdate` and `LockKeys` sort and deduplicate raw key bytes, allocate one shared `forUpdateTs`, then acquire keys in order. The coordinator tracks every sent key as uncertain until its response proves success or failure.

The Region request carries `startTs`, fixed Primary, `forUpdateTs`, HLC expiry, remaining RPC budget, and whether a value is requested. Under the scheduler's existing short-lived key latch, the Region prepares a physical MVCC lock mutation only if:

- no other transaction owns the lock;
- no committed version is newer than `forUpdateTs`; and
- an existing same-transaction pessimistic lock can be advanced idempotently.

The prepared mutation is proposed through that Region's Raft group. Success is returned only after Apply persists the lock. The response then returns the newest committed value at or before `forUpdateTs`; absence is a normal locked result. Retrying after an Apply/response gap reuses the same durable lock and reads the same protected version.

If any batch member fails, the transaction becomes abort-only, returns no partial values, and rolls back all confirmed and uncertain batch keys. Unconfirmed cleanup returns `CleanupPending`.

### 5. Prewrite upgrades an owned pessimistic lock

MVCC lock encoding appends `forUpdateTs` and absolute HLC expiry after existing fields. Legacy locks decode with `forUpdateTs = startTs` and a conservative legacy-expiry policy. An old binary may parse trailing fields but would drop them on rewrite, so new locking traffic is capability-gated until every relevant binary is upgraded.

Prewrite accepts a same-`startTs` pessimistic lock only when Primary matches and the mutation request proves it observed at least the lock's `forUpdateTs`. Apply atomically replaces it with the normal Prewrite lock. Another owner's lock remains a conflict. Commit timestamps are allocated until `commitTs > max(startTs, maxForUpdateTs)`.

Pure pessimistic locks not upgraded by mutations are released after the written keys commit. A lock-only transaction performs durable rollback-style lock release but reports successful Commit to the caller.

### 6. Make transaction status a Raft-ordered typed result

Replace optional commit-timestamp probing with:

`TxnStatus { Locked(lock), Committed(commitTs), RolledBack, NotFound }`

`TxnCheckStatus` targets the Primary Region Leader and executes through the scheduler/Raft Apply boundary. It examines both MVCC lock and write type. With `rollbackIfExpired=true`, it may atomically write a rollback record only when the Primary is still uncommitted and its HLC expiry has passed. Transport failures remain errors and never become `NotFound`.

`TxnResolveLock` applies a supplied authoritative decision to one or more secondary keys in a Region. `Committed(commitTs)` rolls forward; `RolledBack` rolls back; unknown states do nothing. Both operations are idempotent and use versioned protobuf prepared commands.

After an ambiguous primary Commit, the coordinator performs bounded status checks. A committed Primary causes secondary roll-forward; a rolled-back Primary causes cleanup; an unresolved check returns `ResultUnknown(startTs, primaryKey)` and preserves the queryable handle.

Alternative rejected: compose `FindCommitTs` and `GetLock` client reads. It is non-atomic, currently conflates rollback records with commits, and collapses transport failure into absence.

### 7. Put recovery and GC under one Node-owned service

Each physical `NodeServer` owns exactly one joinable `TxnRecoveryManager`. It owns a bounded worker queue, a shared remote TSO client configured by `--tso-endpoints`, and no Raft state. Its scan loop enumerates expired locks in local Regions, obtains one authoritative HLC per scan round, checks the remote Primary status, and submits resolve commands through the existing Node scheduler to the current Region Leader. Reads may still trigger the same resolver opportunistically.

The same manager periodically advances local Region GC with a default five-minute HLC retention cutoff. Because transactions older than 60 seconds are rejected and locks live at most 120 seconds without heartbeat, five minutes leaves a conservative scheduling/retry margin. Coordinator-owned `LockManager` and `DataGcManager` instances are removed or made non-authoritative; no SDK/Gateway process may advance GC.

The manager starts after Region recovery and shared RPC/TSO clients are ready, stops accepting work during Node shutdown, joins its scan/work threads, and is destroyed before Region/RocksDB owners. Recovery tasks never execute in Gateway Fibers.

### 8. Gate the new protocol across TSO, storage, and clients

TSO status advertises HLC protocol version. Storage status advertises pessimistic-transaction command and lock-format versions. SDK/Gateway connection setup enables the new APIs only when the configured TSO and all configured storage endpoints report compatible versions. RPC handlers independently reject unsupported versions so a stale client cannot bypass the gate.

Proto3 messages gain new tags; old tags are never reused. New Raft transaction commands use a bumped command version and are proposed only after the gate is open. Ordinary SI operations retain the old behavior and remain available throughout rolling upgrade.

### 9. Expose the same contract at every public edge

The SDK exposes typed results for single/batch locking reads, lock-only calls, transaction-status queries, cleanup pending, and unknown commit outcome. Gateway stores a transaction handle until its outcome is final and maps the same states to stable HTTP responses. CLI commands call the SDK contract and print retry/status metadata. No endpoint silently maps `ResultUnknown` to failure or removes the only handle needed to query it.

### 10. Verify correctness before measuring performance

Tests are layered:

- Timestamp tests cover same-millisecond allocation, logical overflow, migration, persistence, clock rollback, Raft leader change, and monotonic Observe.
- MVCC unit tests cover absent-key locks, compatible lock encoding, current read, idempotent reacquire, owned-lock upgrade, typed status, rollback records, resolve actions, and GC cutoffs.
- Scheduler/Region tests prove responses occur only after Apply and survive restart/leader change.
- Coordinator tests cover deterministic cross-Region acquisition, abort-only, uncertain cleanup, stable Primary, lock-only Commit, and unknown primary Commit resolution.
- End-to-end SDK/Gateway/CLI tests deterministically reproduce unprotected write skew and then preserve the same invariant with a shared read set or guard key.
- Fault tests stop clients and leaders to prove Node recovery convergence. Performance comparisons run only after correctness tests and use identical workloads.

## Risks / Trade-offs

- [HLC migration emits a much larger timestamp than legacy values] → preserve uint64 ordering, migrate once under a versioned durable state record, and forbid rollback to an allocator that can issue smaller values.
- [18 logical bits can overflow under extreme same-millisecond load] → advance the physical component by one millisecond while preserving monotonic order; report an overflow counter.
- [No heartbeat limits transaction duration] → enforce the 60-second hard limit before every operation and keep lock TTL/GC retention comfortably larger.
- [Node recovery can amplify TSO/Raft load] → batch one HLC per scan round, cap workers and keys per round, use jittered scan intervals, and retain opportunistic resolution.
- [Fail-fast contention may cause starvation] → return a retryable error and document exponential backoff with jitter; add waiting/deadlock detection only in a later change backed by contention data.
- [Cross-Region acquisition is not instantaneously atomic] → deterministic ordering, abort-only state, uncertain-key tracking, and idempotent cleanup make the public batch all-or-fail.
- [Capability discovery can become stale during rolling operations] → validate at connection time and reject protocol versions again at every Region handler.
- [Five-minute retention increases RocksDB space and compaction work] → expose configuration and GC metrics, but never reduce it below the validated transaction/lock safety margin.

## Migration Plan

1. Upgrade the TSO group to the version that understands both legacy state and HLC v2; verify all members advertise HLC and the high water is greater than the legacy value.
2. Upgrade storage nodes with new lock decoding, command parsing, status/resolve handlers, and disabled-by-default recovery/GC manager. Keep new public APIs gated.
3. Verify every storage endpoint advertises the same protocol versions, configure Node `--tso-endpoints`, and enable Node recovery/GC while disabling coordinator-side GC advancement.
4. Upgrade Gateway, CLI, and SDK clients. Open the pessimistic capability gate only after cluster-wide checks pass.
5. Run fault and write-skew acceptance suites, then record equivalent optimistic/pessimistic workload metrics.

Rollback first closes the public feature gate and waits for `TxnRecoveryManager` to resolve all new-format locks. Verify no pending v2 locks or transactions remain before downgrading storage. TSO may remain on HLC; reverting to legacy allocation is allowed only if the old implementation is explicitly seeded above the HLC high water, otherwise it is forbidden.
