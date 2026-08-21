## 1. HLC Timestamp Foundation

- [x] 1.1 Add shared HLC compose/extract/advance helpers and convert local timestamp allocation/Observe to physical-plus-logical ordering; verify with timestamp unit cases for same-millisecond allocation, logical overflow, and wall-clock rollback.
- [x] 1.2 Version the persistent TSO state, migrate legacy decimal high water atomically, and reject unknown versions; verify with restart, legacy migration, truncation, and monotonic high-water tests.
- [x] 1.3 Update TSO consensus/status capability reporting and preserve monotonic allocation across Apply and leader readiness; verify with the TSO consensus failover test and `ctest -R tso`.

## 2. MVCC Pessimistic Primitives

- [x] 2.1 Extend transaction/MVCC status types, lock fields, and compatible lock encoding for `forUpdateTs`, fixed Primary, and HLC expiry; verify legacy/new round-trip and restart recovery in MVCC unit tests.
- [x] 2.2 Implement exact-key pessimistic acquire/current-read preparation for present and absent keys, same-owner idempotency, and fail-fast conflicts; verify direct MVCC tests for each result and version race.
- [x] 2.3 Implement same-owner pessimistic-lock-to-Prewrite upgrade and stable Primary validation; verify owned upgrade, foreign-owner rejection, duplicate Prewrite, and commit timestamp ordering tests.
- [x] 2.4 Implement typed `TxnCheckStatus` and idempotent commit/rollback resolution that distinguishes rollback records, missing state, and transport errors; verify status and roll-forward/rollback unit tests.

## 3. Region Raft and RPC Protocol

- [x] 3.1 Extend protobuf requests/responses and versioned prepared commands for pessimistic acquire, status check, resolve, remaining budgets, and protocol capabilities; verify generated sources build and old-field defaults are covered by compatibility tests.
- [x] 3.2 Route new commands through NodeServer, RegionPeer, NodeTxnScheduler, Raft proposal, Apply, and RocksDB persistence; verify callbacks occur only after Apply and duplicate requests remain idempotent in scheduler/Region tests.
- [x] 3.3 Preserve distinct transport, timeout, NotFound, conflict, cleanup-pending, and result-unknown outcomes in `RaftMvccStorage`; verify injected unavailable/timeout responses never become missing transaction state.

## 4. Coordinator State Machine and APIs

- [x] 4.1 Add Active/AbortOnly/CleanupPending/ResultUnknown/Finished state, fixed Primary, max `forUpdateTs`, hard transaction deadline, and confirmed/uncertain lock tracking; verify operation gating and state-transition unit tests.
- [x] 4.2 Implement `GetForUpdate`, `BatchGetForUpdate`, and `LockKeys` with one TSO value per call/batch, bytewise sort/dedup, all-or-fail values, and partial cleanup; verify single- and cross-Region coordinator tests.
- [x] 4.3 Update Commit/Rollback for owned-lock upgrade, pure-lock success, cleanup status propagation, and `commitTs > max(startTs, forUpdateTs)`; verify mutation, read-only-lock, abort-only, and duplicate cleanup cases.
- [x] 4.4 Resolve ambiguous primary Commit through authoritative status and expose queryable `ResultUnknown` without blind rollback; verify lost-response tests for committed, rolled-back, and unavailable Primary outcomes.

## 5. Node-Owned Recovery and GC

- [x] 5.1 Add a NodeServer-owned, bounded, joinable `TxnRecoveryManager` using configured TSO endpoints and existing scheduler/Raft paths; verify startup/shutdown ownership and recovery after all coordinator processes exit.
- [x] 5.2 Move expired-lock scanning to the Node manager using HLC expiry and typed Primary decisions; verify committed transactions roll forward and expired uncommitted transactions roll back across restart.
- [x] 5.3 Move GC advancement from coordinator processes to Node, use an HLC cutoff with a default five-minute retention, and disable client-side 10-second safe points; verify a legal 60-second snapshot retains its visible version and expired transactions are rejected.

## 6. Public Surfaces and Upgrade Gate

- [x] 6.1 Extend the public C++ SDK with locking-read results, structured retry/cleanup/unknown statuses, explicit Rollback results, and transaction-status query; verify SDK contract tests and backward-compatible ordinary transactions.
- [x] 6.2 Add equivalent Gateway routes and CLI commands while keeping blocking work on bounded pthread pools and preserving unresolved handles; verify HTTP/CLI success, conflict, cleanup-pending, and result-unknown cases.
- [x] 6.3 Add TSO/storage capability discovery, Node `--tso-endpoints` configuration, and cluster-wide enablement checks; verify mixed-version simulations reject new APIs while old SI traffic succeeds.

## 7. Correctness, Fault, and Compatibility Tests

- [x] 7.1 Add a deterministic SI write-skew control and shared-read-set/guard-key prevention test; verify the control violates the invariant and the locking variants preserve it through full transaction retry.
- [x] 7.2 Add cross-Region partial acquisition, Apply/response loss, coordinator crash, Region restart, and Leader-change fault tests; verify every transaction converges without orphan locks or split commit/rollback.
- [x] 7.3 Add HLC migration/restart/clock-rollback, legacy lock decoding, new-lock downgrade gate, transaction-timeout, and GC safety coverage; verify all targeted tests pass under `ctest --output-on-failure`.

## 8. Observability, Performance, and Documentation

- [x] 8.1 Add distinguishable counters/status for lock attempts, conflicts, idempotent retries, partial cleanup, unknown results, recovery decisions, timeouts, and abort-only transitions; verify status output changes under targeted tests.
- [x] 8.2 Add an equivalent optimistic-versus-pessimistic workload comparison reporting throughput and p50/p95/p99 without asserting a speedup; verify the benchmark runs with identical data, concurrency, and durability settings.
- [x] 8.3 Document exact-key/guard-key usage, non-Serializable boundaries, retry rules, timeout configuration, rolling upgrade, recovery, and rollback; verify commands/examples against the built CLI and keep the separate `docs/` repository untouched unless explicitly entered.
- [x] 8.4 Run formatting/build, full CTest, reliability/fault suites, strict OpenSpec validation, and review the final diff for unrelated user changes; record commands/results and check every task only when its verification passes.
