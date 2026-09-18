## Purpose

Make every data request topology-aware and able to refresh, reroute, and regroup safely when Region leadership, ranges, or peer membership change, while preserving transaction and request identity across retries.

## ADDED Requirements

### Requirement: Versioned Region request header

Every Region-scoped storage request SHALL carry a common header containing Region identifier, target peer identifier, Region epoch, and request identity. Transactional requests SHALL also preserve their original transaction identity and timestamps. A storage peer SHALL validate the header before performing request side effects.

#### Scenario: Current header reaches the target peer

- **WHEN** the target peer serves the named Region and the request epoch equals its current epoch
- **THEN** the peer executes the request under the existing Raft and MVCC rules

#### Scenario: Request targets the wrong peer or Region

- **WHEN** the target peer does not serve the named Region or the request key lies outside its current range
- **THEN** the peer rejects the request with a structured routing error before proposing or applying any command

#### Scenario: Request carries a stale epoch

- **WHEN** either epoch component in a request is older than the peer's current descriptor
- **THEN** the peer returns an epoch-mismatch error containing enough current Region descriptors to refresh the affected key range and performs no request side effects

### Requirement: Structured routing errors

Region services SHALL distinguish leader changes, epoch mismatches, missing Regions or peers, server overload, timeout, and non-routing storage failures. Retryable errors SHALL include safe hints when available and SHALL never encode routing state solely in human-readable text.

#### Scenario: Leader moves without a topology change

- **WHEN** a request reaches a follower with a current epoch
- **THEN** the response identifies `NotLeader` and includes the current leader peer when known

#### Scenario: Region has been replaced

- **WHEN** a request reaches a server after the requested Region has been replaced by newer descriptors
- **THEN** the response identifies `EpochNotMatch` or `RegionNotFound` and includes authoritative replacement descriptors when locally known

#### Scenario: Storage execution fails

- **WHEN** a request reaches the correct leader with a current epoch but RocksDB or MVCC execution fails
- **THEN** the response reports a non-routing storage error and the routing layer does not treat it as evidence to change Region topology

### Requirement: Concurrent dynamic Region cache

Clients SHALL maintain a thread-safe cache of immutable Region descriptors ordered by key range. A cache update SHALL become visible atomically and SHALL be accepted only if it is internally gap-free for the replaced interval and is newer than the descriptors it supersedes.

#### Scenario: Concurrent reads during a refresh

- **WHEN** one thread refreshes an affected Region while other threads route requests
- **THEN** each routing lookup observes either the complete old view or the complete new view, never a partially updated set of ranges

#### Scenario: Older response arrives after a newer refresh

- **WHEN** concurrent metadata responses complete out of order
- **THEN** the cache rejects the older revision or epoch and preserves the newer routing view

#### Scenario: Multiple callers miss the same range

- **WHEN** concurrent callers need the same unavailable or stale Region range
- **THEN** the client coalesces equivalent refresh work when possible, bounds all waiters by their own deadlines, and does not hold a cache lock during network I/O

### Requirement: Bounded topology-aware retries

Routing retries SHALL obey the caller's original deadline, use bounded backoff, and preserve request, client, transaction, and timestamp identities. A leader error SHALL update only the leader hint; an epoch or missing-Region error SHALL invalidate and refresh the affected range.

#### Scenario: Leader changes during a write

- **WHEN** a write receives `NotLeader` before its deadline
- **THEN** the client retries the same logical request against the hinted or discovered leader without allocating a new transaction or request identity

#### Scenario: Epoch changes repeatedly

- **WHEN** topology continues changing while a request is retried
- **THEN** the client refreshes and reroutes only until the original attempt or time budget is exhausted, then returns a retry-exhausted or deadline error

#### Scenario: Response is lost after commit

- **WHEN** a write commits but its response is lost and the client retries after a route refresh
- **THEN** the retry retains the same request identity so the storage path can return the prior logical result rather than creating a second logical write

### Requirement: Transactional batch regrouping

Before each Region-scoped transaction phase, the coordinator SHALL derive Region batches from the current cache. If a routing error changes the affected boundaries, the coordinator SHALL discard only the stale grouping, refresh the range, and regroup the unfinished keys while preserving the transaction start timestamp, primary key, commit timestamp, and mutation identity.

#### Scenario: Region boundaries change before prewrite

- **WHEN** a prewrite batch receives an epoch mismatch before any mutation in that batch is proposed
- **THEN** the coordinator refreshes topology, splits the mutations into current Region batches, and retries them under the same transaction identity

#### Scenario: Some Region batches already succeeded

- **WHEN** completed batches are followed by an epoch mismatch in another batch
- **THEN** the coordinator does not blindly repeat completed work, uses idempotent transaction operations for necessary retries, and continues or rolls back according to the existing transaction protocol

#### Scenario: Regrouping cannot finish by deadline

- **WHEN** repeated topology changes prevent all batches from completing before the transaction deadline
- **THEN** the coordinator returns a retryable transaction error and invokes the existing cleanup or lock-resolution path for mutations that may have been written

### Requirement: Routing bootstrap and compatibility mode

In replicated metadata mode, a client SHALL bootstrap its Region cache from configured metadata endpoints before serving requests. In explicitly selected legacy mode, it SHALL construct equivalent versioned descriptors from the static Region configuration.

#### Scenario: Metadata bootstrap succeeds

- **WHEN** a client connects to a metadata quorum
- **THEN** it obtains a consistent initial Region view and begins routing with descriptor epochs and peer identifiers

#### Scenario: Metadata bootstrap fails

- **WHEN** no consistent Region view can be obtained before the connection deadline
- **THEN** client initialization fails explicitly and no data request is sent with guessed topology

#### Scenario: Legacy configuration is used

- **WHEN** legacy mode is explicitly selected
- **THEN** the client derives deterministic compatibility epochs and peer identifiers so the common request-validation path remains active

### Requirement: Routing observability

The routing layer SHALL expose cache revision and entry count, cache hits and misses, refresh latency and outcomes, structured error counts, retries by cause, regroup counts, and exhausted deadlines.

#### Scenario: Throughput drops during topology churn

- **WHEN** route refreshes or retries increase during a workload
- **THEN** operators can correlate request latency with refresh, retry, and error-cause metrics without exposing user key or value contents

