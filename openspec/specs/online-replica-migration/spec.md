# online-replica-migration Specification

## Purpose

Provide a crash-safe, online replica migration mechanism for Region peers using single-step Raft ConfChange and Learner-based state transfer: an operator or scheduler moves a replica from one storage node to another, the cluster streams logs or snapshots without stopping traffic, advances configuration version monotonically, and safely reclaims the decommissioned replica's storage resources without creating split-brain or data inconsistency.

## Requirements

### Requirement: Raft single-step membership change

The system SHALL provide a Raft consensus command `ConfChange` supporting single-step topology mutations: `AddLearner`, `PromoteLearner`, and `RemovePeer`. The Raft Leader SHALL reject any new `ConfChange` proposal with an error if a previous `ConfChange` entry is already in flight (proposed but not yet applied). Each applied `ConfChange` SHALL monotonically advance the Region's `epoch.conf_version` by 1 across all replicas.

#### Scenario: Single-step addition is proposed and applied

- **WHEN** the Leader proposes an `AddLearner` or `PromoteLearner` ConfChange and quorum commits it
- **THEN** every replica updates its active voting or learner configuration, advances `epoch.conf_version`, and begins replicating according to the new membership

#### Scenario: Concurrent ConfChange is rejected

- **WHEN** a client or admin proposes a `ConfChange` while another `ConfChange` is committed in log but not yet applied, or uncommitted
- **THEN** the Leader immediately rejects the second proposal with a structured `CONF_CHANGE_IN_FLIGHT` error without altering Raft state

#### Scenario: Removed peer ceases participation

- **WHEN** a replica applies a `RemovePeer` entry targeting its own peer ID
- **THEN** it immediately stops initiating elections, ceases responding to heartbeat RPCs, and transitions to a stopped lifecycle state

### Requirement: Learner synchronization and state transfer

When a new replica is introduced on a target node, it SHALL initially join as a non-voting `Learner`. The Learner SHALL NOT count toward Raft election or entry commitment quorums. If the Learner's log gap exceeds the compaction boundary, the Leader SHALL send a consistent snapshot generated from RocksDB Checkpoint via chunked streaming RPC. When the Learner's log replication lag falls within a configured catch-up threshold, the system SHALL propose a `PromoteLearner` ConfChange to elevate it to a full voting member.

#### Scenario: Learner catches up via AppendEntries

- **WHEN** a Learner is added and the Leader retains all required log entries
- **THEN** the Leader streams `AppendEntries` to the Learner until the Learner's match index catches up with the Leader's commit index

#### Scenario: Learner receives RocksDB snapshot

- **WHEN** a Learner is added but the Leader has garbage-collected logs below the required index
- **THEN** the Leader creates a RocksDB checkpoint snapshot and streams it to the target node; the target node unpacks it, initializes the engine, and resumes normal log replication

#### Scenario: Learner promotion

- **WHEN** a Learner's log lag falls below the configured synchronization threshold
- **THEN** the Leader proposes a `PromoteLearner` entry and, upon quorum commit, the Learner becomes a voting member without disrupting ongoing write consensus

### Requirement: Metadata control plane coordination

The metadata control plane (MetaServer) SHALL coordinate migration operations by allocating globally unique `peer_id`s from durable high-water marks and recording in-flight migration status. Upon successful migration milestones, the metadata state machine SHALL atomically update the Region descriptor's `peers` set, advance `epoch.conf_version`, and publish the new topology revision. The mutation SHALL be idempotent for the same mutation ID.

#### Scenario: Valid metadata migration mutation commits

- **WHEN** a migration step commits on the MetaServer
- **THEN** subsequent metadata lookups and scans reflect the updated peer list and incremented `conf_version` with no duplicate peer IDs

#### Scenario: Stale epoch or configuration rejected

- **WHEN** a migration command presents an `expected_epoch` that does not match the active descriptor
- **THEN** MetaServer rejects the mutation with an actionable error and the cluster topology remains unchanged

#### Scenario: Idempotent replay

- **WHEN** the same migration mutation identifier is submitted again after a network timeout or leader failover
- **THEN** MetaServer returns the previously committed result without advancing `conf_version` or allocating redundant identifiers

### Requirement: Dynamic storage node lifecycle and resource cleanup

A storage node SHALL dynamically instantiate and register a new `RegionPeer` upon receiving a migration preparation RPC from the coordinator. Conversely, upon applying a `RemovePeer` command matching its local replica, the storage node SHALL transition the peer to `Retiring` status, wait for in-flight read and apply operations to drain within a bounded timeout, atomically unregister the peer from `RegionRegistry`, and asynchronously delete its local RocksDB directory.

#### Scenario: Target node dynamically mounts new peer

- **WHEN** a migration request initiates on a target node
- **THEN** `NodeServer` initializes the local directory, starts the `RegionPeer` instance, and registers it into the local `RegionRegistry` without restarting the node process

#### Scenario: Source node cleans up decommissioned replica

- **WHEN** a replica is removed via applied `RemovePeer`
- **THEN** the node retires the peer handle, unregisters it from routing, closes RocksDB, and safely removes the replica's disk directory

### Requirement: Transparent routing and transaction consistency

Clients encountering a decommissioned or newly elected replica SHALL observe standard `NOT_LEADER` or `EPOCH_NOT_MATCH` error codes carrying the updated Region descriptor. SDK `RegionCache` SHALL automatically refresh the peer endpoints and retry without failing ongoing transactions. Active distributed transactions (Percolator 2PC) running during replica migration SHALL complete commit or rollback with full serializability and zero lost writes.

#### Scenario: Client route refreshes after replica migration

- **WHEN** a client sends a request to a decommissioned peer or with an outdated `conf_version`
- **THEN** the server returns `EPOCH_NOT_MATCH`, prompting the SDK to refresh its cache and redirect to an active peer

#### Scenario: Distributed transaction concurrency across ConfChange

- **WHEN** read, prewrite, or commit operations overlap with a `ConfChange` commit and replica relocation
- **THEN** all transactions either commit cleanly or retry without isolation anomalies or orphan locks

### Requirement: Operator migration command and observability

The system SHALL provide an operator CLI command `move-peer(region_id, from_store_id, to_store_id)` that executes only in dynamic topology mode. The system SHALL expose structured status fields tracking migration progress: current phase (`NotStarted`, `PreparingTarget`, `CatchingUp`, `Promoted`, `RetiringSource`, `Complete`), transferred bytes, and log replication lag. All metrics and structured logs SHALL record `region_id`, `peer_id`, `store_id`, `epoch`, and `phase`, strictly omitting raw user keys or values.

#### Scenario: Operator executes move-peer successfully

- **WHEN** an operator initiates `move-peer` for a valid Region between healthy stores
- **THEN** the command orchestrates target initialization, Learner catch-up, promotion, source retirement, and outputs phase progression until completion

#### Scenario: Preflight failure aborts move-peer

- **WHEN** `move-peer` is invoked with a non-existent Region, an invalid source or target store, or when an in-flight migration is already active
- **THEN** the command fails immediately with an actionable error and leaves the cluster state untouched
