## Purpose

Provide storage nodes with a concurrency-safe Region registry and request-dispatch contract that can publish, replace, and retire Region peers safely as later split and membership workflows create topology changes.

## ADDED Requirements

### Requirement: Atomic Region registry publication

A storage node SHALL resolve Region identifiers through a thread-safe registry of Region peer handles. Publishing, replacing, or removing a registry entry SHALL appear atomic to request dispatchers, and network or disk I/O SHALL NOT occur while holding the registry's publication lock.

#### Scenario: Region is published

- **WHEN** a fully initialized Region peer is registered
- **THEN** subsequent lookups resolve the complete peer while lookups begun before publication either resolve the previous entry or report it absent

#### Scenario: Region is replaced

- **WHEN** a newer compatible instance of an existing Region is atomically published
- **THEN** new requests resolve the new instance and no request observes a partially initialized peer

#### Scenario: Concurrent lookups occur during removal

- **WHEN** a Region is removed while requests are being dispatched
- **THEN** new lookups report a structured missing-Region error while requests that already acquired a valid handle retain a safe object lifetime

### Requirement: Conflict-safe registration

Registration SHALL be idempotent for the same Region, peer identity, and epoch. It SHALL reject a conflicting identity, range, or older epoch without replacing the active entry.

#### Scenario: Registration is retried

- **WHEN** the same initialized Region peer is registered more than once
- **THEN** registration succeeds idempotently and the registry contains one active entry

#### Scenario: Older Region instance arrives late

- **WHEN** an instance with an older epoch attempts to replace a newer active instance
- **THEN** registration fails with a stale-epoch result and the newer instance remains active

#### Scenario: Region identifier conflicts

- **WHEN** a different peer identity or incompatible range attempts to reuse an active Region identifier without an authorized topology transition
- **THEN** registration fails and emits a conflict diagnostic

### Requirement: Safe Region peer lifetime

The node SHALL keep a Region peer alive while any accepted request, Raft callback, apply task, or transaction scheduler work item still references it. Retirement SHALL prevent new work before shutdown and resource destruction, and shutdown SHALL be bounded and observable.

#### Scenario: In-flight request overlaps retirement

- **WHEN** a request acquires a peer handle before the Region begins retiring
- **THEN** the request either completes under the accepted descriptor or receives a structured retryable routing error, and it never dereferences destroyed state

#### Scenario: Retirement has pending apply work

- **WHEN** a peer is retired with queued or executing apply work
- **THEN** the peer drains or cancels that work according to an explicit shutdown policy before closing its RocksDB and Raft resources

#### Scenario: Shutdown exceeds its deadline

- **WHEN** a retiring peer cannot drain by the configured deadline
- **THEN** the node reports the blocking component and keeps resources in a safe state rather than silently destroying live objects

### Requirement: Descriptor validation before dispatch

The node SHALL validate a request's Region identifier, target peer identifier, epoch, and key range against the descriptor attached to the resolved peer before handing the operation to Raft or MVCC execution.

#### Scenario: Registry entry is current

- **WHEN** request header and key range match the resolved peer descriptor
- **THEN** dispatch proceeds to the Region service

#### Scenario: Registry entry is absent

- **WHEN** a request names a Region not present in the registry
- **THEN** dispatch returns a structured missing-Region response before invoking a Region service

#### Scenario: Registry entry has a newer descriptor

- **WHEN** the resolved peer's epoch is newer than the request header
- **THEN** dispatch returns a structured epoch error and the current descriptor before any Raft proposal or MVCC side effect

### Requirement: Deterministic node bootstrap

A storage node SHALL construct and validate the complete initial registry before accepting Region traffic. Replicated metadata mode SHALL reconcile configured local stores and peers with a consistent metadata revision; legacy mode SHALL derive the same registry contract from static configuration.

#### Scenario: Metadata bootstrap is consistent

- **WHEN** all peers assigned to the local Store are initialized and match one metadata revision
- **THEN** the node atomically publishes the initial registry and begins accepting Region traffic

#### Scenario: One assigned peer fails initialization

- **WHEN** RocksDB, Raft, or descriptor initialization fails for an assigned Region
- **THEN** the node does not publish a partially valid initial registry and startup reports the failed Region and component

#### Scenario: Legacy node starts

- **WHEN** legacy static mode is explicitly configured
- **THEN** all configured local Region peers are published through the dynamic registry using deterministic compatibility descriptors

### Requirement: Registry observability

The node SHALL expose active, initializing, and retiring Region counts; registry revision; registration conflicts; lookup misses; retirement duration; and shutdown failures.

#### Scenario: Request reaches a node after a topology update

- **WHEN** registry lookup or epoch validation rejects the request
- **THEN** logs and metrics identify the Region, Store, peer, error category, and observed epoch without logging user keys or values

