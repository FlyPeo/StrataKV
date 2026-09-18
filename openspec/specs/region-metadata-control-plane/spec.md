# region-metadata-control-plane Specification

## Purpose
Provide a replicated, durable source of truth for Region ranges, peer placement, leaders, and epochs so clients and storage nodes can detect stale topology and route requests safely during failures and future Region changes.

## Requirements

### Requirement: Replicated authoritative metadata

The system SHALL store Region and Store metadata in a dedicated three-member consensus group. A metadata mutation SHALL be acknowledged only after it is committed by a quorum and applied to the metadata state machine. Reads used for routing SHALL observe a linearizable state or fail with an explicit retryable error.

#### Scenario: Metadata mutation succeeds with a quorum

- **WHEN** the metadata leader proposes a valid mutation and a quorum of metadata members remains available
- **THEN** the mutation is committed, applied, and returned by subsequent routing reads

#### Scenario: Metadata quorum is unavailable

- **WHEN** fewer than a quorum of metadata members can communicate
- **THEN** metadata mutations and linearizable routing reads fail by their configured deadline and do not report an uncommitted state as successful

#### Scenario: Metadata leader changes during a request

- **WHEN** a client sends a metadata request to a former leader
- **THEN** the service returns a retryable leader error with a leader hint when known, and retrying against the current leader observes only committed metadata

#### Scenario: Duplicate metadata mutation is retried

- **WHEN** a client retries the same mutation identifier after losing the original response
- **THEN** the state machine applies the mutation at most once and returns the original logical result

### Requirement: Complete and versioned Region topology

The metadata state SHALL describe a gap-free, non-overlapping partition of the configured keyspace. Every Region descriptor SHALL contain a stable Region identifier, half-open key range, peers with stable peer and Store identifiers, a leader hint, and an epoch containing `version` and `conf_version`. Range changes SHALL increase `version`; peer-set changes SHALL increase `conf_version`; neither component SHALL decrease.

#### Scenario: Valid topology is published

- **WHEN** a mutation replaces one Region descriptor with descriptors whose ranges exactly cover the original range and whose epochs advance correctly
- **THEN** the metadata state atomically exposes the new descriptors and no reader observes a gap or overlap

#### Scenario: Invalid topology is proposed

- **WHEN** a mutation would create a key-range gap, overlap, duplicate Region or peer identifier, or a non-increasing required epoch component
- **THEN** the mutation is rejected without changing the committed topology

#### Scenario: Peer membership changes without a range change

- **WHEN** a committed mutation changes a Region's peer set but preserves its key range
- **THEN** `conf_version` increases and `version` remains unchanged

### Requirement: Durable identifier allocation

The metadata service SHALL allocate Region, peer, and Store identifiers from durable, monotonically increasing namespaces. An identifier SHALL NOT be reused after a restart, leader change, timeout, or abandoned operation.

#### Scenario: Allocation response is lost

- **WHEN** an identifier allocation commits but the caller does not receive the response
- **THEN** a retry with the same mutation identifier returns the same allocated range, while a new request receives identifiers above the committed high-water mark

#### Scenario: Leader changes after allocation

- **WHEN** leadership changes after an allocation is committed
- **THEN** the new leader continues from the committed high-water mark and never allocates a duplicate identifier

### Requirement: Region discovery

The metadata service SHALL support lookup by key, lookup by Region identifier, and bounded scans beginning at a key. Each successful response SHALL be derived from one committed metadata revision and SHALL include the revision needed to order cache updates.

#### Scenario: Lookup by key succeeds

- **WHEN** a client looks up a key covered by the configured keyspace
- **THEN** the service returns the single committed Region descriptor whose half-open range contains that key

#### Scenario: Scan crosses Region boundaries

- **WHEN** a client requests a bounded Region scan beginning inside one Region
- **THEN** the response returns descriptors in key order without gaps or overlaps and labels them with one metadata revision

#### Scenario: Lookup exceeds its deadline

- **WHEN** a linearizable result cannot be obtained before the caller's deadline
- **THEN** the service returns a timeout or unavailable error and does not substitute an unverified stale result

### Requirement: Crash recovery and snapshot installation

Every committed metadata mutation SHALL survive process restart. A recovering or newly joined metadata member SHALL reconstruct exactly a committed state from its log and snapshot, and partially written state SHALL never become visible as committed topology.

#### Scenario: Leader crashes after commit

- **WHEN** the metadata leader crashes after a mutation commits but before its response is delivered
- **THEN** the elected leader exposes the committed mutation and a duplicate retry remains idempotent

#### Scenario: Member restarts during persistence

- **WHEN** a metadata member restarts after an interrupted local persistence operation
- **THEN** it discards or repairs the incomplete local state and catches up to a valid committed revision before serving linearizable reads

#### Scenario: Lagging member installs a snapshot

- **WHEN** a metadata member is behind the retained log and receives a snapshot
- **THEN** it validates and atomically installs the snapshot before applying later log entries

### Requirement: Controlled static-topology compatibility

Deployments SHALL explicitly select either replicated metadata routing or legacy static topology routing. Failure of the metadata service in replicated mode SHALL NOT silently switch a running client to static topology.

#### Scenario: Legacy mode is selected

- **WHEN** a deployment explicitly starts in legacy static mode
- **THEN** the existing Region configuration remains usable without a metadata quorum

#### Scenario: Metadata becomes unavailable in replicated mode

- **WHEN** a client running in replicated mode cannot refresh metadata
- **THEN** it may use an already validated cached descriptor within retry policy, but it does not load static topology as an automatic fallback

### Requirement: Metadata observability

The service SHALL expose metadata leader identity, applied revision, commit and apply latency, request outcomes, retry counts, snapshot activity, and validation rejection counts.

#### Scenario: Operator diagnoses metadata unavailability

- **WHEN** metadata requests time out because no quorum is available
- **THEN** metrics and logs identify the affected group, last applied revision, request type, deadline outcome, and current leadership state without logging user keys or values
