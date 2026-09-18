# crash-safe-region-split

## Purpose

Provide a manually triggered, crash-safe Region split: an operator splits one Region at a chosen key, the decision replicates through the parent Region's Raft log, every replica materializes the child deterministically, and the cluster keeps serving through the whole transition without losing acknowledged writes or creating overlapping/holes topology.

## ADDED Requirements

### Requirement: Split command and validation

The system SHALL provide an administrator command `split-region(region_id, expected_epoch, split_key)` that only executes in dynamic metadata mode. The command SHALL be rejected, without any side effect, when the Region does not exist, the expected epoch does not match the metadata descriptor, the split key is not strictly inside the Region's half-open range, or the Region already has a split in flight. The key SHALL identify the boundary by user-key bytewise ordering: keys strictly less than the split key remain in the parent; keys greater or equal belong to the new child.

#### Scenario: Valid split is accepted

- **WHEN** an operator submits a split with a matching epoch and a key strictly inside the Region range
- **THEN** the system allocates child identifiers and begins the split, returning a command handle that reports progress

#### Scenario: Stale epoch is rejected

- **WHEN** the expected epoch does not match the current metadata descriptor
- **THEN** the command fails with the current descriptor returned and no topology or data change occurs

#### Scenario: Boundary keys are invalid

- **WHEN** the split key equals or falls outside the Region's half-open range, or an in-flight split exists
- **THEN** the command is rejected with an actionable message and nothing changes

### Requirement: Metadata two-phase split commit

The metadata control plane SHALL commit a split in two phases. Phase one allocates child Region and peer identifiers from the durable high-water marks and records a `split-in-progress` association for the parent. Phase two publishes the new topology atomically: the parent's range shrinks to `[start, split_key)` with `epoch.version` incremented, the child owns `[split_key, end)` with fresh identifiers and a fresh epoch, and the replacement covers the parent's exact interval with no gap or overlap. Each phase SHALL be idempotent for the same mutation identifier. Between the phases the parent descriptor SHALL carry a marker that lets nodes detect a committed-but-unpublished split.

#### Scenario: Two-phase split commits atomically

- **WHEN** the metadata quorum commits phase two for a split prepared in phase one
- **THEN** readers observe either the old single descriptor or the new parent+child pair, never a gap, overlap, or duplicate key ownership

#### Scenario: Duplicate split mutation is retried

- **WHEN** the same split mutation identifier is replayed after a lost response or leader change
- **THEN** the state machine returns the original result without re-allocating identifiers or double-applying the topology change

#### Scenario: Leader changes between phases

- **WHEN** the metadata leader changes after phase one commits but before phase two
- **THEN** the new leader resumes or completes the split from committed state, and the child identifiers are never reused for a different split

### Requirement: Replicated split application

The parent Region's leader SHALL propose an `AdminSplit` entry carrying the split key and the metadata revision after the metadata topology is committed. Every replica SHALL apply the split at the same log position: shrink the parent descriptor to `[start, split_key)` with the new epoch, and materialize the child Region data for `[split_key, end)` from a local RocksDB checkpoint of the parent. The apply step SHALL re-validate the split key and epoch against the pre-split descriptor and produce a replicated no-op result when validation fails. A data write whose key falls on the child side SHALL take effect in the child's data set; a write on the parent side SHALL remain in the parent; no acknowledged write SHALL be lost or duplicated across the boundary.

#### Scenario: Replicas split at the same log position

- **WHEN** the AdminSplit entry commits and each replica applies it
- **THEN** every replica serves the same parent range with the same new epoch and holds a child data set covering `[split_key, end)`, and no acknowledged write is missing from either side

#### Scenario: Concurrent write across the split moment

- **WHEN** a write proposal is in flight while the AdminSplit applies, on either side of the boundary
- **THEN** the write lands wholly in one side's data set according to its key, and subsequent reads of that key return the written value from whichever Region now owns it

#### Scenario: Apply validation fails

- **WHEN** the applied AdminSplit does not match the local pre-split descriptor (stale epoch or out-of-range key)
- **THEN** the apply is a replicated no-op and the replica's data and descriptor are unchanged

### Requirement: Crash-safe local materialization

Each replica SHALL persist a local `RegionLocalState` recording the split phase (not-started, checkpointing, child-ready, parent-shrunk, complete), the parent and child descriptors, and a monotonically increasing data generation for the child directory. The sequence SHALL be ordered so that a crash at any point is recoverable by forward progress: the child checkpoint is created before the child is announced locally, the persisted state is durable before any in-memory registration, and re-applying the same AdminSplit after restart resumes from the persisted phase instead of restarting the split. A partially written checkpoint SHALL be detected via the generation marker and discarded or resumed without serving from it. The parent SHALL NOT serve keys outside its shrunken range after the split applies, and out-of-range records in the parent's RocksDB SHALL be removed asynchronously with a bounded rate.

#### Scenario: Crash during checkpoint

- **WHEN** a replica crashes after the AdminSplit entry commits but before the child checkpoint completes
- **THEN** on restart the replica resumes the checkpoint from the persisted phase, and no partially created child directory is ever registered or served

#### Scenario: Crash before parent shrink completes

- **WHEN** a replica restarts after the child is materialized but before the parent finished shrinking
- **THEN** the replica completes the shrink and out-of-range cleanup from the persisted phase and serves the correct shrunken range

#### Scenario: Restart with committed split and no local state

- **WHEN** a replica that never applied the AdminSplit (for example a lagging member restored from snapshot) restarts
- **THEN** it derives its post-split state from the Raft log replay or snapshot install, matching the metadata descriptor for the same revision

### Requirement: Child service上线 and registry replacement

A node SHALL create the child RegionPeer only after its local data is materialized and its descriptors are durable. The node SHALL publish the post-split registry — parent entry updated and child entry added — as one atomic replacement, and the child SHALL begin in the normal serving lifecycle only after publication. The node SHALL report the materialized children to the metadata service, which SHALL be idempotent for the same split. The child Region's Raft group SHALL start with the parent's committed log history for the split entry so that membership and durability are preserved, or SHALL start empty and rely on the cloned checkpoint as its data source; the chosen mechanism SHALL guarantee that every write acknowledged before the split remains readable from the owning side.

#### Scenario: Atomic registry replacement

- **WHEN** a node finishes local materialization
- **THEN** requests routed after publication resolve the shrunken parent and the new child atomically; requests that already held the parent's handle complete safely under the registry's retirement semantics

#### Scenario: Node reports children idempotently

- **WHEN** the node re-reports a split after a retry or restart
- **THEN** the metadata service accepts the report without duplicating child descriptors or advancing identifiers

#### Scenario: Child serves acknowledged history

- **WHEN** a client reads a key in the child range that was last written before the split
- **THEN** the value is returned from the child Region with its pre-split MVCC history intact

### Requirement: Routing and transaction behavior across a split

Clients SHALL treat an `EPOCH_NOT_MATCH` from a shrunken parent as a cache invalidation for the affected range and refresh from metadata. A transaction batch that no longer fits one Region SHALL return regroup-required, and the coordinator SHALL regroup only unfinished or uncertain mutations while preserving `start_ts`, primary key, `commit_ts`, and mutation identity. Locks and MVCC records of one user key SHALL always resolve within the single Region owning that key after the split; a lock resolution or cleanup for a pre-split transaction SHALL be routed by key to the owning child. In-flight pre-split transactions SHALL complete through the normal protocol without requiring operator action.

#### Scenario: Stale parent route is refreshed

- **WHEN** a request with the pre-split epoch reaches the shrunken parent for a key in the child range
- **THEN** the parent responds with `EPOCH_NOT_MATCH` carrying the new descriptors, and the client refreshes and retries the same logical request with unchanged identity

#### Scenario: Transaction batch is regrouped

- **WHEN** a batched prewrite spans the new boundary after a refresh
- **THEN** the coordinator regroups the unfinished mutations into current Region batches under the same transaction identity and completes or cleans up by the existing protocol

#### Scenario: Pre-split lock resolves to the owning child

- **WHEN** a lock left by a pre-split transaction is resolved after the split
- **THEN** resolution reaches the Region that now owns the key and observes the transaction's committed or rolled-back outcome exactly once

### Requirement: Split observability and safety invariants

The system SHALL expose split progress per Region (phase, checkpoint generation, data volume cloned, cleanup remaining) and SHALL log phase transitions with RegionId, peer, epoch, and metadata revision, never raw keys or values. The following invariants SHALL hold at every instant: every key is owned by at most one writable Region; topology snapshots are gap-free and overlap-free; an acknowledged write is readable from the owning side after any crash recovery; child identifiers allocated for one split are never reused; a Region serves traffic only after its descriptor and data are durable and it is published in the registry.

#### Scenario: Operator tracks split progress

- **WHEN** a split is in progress
- **THEN** the command handle and logs report the current phase and remaining cleanup without exposing user data

#### Scenario: Invariants hold during concurrent load

- **WHEN** reads, single-key writes, and multi-Region transactions run continuously through a split
- **THEN** no client observes a lost acknowledged write, a duplicated or missing key across Regions, or a topology gap at any point
