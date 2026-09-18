## Why

StrataKV currently loads one immutable Region catalog from `regions.conf` at process startup, so clients cannot distinguish a Leader change from a Region range or membership change. Dynamic split, peer migration, and automatic scheduling all require a replicated source of truth, monotonic Region epochs, and clients that can refresh stale routes without losing transaction idempotency.

## What Changes

- Add a three-member replicated metadata control plane that stores the authoritative Region range tree, peer descriptors, Store descriptors, and monotonic ID allocation state.
- Extend Region metadata with stable peer IDs and a two-part epoch: `version` for range changes and `conf_version` for membership changes.
- Add typed request headers and typed Region errors so storage nodes can distinguish stale epochs, wrong peers, wrong key ranges, missing Regions, and changed Leaders.
- Replace the SDK/Gateway's immutable startup catalog with a thread-safe Region cache backed by the metadata control plane.
- Retry stale-route requests within the existing deadline while preserving transaction timestamps and request identity; regroup Region-batched work when a refreshed boundary changes ownership.
- Add dynamic, thread-safe Region registration and lookup interfaces to `NodeServer` as groundwork for later split and peer lifecycle changes. This change does not yet create, split, move, or destroy Region peers at runtime.
- Add route/epoch metrics and structured diagnostic fields for cache refreshes, epoch mismatches, and metadata availability.
- **BREAKING**: Internal KV RPC requests gain a required Region header and return structured Region errors. The repository already treats these Protobuf RPCs as internal and does not promise wire compatibility.
- Preserve the current static three-Region deployment as a compatibility and rollback mode. When dynamic metadata is disabled, the existing `regions.conf` catalog remains the source of routes and peers.

### Goals

- Establish one authoritative and highly available mapping from every key to exactly one active Region.
- Make stale client routes recoverable across concurrent requests, Leader changes, future splits, and future peer changes.
- Preserve transaction identity, MVCC timestamps, deadlines, and Region-batch semantics across route refresh and retry.
- Provide correctness and observability foundations for the subsequent crash-safe split, online peer reconfiguration, and balancing changes.

### Non-goals

- Executing Region split or merge operations.
- Adding or removing Raft voters or learners online.
- Moving Region data, balancing Leaders, or scheduling by size or load.
- Replacing the existing TSO protocol or moving transaction/Raft work onto Pulsar fibers.
- Claiming a throughput improvement before a reproducible benchmark demonstrates one.

### Correctness and performance expectations

The main correctness risks are accepting a write under stale range ownership, retrying a transaction operation with changed identity, exposing partially committed metadata, and deadlocking callers during metadata failover. Metadata mutations therefore pass through a quorum-backed state machine, storage requests validate epochs before execution, and retries retain the caller's original transaction and request identifiers. Region cache hits stay local; metadata RPCs occur on cache miss, explicit invalidation, or bounded refresh, so the steady-state data path should add only header validation and cache lookup overhead.

### Rollback strategy

Dynamic metadata and routing are guarded by deployment configuration. Operators can stop the dynamic control plane and restart all clients and nodes in static-catalog mode using the unchanged `regions.conf` topology, provided no later change has performed a dynamic split or membership update. The metadata store uses a separate versioned data directory so rollback does not overwrite Region data or the existing TSO state.

## Capabilities

### New Capabilities

- `region-metadata-control-plane`: Replicated Region/Store metadata, epoch and ID semantics, metadata lookup, and failover behavior.
- `dynamic-region-routing`: Request headers, typed Region errors, Region cache refresh, retry, and transaction regrouping behavior.
- `dynamic-region-registry`: Thread-safe storage-node Region lookup/registration contracts required by later lifecycle operations.

### Modified Capabilities

None.

## Impact

- New metadata service executable, RPC schema, replicated state machine, persistence directory, client, and deployment lifecycle.
- Changes to `RegionMetadata`, `RegionCatalog`, SDK construction, `ShardRouter`, transaction coordination, `RaftMvccStorage`, Gateway wiring, and administration tools.
- Changes to KV and Raft request headers, storage-side request validation, error mapping, retry policy, metrics, and logs.
- `NodeServer` Region ownership changes from immutable startup containers to a synchronized registry while retaining one shared listener and RPC worker pool per physical node.
- New unit, integration, fault-injection, compatibility, and performance-overhead tests. No third-party dependency is required; the existing Raft, MPRPC, Protobuf, RocksDB, and bounded pthread components are reused.
