## Why

StrataKV can now split Regions and move replicas online, but both operations require an operator to choose the Region and destination. A cluster that grows, loses a Store, or develops hot spots therefore cannot converge back to a healthy placement without continuous manual intervention.

This change adds a metadata-leader-owned Auto-Balancer that turns durable Store and Region observations into bounded, recoverable scheduling decisions while reusing the existing split and `move-peer` execution paths.

## What Changes

- Add periodic Store and Region heartbeats carrying capacity, disk usage, Region size, request/load counters, peer role, and migration/split state without user keys or values.
- Add a deterministic scheduler on the MetaServer leader that detects replica-count violations, unavailable Stores, capacity skew, hot Regions, and oversized Regions.
- Add placement safeguards: never place two peers of one Region on the same Store, preserve quorum, serialize topology changes per Region, limit cluster/store concurrency, and apply cooldown plus hysteresis.
- Reuse the online migration protocol for automatic replica moves and the crash-safe split protocol for automatic splits; scheduler commands remain idempotent across timeout, retry, and leader change.
- Persist scheduling operators and their progress in replicated metadata so a new metadata leader can resume, cancel, or reconcile work without issuing duplicate topology changes.
- Add operator controls for enable/disable, dry-run, pause/resume, status inspection, and explicit cancellation. Automatic scheduling is opt-in and disabled by default for compatibility.
- Add fault-injection and convergence tests for expansion, Store failure, skew, hot/oversized Regions, duplicate heartbeats, metadata leader change, partial execution failure, and restart recovery.

### Goals

- Restore configured replica placement after Store failure or expansion without operator-selected source and target Stores.
- Converge capacity and leader/peer distribution within explicit tolerances while avoiding scheduling oscillation.
- Trigger safe Region splits from sustained size or load evidence rather than transient spikes.
- Keep all scheduling decisions observable, bounded, idempotent, and recoverable.

### Non-goals

- Region merge, cross-cluster replication, geographic placement labels, or a general-purpose constraint language.
- Automatically changing replication factor or consistency policy.
- Replacing the existing Raft, migration, split, SDK routing, or transaction protocols.
- Claiming throughput or latency improvement without a reproducible before/after workload.

### Correctness and performance expectations

- Scheduler failover MUST NOT create duplicate migrations, concurrent ConfChanges for one Region, topology gaps, or a placement with fewer voters than the configured replication factor.
- Missing or stale heartbeats MUST make a Store ineligible as a destination; they MUST NOT by themselves authorize unsafe peer removal.
- Scheduling scans and heartbeat persistence add bounded control-plane work. Limits, coalescing, cooldown, and incremental evaluation prevent the scheduler from overwhelming metadata consensus or data-plane recovery.

### Compatibility and rollback

- Existing dynamic and legacy deployments retain current behavior until Auto-Balancer is explicitly enabled; legacy static topology cannot enable it.
- Operators can pause new scheduling while allowing committed operators to reconcile. Disabling the feature leaves the last committed topology intact and returns the cluster to manual `split-region` and `move-peer` operation.
- Before promotion or split commit, an operator can be cancelled through the existing safe cancellation paths. After completion, topology can be restored with an explicit reverse migration; split rollback remains a forward-only operational decision because Region merge is out of scope.

## Capabilities

### New Capabilities

- `automatic-region-scheduling`: Store/Region telemetry, deterministic scheduling policy, durable operators, automatic replica recovery/rebalancing and split triggering, safety limits, observability, and operator control.

### Modified Capabilities

- None. Existing online migration and crash-safe split contracts remain the execution primitives; this change adds automatic decision-making above them.

## Impact

- **Metadata protocol/state:** heartbeat, scheduler configuration, durable operator records, placement health, and scheduling status become replicated metadata state.
- **MetaServer:** one active scheduler runs only on the metadata leader and reconciles operators through bounded worker execution.
- **Storage nodes:** NodeServer reports Store/Region observations and executes existing migration/split commands; no new Raft Group or per-Region scheduling thread is introduced.
- **Administration:** `stratakv-admin` gains balancer configuration, dry-run, status, pause/resume, and cancellation commands.
- **Tests and documentation:** new deterministic policy tests, failover/convergence integration tests, and operational documentation.
