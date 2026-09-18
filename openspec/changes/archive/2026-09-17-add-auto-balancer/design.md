## Context

See `proposal.md` for motivation. The prerequisite execution mechanisms already exist: metadata mutations are replicated by the metadata Raft group, `split-region` performs a crash-safe two-phase split, and `move-peer` performs learner catch-up followed by promotion and source removal. The missing layer is a durable decision loop that observes the cluster, admits at most one topology operator per Region, and drives those mechanisms without depending on a long-lived CLI process.

The control plane uses synchronous MPRPC. Blocking scheduler RPC work therefore runs on bounded pthread workers, never on Pulsar Fiber. A storage process still owns exactly one `NodeServer`; each local `RegionPeer` remains an independent Raft Group with its own Apply/MVCC/RocksDB state.

## Goals / Non-Goals

**Goals:**

- Make planning deterministic and side-effect free, with admission separately committed through metadata consensus.
- Persist the minimum state needed to survive duplicate reports, response loss, metadata leader change, and process restart.
- Reuse one shared topology-operation executor for manual and automatic split/migration paths.
- Bound thread count, queued work, heartbeat write rate, active topology work, and Store resource impact.
- Preserve current SDK, Gateway, 2PC, Raft Apply, MVCC, and RocksDB request paths.

**Non-Goals:**

- A distributed scheduler on every metadata replica; only the current leader is active.
- Per-Region scheduler threads, user-key sampling in telemetry, Region merge, label-aware placement, or dynamic replication-factor changes.
- A separate leader-transfer protocol in this change. Leader count contributes to load scoring, but peer placement remains the topology action.
- Replacing existing migration or split safety checks with scheduler assumptions.

## Decisions

### 1. Replicated telemetry snapshots with monotonic admission

`metadata_rpc.proto` gains Store/Region observation messages and a `ReportStoreHeartbeat` RPC. One `NodeServer`-owned `StoreHeartbeatReporter` thread periodically snapshots registry and peer counters, releases all registry/peer locks, and then performs the blocking RPC. It has one bounded retry slot: a newer local sample coalesces an unsent sample rather than creating an unbounded queue.

The metadata state machine stores one latest observation per Store plus the last accepted sequence. Applying a duplicate is a no-op; an older sequence or Region epoch is rejected without changing the revision. Wall-clock receipt time is converted to a leader-provided logical expiry deadline in the command, so snapshot restore preserves the accepted freshness boundary. A new leader waits for a linearizable read and uses the replicated deadlines; it does not trust an uncommitted follower cache.

Alternatives considered:

- **Leader-memory-only heartbeats:** lower Raft traffic, but leader failover loses eligibility and consecutive-window evidence. Rejected for the first implementation.
- **One metadata entry per peer:** simple but multiplies consensus traffic. One Store heartbeat batches all local observations and changes are coalesced before proposal.

### 2. Pure planner plus consensus admission

`AutoBalancerPlanner` is a pure component: input is an immutable `ClusterSchedulingSnapshot`, output is a ranked list of `OperatorPlan` values plus rejection reasons. It owns no threads and performs no RPC. Stable ordering is `(priority, score benefit descending, region_id, source_store_id, target_store_id)`.

Priority order is:

1. repair replica-count/failed-Store risk;
2. split a Region with sustained size/load evidence;
3. reduce capacity and peer-count skew.

`MetadataStateMachine` performs final admission with a `CreateSchedulingOperator` mutation. It compares the plan's source metadata revision and Region epoch, verifies fresh Store eligibility, checks the per-Region active-operator index and cluster/per-Store budgets, then atomically stores the operator. This compare-and-create boundary prevents stale planners, manual operations, or two leaders from both winning.

Alternative considered: make the scheduler mutate topology directly while scanning. Rejected because planning would hold metadata locks across RPC and would be difficult to test deterministically.

### 3. One leader-owned scheduler and a bounded executor

`MetaServer` owns exactly one `AutoBalancer` object. Its lifetime is bounded by the metadata service lifetime:

- one evaluation `std::thread` wakes on a configurable interval or committed-state notification;
- one executor `std::thread` reconciles a bounded queue of admitted operators;
- a mutex and condition variable protect only the in-memory wake/queue state;
- replicated operator state is accessed through state-machine snapshot/query APIs, never while the scheduler mutex is held;
- leadership loss or shutdown sets a stop token, wakes both threads, prevents new dispatch, and joins them.

The executor processes a configurable maximum number of active operations globally and per Store. The initial implementation uses one worker for conservative serialization, while the replicated budget fields and queue permit a later bounded pool without changing the contract.

No scheduler object or callback owns a `RegionPeer`. RPC calls address Store/Region identities and resolve a peer through `NodeServer` for the duration of the request. This avoids `MetaServer → executor → RegionPeer → callback → MetaServer` ownership cycles.

### 4. Shared topology-operation coordinator

The orchestration currently embedded in `stratakv-admin` is extracted behind `TopologyOperationCoordinator` with two entry points:

- `DriveMovePeer(operator_id, region, source, target, deadline)`;
- `DriveSplitRegion(operator_id, region, split_candidate, deadline)`.

Manual CLI commands and `AutoBalancer` call the same coordinator, so preflight, mutation identifiers, phase transitions, deadline behavior, and cancellation do not diverge. The coordinator uses synchronous metadata/NodeServer/RegionLeader RPC on the bounded executor thread. It does not enter Gateway, SDK/2PC, MVCC, or RocksDB code directly.

Every subcommand derives its mutation ID from the stable operator ID and phase. Retrying after an unknown response therefore observes the original result. The coordinator records phase progress through replicated `UpdateSchedulingOperator` mutations after reconciling the actual Region descriptor and migration/split status.

For automatic split, the heartbeat carries only an opaque split-candidate generation. At dispatch time the coordinator asks the current Region leader to resolve that generation to a boundary, immediately submits it through the existing split path, and never includes the boundary in metrics or logs. The existing split metadata may persist the boundary as required for deterministic recovery.

Alternative considered: shell out to `stratakv-admin`. Rejected because process lifetime, output parsing, cancellation, and idempotent recovery would not be part of the service contract.

### 5. Operator state machine and recovery

Replicated operator phases are:

```text
Pending -> Dispatching -> Waiting -> Succeeded
    |          |             |
    +----------+-------------+-> Cancelling -> Cancelled
                               \-> Failed
```

`Succeeded`, `Cancelled`, and `Failed` are terminal. `CancelRequested` is a flag rather than a competing phase, allowing reconciliation to decide whether the irreversible point has passed. Migration becomes forward-only after learner promotion; split becomes forward-only after metadata phase two commits. After those points, cancellation is rejected and recovery completes forward.

On startup or leadership acquisition, the executor scans non-terminal operators and compares them with current metadata and data-plane status:

- no target or prepared split exists: retry dispatch with the same identity;
- learner exists before promotion: resume catch-up or cancel on deadline;
- target is promoted: finish source removal and metadata commit;
- split topology is published: finish local materialization/status reconciliation;
- observed epoch no longer matches and the intended outcome is absent: record a stale-plan failure and cooldown.

Terminal operators are retained for a configurable history window, then compacted into counters and last-outcome summaries during metadata snapshot creation.

### 6. Policy, hysteresis, and configuration

`AutoBalancerConfig` is replicated and versioned. Initial defaults are conservative and disabled:

- evaluation interval and heartbeat timeout;
- minimum free bytes and maximum disk-used ratio;
- replica count target;
- capacity/load imbalance threshold;
- consecutive windows required for a split;
- Region size and request-rate split thresholds;
- Region cooldown;
- global and per-Store active-operation limits.

The planner computes normalized Store scores from available capacity, Region count, approximate Region bytes, and recent request deltas. Each component and weight is reported in dry-run output. A move must improve the source/destination score difference by more than hysteresis. Safety repair bypasses cooldown but never bypasses placement, freshness, epoch, quorum, or in-flight-operation checks.

Configuration updates validate ranges as one metadata mutation. Enable, pause, and disable are distinct: pause stops new admission while existing operators reconcile; disable also stops evaluation but does not roll topology back.

### 7. Request and state flows

Heartbeat flow:

```text
NodeServer reporter -> snapshot Registry/Region metrics -> Metadata RPC
       Metadata leader -> Raft proposal -> MetadataStateMachine apply
       AutoBalancer wake <- committed scheduling snapshot
```

Automatic migration flow:

```text
Planner -> ranked move plan
  -> metadata compare-and-create operator
  -> executor/coordinator
  -> PrepareMovePeer -> mount target -> AddLearner/catch-up
  -> PromoteLearner -> RemovePeer -> CommitMovePeer
  -> metadata terminal operator + cooldown
```

Automatic split flow:

```text
consecutive observations -> ranked split plan
  -> metadata compare-and-create operator
  -> resolve opaque candidate at current Region leader
  -> existing split prepare/apply/publish path
  -> metadata terminal operator + cooldown
```

Client traffic is unchanged. During topology changes, Gateway requests continue through the SDK/2PC bounded pthread pools; routing errors refresh `RegionCache`; each Region independently commits through its Raft Apply loop and applies MVCC/RocksDB changes as before.

### 8. Safety invariants

1. At most one non-terminal topology operator exists per Region.
2. A plan is admitted only against its source metadata revision and Region epoch.
3. A stale Store is never a destination; stale heartbeat evidence alone never removes a voter.
4. Existing migration and split protocols remain the authority for quorum, ConfChange, epoch, range, transaction, and durability safety.
5. No scheduler lock is held during consensus proposal, RPC, Region lookup, Raft Apply, MVCC, or RocksDB work.
6. Losing metadata leadership prevents new dispatch; replay with stable identities is idempotent.
7. Pause/disable never abandons an admitted operator in an unsafe intermediate state.

### 9. Observability

Metrics include heartbeat accepted/stale/duplicate counts, evaluation duration, candidates by reason, rejection counts, queue depth, active operators by type/phase, retry/cancel/failure counts, cooldown suppressions, and per-Store scheduling score components. Structured logs include operator, Region, peer, Store, epoch, revision, phase, and reason only. Dry-run returns the same ranked decisions without admission.

## Risks / Trade-offs

- **[Heartbeat consensus traffic]** -> Batch per Store, use multi-second intervals, coalesce unsent updates, and benchmark metadata apply latency before changing defaults.
- **[Telemetry noise causes oscillation]** -> Require consecutive evidence, hysteresis, stable tie-breaking, and post-operation cooldown.
- **[Scheduler competes with foreground replication]** -> Default to one active operation, enforce Store budgets, and rely on existing snapshot throttling.
- **[A failed Store is mistaken for a partition]** -> Treat it as ineligible but require learner promotion and Raft membership confirmation before removal.
- **[Four-voter promotion window reduces write availability]** -> Keep promotion/removal adjacent and never start another ConfChange for that Region.
- **[Automatic split has no merge rollback]** -> Require sustained evidence and expose dry-run; after commit recovery is forward-only.
- **[Leader change duplicates side effects]** -> Persist operator identity and phase, derive mutation IDs deterministically, and reconcile state before retry.

## Migration Plan

1. Add protocol fields and metadata snapshot compatibility while the balancer remains disabled.
2. Deploy heartbeat reporters and validate accepted/stale metrics without admitting operators.
3. Enable dry-run to validate scores, constraints, and convergence plans against a test cluster.
4. Enable one global operator with conservative thresholds; increase limits only after correctness and control-plane measurements.
5. Roll back by pausing and then disabling scheduling. Non-terminal operators reconcile to a safe terminal state; completed topology remains valid and manual commands continue to work.
