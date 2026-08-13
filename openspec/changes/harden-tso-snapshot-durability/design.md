## Context

See `proposal.md - Why` for the motivation. The current implementation already has a three-member TSO Raft group, a durable local timestamp reservation fence, leader failover, and a cluster-aware `RemoteTimestampOracle`, but four implementation details prevent those pieces from forming one reliability boundary:

- `Raft::InstallSnapshot` advances `m_commitIndex` and `m_lastApplied` before it enqueues the snapshot. Both `TsoConsensusNode::ApplyLoop` and `RegionPeer::GetSnapShotFromRaft` then call `CondInstallSnapshot`; that method rejects an index that is not greater than `m_commitIndex`. A received snapshot can therefore be marked applied by Raft while never reaching the state machine.
- `Persister::Save` and `SaveRaftState` truncate the live files and call C++ stream `flush()`. There is no checksum, generation, `fsync`, atomic publication, or recoverable pairing between Raft state and snapshot. `Raft::readPersist` also logs and ignores malformed binary state, which can turn corruption into a term-zero bootstrap.
- `RemoteTimestampOracle` performs up to 20 complete endpoint rounds. Each `MprpcChannel` has an independent 500 ms connect timeout and 5 s socket timeout, so retries do not share one operation deadline and can occupy a Gateway request pthread much longer than the intended transaction boundary.
- `stratakv-tso` accepts an ordered `--peers` list but no deployment mode or fault-domain identity. The default launcher starts all three members on loopback, which is process redundancy only.

The existing architectural boundaries remain unchanged. In Fiber mode, Gateway Fibers only perform HTTP socket read/wait/write; the 16-worker bounded request executor runs blocking SDK and 2PC calls, and the coordinator's 8-worker bounded Region executor runs concurrent synchronous Region RPCs. A storage process owns one `NodeServer`, one `RpcProvider`/port and 32 shared RPC workers; `NodeServer` routes by `RegionId` to independent `RegionPeer` Raft groups. TSO is a separate control-plane Raft group and is not a `NodeServer` or Region.

## Goals / Non-Goals

**Goals:**

- Give snapshot receipt, durable staging, state-machine installation, and applied-index publication one explicit state transition shared by TSO and Region peers.
- Make every acknowledged Raft stable-state mutation recoverable as either the previous complete generation or the new complete generation after process crash or host power loss.
- Bound each remote TSO operation by one monotonic-clock deadline across pool waiting, connect, send, receive, endpoint rotation, retry, and backoff.
- Make local-process and three-fault-domain deployment explicit and mechanically verifiable without changing Raft membership at runtime.
- Add deterministic fault, timeout, recovery, and performance evidence suitable for regression testing and rollout decisions.

**Non-Goals:**

- The snapshot change does not add dynamic membership, snapshot streaming/chunking, or a second apply pool.
- The persistence change does not replace RocksDB durability or introduce a general storage engine/WAL for every file in the repository.
- Fault-domain labels document and validate placement; they do not discover hosts, provision machines, fence a failed host, or provide TLS identity.
- The client deadline does not make synchronous MPRPC asynchronous and does not move SDK/2PC work onto Pulsar Fibers.
- TSO batching, timestamp leases, multi-TSO-group routing, and cross-datacenter consensus remain out of scope.

## Decisions

### 1. State-machine acknowledgement is the authoritative apply boundary

`Raft` will distinguish three indices instead of overloading `m_lastApplied`:

- `commitIndex`: the highest index committed by Raft; for an installed snapshot it may advance after the snapshot and matching Raft state are durably staged.
- `dispatchedIndex`: the highest command or snapshot placed on the existing `LockQueue<ApplyMsg>`.
- `appliedIndex`: the highest index positively acknowledged by the owning state machine. `NodeStatus.lastApplied` will expose this value.

The shared API will replace `CondInstallSnapshot` with explicit completion calls, conceptually `AcknowledgeApplied(index, kind)` and `FailApply(index, error)`. The exact method names may follow local style, but the ownership rule is fixed: only the TSO or Region apply loop can advance `appliedIndex`, and only after its durable state-machine operation succeeds.

Snapshot receipt follows this order:

1. The Raft RPC worker validates term and monotonic snapshot index under `Raft::m_mtx`.
2. `Persister::Save(raftState, snapshot)` durably publishes a complete generation before the follower returns from `InstallSnapshot`.
3. Raft updates the compacted-log boundary and `commitIndex`, records a pending snapshot, and enqueues one `ApplyMsg`. It does not advance `appliedIndex`.
4. The existing TSO `ApplyLoop` parses the high-water mark and calls `PersistentTimestampOracle::Observe`; the existing Region apply loop restores MVCC/RocksDB snapshot state. A stale index at or below the state machine's own applied index is treated as an idempotent acknowledgement without changing data.
5. After success, the apply loop acknowledges the index to Raft. Raft advances `appliedIndex`, clears the pending recovery barrier, and permits dispatch of committed entries beginning at `snapshotIndex + 1`.
6. A parse, RocksDB, MVCC, or timestamp-fence persistence failure is fatal for that member. It logs the identity, generation and index and exits without reporting readiness or serving requests.

On startup, `Raft::init` reads one validated `PersistedRaftBundle`. If it contains a snapshot, Raft enqueues that snapshot before any post-snapshot log and starts with the recovery barrier closed. `TsoConsensusNode` and `RegionPeer` will no longer read and install `Persister::ReadSnapshot()` independently. This eliminates the two competing snapshot owners. The apply queue preserves order, and Raft does not dispatch `N+1` until snapshot `N` is acknowledged.

For regular commands, the TSO and Region apply loops also acknowledge after their state-machine write succeeds. This makes `lastApplied` a real state-machine value instead of a delivery cursor. A separate dispatch cursor prevents duplicate queueing while acknowledgement is pending.

Alternative considered: keep the current early `m_lastApplied` update and make the upper layer install snapshots unconditionally. That fixes the immediate predicate bug, but a freshly elected member could still advertise itself as caught up before the state-machine write completes. The explicit acknowledgement is chosen because it closes both the data path and readiness race.

### 2. Leadership and service readiness are gated separately

Raft election is allowed while a state machine is catching up so that consensus traffic is not unnecessarily stopped, but application service is gated:

- When a candidate becomes Leader, Raft records the index of its current-term no-op as `leadershipBarrierIndex`.
- `Raft::IsStateMachineReady()` is true only when no snapshot is pending and `appliedIndex >= leadershipBarrierIndex` for the current term.
- `TsoConsensusNode::RequireReadyLeader` checks this readiness result before `Next`, `Peek`, or `Observe`. Its first candidate is chosen only after the recovered snapshot, inherited committed log, and current-term barrier have all been applied.
- `Raft::Start` rejects new application proposals during recovery. `RegionPeer` also rejects Leader reads until the same recovery barrier is open; follower-read rules remain unchanged.

TSO keeps its local reservation fence: `PersistentTimestampOracle::Observe` is completed before a timestamp becomes the committed high-water mark. Thus gaps remain legal, but duplicates and regression do not. A member that obtained Raft leadership but is not ready returns a retryable not-leader/not-ready result and never allocates locally.

Alternative considered: prevent an unready member from participating in elections. This can reduce availability because the member may be needed for quorum even though its state-machine apply is briefly behind. Separating consensus leadership from application readiness retains quorum progress while failing application requests safely.

### 3. Persister uses immutable generations and two atomic manifest slots

Each Raft peer continues to own exactly one `Persister`; Persisters are never shared between TSO members or Region peers. Its storage directory will contain:

- immutable `raft.<generation>` files;
- immutable `snapshot.<generation>` files only when a new snapshot is published;
- two alternating `manifest.0` and `manifest.1` slots, each naming one Raft-state generation and one snapshot generation;
- a storage identity marker binding Raft group, peer/node ID, format version, and directory.

Every file envelope contains magic, format version, generation, payload length, and CRC32C. CRC is corruption detection, not authentication. Integer fields use a defined little-endian representation.

`SaveRaftState` writes a new Raft-state generation that references the currently published snapshot generation. `Save` writes both a new Raft-state file and new snapshot file. Publication order is:

1. create a same-directory temporary file with exclusive creation;
2. write the complete envelope and `fdatasync`/`fsync` it;
3. atomically rename it to its immutable generation name;
4. after every required data file is durable, write and sync the next manifest slot through its own temporary file and atomic rename;
5. `fsync` the directory before returning success;
6. retain the newest two complete manifest generations and remove older unreferenced files only after publication.

Recovery scans both manifest slots, validates every referenced file and identity, and selects the highest complete generation. A torn newest generation therefore falls back only to a completely validated previous pair. If established storage has no valid pair, startup fails closed; it never silently becomes an empty Raft peer.

The in-process `Persister::m_mtx` serializes generation assignment, publication, reads, and cleanup. The API returns a structured result or throws; empty content is no longer overloaded to mean missing, valid empty snapshot, and read failure.

Fresh storage is recognized before any files are created and receives an identity marker. Existing fixed-name files are read through a one-way legacy decoder. Valid legacy state is migrated on the first successful write. Ambiguous empty or malformed established legacy state requires an explicit operator initialization/recovery action. The old binary can read the backup copied before migration, but it cannot read new generations; direct downgrade after new-format publication is blocked.

`PersistentTimestampOracle` retains its segment reservation algorithm and its existing write-sync-rename-directory-sync ordering. Its file gains a version, length, and checksum envelope while continuing to read the legacy decimal limit; the first new reservation migrates it. This keeps the local high-water fence independently detectable even when Raft files are valid.

Alternative considered: atomically rename two live files. Two renames cannot be made one atomic transaction, so recovery can observe a new Raft state with an old snapshot. A manifest that publishes immutable files as one pair provides an unambiguous commit point without rewriting a potentially large snapshot on every term or vote update.

### 4. Stable-state writes precede success responses

The following ordering is mandatory:

- A follower persists a higher term, vote, or changed log before returning a successful `RequestVote` or `AppendEntries` response.
- `InstallSnapshot` returns only after the bundle is published, but does not claim state-machine readiness until the apply acknowledgement.
- A Leader counts a follower response toward a majority only after that follower has completed its durable write.
- TSO returns a timestamp only after the command is majority committed and applied to its local durable reservation fence.

Persistence failure aborts the affected member. Continuing as a volatile Raft participant after a failed stable-state write could acknowledge data it cannot recover and is therefore not allowed.

### 5. Remote TSO calls carry one absolute deadline

An immutable `TsoClientOptions` is added with a default operation timeout of 5 seconds and a 50 ms election backoff. Existing `RemoteTimestampOracle` constructors and `Client::Connect` remain source-compatible and use the default; an overload permits tests and deployments to supply a different timeout.

At entry to `Next`, `Peek`, or `Observe`, the client computes one `steady_clock::time_point deadline`. Every endpoint attempt receives the same absolute deadline through `MprpcController`. The channel uses the remaining budget for:

- waiting for a pooled connection lock;
- nonblocking connect/poll, capped by the existing 500 ms connect limit;
- complete frame send and response receive, capped by the remaining operation budget;
- endpoint rotation and retry backoff.

`MprpcChannel::Connection::mutex` becomes deadline-aware so a request cannot exceed its budget while queued behind another synchronous call. Existing RPC callers without an explicit deadline retain their current per-attempt connect and I/O defaults. No attempt or sleep starts when no budget remains.

The leader cache remains the atomic preferred endpoint. Calls rotate from that endpoint through the stable member order. A transport failure after `Next` was committed but before its reply is ambiguous; retrying may allocate and return a later timestamp, producing a legal gap. The client never reconstructs or reuses the unknown timestamp. `Observe` is monotonic and idempotent, and `Peek` has no allocation side effect.

One `RemoteTimestampOracle` may be shared by all Gateway request pthreads. Endpoint lists/options are immutable, the preference and counters are atomic, and each pooled socket is serialized independently. There is no client-wide mutex and no new client thread.

At the transaction layer, failure to obtain `startTs` fails `Begin` before any mutation RPC. Failure to obtain `commitTs` after Prewrite prevents Primary Commit and invokes the existing rollback path for prewritten keys. A timeout after a Primary Commit request is sent remains an unknown transaction outcome and must be resolved through Primary lock/commit-status lookup, not blind rollback.

Alternative considered: retain a fixed retry count and shorten every socket timeout. Fixed attempts still multiply with endpoint count and pool contention, while short timeouts can prematurely fail a healthy slow request. One absolute deadline expresses the externally useful bound directly.

### 6. A single ordered topology file defines deployment identity

Distributed mode adds a line-oriented topology file that can be parsed without a new third-party dependency. It records `mode`, then exactly three ordered member records containing `node_id`, routable `host:port`, `fault_domain`, and `data_dir`. All members load the same content and derive the same peer order from ascending node ID.

`stratakv-tso --node-id N --topology <path>` starts one member on its host. Existing `--node-id --peers --state-file` remains the local/compatibility form. The local `stratakv-server` launcher continues to start `127.0.0.1:26300..26302`, labels the result `local-process`, and does not pretend to orchestrate remote machines.

Before opening a listener or creating persistent files, validation rejects missing IDs, duplicate IDs/endpoints/data directories, non-resolvable or loopback endpoints in distributed mode, duplicate/empty fault domains, non-writable local member storage, and a storage identity marker that conflicts with the selected member. Endpoint order is generated, not separately hand-authored, so members cannot disagree about ID mapping.

Fault-domain identity is reported through additive TSO status fields. Deployment verification contacts all configured endpoints and checks member identity, one Leader, a reachable majority, Leader state-machine readiness, and applied/commit indices. Three listening processes alone are not considered healthy. Loss of one domain leaves two members able to commit; loss of two makes `Next` and `Observe` fail within the client deadline.

Alternative considered: add only a `--fault-domain` flag to each process. Independent command lines cannot prove that all members use the same ordered membership or that domains and directories are unique. A shared topology document gives validation and operations one source of truth.

### 7. Ownership, threads, queues, and lifetime remain explicit

| Scope | Instances | Thread ownership | Queues and synchronization | Lifetime/change |
|---|---:|---|---|---|
| Gateway process | One shared `Client`/`RemoteTimestampOracle`; three `EndpointClient`s by default | Fiber mode keeps the configured HTTP I/O pthreads (default 4), 16 blocking request pthreads, and the coordinator's 8 Region pthreads | Four socket connections and per-connection locks per TSO endpoint; atomic preferred endpoint; bounded request/Region queues | Process lifetime; this change adds deadlines/counters, no threads |
| Each TSO member process | One `TsoService`, `TsoConsensusNode`, `Raft`, `Persister`, `PersistentTimestampOracle`, apply queue, and `RpcProvider` | 32 generic RPC workers; for a three-peer Raft, existing heartbeat, election, proposal, two replication, and Raft applier threads; existing TSO state-machine apply and status threads | `Raft::m_mtx`, apply condition/queue, `proposalMutex_`, `waitersMutex_`, `Persister::m_mtx`, timestamp-oracle mutex | Process lifetime; fatal apply/persist errors terminate the member; no new long-lived thread |
| Each storage node | One `NodeServer`, `RpcProvider`/port and 32 shared RPC workers | Existing node RPC listener/workers | Region dispatch map guarded by existing node synchronization | Unchanged |
| Each local Region | One `RegionPeer`, its own `Raft`, `Persister`, apply loop, MVCC/Persister path and RocksDB-backed data | Existing Region Raft and apply/status threads; RPC workers remain node-shared | Region apply queue, Raft lock, Region/MVCC locks and TxnScheduler latches | Snapshot acknowledgement is added to the existing apply loop; no per-Region RpcProvider |

The 32 RPC workers are transport/service workers, not Raft threads. The Gateway Fibers do not execute TSO retry loops, 2PC, or synchronous RPC; those remain on bounded pthread executors.

### 8. Observability exposes each safety gate

The additive TSO `Status` response and status JSON include deployment mode, endpoint, fault domain, storage identity/generation, Raft term/role/commit index, dispatched index, state-machine applied index, pending snapshot index, leadership barrier index, high-water mark, and a final `ready` boolean.

Gateway metrics add TSO operation latency, attempts, endpoint failovers, not-leader responses, transport failures, ambiguous `Next` retries, and deadline-exceeded totals. Persistence and snapshot failures emit structured logs containing group/peer identity, generation, index, operation stage and `errno`; timestamp values and mutation contents are not logged.

### 9. Verification is deterministic and includes before/after evidence

Tests use stage-addressable fault injection at Persister write, data sync, rename, manifest sync, and directory sync boundaries. Hooks are dependency-injected/test-only and add no production sleeps. Each injected crash restarts a subprocess and verifies selection of the newest complete generation or the previous complete generation, never an empty state. Separate tests truncate and corrupt every envelope and verify fail-closed startup.

Snapshot integration stops one follower, advances past the 256-entry TSO snapshot threshold and retained log boundary, restarts it, verifies snapshot acknowledgement and post-snapshot log continuity, then makes it Leader and asserts its first timestamp is greater than every previously observed timestamp. The shared contract is also exercised with a Region follower and persisted MVCC data.

Timeout tests use a blackhole endpoint plus configurable short client deadlines. They measure wall-clock duration for pool wait, connect, partial send/receive, all endpoints down, Leader kill/election, and concurrent use of one shared client. Transaction tests separately cover startTs failure, commitTs failure before Primary Commit, and ambiguous Primary Commit resolution.

Topology tests validate every rejected configuration and run three loopback-isolated processes labeled as distinct simulated domains; deployment documentation also records a real three-host verification procedure because loopback tests cannot prove physical independence.

The benchmark records identical pre-change and post-change TSO workloads, hardware/build flags, segment size, durability filesystem, concurrency, throughput, and p50/p95/p99 latency. Results are recorded rather than hidden by a relaxed mode. Correctness tests always run with durability enabled; any material regression is explained and optimized without bypassing `fsync` ordering.

## Risks / Trade-offs

- [Extra `fsync` latency lowers Raft/TSO throughput] → Publish only changed state plus a small manifest, keep timestamp segment reservation, measure the same workload before/after, and do not rewrite snapshots on ordinary term/log updates.
- [Two retained generations consume more disk] → Cleanup only unreferenced generations after directory sync and expose retained bytes/generation in diagnostics.
- [A member can be Raft Leader but temporarily not application-ready] → Expose both states, return a retryable response, and let the client rotate within its deadline.
- [Crash between durable snapshot staging and state-machine apply repeats installation] → State-machine restore is monotonic/idempotent and applied acknowledgement is volatile until the restore succeeds again.
- [Legacy empty files cannot be distinguished reliably from truncated history] → Recognize a truly new directory before file creation; require explicit operator action for ambiguous established storage and never silently bootstrap it.
- [Deadline-aware socket changes affect shared MPRPC code] → Activate absolute-deadline behavior only when the controller supplies one, retain current defaults for other RPCs, and add transport-level boundary tests.
- [Fault-domain labels may claim independence that deployment does not have] → Label them as operator assertions, reject obvious loopback/duplicates in distributed mode, and make the runbook require host/storage evidence.
- [Detached process-lifetime threads complicate graceful shutdown tests] → Reliability tests use subprocess boundaries and fatal exit semantics; converting all Raft threads to joinable lifecycle management is a separate change.

## Migration Plan

1. Capture baseline TSO correctness and performance results and back up every TSO/Region Raft directory and TSO reservation file.
2. Deploy readers that understand legacy and generation formats while still preserving the old files. Validate one member offline against copied data before changing a quorum member.
3. Roll TSO members one at a time. Wait for snapshot/log catch-up, state-machine readiness, and a healthy two-member majority before proceeding. Keep static membership and endpoints throughout the rollout.
4. Roll storage nodes one at a time so Region peers migrate their individual Persisters without losing Region quorum.
5. Enable the 5 s Gateway TSO deadline and new metrics, then validate Leader kill, one-domain loss, all-endpoints-down latency, and transaction recovery behavior.
6. For production, start one TSO member in each declared fault domain from the same validated topology file and switch health checks from process count to readiness/quorum checks.
7. Compare post-change benchmark results with the recorded baseline and attach both to the optimization record.

Rollback is permitted before a member publishes the new format by restoring the old binary. After publication, stop the affected member and restore its pre-upgrade backup (or use an explicit offline export tool that recreates the legacy pair); never point an old binary at new-format storage. Roll back one member at a time while retaining quorum. Configuration rollback may return to local mode only for development, not as a production high-availability substitute.
