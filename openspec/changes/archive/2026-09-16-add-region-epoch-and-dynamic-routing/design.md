## Context

See [proposal.md](proposal.md) for motivation. Today RegionCatalog is loaded once from regions.conf; ShardRouter holds parallel immutable Region and MvccStorage vectors; RaftMvccStorage owns a fixed channel/stub list; and NodeServer builds a vector plus an unsynchronized RegionId map in its constructor. Requests carry only RegionId, and most routing failures collapse into ErrWrongLeader or StorageError.

The change crosses the metadata service, SDK, transaction coordinator, RPC schema, NodeServer, RegionPeer, Raft dispatch, tests, and deployment. It must retain the established execution model:

- Gateway fibers handle HTTP socket concurrency only.
- SDK, 2PC, metadata RPC, and data RPC remain synchronous work on bounded pthread pools.
- One physical storage node owns one NodeServer, one RpcProvider/listener, and one shared RPC worker pool.
- Each RegionPeer remains an independent Raft group with its own apply loop, MVCC state, Persister, and RocksDB directory.
- TSO remains a separate consensus service so metadata load or failover cannot block timestamp allocation.
- Docs is a separate repository and is updated independently.

The existing Raft implementation already provides ReadIndex, apply-index waiting, durable Persister state, and snapshot installation. The metadata control plane reuses those primitives and does not add a consensus library.

## Goals / Non-Goals

**Goals:**

- Define one descriptor model shared by metadata, clients, storage dispatch, and later split and membership workflows.
- Make metadata writes linearizable and metadata reads safe across leader changes.
- Keep the steady-state route path local and O(log Region count).
- Preserve request and transaction identity through leader retry, metadata refresh, and Region regrouping.
- Make Region lookup and peer lifetime safe when later changes publish or retire peers concurrently.
- Keep a staged migration path in which upgraded binaries can run the unchanged static topology before dynamic mode is activated.

**Non-Goals:**

- A scheduler, Store heartbeat loop, split command, tablet clone, learner promotion, or peer removal.
- A generic cluster-management framework or a replacement for TSO.
- Shared RocksDB instances across Regions.
- Moving blocking transaction or RPC work onto Gateway fibers.
- Automatically trusting static configuration when a dynamic metadata quorum is unavailable.

## Decisions

### 1. Use one canonical descriptor and typed RPC envelopes

Add Protobuf messages RegionEpoch, PeerDescriptor, RegionDescriptor, StoreDescriptor, RegionRequestHeader, RegionResponseHeader, RegionError, and MetadataRevision. RegionEpoch has independent unsigned version and conf_version counters. RegionDescriptor uses a half-open range [start_key, end_key), stable RegionId, ordered peers, optional leader_peer_id, and the metadata revision at which it was read. An empty start or end key represents the configured lower or upper keyspace bound; comparison is bytewise and unsigned.

All Region-scoped KV requests gain RegionRequestHeader. Replies gain RegionResponseHeader containing a RegionError enum and optional leader or current Region descriptors. Existing transaction/business status remains separate from routing status, so an MVCC conflict cannot accidentally trigger route invalidation. Raft transport messages also gain stable source and target peer identifiers plus Region epoch. The first change populates these fields for the fixed peer set; the later membership change will enforce them during joint transitions.

The existing RegionId and Err fields retain their field numbers for staged upgrade. In explicit legacy mode, a server may translate a missing header plus the old RegionId into a deterministic compatibility descriptor. Dynamic mode requires the new header. New clients inspect the structured header first and map a legacy reply only while configured in legacy mode.

Alternative considered: continue parsing Err strings. This was rejected because strings cannot safely carry replacement descriptors or distinguish topology errors from storage failures.

### 2. Run metadata as a separate three-member service

Each stratakv-meta process owns one MetadataServer, one RpcProvider/listener, one MetadataConsensusNode, and one metadata Raft member. The cluster has exactly one metadata Raft group in this phase. MetadataConsensusNode owns the Raft object, an apply queue, one apply thread, waiter tables for proposals and reads, and an immutable published MetadataView. Raft owns its existing replication threads. RPC workers enqueue proposals or request ReadIndex; they never execute state-machine mutation directly.

Metadata mutations are deterministic commands containing a mutation_id, expected metadata revision, and the complete preconditions needed to validate the replacement. The leader performs an early validation for useful errors; the apply thread repeats validation against the committed state. The state machine atomically advances the revision, updates the Region range index or Store/ID state, and records the mutation result in a bounded, snapshotted deduplication table. Invalid commands produce a replicated no-change result, so a response remains stable across retry and leader change.

Routing reads go only through the current metadata leader. The handler obtains ReadIndex, waits until the apply thread reaches the returned index, atomically loads MetadataView, and performs lookup by key, RegionId, or bounded range scan. Followers return NotLeader with a hint. This uses the existing ReadIndex path rather than adding no-op entries for every read.

Metadata state lives in memory as:

- an ordered start-key map for range lookup and scans;
- a RegionId map referring to the same immutable descriptors;
- a StoreId map;
- monotonically increasing high-water marks for Region, peer, and Store IDs;
- metadata revision and mutation deduplication records.

The Raft snapshot serializes a versioned MetadataSnapshot containing all of the above. Snapshot bytes are staged and atomically committed through Persister. Recovery restores a complete validated snapshot, then replays committed log entries. A malformed snapshot prevents that member from serving and requires catch-up or operator repair; it never publishes a partial view.

Initial metadata is installed once by a BootstrapCluster command generated from regions.conf. The command includes a deterministic configuration digest and allocates stable peer IDs and Store IDs as part of committed state. Repeated bootstrap with the same mutation ID is idempotent; a different topology is rejected after initialization.

Alternative considered: embed metadata in TSO. This was rejected because topology reads, future Store heartbeats, and scheduling can be much noisier than timestamp allocation and should have an independent failure and capacity domain.

Alternative considered: persist metadata only in a local RocksDB. This was rejected because local durability does not define cluster-wide commit order. Raft log plus versioned snapshots remains the source of recovery.

### 3. Add a bounded MetadataClient

MetadataClient owns configured metadata endpoints, one channel/stub per endpoint, an atomic leader hint, and request counters. Calls use the caller's steady-clock deadline, bounded exponential backoff with jitter, and a fixed attempt ceiling when no deadline is supplied. NotLeader updates the hint; transport failures rotate endpoints. A timeout never becomes an empty successful topology response.

Metadata mutations carry a stable mutation ID across retries. Lookup responses carry one metadata revision. The client validates descriptor syntax and range coverage before returning data to RegionCache.

MetadataClient is shared by the SDK-side routing stack and by storage-node bootstrap. It performs synchronous MPRPC calls from existing bounded worker threads; it creates no Pulsar fibers and holds no Region cache or registry lock during network I/O.

### 4. Replace ShardRouter storage vectors with a RegionCache snapshot

RegionCache owns an atomically loaded shared_ptr<const RouteTable>. RouteTable contains a sorted descriptor vector, a RegionId index, and its metadata revision. Readers take one shared pointer and use upper_bound, so they need no mutex and retain a consistent view for the duration of grouping or request construction.

Updates use copy-on-write:

1. Fetch descriptors without holding a cache lock.
2. Lock the short publication mutex.
3. Compare metadata revision and Region epochs with the current table.
4. Build and validate a replacement table off to the side.
5. Atomically publish the complete table.

An update may replace only the interval covered by returned descriptors. The replacement must exactly cover that interval, contain no overlaps, and be newer than all superseded entries. An older response is discarded. Per-range refresh slots protected by a small mutex/condition-variable table coalesce concurrent misses; each waiter keeps its own deadline. Slots are removed when the refresh completes.

ShardRouter becomes a façade over RegionCache and returns an immutable RegionRouteHandle containing the descriptor snapshot and a channel-pool reference. RegionChannelPool owns MPRPC channels keyed by StoreId and advertised address; descriptors do not own live Region peers or storage objects. Static mode constructs one RouteTable and deterministic epochs/peer IDs from regions.conf through the same interfaces.

Alternative considered: mutate the current parallel vectors in place under a shared mutex. Copy-on-write was chosen because the route table is read on every operation, topology writes are rare, and immutable handles make batch consistency and object lifetime easier to reason about.

### 5. Centralize data RPC routing and retry

RegionRequestSender is shared by DynamicMvccStorage and non-transactional SDK operations. For each logical operation it:

1. receives an operation identity and original deadline;
2. routes the key or key batch to a RegionRouteHandle;
3. populates RegionRequestHeader and remaining budget;
4. selects the leader hint, then other peers when needed;
5. classifies the structured response;
6. updates only the leader hint for NotLeader, or refreshes the affected range for EpochNotMatch and RegionNotFound;
7. returns a regroup-required result when refreshed boundaries no longer contain the complete batch.

Retries retain the current client_id/request_id pair and all transaction fields. Existing per-lane mutation ordering remains in place. Transactional MVCC commands are already defined by key and start_ts/commit_ts; the existing Raft request deduplication stream continues to suppress transport retry duplicates. Raw writes keep their existing client/request identity. Reads also receive an identity for tracing and consistent envelopes, although they do not need mutation deduplication.

Transport retry, leader retry, and topology refresh share one RetryContext with an absolute deadline, attempt count, backoff state, and last typed error. Backoff never sleeps past the deadline. Storage, MVCC conflict, and protocol errors return directly and do not mutate the cache.

Alternative considered: hide refresh inside each per-Region RaftMvccStorage instance. This was rejected because a split can replace one instance with multiple Regions and the coordinator must see that regrouping is required.

### 6. Regroup transaction work at phase boundaries

DistributedTransactionCoordinator obtains one RouteTable snapshot while forming batches for a phase. Every batch stores the descriptor epoch used to form it. Before prewrite, commit, rollback, lock resolution, and recovery RPCs, RegionRequestSender validates that all keys still fit that descriptor.

If a batch receives a topology error before the Region peer accepts it, the sender refreshes and returns RegroupRequired with the affected keys. The coordinator reuses the same transaction start_ts, primary key, commit_ts, mutation data, and deadline, then creates current batches only for unfinished or uncertain work. Completed batches remain recorded. Operations whose outcome is unknown use the existing idempotent MVCC status/cleanup flow rather than being assumed absent.

No cache lock is held while a coordinator latch, Region scheduler latch, MPRPC call, ReadIndex wait, or lock-resolution operation is active. This lock-order rule prevents a metadata outage from blocking unrelated cached Regions.

### 7. Introduce RegionRegistry with shared peer lifetime

NodeServer owns exactly one RegionRegistry. The registry publishes an immutable map from RegionId to shared_ptr<RegionPeer> through atomic shared-pointer load/store. Updates serialize under one short publication mutex and perform initialization, start, drain, and disk/network work outside it. Both KV and Raft dispatchers acquire and retain a shared_ptr for the entire callback, replacing the current raw RegionPeer pointer.

Each RegionPeer owns an immutable current RegionDescriptor and an atomic lifecycle state: Initializing, Serving, Retiring, or Stopped. Registration accepts only a fully initialized peer, is idempotent for identical identity and epoch, and rejects older or conflicting descriptors. Removal first publishes a map without the peer, then marks it Retiring and drains apply and scheduler work before destroying Raft, Persister, MVCC, and RocksDB state. Existing callbacks keep the shared_ptr alive. This change implements and tests the registry contract but invokes publish/remove dynamically only in tests and startup; the split change will use it in production.

NodeTxnScheduler gains paired RegisterRegion and UnregisterRegion operations and stores lifetime-safe peer handles. TxnRecoveryManager resolves Regions through RegionCache/RegionRegistry instead of retaining the startup catalog. NodeServer still owns one listener and shared RPC worker pool; a Region never creates another NodeServer or listener.

At startup, the listener may answer transport requests while peers initialize, but data and Raft dispatch return a typed unavailable response until the complete initial registry is atomically published. RecoveryManager starts only after publication.

Alternative considered: protect the existing unordered_map with a shared_mutex and return raw pointers. This was rejected because unlocking the map would still allow a concurrent removal to destroy the pointed-to Region.

### 8. Validate topology before any data side effect

NodeServer dispatch first resolves RegionId and target peer ID. RegionPeer then compares both epoch components and validates every key in a single or batch request against its descriptor. Validation completes before a Raft proposal, ReadIndex, scheduler enqueue, MVCC latch acquisition, or RocksDB access.

NotLeader is evaluated after identity, epoch, and range validation so a stale client learns that its topology is stale rather than rotating forever among obsolete peers. EpochNotMatch returns descriptors only from a committed local control-plane update; when none are safely known, it returns the observed current descriptor and forces metadata lookup. User keys and values are not included in diagnostic logs.

The safety invariants are:

1. A data mutation is accepted only by a serving peer whose RegionId, peer ID, epoch, and range match the request.
2. Every published metadata revision describes each configured key exactly once.
3. version advances for range ownership changes; conf_version advances for peer-set changes; neither decreases.
4. A cache or registry reader sees one complete immutable snapshot.
5. A committed metadata mutation and allocated ID survive leader change and restart.
6. A route retry preserves logical operation and transaction identity.
7. No cache or registry publication lock is held during RPC, disk I/O, Raft wait, apply wait, or transaction latch wait.
8. A RegionPeer remains alive until all accepted callbacks and queued work release it.

### 9. Add focused observability and performance gates

Metadata exposes leader, term, commit index, applied index/revision, request latency and result, validation rejects, dedup hits, ReadIndex latency, snapshot count/size/install failures, and ID high-water marks.

Clients expose cache revision/size, hit/miss, refresh single-flight waiters, refresh latency/result, retries by typed cause, leader-hint changes, regroup counts, retry exhaustion, and remaining deadline at failure. Nodes expose registry revision, active/initializing/retiring peers, lookup misses, epoch/range rejects, registration conflicts, and retirement time.

Structured logs use RegionId, peer ID, StoreId, epoch, metadata revision, request type, and request ID. They omit raw keys, values, and transaction payloads.

The performance gate compares static compatibility mode before and after the change and dynamic mode with a warm cache. Measure single-key read/write and multi-Region transaction throughput, p50/p95/p99 latency, CPU, allocations, metadata QPS, and refresh amplification. The warm-cache target is no metadata RPC and no contended mutex on the request path; any material regression must be explained before the next split change starts.

## Request and State Flows

### Cached transactional request

Gateway accepts HTTP work on a Pulsar fiber and submits blocking SDK/2PC work to its bounded pthread pool. The coordinator loads a RouteTable snapshot, groups mutations, and calls RegionRequestSender. Synchronous MPRPC reaches the physical node's shared RpcProvider. NodeServer retains a RegionPeer handle from RegionRegistry; RegionPeer validates the header and key range, then uses its existing Region scheduler and Raft group. The Region apply loop applies MVCC state to that Region's RocksDB. The response travels back through the same layers. No metadata RPC occurs on a cache hit.

### Stale route

The storage peer rejects the header before side effects and returns a typed error. RegionRequestSender either changes the leader hint or asks MetadataClient for the affected range. RegionCache validates and atomically publishes the newer descriptors. A single-key operation reroutes directly; a batch returns to the coordinator for regrouping. The same RetryContext, transaction timestamps, and request identity continue until success, a non-routing error, or the original deadline.

### Metadata mutation and failover

An administrative caller sends a stable mutation ID to the metadata leader. The leader proposes the command to its Raft group. After quorum commit, the apply thread validates and atomically publishes a new MetadataView, advances applied revision, records the result, and wakes the RPC waiter. If the response or leader is lost, retrying the mutation reaches the same deduplication result. Linearizable reads on the new leader use ReadIndex and wait for its apply thread before loading the view.

### Node bootstrap and later retirement

The node obtains one metadata revision, initializes all locally assigned RegionPeer objects and their independent Raft/MVCC/RocksDB resources, registers them with NodeTxnScheduler, and atomically publishes the complete RegionRegistry. Later split work will remove a parent entry, publish child entries only after their durable initialization, and retire the parent after callbacks drain; that transition is outside this change.

## Risks / Trade-offs

- [Metadata quorum becomes a new operational dependency] → Keep cached routes usable for matching epochs, deploy three members across failure domains, expose quorum health, and require explicit mode selection.
- [A stale cached route can add retry latency] → Return typed local hints, coalesce refreshes, cap backoff by the original deadline, and measure refresh amplification.
- [Copy-on-write tables allocate during topology churn] → Keep updates rare, replace only affected intervals logically, and benchmark allocation/CPU before split scheduling is enabled.
- [ReadIndex depends on current Raft correctness] → Reuse the exercised Region/TSO implementation and add leader-change, timeout, and apply-lag fault tests specifically for metadata.
- [Request-header migration can break mixed binaries] → Preserve old field numbers, add legacy translation only under explicit static mode, and upgrade all binaries before activating dynamic mode.
- [A registry retirement can wait indefinitely on callbacks] → Stop admission first, track outstanding work by component, use a bounded drain deadline, and report rather than destroy live resources.
- [Metadata snapshots can grow with mutation deduplication records] → Bound records by applied revision and retention window while never evicting an entry that an in-flight retry window can still reference.
- [Metadata leader hints may be stale] → Treat hints as optimization only; endpoint rotation and terms/revisions determine correctness.
- [This foundation adds overhead before it adds capacity] → Gate on warm-cache benchmarks and keep static compatibility mode available until the split change is verified.

## Migration Plan

1. Add Protobuf fields and compatibility parsing, Region descriptor types, typed errors, RegionRegistry, RegionCache, and tests while keeping static mode as the default.
2. Deploy upgraded storage, Gateway, SDK, TSO-compatible, and administration binaries in static mode. Verify deterministic compatibility descriptors and baseline performance.
3. Deploy three metadata members with separate versioned data directories. Commit BootstrapCluster from the current regions.conf and compare the resulting topology digest with every node's static catalog.
4. Restart storage nodes in dynamic-bootstrap mode, one node at a time, while clients remain static. Verify registry contents, peer epochs, and Raft health.
5. Enable dynamic routing for canary clients, then all Gateways/SDKs. Monitor cache refresh, structured errors, metadata quorum, transaction result-unknown rate, and latency.
6. Retain regions.conf as the audited bootstrap/rollback snapshot until the later split or membership changes first mutate topology.

Rollback before any dynamic topology mutation consists of disabling dynamic routing, restarting clients and nodes in static compatibility mode, and stopping metadata members. Region RocksDB and TSO data remain untouched. After a future split or peer reconfiguration commits, rollback requires a separately planned topology reconciliation and cannot use the old static catalog directly.
