# automatic-region-scheduling Specification

## Purpose

Provide opt-in, crash-recoverable automatic Region scheduling that uses fresh cluster telemetry to repair replica placement, rebalance capacity, and trigger safe splits without bypassing existing Raft, migration, or transaction safety contracts.

## Requirements

### Requirement: Fresh and monotonic cluster telemetry

Each dynamic-mode Store SHALL periodically report a Store heartbeat and observations for its local Region peers. A Store observation SHALL include Store identity, sequence number, capacity and available bytes, load counters, and health time; a Region observation SHALL include Region and peer identity, epoch, approximate size, request counters, peer role, and active topology-operation state. The metadata service MUST accept heartbeats idempotently, MUST ignore an older sequence or older Region epoch, and MUST classify an observation as stale after a configurable timeout. Telemetry and diagnostics MUST NOT contain raw user keys or values.

#### Scenario: Current heartbeat updates scheduling input

- **WHEN** a Store reports a heartbeat with a sequence and Region epochs newer than its last accepted observation
- **THEN** subsequent scheduling evaluations use the new capacity, load, health, and Region information

#### Scenario: Duplicate or reordered heartbeat arrives

- **WHEN** the metadata service receives the same heartbeat again or receives one with an older sequence or Region epoch
- **THEN** it acknowledges the report idempotently without rolling back accepted telemetry or scheduling state

#### Scenario: Heartbeat persistence response is lost

- **WHEN** a heartbeat is committed but the Store times out before receiving the response and retries it
- **THEN** the retry produces one logical observation and does not double-count its load counters

#### Scenario: Store observation becomes stale

- **WHEN** no valid heartbeat is accepted before the configured health timeout
- **THEN** the Store becomes ineligible as a scheduling destination and its last observation remains distinguishable from fresh evidence

### Requirement: Leader-exclusive and opt-in scheduling

Automatic scheduling SHALL be enabled by default in replicated dynamic metadata mode for newly bootstrapped clusters, and an operator MAY disable or pause it at any time. The balancer configuration SHALL be durably replicated with the metadata state, so a cluster that has explicitly disabled (or enabled) scheduling keeps that choice across restarts and upgrades while only fresh clusters receive the enabled default. Exactly the current metadata leader SHALL evaluate and dispatch automatic operators. A follower or a former leader MUST NOT dispatch work, and a metadata quorum loss MUST stop new scheduling decisions while leaving committed topology unchanged.

#### Scenario: Fresh dynamic cluster schedules automatically

- **WHEN** a new dynamic-topology cluster bootstraps and the metadata leader forms without any explicit balancer configuration
- **THEN** the balancer is enabled from the committed default and the leader begins evaluations once heartbeats arrive

#### Scenario: Operator enables automatic scheduling

- **WHEN** an operator enables the balancer with valid thresholds and limits on a healthy metadata quorum
- **THEN** the configuration is durably replicated and the current metadata leader begins evaluations from one committed revision

#### Scenario: Operator disables automatic scheduling

- **WHEN** an operator disables the balancer on a healthy metadata quorum
- **THEN** the configuration is durably replicated, the leader stops dispatching new automatic operators, and the choice survives restart

#### Scenario: Existing cluster keeps its explicit choice

- **WHEN** a cluster that explicitly disabled (or enabled) the balancer upgrades to a build whose default is enabled
- **THEN** the cluster's persisted configuration is preserved and no default is re-applied over it

#### Scenario: Metadata leadership changes

- **WHEN** leadership moves while an evaluation or operator dispatch is in progress
- **THEN** the former leader stops dispatching and the new leader reconstructs scheduling state from committed metadata before resuming

#### Scenario: Metadata quorum is unavailable

- **WHEN** the scheduler cannot commit or linearly read its decision before its deadline
- **THEN** it creates no new automatic operator and reports a retryable control-plane failure

#### Scenario: Legacy or disabled deployment runs

- **WHEN** a node uses legacy static topology or automatic scheduling has been explicitly disabled
- **THEN** no automatic split or peer movement is issued and existing manual commands retain their behavior

### Requirement: Safe replica repair and placement

The scheduler SHALL detect Regions whose live voter count or placement differs from the configured replication policy and SHALL prefer restoring safety before balancing load. A destination MUST be fresh, healthy, have sufficient free capacity, and not already host a peer for the Region. The scheduler MUST preserve a valid quorum through the existing learner-first migration protocol and MUST NOT remove a stale or unreachable peer solely from heartbeat evidence unless the replacement has been promoted and the existing membership-change safety checks authorize removal.

#### Scenario: Healthy Store is added

- **WHEN** a new Store reports fresh capacity and eligible Regions are imbalanced
- **THEN** the scheduler may choose it as a migration destination while maintaining one peer per Store and the configured replica count

#### Scenario: Store stops reporting

- **WHEN** a Store heartbeat expires while its Regions still depend on its voters
- **THEN** the scheduler marks those Regions under-replicated or at risk, prioritizes safe replacement, and does not treat the missing peer as removed until Raft membership commits the change

#### Scenario: No safe destination exists

- **WHEN** every candidate Store is stale, full, already hosts the Region, or would violate the placement policy
- **THEN** no operator is created and status reports the unsatisfied constraint rather than attempting an unsafe change

#### Scenario: Placement changes during evaluation

- **WHEN** a concurrent manual operation advances a candidate Region epoch before the automatic decision commits
- **THEN** the stale automatic decision is rejected and a later evaluation recomputes from the current descriptor

### Requirement: Stable and bounded balancing decisions

For otherwise healthy Regions, the scheduler SHALL rank candidate peer movements from a deterministic snapshot using documented capacity and load scores. It MUST require configured hysteresis before considering an imbalance actionable, MUST observe per-Region cooldown after completion or failure, and MUST enforce cluster-wide and per-Store limits for active migrations and splits. Equal inputs MUST produce a stable ordering by Region and Store identity so retries and leader changes do not create oscillating choices.

#### Scenario: Capacity skew exceeds hysteresis

- **WHEN** fresh observations show an eligible source and destination whose score difference remains above the configured threshold
- **THEN** the scheduler selects a deterministic move that reduces the measured skew without violating placement or concurrency limits

#### Scenario: Skew is below the threshold

- **WHEN** score differences fall inside the configured hysteresis band
- **THEN** the scheduler emits no balancing operator and retains the current topology

#### Scenario: Concurrency budget is exhausted

- **WHEN** the cluster or either affected Store already has the configured maximum active operators
- **THEN** the candidate remains pending for a later evaluation and no additional operation is dispatched

#### Scenario: Recently handled Region is reevaluated

- **WHEN** a Region completed or failed an automatic operation within its cooldown window
- **THEN** it is not selected again until the window expires unless replica safety requires urgent repair

### Requirement: Sustained evidence triggers automatic split

The scheduler SHALL consider a Region for automatic split only when fresh observations exceed a configured size or load threshold for the configured number of consecutive evaluation windows. It MUST reject a Region that already has a split, migration, or membership change in flight. The selected split boundary MUST come from a safe boundary supplied by the Region and MUST satisfy the existing split validation contract; the scheduler MUST NOT derive or log a raw user key in metadata diagnostics.

#### Scenario: Oversized Region remains above threshold

- **WHEN** an eligible Region reports a valid split boundary and exceeds the size threshold for the required consecutive windows
- **THEN** the scheduler creates one idempotent split operator using the current Region epoch

#### Scenario: Transient load spike subsides

- **WHEN** a Region exceeds a load threshold for fewer than the required consecutive windows and then returns below it
- **THEN** the evidence counter resets and no split operator is created

#### Scenario: Topology work is already active

- **WHEN** an otherwise eligible Region has a migration, ConfChange, or split in progress
- **THEN** automatic split is deferred until the Region is stable and observed at a current epoch

#### Scenario: Proposed boundary becomes stale

- **WHEN** the Region epoch or range changes before the automatic split commits
- **THEN** existing split validation rejects the command without side effects and the scheduler recomputes rather than retrying the stale boundary

### Requirement: Durable idempotent operator lifecycle

Every automatic action SHALL have a globally unique operator identifier and a replicated record containing its type, source revision, Region epoch, inputs, phase, attempts, deadline, last error, and terminal outcome. At most one topology-changing operator SHALL be active for a Region. Dispatch and phase updates MUST be idempotent; after process restart or leader change, the active metadata leader SHALL reconcile observed topology with the operator record and either resume forward progress, wait safely, cancel before the irreversible point, or mark a terminal actionable failure.

#### Scenario: Dispatch response is lost

- **WHEN** execution accepts a migration or split step but its response does not reach the scheduler
- **THEN** retrying with the same operator and mutation identity observes or resumes the original operation rather than creating a duplicate

#### Scenario: Metadata leader fails during execution

- **WHEN** the metadata leader stops after persisting an operator but before recording its next phase
- **THEN** the new leader compares committed metadata and Region state, then continues from the first incomplete safe phase

#### Scenario: Target Store fails before promotion

- **WHEN** an automatic migration cannot catch up its learner before the operator deadline
- **THEN** the operator invokes the safe pre-promotion cancellation path, records the failure and cooldown, and leaves the original voter set serving

#### Scenario: Source fails after target promotion

- **WHEN** the source becomes unavailable after the replacement peer is promoted
- **THEN** reconciliation finishes or safely retries source removal and metadata publication without rolling back the promoted peer

#### Scenario: Duplicate operator proposal races

- **WHEN** two evaluations attempt to create topology operators for the same Region and epoch
- **THEN** replicated admission accepts at most one and returns the accepted operator to both retry paths

### Requirement: Operator control and explainable status

Operators SHALL be able to inspect current configuration, Store health, placement violations, ranked dry-run candidates, active and recent operators, cooldowns, and rejection reasons. Pause MUST prevent admission of new automatic operators while allowing active operators to reach a safe terminal state. Cancellation MUST succeed only before the operation's documented irreversible point and MUST be idempotent. Status, metrics, and logs MUST identify Store, Region, peer, epoch, operator, phase, and reason without exposing raw user keys or values.

#### Scenario: Dry-run is requested

- **WHEN** an operator requests a dry-run at a committed metadata revision
- **THEN** the service returns the deterministic ranked actions and rejection reasons without creating operator records or changing topology

#### Scenario: Scheduler is paused

- **WHEN** an operator pauses automatic scheduling while topology operators are active
- **THEN** no new operator is admitted and each active operator continues only as needed to reach a safe reconciled outcome

#### Scenario: Cancellation is retried

- **WHEN** a caller repeats cancellation for an operator that was already safely cancelled
- **THEN** the service returns the same terminal state without issuing additional cleanup or topology changes

#### Scenario: Cancellation is too late

- **WHEN** cancellation is requested after a promoted replacement or committed split has crossed its irreversible point
- **THEN** the request is rejected with the current phase and recovery guidance while reconciliation continues forward
