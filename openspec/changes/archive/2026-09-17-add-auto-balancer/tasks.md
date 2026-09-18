## 1. Replicated scheduling state and protocol

- [x] 1.1 Add heartbeat, balancer configuration, operator lifecycle, query/control, and snapshot protocol/state-machine support with monotonic sequence/epoch admission, idempotent mutations, one-active-operator-per-Region enforcement, and backward-compatible restore; preserve unrelated dirty files. Verify with a focused `stratakv-test-auto-balancer-state` test covering duplicate/reordered reports, stale epochs, invalid configuration, admission races, response-loss replay, snapshot restore, and terminal/cancellation transitions.

## 2. Deterministic planning policy

- [x] 2.1 Implement the side-effect-free scheduling snapshot and planner for safety repair, capacity/peer skew reduction, cooldown/hysteresis/concurrency constraints, stable tie-breaking, and sustained automatic-split evidence without exposing user keys; preserve unrelated dirty files. Verify with `stratakv-test-auto-balancer-planner` table-driven tests covering healthy expansion, failed Store, no eligible destination, stale plans, budget exhaustion, below-threshold skew, transient spikes, current topology work, and deterministic repeated evaluation.

## 3. Storage telemetry and split candidates

- [x] 3.1 Add one bounded, stoppable heartbeat reporter per dynamic `NodeServer`, collect Store/Region observations without holding registry or peer locks across RPC, coalesce retries, classify freshness, and resolve opaque split candidates only at execution time; preserve unrelated dirty files. Verify with `stratakv-test-auto-balancer-heartbeat` covering reporter ownership/shutdown, duplicate timeout retry, newer-sample coalescing, stale Store exclusion, Region epoch monotonicity, and absence of raw keys/values in telemetry and diagnostics.

## 4. Leader scheduler and reusable execution

- [x] 4.1 Add the metadata-leader-owned evaluation/executor lifecycle, extract shared split/migration orchestration for both CLI and automatic operators, and implement idempotent reconciliation across leadership loss, deadline, target failure, post-promotion source failure, pause, and restart without holding scheduler locks across RPC or consensus; preserve unrelated dirty files. Verify with `stratakv-test-auto-balancer-recovery` plus the existing split/migration tests, including leader handoff at every operator phase and confirmation that a former leader dispatches no new work.

## 5. Operator controls and end-to-end convergence

- [x] 5.1 Add `stratakv-admin` enable/disable, pause/resume, dry-run, status, and cancel commands with explainable ranked decisions and structured metrics/logs, then exercise automatic recovery, rebalancing, and split under concurrent reads/writes and cross-Region 2PC; preserve unrelated dirty files. Verify with `stratakv-test-auto-balancer-integration` covering Store expansion/failure, skew convergence, concurrency limits, manual-operation races, irreversible cancellation, automatic split, no lost acknowledged writes/orphan locks, and legacy/disabled compatibility.

## 6. Control-plane performance, regression, and documentation

- [ ] 6.1 Measure disabled versus reporting versus enabled Auto-Balancer under the same Release build and fixed workload, recording heartbeat proposal/apply rate, metadata commit/apply latency, scheduling scan duration, data-plane throughput/latency, and correctness without claiming improvement; run `ctest --test-dir build/stratakv/release --output-on-failure` with 100% pass, and update `README.md` plus `Docs/9-优化记录/` with architecture, configuration, operations, failure recovery, limitations, evidence, and rollback while preserving unrelated dirty files.
