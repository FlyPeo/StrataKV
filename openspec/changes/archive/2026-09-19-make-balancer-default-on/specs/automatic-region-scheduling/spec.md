# automatic-region-scheduling Delta Spec

## MODIFIED Requirements

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
