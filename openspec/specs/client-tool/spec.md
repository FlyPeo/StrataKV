# client-tool Specification

## Purpose
Provide the packaged, resident command-line client for business-style access to StrataKV: it connects once, stays connected, and exposes point reads/writes plus range listing built on the transactional SDK.

## Requirements

### Requirement: Resident client connection

The client tool SHALL connect to the cluster once at startup using the supplied Region configuration and TSO endpoints (static mode) or metadata endpoints (dynamic mode), and SHALL reuse that connection for every subsequent command in the session. It MUST NOT require reconnection or process restart between commands.

#### Scenario: Session of commands over one connection

- **WHEN** a user runs put, get, and list commands in one interactive session
- **THEN** each command executes against the cluster without any reconnect step or process restart

### Requirement: Command surface

The client SHALL support these commands: `get <key>`, `put <key> <value>`, `del <key>`, `list` / `list <prefix>`, `scan <startKey> <endKey> [limit]`, `begin [lockTtlMs]`, `commit`, `rollback`, `get-for-update <key>`, `batch-get-for-update <key>...`, `lock-keys <key>...`, `multi put ...` / `multi del ...`, `txn-status`, `metrics`, and `quit`. Outside a session, `put`, `del`, `get`, `list`, and `scan` each auto-commit or snapshot-read in their own single-statement transaction. Every command SHALL print a definite outcome line; unknown input SHALL produce a usage hint rather than silent behavior.

#### Scenario: Prefix listing

- **WHEN** the user runs `list user:` after putting `user:1` and `order:9`
- **THEN** only `user:1` is listed

#### Scenario: List reflects committed state only

- **WHEN** a key was deleted and committed, and the user runs `list`
- **THEN** the deleted key does not appear

#### Scenario: Session commands outside a session

- **WHEN** the user runs `commit` without an active session
- **THEN** the client prints a definite error instead of silently doing nothing

### Requirement: Interactive and scripted operation

The tool SHALL work interactively and also accept a command stream on standard input, executing each line in order and exiting on `quit` or end of input. Scripted use SHALL produce the same outcomes as the equivalent interactive session.

#### Scenario: Piped command stream

- **WHEN** a put/get/quit command stream is piped into the tool against a running cluster
- **THEN** the tool executes the commands in order, prints each outcome, and exits zero

### Requirement: Cross-region atomic operations

A `put` or `del` SHALL commit through the transactional path so that keys landing in any Region are atomic and conflict-retried exactly as the SDK guarantees; a `list` SHALL reflect a consistent snapshot per invocation.

#### Scenario: Retryable conflict surfaces as retry

- **WHEN** concurrent writers conflict on the same key through the tool
- **THEN** the tool retries per the SDK conflict semantics and reports the final committed outcome instead of surfacing an internal conflict error

### Requirement: Transactional session

The client SHALL provide a transactional session: `begin [lockTtlMs]` starts one transaction, subsequent `put`, `del`, and `get` commands operate inside it (writes are staged locally, `get` returns read-your-writes including staged values), and `commit` atomically commits all staged writes across any number of Regions while `rollback` discards them. At most one session transaction SHALL be active at a time; committing or rolling back SHALL end the session; exiting the client with an active session SHALL roll it back and print a notice.

#### Scenario: Multi-key cross-region atomic commit

- **WHEN** the user begins a session, stages writes to keys that reside in different Regions, and commits
- **THEN** all staged keys become visible atomically as one committed transaction

#### Scenario: Rollback discards staged writes

- **WHEN** the user begins a session, stages writes, and rolls back
- **THEN** no staged key becomes visible

#### Scenario: Read-your-writes inside the session

- **WHEN** the user stages a put for a key and then gets that key before committing
- **THEN** the get returns the staged value

#### Scenario: Exit with an open session

- **WHEN** the client exits while a session transaction is active
- **THEN** the transaction is rolled back and a notice is printed

### Requirement: Locking read operations

The client SHALL expose the SDK's pessimistic locking reads: `get-for-update <key>`, `batch-get-for-update <key>...`, and `lock-keys <key>...`. These commands SHALL require an active transactional session (they operate on the session transaction and their locks are released by its commit or rollback); outside a session they SHALL fail with a hint to begin one.

#### Scenario: Locking read inside a session

- **WHEN** the user begins a session and runs `get-for-update account:1`
- **THEN** the command returns the current value and stages a locking write intent on that key within the session transaction

#### Scenario: Locking read outside a session

- **WHEN** the user runs `get-for-update account:1` without an active session
- **THEN** the command fails with an explicit hint to run `begin` first, without acquiring any lock

### Requirement: Multi-key one-shot write

The client SHALL support `multi put <key> <value> [<key> <value> ...]` and `multi del <key> [<key> ...]`: one transaction staging all keys and committing atomically (with the SDK's conflict retry semantics). A malformed `multi` (odd number of arguments for put, no keys) SHALL be rejected with a usage hint before any write is attempted.

#### Scenario: One-shot cross-region atomic write

- **WHEN** the user runs `multi put` with keys residing in different Regions
- **THEN** all keys commit atomically or, on a non-retryable failure, none is visible

### Requirement: Transaction status and client metrics

The client SHALL expose `txn-status` (the SDK's transaction status query for the session transaction) and `metrics` (the SDK's client metrics: routing attempts, leader retries, epoch refreshes, and related counters).

#### Scenario: Metrics reflect client activity

- **WHEN** the user runs `metrics` after several commands
- **THEN** the printed counters include routing and retry statistics from the session

### Requirement: Raw range scan command

The client SHALL expose `scan <startKey> <endKey> [limit]` returning entries in `[startKey, endKey)` at the invocation's snapshot, with empty arguments meaning unbounded bounds; `list` remains prefix sugar over the same scan.

#### Scenario: Bounded raw scan

- **WHEN** the user runs `scan a z 2` with five matching keys
- **THEN** exactly the first two keys of the range are returned in ascending order
