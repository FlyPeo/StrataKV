# client-tool Delta Spec

## Purpose

Provide the packaged, resident command-line client for business-style access to StrataKV: it connects once, stays connected, and exposes point reads/writes plus range listing built on the transactional SDK.

## ADDED Requirements

### Requirement: Resident client connection

The client tool SHALL connect to the cluster once at startup using the supplied Region configuration and TSO endpoints (static mode) or metadata endpoints (dynamic mode), and SHALL reuse that connection for every subsequent command in the session. It MUST NOT require reconnection or process restart between commands.

#### Scenario: Session of commands over one connection

- **WHEN** a user runs put, get, and list commands in one interactive session
- **THEN** each command executes against the cluster without any reconnect step or process restart

### Requirement: Command surface

The client SHALL support these commands: `get <key>` (print the value, or a not-found marker), `put <key> <value>` (transactional write with commit), `del <key>` (transactional delete), `list` (list all user-visible entries), `list <prefix>` (list entries whose key starts with the prefix), and `quit`. Every command SHALL print a definite outcome line; unknown input SHALL produce a usage hint rather than silent behavior.

#### Scenario: Prefix listing

- **WHEN** the user runs `list user:` after putting `user:1` and `order:9`
- **THEN** only `user:1` is listed

#### Scenario: List reflects committed state only

- **WHEN** a key was deleted and committed, and the user runs `list`
- **THEN** the deleted key does not appear

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
