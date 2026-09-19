# sdk-range-scan Delta Spec

## Purpose

Let transactional clients enumerate keys over a range with the same snapshot and visibility semantics as single-key reads, across any number of Regions, through the public SDK.

## ADDED Requirements

### Requirement: Snapshot-consistent range scan

The SDK SHALL provide a scan operation that returns all user-visible key-value pairs in `[startKey, endKey)` as observed at one transaction's start timestamp. Empty `startKey` SHALL mean from the beginning of the keyspace, and empty `endKey` SHALL mean to the end. Scanned entries SHALL be ordered by key ascending and SHALL NOT include MVCC internal records, uncommitted writes of other transactions, or the scanning transaction's own absence of writes beyond the transaction's snapshot.

#### Scenario: Cross-region scan at one snapshot

- **WHEN** keys of interest live in three Regions and a client scans a range covering all of them inside one transaction
- **THEN** every committed entry in the range at that transaction's start timestamp is returned in ascending key order, with no duplicate and no missing entry

#### Scenario: Writes committed after the snapshot are invisible

- **WHEN** another client commits a write into the scanned range after the scanning transaction began
- **THEN** the scan result does not contain that write

### Requirement: Region-transparent aggregation

The scan SHALL automatically cover every Region overlapping the requested range, including Regions created by a split during the scan's lifetime, by clipping the range to each Region's bounds and merging per-Region results in key order. Callers SHALL NOT need to know Region boundaries.

#### Scenario: Range clipped across Region boundaries

- **WHEN** the requested range spans partial Regions at both ends
- **THEN** edge Regions contribute only their intersection with the range and the merged output is continuous and ordered

### Requirement: Limit and continuation

A positive limit SHALL cap the number of returned entries; the scan MAY return fewer entries than the limit when the range is exhausted. To continue, the caller SHALL re-scan from the last returned key's immediate successor. Limit SHALL NOT change the visibility semantics of the entries returned.

#### Scenario: Limited scan continues seamlessly

- **WHEN** a scan with limit N returns N entries from a larger range and the client re-scans starting at the successor of the last returned key
- **THEN** the union of both scans equals the full range scan with no gap and no duplicate

### Requirement: Conflict and error semantics

If a key in the range is locked by another unfinished transaction whose start timestamp is within the scan's snapshot, the scan SHALL fail with the same retryable lock-conflict outcome as a single-key read of that key, rather than silently skipping the key or returning uncommitted data. Routing and transport failures SHALL surface as retryable errors consistent with existing single-key read behavior.

#### Scenario: Locked key blocks the scan like Get

- **WHEN** a key in the range carries an unfinished lock visible at the scan snapshot
- **THEN** the scan returns a retryable lock-conflict outcome instead of partial silent results
