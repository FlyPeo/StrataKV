#include "tso_consensus.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <set>
#include <thread>

#include "config.h"
#include "raft_rpc_util.h"

namespace {
constexpr int kTsoRaftGroupId = -1;
// A default 4096-value range makes this roughly one snapshot per 128K
// allocations while keeping snapshot/restart behavior practical to exercise.
constexpr int kSnapshotEveryAppliedEntries = 32;
constexpr auto kFenceDuration = std::chrono::milliseconds(150);
constexpr auto kFenceRefreshTimeout = std::chrono::milliseconds(CONSENSUS_TIMEOUT * 4);
constexpr uint64_t kMaxHlcPhysicalLagMs =
    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(kFenceDuration)
                              .count());

uint64_t ParseTimestamp(const std::string& value) {
  size_t consumed = 0;
  const uint64_t timestamp = std::stoull(value, &consumed);
  if (consumed != value.size() || timestamp == 0) {
    throw std::runtime_error("invalid timestamp in TSO Raft command");
  }
  return timestamp;
}

uint64_t ValidateRangeSize(uint64_t value) {
  if (value == 0) throw std::invalid_argument("TSO range size must be positive");
  return value;
}
}  // namespace

TsoConsensusNode::TsoConsensusNode(int nodeId, std::vector<Endpoint> peers,
                                   std::string statePath, uint64_t segmentSize)
    : nodeId_(nodeId),
      peers_(std::move(peers)),
      clientId_("tso-" + std::to_string(nodeId) + "-" + std::to_string(getpid())),
      rangeSize_(ValidateRangeSize(segmentSize)),
      // Raft is the cluster authority. The local oracle uses segment size one
      // and is consulted only on the slow path as a durable per-member floor;
      // it never grants a range and never publishes a timestamp by itself.
      localOracle_(std::make_shared<PersistentTimestampOracle>(std::move(statePath), 1)),
      persister_(std::make_shared<Persister>("tso_control_node" + std::to_string(nodeId))),
      applyChannel_(std::make_shared<LockQueue<ApplyMsg>>()),
      raft_(std::make_shared<Raft>()) {
  if (peers_.size() < 3 || peers_.size() % 2 == 0) {
    throw std::invalid_argument("TSO consensus requires an odd peer count of at least 3");
  }
  if (nodeId_ < 0 || nodeId_ >= static_cast<int>(peers_.size())) {
    throw std::invalid_argument("TSO node id is outside the peer list");
  }
  std::set<Endpoint> uniquePeers(peers_.begin(), peers_.end());
  if (uniquePeers.size() != peers_.size()) {
    throw std::invalid_argument("TSO peer endpoints must be unique");
  }
}

void TsoConsensusNode::Start() {
  std::vector<std::shared_ptr<RaftRpcUtil>> peers;
  peers.reserve(peers_.size());
  for (size_t index = 0; index < peers_.size(); ++index) {
    if (static_cast<int>(index) == nodeId_) {
      peers.push_back(nullptr);
    } else {
      peers.push_back(std::make_shared<RaftRpcUtil>(peers_[index].first, peers_[index].second,
                                                   kTsoRaftGroupId));
    }
  }
  raft_->init(std::move(peers), nodeId_, persister_, applyChannel_);

  const std::string snapshot = persister_->ReadSnapshot();
  if (!snapshot.empty()) {
    const uint64_t highWater = ParseTimestamp(snapshot);
    localOracle_->Observe(highWater);
    committedHighWater_.store(std::max(committedHighWater_.load(), highWater),
                              std::memory_order_release);
  }
  stateMachineAppliedIndex_.store(raft_->GetStatus().lastApplied, std::memory_order_release);

  std::thread applyThread(&TsoConsensusNode::ApplyLoop, this);
  applyThread.detach();
  std::thread statusThread(&TsoConsensusNode::StatusLoop, this);
  statusThread.detach();
}

Raft::NodeStatus TsoConsensusNode::Status() const { return raft_->GetStatus(); }

uint64_t TsoConsensusNode::Next() {
  for (;;) {
    int term = ValidFenceTerm();
    if (term >= 0) {
      const uint64_t timestamp = TryAllocateFast(term);
      if (timestamp != 0) return timestamp;
    }

    std::unique_lock<std::mutex> lock(slowPathMutex_);
    term = RequireValidFenceLocked();
    const uint64_t timestamp = TryAllocateFast(term);
    if (timestamp != 0) return timestamp;

    ReserveRangeLocked(0, term);
    term = ValidFenceTerm();
    if (term < 0) {
      throw TsoNotLeaderError("TSO leadership fence expired after range reservation");
    }
    const uint64_t reserved = TryAllocateFast(term);
    if (reserved != 0) return reserved;
    throw std::runtime_error("TSO committed range was not published");
  }
}

uint64_t TsoConsensusNode::Peek() {
  std::unique_lock<std::mutex> lock(slowPathMutex_);
  const int term = RequireValidFenceLocked();
  const uint64_t wall = HlcTimestamp::Compose(HlcTimestamp::WallClockMs(), 0);
  const uint64_t next = std::max(nextTimestamp_.load(std::memory_order_acquire), wall);
  if (activeRangeTerm_.load(std::memory_order_acquire) == term &&
      nextTimestamp_.load(std::memory_order_acquire) <=
          activeRangeHighWater_.load(std::memory_order_acquire)) {
    const uint64_t activeNext = nextTimestamp_.load(std::memory_order_acquire);
    const uint64_t physicalMs = HlcTimestamp::PhysicalMs(activeNext);
    const uint64_t wallMs = HlcTimestamp::WallClockMs();
    if (wallMs <= physicalMs || wallMs - physicalMs <= kMaxHlcPhysicalLagMs) return activeNext;
  }

  const uint64_t highWater = committedHighWater_.load(std::memory_order_acquire);
  if (highWater == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("TSO timestamp space is exhausted");
  }
  return std::max({highWater + 1, localOracle_->Peek(), wall});
}

void TsoConsensusNode::Observe(uint64_t timestamp) {
  if (timestamp == 0) return;
  if (timestamp == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("observed maximum TSO timestamp");
  }

  std::unique_lock<std::mutex> lock(slowPathMutex_);
  const int term = RequireValidFenceLocked();
  // Persisting a node-local floor before consensus can only create a gap if
  // the proposal fails. It never authorizes the fast path.
  localOracle_->Observe(timestamp);

  if (activeRangeTerm_.load(std::memory_order_acquire) == term &&
      timestamp < activeRangeHighWater_.load(std::memory_order_acquire)) {
    uint64_t current = nextTimestamp_.load(std::memory_order_acquire);
    while (current <= timestamp &&
           !nextTimestamp_.compare_exchange_weak(current, timestamp + 1,
                                                 std::memory_order_release,
                                                 std::memory_order_acquire)) {
    }
    if (ValidFenceTerm() != term) {
      throw TsoNotLeaderError("TSO leadership changed while observing timestamp");
    }
    return;
  }

  if (timestamp <= committedHighWater_.load(std::memory_order_acquire)) {
    activeRangeTerm_.store(-1, std::memory_order_release);
    return;
  }
  ReserveRangeLocked(timestamp + 1, term);
}

bool TsoConsensusNode::HasValidFence() const { return ValidFenceTerm() >= 0; }

int64_t TsoConsensusNode::SteadyNowNanos() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

int TsoConsensusNode::ValidFenceTerm() const {
  const int64_t deadline = fenceDeadlineNanos_.load(std::memory_order_acquire);
  if (deadline <= SteadyNowNanos()) return -1;
  const int term = fenceTerm_.load(std::memory_order_acquire);
  if (term < 0 || !raft_->IsLeaderInTerm(term)) return -1;
  return term;
}

bool TsoConsensusNode::WaitUntilApplied(
    int index, std::chrono::steady_clock::time_point deadline) {
  std::unique_lock<std::mutex> lock(appliedMutex_);
  return appliedCondition_.wait_until(lock, deadline, [this, index]() {
    return stateMachineAppliedIndex_.load(std::memory_order_acquire) >= index;
  });
}

void TsoConsensusNode::RefreshFenceLocked() {
  const auto startedAt = std::chrono::steady_clock::now();
  const auto deadline = startedAt + kFenceRefreshTimeout;
  const Raft::ReadIndexResult read = raft_->ReadIndex(deadline);
  if (!read.ok()) {
    fenceDeadlineNanos_.store(0, std::memory_order_release);
    throw TsoNotLeaderError("TSO leader could not confirm a current-term majority");
  }
  if (!WaitUntilApplied(read.readIndex, deadline) || !raft_->IsLeaderInTerm(read.term)) {
    fenceDeadlineNanos_.store(0, std::memory_order_release);
    throw TsoNotLeaderError("TSO leader changed before the fencing barrier applied");
  }

  // The deadline is measured from before the quorum round. Followers reset
  // their election timers after that point, so a duration below the minimum
  // 300 ms election timeout is conservative even when a reply is delayed.
  const auto fenceDeadline = startedAt + kFenceDuration;
  if (fenceDeadline <= std::chrono::steady_clock::now()) {
    fenceDeadlineNanos_.store(0, std::memory_order_release);
    throw TsoNotLeaderError("TSO quorum confirmation arrived after its safe lease window");
  }
  fenceTerm_.store(read.term, std::memory_order_release);
  fenceDeadlineNanos_.store(
      std::chrono::duration_cast<std::chrono::nanoseconds>(fenceDeadline.time_since_epoch())
          .count(),
      std::memory_order_release);
}

int TsoConsensusNode::RequireValidFenceLocked() {
  int term = ValidFenceTerm();
  if (term >= 0) return term;
  RefreshFenceLocked();
  term = ValidFenceTerm();
  if (term < 0) throw TsoNotLeaderError("TSO leadership fence is not valid");
  return term;
}

uint64_t TsoConsensusNode::TryAllocateFast(int term) {
  if (term < 0 || activeRangeTerm_.load(std::memory_order_acquire) != term) return 0;
  const uint64_t highWater = activeRangeHighWater_.load(std::memory_order_acquire);
  uint64_t current = nextTimestamp_.load(std::memory_order_acquire);
  for (;;) {
    // A range is a contiguous set of HLC values. Jumping to wall-clock time on
    // every call would skip 2^18 values per millisecond and defeat a 4096-value
    // reservation. Keep advancing the logical component within the range, but
    // abandon it when its physical component trails wall time by more than the
    // fencing interval. This preserves useful batching without allowing the
    // physical component used by TTL/GC to drift indefinitely.
    const uint64_t wallMs = HlcTimestamp::WallClockMs();
    const uint64_t physicalMs = HlcTimestamp::PhysicalMs(current);
    if (wallMs > physicalMs && wallMs - physicalMs > kMaxHlcPhysicalLagMs) return 0;
    const uint64_t candidate = current;
    if (candidate == std::numeric_limits<uint64_t>::max() || candidate > highWater) return 0;
    if (nextTimestamp_.compare_exchange_weak(current, candidate + 1,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
      // A failed post-check discards this value as a harmless gap. No value
      // can escape after the local node has observed a term/fence change.
      if (ValidFenceTerm() != term) {
        throw TsoNotLeaderError("TSO leadership changed during local range allocation");
      }
      timestampAllocationCount_.fetch_add(1, std::memory_order_relaxed);
      return candidate;
    }
  }
}

void TsoConsensusNode::ReserveRangeLocked(uint64_t minimumNext, int term) {
  if (term != RequireValidFenceLocked()) {
    throw TsoNotLeaderError("TSO leadership term changed before range reservation");
  }

  const uint64_t committed = committedHighWater_.load(std::memory_order_acquire);
  if (committed == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("TSO timestamp space is exhausted");
  }
  const uint64_t wall = HlcTimestamp::Compose(HlcTimestamp::WallClockMs(), 0);
  const uint64_t first = std::max({committed + 1, localOracle_->Peek(), wall, minimumNext});
  const uint64_t extra = rangeSize_ - 1;
  // Keep UINT64_MAX as an exhaustion sentinel because nextTimestamp stores
  // the next unallocated value.
  if (first >= std::numeric_limits<uint64_t>::max() - extra) {
    throw std::overflow_error("TSO timestamp space is exhausted");
  }
  const uint64_t highWater = first + extra;

  localOracle_->Observe(highWater);
  int proposalTerm = -1;
  ProposeHighWater(highWater, &proposalTerm);
  if (proposalTerm != term) {
    activeRangeTerm_.store(-1, std::memory_order_release);
    throw TsoNotLeaderError("TSO leadership changed while reserving a range");
  }

  const int currentTerm = ValidFenceTerm();
  if (currentTerm != proposalTerm) {
    activeRangeTerm_.store(-1, std::memory_order_release);
    throw TsoNotLeaderError("TSO range committed after its leader fence expired");
  }

  nextTimestamp_.store(first, std::memory_order_release);
  activeRangeHighWater_.store(highWater, std::memory_order_release);
  activeRangeTerm_.store(proposalTerm, std::memory_order_release);
}

void TsoConsensusNode::ProposeHighWater(uint64_t highWater, int* proposalTerm) {
  const int requestId = nextRequestId_.fetch_add(1, std::memory_order_relaxed);
  if (requestId <= 0) throw std::overflow_error("TSO request id space is exhausted");

  Op command;
  command.Operation = "TsoReserveHighWater";
  command.Value = std::to_string(highWater);
  command.ClientId = clientId_;
  command.RequestId = requestId;
  const std::string requestKey = NextRequestKey(requestId);
  const WaitQueue queue = std::make_shared<LockQueue<ProposalResult>>();
  {
    std::lock_guard<std::mutex> lock(waitersMutex_);
    waiters_[requestKey] = queue;
  }

  int raftIndex = -1;
  int term = -1;
  bool isLeader = false;
  raft_->Start(command, &raftIndex, &term, &isLeader);
  if (!isLeader) {
    RemoveWaiter(requestKey, queue);
    throw TsoNotLeaderError("TSO leadership changed before proposal");
  }
  *proposalTerm = term;
  rangeProposalCount_.fetch_add(1, std::memory_order_relaxed);

  ProposalResult result;
  if (!queue->timeOutPop(CONSENSUS_TIMEOUT * 4, &result)) {
    RemoveWaiter(requestKey, queue);
    throw TsoNotLeaderError("TSO range reservation did not reach a majority");
  }
  RemoveWaiter(requestKey, queue);
  if (result.rejected) {
    throw TsoNotLeaderError("TSO range reservation was rejected after step-down");
  }
  const Op& applied = result.command;
  if (applied.Operation != command.Operation || applied.ClientId != clientId_ ||
      applied.RequestId != requestId || ParseTimestamp(applied.Value) != highWater) {
    throw std::runtime_error("TSO applied range does not match proposal");
  }
}

void TsoConsensusNode::ApplyLoop() {
  while (true) {
    const ApplyMsg message = applyChannel_->Pop();
    try {
      if (message.CommandValid) {
        Op command;
        if (!command.parseFromString(message.Command)) {
          throw std::runtime_error("invalid TSO Raft command");
        }
        if (command.Operation == "TsoReserveHighWater" || command.Operation == "TsoAllocate" ||
            command.Operation == "TsoObserve") {
          ApplyHighWater(command, message.CommandIndex);
        }
        stateMachineAppliedIndex_.store(message.CommandIndex, std::memory_order_release);
        appliedCondition_.notify_all();
      }
      if (message.ProposalRejected) {
        Op command;
        if (command.parseFromString(message.Command)) CompleteRejectedProposal(command);
      }
      if (message.SnapshotValid) {
        // Raft has already accepted this snapshot and advanced its boundary
        // before publishing the ApplyMsg. Re-checking CondInstallSnapshot here
        // would reject the same index and leave the TSO state machine stale.
        const uint64_t highWater = ParseTimestamp(message.Snapshot);
        localOracle_->Observe(highWater);
        committedHighWater_.store(std::max(committedHighWater_.load(), highWater),
                                  std::memory_order_release);
        activeRangeTerm_.store(-1, std::memory_order_release);
        stateMachineAppliedIndex_.store(message.SnapshotIndex, std::memory_order_release);
        appliedCondition_.notify_all();
      }
    } catch (const std::exception& error) {
      // Continuing after a state-machine persistence error could return an
      // unsafe timestamp. Fail the member and let the remaining quorum elect.
      std::cerr << "fatal TSO apply error: " << error.what() << std::endl;
      std::abort();
    }
  }
}

void TsoConsensusNode::StatusLoop() {
  while (true) {
    const Raft::NodeStatus status = raft_->GetStatus();
    std::ofstream output("run_data/tso_raft_status.json", std::ios::trunc);
    output << "{\"nodeId\":" << nodeId_ << ",\"term\":" << status.term
           << ",\"isLeader\":" << (status.isLeader ? "true" : "false")
           << ",\"commitIndex\":" << status.commitIndex
           << ",\"lastApplied\":" << status.lastApplied
           << ",\"lastLogIndex\":" << status.lastLogIndex
           << ",\"highWater\":" << committedHighWater_.load(std::memory_order_acquire)
           << ",\"nextTimestamp\":" << nextTimestamp_.load(std::memory_order_acquire)
           << ",\"activeRangeHighWater\":"
           << activeRangeHighWater_.load(std::memory_order_acquire)
           << ",\"rangeProposals\":" << rangeProposalCount_.load(std::memory_order_relaxed)
           << ",\"rangeReservations\":"
           << rangeReservationCount_.load(std::memory_order_relaxed)
           << ",\"timestampsAllocated\":"
           << timestampAllocationCount_.load(std::memory_order_relaxed)
           << ",\"fenceValid\":" << (HasValidFence() ? "true" : "false") << "}";
    output.close();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void TsoConsensusNode::ApplyHighWater(const Op& command, int raftIndex) {
  const uint64_t highWater = ParseTimestamp(command.Value);
  localOracle_->Observe(highWater);
  uint64_t current = committedHighWater_.load(std::memory_order_acquire);
  while (current < highWater &&
         !committedHighWater_.compare_exchange_weak(current, highWater, std::memory_order_release,
                                                     std::memory_order_acquire)) {
  }
  if (command.Operation == "TsoReserveHighWater") {
    rangeReservationCount_.fetch_add(1, std::memory_order_relaxed);
  }

  WaitQueue queue;
  {
    std::lock_guard<std::mutex> lock(waitersMutex_);
    const auto found = waiters_.find(command.ClientId + "_" + std::to_string(command.RequestId));
    if (found != waiters_.end()) queue = found->second;
  }
  if (queue) queue->Push(ProposalResult{command, false});

  if (raftIndex > 0 && raftIndex % kSnapshotEveryAppliedEntries == 0) {
    raft_->Snapshot(raftIndex, std::to_string(committedHighWater_.load(std::memory_order_acquire)));
  }
}

void TsoConsensusNode::CompleteRejectedProposal(const Op& command) {
  WaitQueue queue;
  {
    std::lock_guard<std::mutex> lock(waitersMutex_);
    const auto found = waiters_.find(command.ClientId + "_" + std::to_string(command.RequestId));
    if (found != waiters_.end()) queue = found->second;
  }
  if (queue) queue->Push(ProposalResult{command, true});
}

std::string TsoConsensusNode::NextRequestKey(int requestId) const {
  return clientId_ + "_" + std::to_string(requestId);
}

void TsoConsensusNode::RemoveWaiter(const std::string& key, const WaitQueue& queue) {
  std::lock_guard<std::mutex> lock(waitersMutex_);
  const auto found = waiters_.find(key);
  if (found != waiters_.end() && found->second == queue) waiters_.erase(found);
}
