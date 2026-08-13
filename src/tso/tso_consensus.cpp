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
constexpr int kSnapshotEveryAppliedEntries = 256;

uint64_t ParseTimestamp(const std::string& value) {
  size_t consumed = 0;
  const uint64_t timestamp = std::stoull(value, &consumed);
  if (consumed != value.size() || timestamp == 0) {
    throw std::runtime_error("invalid timestamp in TSO Raft command");
  }
  return timestamp;
}
}  // namespace

TsoConsensusNode::TsoConsensusNode(int nodeId, std::vector<Endpoint> peers,
                                   std::string statePath, uint64_t segmentSize)
    : nodeId_(nodeId),
      peers_(std::move(peers)),
      clientId_("tso-" + std::to_string(nodeId) + "-" + std::to_string(getpid())),
      localOracle_(std::make_shared<PersistentTimestampOracle>(std::move(statePath), segmentSize)),
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
  const uint64_t next = localOracle_->Peek();
  committedHighWater_.store(next == 0 ? 0 : next - 1, std::memory_order_release);
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
    const uint64_t timestamp = ParseTimestamp(snapshot);
    localOracle_->Observe(timestamp);
    committedHighWater_.store(std::max(committedHighWater_.load(), timestamp),
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
  // Preserve candidate order in the Raft proposal queue, but release the
  // mutex before waiting for a majority so concurrent RPCs can be batched.
  std::unique_lock<std::mutex> lock(proposalMutex_);
  RequireReadyLeader();
  const uint64_t candidate = localOracle_->Next();
  return ProposeTimestamp(candidate, "TsoAllocate", &lock);
}

uint64_t TsoConsensusNode::Peek() {
  int term = 0;
  bool isLeader = false;
  raft_->GetState(&term, &isLeader);
  if (!isLeader) throw TsoNotLeaderError("TSO node is not leader");
  const uint64_t highWater = committedHighWater_.load(std::memory_order_acquire);
  if (highWater == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("TSO timestamp space is exhausted");
  }
  return highWater + 1;
}

void TsoConsensusNode::Observe(uint64_t timestamp) {
  if (timestamp == 0) return;
  std::unique_lock<std::mutex> lock(proposalMutex_);
  RequireReadyLeader();
  if (timestamp <= committedHighWater_.load(std::memory_order_acquire)) return;

  // Move the local durable fence before publication. If consensus fails this
  // only creates a harmless gap; it can never make a timestamp repeat.
  localOracle_->Observe(timestamp);
  ProposeTimestamp(timestamp, "TsoObserve", &lock);
}

void TsoConsensusNode::RequireReadyLeader() {
  const Raft::NodeStatus status = raft_->GetStatus();
  if (!status.isLeader) throw TsoNotLeaderError("TSO node is not leader");
  if (readyTerm_.load(std::memory_order_acquire) == status.term) return;

  // A newly elected Raft leader appends a no-op in its own term. Waiting until
  // the TSO state machine has applied through that entry guarantees inherited
  // committed allocations are visible before this leader chooses a candidate.
  if (stateMachineAppliedIndex_.load(std::memory_order_acquire) < status.lastLogIndex) {
    throw TsoNotLeaderError("TSO leader is still applying its Raft log");
  }
  readyTerm_.store(status.term, std::memory_order_release);
}

uint64_t TsoConsensusNode::ProposeTimestamp(uint64_t timestamp, const std::string& operation,
                                            std::unique_lock<std::mutex>* proposalLock) {
  const int requestId = nextRequestId_.fetch_add(1, std::memory_order_relaxed);
  if (requestId <= 0) throw std::overflow_error("TSO request id space is exhausted");

  Op command;
  command.Operation = operation;
  command.Value = std::to_string(timestamp);
  command.ClientId = clientId_;
  command.RequestId = requestId;
  const std::string requestKey = NextRequestKey(requestId);
  const WaitQueue queue = std::make_shared<LockQueue<Op>>();
  {
    std::lock_guard<std::mutex> lock(waitersMutex_);
    waiters_[requestKey] = queue;
  }

  int raftIndex = -1;
  int term = -1;
  bool isLeader = false;
  raft_->Start(command, &raftIndex, &term, &isLeader);
  proposalLock->unlock();
  if (!isLeader) {
    RemoveWaiter(requestKey, queue);
    throw TsoNotLeaderError("TSO leadership changed before proposal");
  }

  Op applied;
  if (!queue->timeOutPop(CONSENSUS_TIMEOUT * 4, &applied)) {
    RemoveWaiter(requestKey, queue);
    throw TsoNotLeaderError("TSO proposal did not reach a majority");
  }
  RemoveWaiter(requestKey, queue);
  if (applied.Operation != operation || applied.ClientId != clientId_ ||
      applied.RequestId != requestId || ParseTimestamp(applied.Value) != timestamp) {
    throw std::runtime_error("TSO applied command does not match proposal");
  }
  return timestamp;
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
        if (command.Operation == "TsoAllocate" || command.Operation == "TsoObserve") {
          ApplyTimestamp(command, message.CommandIndex);
        }
        stateMachineAppliedIndex_.store(message.CommandIndex, std::memory_order_release);
      }
      if (message.SnapshotValid &&
          raft_->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
        const uint64_t timestamp = ParseTimestamp(message.Snapshot);
        localOracle_->Observe(timestamp);
        committedHighWater_.store(std::max(committedHighWater_.load(), timestamp),
                                  std::memory_order_release);
        stateMachineAppliedIndex_.store(message.SnapshotIndex, std::memory_order_release);
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
           << ",\"highWater\":" << committedHighWater_.load(std::memory_order_acquire) << "}";
    output.close();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void TsoConsensusNode::ApplyTimestamp(const Op& command, int raftIndex) {
  const uint64_t timestamp = ParseTimestamp(command.Value);
  localOracle_->Observe(timestamp);
  uint64_t current = committedHighWater_.load(std::memory_order_acquire);
  while (current < timestamp &&
         !committedHighWater_.compare_exchange_weak(current, timestamp, std::memory_order_release,
                                                     std::memory_order_acquire)) {
  }

  WaitQueue queue;
  {
    std::lock_guard<std::mutex> lock(waitersMutex_);
    const auto found = waiters_.find(command.ClientId + "_" + std::to_string(command.RequestId));
    if (found != waiters_.end()) queue = found->second;
  }
  if (queue) queue->Push(command);

  if (raftIndex > 0 && raftIndex % kSnapshotEveryAppliedEntries == 0) {
    raft_->Snapshot(raftIndex, std::to_string(committedHighWater_.load(std::memory_order_acquire)));
  }
}

std::string TsoConsensusNode::NextRequestKey(int requestId) const {
  return clientId_ + "_" + std::to_string(requestId);
}

void TsoConsensusNode::RemoveWaiter(const std::string& key, const WaitQueue& queue) {
  std::lock_guard<std::mutex> lock(waitersMutex_);
  const auto found = waiters_.find(key);
  if (found != waiters_.end() && found->second == queue) waiters_.erase(found);
}
