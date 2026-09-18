#include "metadata_consensus.h"

#include <unistd.h>

#include <set>
#include <thread>

#include "raft_rpc_util.h"

namespace {

constexpr int kMetadataRaftGroupId = -2;

metadataRpcProtocol::MetadataCommandResult TimeoutResult(const std::string& message,
                                                         uint64_t revision) {
  metadataRpcProtocol::MetadataCommandResult result;
  result.set_error(metadataRpcProtocol::METADATA_TIMEOUT);
  result.set_message(message);
  result.set_revision(revision);
  return result;
}

}  // namespace

MetadataConsensusNode::MetadataConsensusNode(int nodeId, std::vector<Endpoint> peers,
                                             std::string dataDirectory,
                                             int snapshotEveryAppliedEntries)
    : nodeId_(nodeId),
      peers_(std::move(peers)),
      clientId_("metadata-" + std::to_string(nodeId) + "-" + std::to_string(getpid())),
      snapshotEveryAppliedEntries_(snapshotEveryAppliedEntries),
      persister_(std::make_shared<Persister>("metadata_node" + std::to_string(nodeId),
                                             std::move(dataDirectory))),
      applyChannel_(std::make_shared<LockQueue<ApplyMsg>>()),
      raft_(std::make_shared<Raft>()) {
  if (peers_.size() != 3) {
    throw std::invalid_argument("metadata consensus requires exactly three peers");
  }
  if (nodeId_ < 0 || nodeId_ >= static_cast<int>(peers_.size())) {
    throw std::invalid_argument("metadata node ID is outside the peer list");
  }
  if (snapshotEveryAppliedEntries_ <= 0) {
    throw std::invalid_argument("metadata snapshot threshold must be positive");
  }
  const std::set<Endpoint> uniquePeers(peers_.begin(), peers_.end());
  if (uniquePeers.size() != peers_.size()) {
    throw std::invalid_argument("metadata peer endpoints must be unique");
  }
}

void MetadataConsensusNode::Start() {
  std::vector<std::shared_ptr<RaftRpcUtil>> peers;
  peers.reserve(peers_.size());
  for (size_t index = 0; index < peers_.size(); ++index) {
    if (static_cast<int>(index) == nodeId_) peers.push_back(nullptr);
    else peers.push_back(std::make_shared<RaftRpcUtil>(peers_[index].first,
                                                       peers_[index].second,
                                                       kMetadataRaftGroupId));
  }
  raft_->init(std::move(peers), nodeId_, persister_, applyChannel_, true);

  const std::string snapshot = persister_->ReadSnapshot();
  if (!snapshot.empty()) {
    std::string error;
    if (!stateMachine_.Restore(snapshot, &error)) {
      throw std::runtime_error("unable to restore metadata snapshot: " + error);
    }
  }
  stateMachineAppliedIndex_.store(raft_->GetStatus().lastApplied, std::memory_order_release);
  applyThread_ = std::thread(&MetadataConsensusNode::ApplyLoop, this);
  raft_->Activate();
}

void MetadataConsensusNode::Stop() {
  bool expected = false;
  if (!stopping_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    if (applyThread_.joinable() && applyThread_.get_id() != std::this_thread::get_id()) {
      applyThread_.join();
    }
    return;
  }
  // A default ApplyMsg carries no valid flag and only exists to wake Pop().
  applyChannel_->Push(ApplyMsg{});
  if (applyThread_.joinable()) {
    if (applyThread_.get_id() == std::this_thread::get_id()) {
      applyThread_.detach();
    } else {
      applyThread_.join();
    }
  }
  // Resolve every pending proposal waiter with an explicit timeout result so
  // no caller blocks past shutdown, then drop the waiter table.
  metadataRpcProtocol::MetadataCommandResult shutdownResult =
      TimeoutResult("metadata node is shutting down", stateMachine_.View()->revision);
  std::lock_guard<std::mutex> lock(waitersMutex_);
  for (auto& entry : waiters_) {
    entry.second->Push(ProposalResult{shutdownResult, false});
  }
  waiters_.clear();
}

MetadataConsensusNode::~MetadataConsensusNode() { Stop(); }

metadataRpcProtocol::MetadataCommandResult MetadataConsensusNode::Mutate(
    const metadataRpcProtocol::MetadataCommand& command,
    std::chrono::steady_clock::time_point deadline) {
  if (stopping_.load(std::memory_order_acquire)) {
    throw MetadataNotLeaderError("metadata node is shutting down");
  }
  Op operation;
  operation.Operation = "MetadataCommand";
  if (!command.SerializeToString(&operation.Value)) {
    throw std::invalid_argument("unable to serialize metadata command");
  }
  operation.ClientId = clientId_;
  operation.RequestId = nextRequestId_.fetch_add(1, std::memory_order_relaxed);
  if (operation.RequestId <= 0) throw std::overflow_error("metadata request ID exhausted");

  const std::string key = WaiterKey(operation);
  const WaitQueue queue = std::make_shared<LockQueue<ProposalResult>>();
  {
    std::lock_guard<std::mutex> lock(waitersMutex_);
    waiters_.emplace(key, queue);
  }

  int index = -1;
  int term = -1;
  bool leader = false;
  raft_->Start(operation, &index, &term, &leader);
  if (!leader) {
    RemoveWaiter(key, queue);
    throw MetadataNotLeaderError("metadata mutation reached a follower");
  }

  const auto now = std::chrono::steady_clock::now();
  if (now >= deadline) {
    RemoveWaiter(key, queue);
    return TimeoutResult("metadata mutation deadline expired", stateMachine_.View()->revision);
  }
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
  const int waitMs = static_cast<int>(std::max<int64_t>(1, remaining.count()));
  ProposalResult applied;
  if (!queue->timeOutPop(waitMs, &applied)) {
    RemoveWaiter(key, queue);
    return TimeoutResult("metadata mutation did not commit before deadline",
                         stateMachine_.View()->revision);
  }
  RemoveWaiter(key, queue);
  if (applied.rejected || !raft_->IsLeaderInTerm(term)) {
    throw MetadataNotLeaderError("metadata leader changed while committing mutation");
  }
  return applied.result;
}

std::shared_ptr<const MetadataView> MetadataConsensusNode::LinearizableView(
    std::chrono::steady_clock::time_point deadline) {
  if (stopping_.load(std::memory_order_acquire)) {
    throw MetadataNotLeaderError("metadata node is shutting down");
  }
  const Raft::ReadIndexResult read = raft_->ReadIndex(deadline);
  if (read.status == Raft::ReadIndexStatus::NotLeader) {
    throw MetadataNotLeaderError("metadata read reached a follower");
  }
  if (!read.ok() || !WaitUntilApplied(read.readIndex, deadline)) {
    throw MetadataTimeoutError("metadata read could not reach an applied quorum barrier");
  }
  if (!raft_->IsLeaderInTerm(read.term)) {
    throw MetadataNotLeaderError("metadata leader changed during read barrier");
  }
  return stateMachine_.View();
}

bool MetadataConsensusNode::WaitUntilApplied(
    int index, std::chrono::steady_clock::time_point deadline) {
  std::unique_lock<std::mutex> lock(appliedMutex_);
  return appliedCondition_.wait_until(lock, deadline, [this, index] {
    return stateMachineAppliedIndex_.load(std::memory_order_acquire) >= index;
  });
}

void MetadataConsensusNode::ApplyLoop() {
  while (true) {
    const ApplyMsg message = applyChannel_->Pop();
    if (stopping_.load(std::memory_order_acquire)) return;
    if (message.CommandValid) {
      Op operation;
      if (!operation.parseFromString(message.Command)) {
        std::abort();
      }
      if (operation.Operation == "MetadataCommand") {
        metadataRpcProtocol::MetadataCommand command;
        if (!command.ParseFromString(operation.Value)) std::abort();
        Complete(operation, ProposalResult{stateMachine_.Apply(command), false});
      }
      stateMachineAppliedIndex_.store(message.CommandIndex, std::memory_order_release);
      appliedCondition_.notify_all();
      if (message.CommandIndex > 0 &&
          message.CommandIndex % snapshotEveryAppliedEntries_ == 0) {
        if (!raft_->Snapshot(message.CommandIndex, stateMachine_.Snapshot())) {
          // A concurrent compaction or leadership change is harmless; a later
          // threshold will retry snapshot creation.
        }
      }
    }
    if (message.ProposalRejected) {
      Op operation;
      if (operation.parseFromString(message.Command) &&
          operation.Operation == "MetadataCommand") {
        ProposalResult result;
        result.rejected = true;
        Complete(operation, std::move(result));
      }
    }
    if (message.SnapshotValid) {
      std::string error;
      if (!stateMachine_.Restore(message.Snapshot, &error)) std::abort();
      stateMachineAppliedIndex_.store(message.SnapshotIndex, std::memory_order_release);
      appliedCondition_.notify_all();
    }
  }
}

void MetadataConsensusNode::Complete(const Op& operation, ProposalResult result) {
  WaitQueue queue;
  {
    std::lock_guard<std::mutex> lock(waitersMutex_);
    const auto found = waiters_.find(WaiterKey(operation));
    if (found != waiters_.end()) queue = found->second;
  }
  if (queue) queue->Push(result);
}

void MetadataConsensusNode::RemoveWaiter(const std::string& key, const WaitQueue& queue) {
  std::lock_guard<std::mutex> lock(waitersMutex_);
  const auto found = waiters_.find(key);
  if (found != waiters_.end() && found->second == queue) waiters_.erase(found);
}

std::string MetadataConsensusNode::WaiterKey(const Op& operation) {
  return operation.ClientId + "_" + std::to_string(operation.RequestId);
}
