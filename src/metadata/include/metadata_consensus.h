#ifndef STRATAKV_METADATA_METADATA_CONSENSUS_H
#define STRATAKV_METADATA_METADATA_CONSENSUS_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "apply_msg.h"
#include "metadata_state_machine.h"
#include "raft.h"
#include "util.h"

class MetadataNotLeaderError final : public std::runtime_error {
 public:
  explicit MetadataNotLeaderError(const std::string& message) : std::runtime_error(message) {}
};

class MetadataTimeoutError final : public std::runtime_error {
 public:
  explicit MetadataTimeoutError(const std::string& message) : std::runtime_error(message) {}
};

class MetadataConsensusNode {
 public:
  using Endpoint = std::pair<std::string, short>;

  MetadataConsensusNode(int nodeId, std::vector<Endpoint> peers, std::string dataDirectory,
                        int snapshotEveryAppliedEntries = 64);
  ~MetadataConsensusNode();

  void Start();
  // Bounded shutdown: exits the apply loop, joins the apply thread, and wakes
  // every pending proposal waiter with an explicit timeout result. Raft's own
  // workers follow the process-lifetime model shared with RegionPeer.
  void Stop();
  bool IsStopped() const { return stopping_.load(std::memory_order_acquire); }
  metadataRpcProtocol::MetadataCommandResult Mutate(
      const metadataRpcProtocol::MetadataCommand& command,
      std::chrono::steady_clock::time_point deadline);
  std::shared_ptr<const MetadataView> LinearizableView(
      std::chrono::steady_clock::time_point deadline);

  Raft* RaftService() const { return raft_.get(); }
  Raft::NodeStatus Status() const { return raft_->GetStatus(); }
  int NodeId() const { return nodeId_; }
  MetadataStateMetrics StateMetrics() const { return stateMachine_.Metrics(); }
  std::shared_ptr<const MetadataView> LocalView() const { return stateMachine_.View(); }
  size_t PendingWaitersForTest() const {
    std::lock_guard<std::mutex> lock(waitersMutex_);
    return waiters_.size();
  }

 private:
  struct ProposalResult {
    metadataRpcProtocol::MetadataCommandResult result;
    bool rejected = false;
  };
  using WaitQueue = std::shared_ptr<LockQueue<ProposalResult>>;

  void ApplyLoop();
  bool WaitUntilApplied(int index, std::chrono::steady_clock::time_point deadline);
  void Complete(const Op& operation, ProposalResult result);
  void RemoveWaiter(const std::string& key, const WaitQueue& queue);
  static std::string WaiterKey(const Op& operation);

  const int nodeId_;
  const std::vector<Endpoint> peers_;
  const std::string clientId_;
  const int snapshotEveryAppliedEntries_;
  std::shared_ptr<Persister> persister_;
  std::shared_ptr<LockQueue<ApplyMsg>> applyChannel_;
  std::shared_ptr<Raft> raft_;
  MetadataStateMachine stateMachine_;
  std::thread applyThread_;
  std::atomic<bool> stopping_{false};
  std::atomic<int> nextRequestId_{1};
  std::atomic<int> stateMachineAppliedIndex_{0};
  std::mutex appliedMutex_;
  std::condition_variable appliedCondition_;
  mutable std::mutex waitersMutex_;
  std::unordered_map<std::string, WaitQueue> waiters_;
};

#endif  // STRATAKV_METADATA_METADATA_CONSENSUS_H
