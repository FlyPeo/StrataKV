#ifndef STRATAKV_TSO_TSO_CONSENSUS_H
#define STRATAKV_TSO_TSO_CONSENSUS_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "apply_msg.h"
#include "raft.h"
#include "timestamp_oracle.h"
#include "util.h"

class TsoNotLeaderError final : public std::runtime_error {
 public:
  explicit TsoNotLeaderError(const std::string& message) : std::runtime_error(message) {}
};

// One member of the TSO control-plane Raft group. Returned timestamps are
// committed Raft commands; PersistentTimestampOracle provides a durable local
// reservation fence so a restarted member can only move the sequence forward.
class TsoConsensusNode final : public TimestampOracle {
 public:
  using Endpoint = std::pair<std::string, short>;

  TsoConsensusNode(int nodeId, std::vector<Endpoint> peers, std::string statePath,
                   uint64_t segmentSize = 4096);

  void Start();
  Raft* RaftService() const { return raft_.get(); }
  int NodeId() const { return nodeId_; }
  Raft::NodeStatus Status() const;
  uint64_t HighWater() const { return committedHighWater_.load(std::memory_order_acquire); }

  uint64_t Next() override;
  uint64_t Peek() override;
  void Observe(uint64_t ts) override;

 private:
  using WaitQueue = std::shared_ptr<LockQueue<Op>>;

  uint64_t ProposeTimestamp(uint64_t timestamp, const std::string& operation,
                            std::unique_lock<std::mutex>* proposalLock);
  void RequireReadyLeader();
  void ApplyLoop();
  void StatusLoop();
  void ApplyTimestamp(const Op& op, int raftIndex);
  std::string NextRequestKey(int requestId) const;
  void RemoveWaiter(const std::string& key, const WaitQueue& queue);

  const int nodeId_;
  const std::vector<Endpoint> peers_;
  const std::string clientId_;
  std::shared_ptr<PersistentTimestampOracle> localOracle_;
  std::shared_ptr<Persister> persister_;
  std::shared_ptr<LockQueue<ApplyMsg>> applyChannel_;
  std::shared_ptr<Raft> raft_;
  std::atomic<uint64_t> committedHighWater_{0};
  std::atomic<int> stateMachineAppliedIndex_{0};
  std::atomic<int> readyTerm_{-1};
  std::atomic<int> nextRequestId_{1};
  std::mutex proposalMutex_;
  std::mutex waitersMutex_;
  std::unordered_map<std::string, WaitQueue> waiters_;
};

#endif  // STRATAKV_TSO_TSO_CONSENSUS_H
