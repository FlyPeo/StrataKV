#ifndef STRATAKV_TSO_TSO_CONSENSUS_H
#define STRATAKV_TSO_TSO_CONSENSUS_H

#include <atomic>
#include <chrono>
#include <condition_variable>
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

// One member of the TSO control-plane Raft group. Raft commits inclusive range
// high-water marks; a fenced leader allocates individual timestamps from the
// committed range without proposing one log entry per timestamp.
class TsoConsensusNode final : public TimestampOracle {
 public:
  static constexpr uint32_t kProtocolVersion = 3;

  using Endpoint = std::pair<std::string, short>;

  TsoConsensusNode(int nodeId, std::vector<Endpoint> peers, std::string statePath,
                   uint64_t rangeSize = 4096);

  void Start();
  Raft* RaftService() const { return raft_.get(); }
  int NodeId() const { return nodeId_; }
  Raft::NodeStatus Status() const;
  uint64_t HighWater() const { return committedHighWater_.load(std::memory_order_acquire); }
  uint64_t RangeProposalCount() const {
    return rangeProposalCount_.load(std::memory_order_relaxed);
  }
  uint64_t RangeReservationCount() const {
    return rangeReservationCount_.load(std::memory_order_relaxed);
  }
  uint64_t TimestampAllocationCount() const {
    return timestampAllocationCount_.load(std::memory_order_relaxed);
  }
  uint64_t RangeSize() const { return rangeSize_; }
  uint64_t NextTimestamp() const { return nextTimestamp_.load(std::memory_order_acquire); }
  uint64_t ActiveRangeHighWater() const {
    return activeRangeHighWater_.load(std::memory_order_acquire);
  }
  bool HasValidFence() const;

  uint64_t Next() override;
  uint64_t Peek() override;
  void Observe(uint64_t ts) override;

 private:
  struct ProposalResult {
    Op command;
    bool rejected = false;
  };

  using WaitQueue = std::shared_ptr<LockQueue<ProposalResult>>;

  int RequireValidFenceLocked();
  void RefreshFenceLocked();
  int ValidFenceTerm() const;
  uint64_t TryAllocateFast(int term);
  void ReserveRangeLocked(uint64_t minimumNext, int term);
  void ProposeHighWater(uint64_t highWater, int* proposalTerm);
  bool WaitUntilApplied(int index, std::chrono::steady_clock::time_point deadline);
  void ApplyLoop();
  void StatusLoop();
  void ApplyHighWater(const Op& op, int raftIndex);
  void CompleteRejectedProposal(const Op& op);
  std::string NextRequestKey(int requestId) const;
  void RemoveWaiter(const std::string& key, const WaitQueue& queue);
  static int64_t SteadyNowNanos();

  const int nodeId_;
  const std::vector<Endpoint> peers_;
  const std::string clientId_;
  const uint64_t rangeSize_;
  // This is only a node-local durable candidate floor. The authoritative
  // cluster high-water is committedHighWater_, reconstructed from Raft.
  std::shared_ptr<PersistentTimestampOracle> localOracle_;
  std::shared_ptr<Persister> persister_;
  std::shared_ptr<LockQueue<ApplyMsg>> applyChannel_;
  std::shared_ptr<Raft> raft_;
  std::atomic<uint64_t> committedHighWater_{0};
  std::atomic<uint64_t> nextTimestamp_{0};
  std::atomic<uint64_t> activeRangeHighWater_{0};
  std::atomic<int> activeRangeTerm_{-1};
  std::atomic<int> stateMachineAppliedIndex_{0};
  std::atomic<int> fenceTerm_{-1};
  std::atomic<int64_t> fenceDeadlineNanos_{0};
  std::atomic<int> nextRequestId_{1};
  std::atomic<uint64_t> rangeProposalCount_{0};
  std::atomic<uint64_t> rangeReservationCount_{0};
  std::atomic<uint64_t> timestampAllocationCount_{0};
  std::mutex slowPathMutex_;
  std::mutex appliedMutex_;
  std::condition_variable appliedCondition_;
  std::mutex waitersMutex_;
  std::unordered_map<std::string, WaitQueue> waiters_;
};

#endif  // STRATAKV_TSO_TSO_CONSENSUS_H
