#ifndef STRATAKV_TRANSACTION_TXN_SCHEDULER_H
#define STRATAKV_TRANSACTION_TXN_SCHEDULER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "bounded_thread_pool.h"
#include "mvcc_storage.h"
#include "util.h"

enum class TxnCommandType {
  Prewrite,
  Commit,
  Rollback,
  PessimisticLock,
  CheckTxnStatus,
  ResolveLock,
  GarbageCollect,
};

enum class TxnLatchMode {
  Keys,
  RegionExclusive,
};

enum class TxnTaskState {
  WaitingLatch,
  Queued,
  Preparing,
  Proposed,
  Applied,
  Failed,
};

struct TxnCommand {
  TxnCommandType type = TxnCommandType::Prewrite;
  TxnLatchMode latchMode = TxnLatchMode::Keys;
  int regionId = -1;
  std::vector<std::string> keys;
  std::string key;
  std::string value;
  std::string primaryKey;
  std::string clientId;
  int requestId = 0;
  uint64_t startTs = 0;
  uint64_t commitTs = 0;
  uint64_t ttlMs = 0;
  uint64_t forUpdateTs = 0;
  uint64_t expireAtPhysicalMs = 0;
  uint64_t currentPhysicalMs = 0;
  uint64_t safePointTs = 0;
  TxnRecordState resolutionState = TxnRecordState::NotFound;
  bool rollbackIfExpired = false;
  bool isDelete = false;
  bool isLockOnly = false;
  std::chrono::steady_clock::time_point deadline;
};

struct TxnScheduleResult {
  std::string status = std::to_string(static_cast<int>(TxnStatus::StorageError));
  uint64_t removedCount = 0;
  int raftIndex = -1;
  bool applied = false;
  bool responseTimedOut = false;
  TxnStatus readStatus = TxnStatus::NotFound;
  std::string value;
  uint64_t valueCommitTs = 0;
  TxnRecordStatus txnRecordStatus;
};

// Region-local storage and Raft operations used by the node-level scheduler.
// The scheduler stores weak references to this interface so it cannot extend a
// RegionPeer lifetime or form a NodeServer/Scheduler/RegionPeer ownership cycle.
class TxnRegionExecutor {
 public:
  virtual ~TxnRegionExecutor() = default;
  virtual int TxnRegionId() const = 0;
  virtual bool IsTxnLeader() = 0;
  virtual PreparedMvccWrite PrepareTxn(const TxnCommand& command) = 0;
  virtual bool ProposeTxn(const Op& op, int* raftIndex) = 0;
  virtual std::vector<std::pair<std::string, MvccLock>> ExpiredLocks(uint64_t currentPhysicalMs) = 0;
};

// One latch table is shared by every RegionPeer on a physical node. Slots are
// shards of distinct (RegionId, key) resources, not mutexes themselves: equal
// key strings in different Regions therefore never block one another, even if
// their hashes happen to select the same slot.
class NodeLatchManager {
 public:
  struct Stats {
    uint64_t acquisitions = 0;
    uint64_t waits = 0;
    uint64_t waitMicros = 0;
    uint64_t currentWaiters = 0;
    uint64_t maxWaiters = 0;
    uint64_t held = 0;
  };

  explicit NodeLatchManager(size_t slotCount = 4096);

  // Registers a command. Returns true when all requested resources are owned;
  // false means the command is queued without blocking its caller.
  bool Acquire(uint64_t commandId, int regionId, const std::vector<std::string>& keys,
               TxnLatchMode mode);

  // Releases or cancels a command and returns command IDs that became ready.
  std::vector<uint64_t> Release(uint64_t commandId);
  std::vector<uint64_t> Cancel(uint64_t commandId);

  bool Holds(uint64_t commandId) const;
  size_t SlotCount() const { return slots_.size(); }
  Stats GetStats() const;

 private:
  struct ResourceId {
    int regionId = -1;
    std::string key;

    bool operator==(const ResourceId& other) const {
      return regionId == other.regionId && key == other.key;
    }
  };

  struct ResourceHash {
    size_t operator()(const ResourceId& resource) const;
  };

  struct ResourceState {
    uint64_t owner = 0;
    std::deque<uint64_t> waiters;
  };

  struct Slot {
    std::unordered_map<ResourceId, ResourceState, ResourceHash> resources;
  };

  struct RegionGate {
    uint64_t exclusiveOwner = 0;
    size_t activeKeyOwners = 0;
    std::deque<uint64_t> exclusiveWaiters;
  };

  struct LockRequest {
    uint64_t commandId = 0;
    int regionId = -1;
    TxnLatchMode mode = TxnLatchMode::Keys;
    std::vector<ResourceId> resources;
    bool acquired = false;
    bool countedWait = false;
    std::chrono::steady_clock::time_point waitStarted;
  };

  size_t SlotIndex(const ResourceId& resource) const;
  bool TryGrantLocked(LockRequest* request);
  std::vector<uint64_t> GrantReadyLocked();
  void FinishWaitLocked(LockRequest* request);
  void RemoveWaiterLocked(std::deque<uint64_t>* waiters, uint64_t commandId);
  void CleanupResourcesLocked(const LockRequest& request);

  mutable std::mutex mutex_;
  std::vector<Slot> slots_;
  std::unordered_map<int, RegionGate> regionGates_;
  std::unordered_map<uint64_t, LockRequest> requests_;
  Stats stats_;
};

// TiKV-style Store/Node-level transaction command scheduler. There is one
// instance per NodeServer. It schedules commands for many RegionPeers while
// each RegionPeer retains its own Raft, Apply loop, MVCC and RocksDB instance.
class NodeTxnScheduler : public std::enable_shared_from_this<NodeTxnScheduler> {
 public:
  using Completion = std::function<void(const TxnScheduleResult&)>;

  struct Stats {
    NodeLatchManager::Stats latches;
    uint64_t accepted = 0;
    uint64_t rejected = 0;
    uint64_t prepareConflicts = 0;
    uint64_t raftProposals = 0;
    uint64_t applied = 0;
    uint64_t responseTimeouts = 0;
    uint64_t lateApplies = 0;
    uint64_t prepareMicros = 0;
    uint64_t applyWaitMicros = 0;
    size_t pendingTasks = 0;
    size_t workerThreads = 0;
    size_t queuedWorkers = 0;
    size_t activeWorkers = 0;
    uint64_t workerRejections = 0;
  };

  explicit NodeTxnScheduler(size_t latchSlots = 4096, size_t workerThreads = 8,
                            size_t queueCapacity = 1024, size_t maxPendingTasks = 4096);
  ~NodeTxnScheduler();

  NodeTxnScheduler(const NodeTxnScheduler&) = delete;
  NodeTxnScheduler& operator=(const NodeTxnScheduler&) = delete;

  void RegisterRegion(const std::shared_ptr<TxnRegionExecutor>& region);
  void OnRegionRemoved(int regionId);
  std::vector<std::shared_ptr<TxnRegionExecutor>> Regions() const;

  // Safe for an RPC worker: this copies/owns the command and never waits for a
  // latch or Raft Apply. Completion can run on a scheduler or Region Apply
  // thread and must therefore own everything it needs after the RPC returns.
  void Schedule(TxnCommand command, Completion completion);

  // Called only after a committed Region log entry has been applied to MVCC /
  // RocksDB. This is the point at which the command's latch can be released.
  void OnApplied(int regionId, const Op& appliedOp, int raftIndex);

  Stats GetStats() const;
  size_t LatchSlotCount() const { return latches_.SlotCount(); }
  size_t WorkerCount() const { return workerPool_.workers(); }

 private:
  struct RequestKey {
    int regionId = -1;
    std::string clientId;
    int requestId = 0;

    bool operator==(const RequestKey& other) const {
      return regionId == other.regionId && requestId == other.requestId &&
             clientId == other.clientId;
    }
  };

  struct RequestKeyHash {
    size_t operator()(const RequestKey& key) const;
  };

  struct WaitingResponse {
    Completion completion;
    std::chrono::steady_clock::time_point deadline;
    bool completed = false;
  };

  struct TaskContext {
    uint64_t commandId = 0;
    TxnCommand command;
    RequestKey requestKey;
    TxnTaskState state = TxnTaskState::WaitingLatch;
    std::weak_ptr<TxnRegionExecutor> region;
    std::vector<WaitingResponse> responses;
    std::chrono::steady_clock::time_point createdAt;
    std::chrono::steady_clock::time_point proposedAt;
    int raftIndex = -1;
    bool proposed = false;
    PreparedMvccWrite preparedResponse;
  };

  static RequestKey MakeRequestKey(const TxnCommand& command);
  static TxnScheduleResult WrongLeaderResult(bool timedOut = false);
  static TxnScheduleResult StatusResult(TxnStatus status);
  static TxnScheduleResult PreparedResult(const PreparedMvccWrite& prepared);
  static Op BuildOp(const TxnCommand& command, const PreparedMvccWrite* prepared);

  std::shared_ptr<TxnRegionExecutor> FindRegion(int regionId) const;
  void Dispatch(uint64_t commandId);
  void DispatchAll(const std::vector<uint64_t>& commandIds);
  void Execute(uint64_t commandId);
  void FinishBeforeProposal(uint64_t commandId, const TxnScheduleResult& result);
  void DeadlineLoop();

  mutable std::mutex mutex_;
  std::condition_variable deadlineChanged_;
  std::unordered_map<int, std::weak_ptr<TxnRegionExecutor>> regions_;
  std::unordered_map<uint64_t, std::shared_ptr<TaskContext>> tasks_;
  std::unordered_map<RequestKey, uint64_t, RequestKeyHash> requests_;
  NodeLatchManager latches_;
  BoundedThreadPool workerPool_;
  const size_t maxPendingTasks_;
  std::atomic<uint64_t> nextCommandId_{0};
  std::atomic<bool> stopping_{false};
  std::thread deadlineThread_;

  std::atomic<uint64_t> accepted_{0};
  std::atomic<uint64_t> rejected_{0};
  std::atomic<uint64_t> prepareConflicts_{0};
  std::atomic<uint64_t> raftProposals_{0};
  std::atomic<uint64_t> applied_{0};
  std::atomic<uint64_t> responseTimeouts_{0};
  std::atomic<uint64_t> lateApplies_{0};
  std::atomic<uint64_t> prepareMicros_{0};
  std::atomic<uint64_t> applyWaitMicros_{0};
};

#endif  // STRATAKV_TRANSACTION_TXN_SCHEDULER_H
