#include "txn_scheduler.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include "config.h"

namespace {

template <typename Atomic>
uint64_t LoadRelaxed(const Atomic& value) {
  return value.load(std::memory_order_relaxed);
}

}  // namespace

size_t NodeLatchManager::ResourceHash::operator()(const ResourceId& resource) const {
  size_t seed = std::hash<int>{}(resource.regionId);
  seed ^= std::hash<std::string>{}(resource.key) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
  return seed;
}

NodeLatchManager::NodeLatchManager(size_t slotCount) : slots_(slotCount) {
  if (slotCount == 0) throw std::invalid_argument("NodeLatchManager requires at least one slot");
}

size_t NodeLatchManager::SlotIndex(const ResourceId& resource) const {
  return ResourceHash{}(resource) % slots_.size();
}

void NodeLatchManager::RemoveWaiterLocked(std::deque<uint64_t>* waiters, uint64_t commandId) {
  const auto found = std::find(waiters->begin(), waiters->end(), commandId);
  if (found != waiters->end()) waiters->erase(found);
}

void NodeLatchManager::FinishWaitLocked(LockRequest* request) {
  if (!request->countedWait) return;
  request->countedWait = false;
  if (stats_.currentWaiters > 0) --stats_.currentWaiters;
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - request->waitStarted)
                          .count();
  if (micros > 0) stats_.waitMicros += static_cast<uint64_t>(micros);
}

bool NodeLatchManager::TryGrantLocked(LockRequest* request) {
  if (request->acquired) return true;
  RegionGate& gate = regionGates_[request->regionId];

  if (request->mode == TxnLatchMode::RegionExclusive) {
    if (gate.exclusiveOwner != 0 || gate.activeKeyOwners != 0 || gate.exclusiveWaiters.empty() ||
        gate.exclusiveWaiters.front() != request->commandId) {
      return false;
    }
    gate.exclusiveWaiters.pop_front();
    gate.exclusiveOwner = request->commandId;
  } else {
    if (gate.exclusiveOwner != 0) return false;
    // Commands queued before an exclusive command are allowed to drain; later
    // key commands wait so Region maintenance cannot starve indefinitely.
    if (!gate.exclusiveWaiters.empty() && gate.exclusiveWaiters.front() < request->commandId) return false;
    for (const ResourceId& resource : request->resources) {
      ResourceState& state = slots_[SlotIndex(resource)].resources[resource];
      if (state.owner != 0 || state.waiters.empty() || state.waiters.front() != request->commandId) {
        return false;
      }
    }
    for (const ResourceId& resource : request->resources) {
      ResourceState& state = slots_[SlotIndex(resource)].resources[resource];
      state.waiters.pop_front();
      state.owner = request->commandId;
    }
    ++gate.activeKeyOwners;
  }

  request->acquired = true;
  ++stats_.acquisitions;
  ++stats_.held;
  FinishWaitLocked(request);
  return true;
}

bool NodeLatchManager::Acquire(uint64_t commandId, int regionId, const std::vector<std::string>& keys,
                               TxnLatchMode mode) {
  if (commandId == 0 || regionId < 0) throw std::invalid_argument("invalid latch command or Region ID");
  std::lock_guard<std::mutex> lock(mutex_);
  if (requests_.find(commandId) != requests_.end()) throw std::logic_error("duplicate latch command ID");

  LockRequest request;
  request.commandId = commandId;
  request.regionId = regionId;
  request.mode = mode;
  if (mode == TxnLatchMode::Keys) {
    request.resources.reserve(keys.size());
    for (const std::string& key : keys) request.resources.push_back({regionId, key});
    std::sort(request.resources.begin(), request.resources.end(), [this](const ResourceId& lhs,
                                                                         const ResourceId& rhs) {
      const size_t lhsSlot = SlotIndex(lhs);
      const size_t rhsSlot = SlotIndex(rhs);
      if (lhsSlot != rhsSlot) return lhsSlot < rhsSlot;
      if (lhs.regionId != rhs.regionId) return lhs.regionId < rhs.regionId;
      return lhs.key < rhs.key;
    });
    request.resources.erase(std::unique(request.resources.begin(), request.resources.end()),
                            request.resources.end());
    if (request.resources.empty()) throw std::invalid_argument("key latch requires at least one key");
  }

  auto inserted = requests_.emplace(commandId, std::move(request));
  LockRequest* stored = &inserted.first->second;
  if (mode == TxnLatchMode::RegionExclusive) {
    regionGates_[regionId].exclusiveWaiters.push_back(commandId);
  } else {
    for (const ResourceId& resource : stored->resources) {
      slots_[SlotIndex(resource)].resources[resource].waiters.push_back(commandId);
    }
  }

  if (TryGrantLocked(stored)) return true;
  stored->countedWait = true;
  stored->waitStarted = std::chrono::steady_clock::now();
  ++stats_.waits;
  ++stats_.currentWaiters;
  stats_.maxWaiters = std::max(stats_.maxWaiters, stats_.currentWaiters);
  return false;
}

void NodeLatchManager::CleanupResourcesLocked(const LockRequest& request) {
  for (const ResourceId& resource : request.resources) {
    Slot& slot = slots_[SlotIndex(resource)];
    const auto found = slot.resources.find(resource);
    if (found != slot.resources.end() && found->second.owner == 0 && found->second.waiters.empty()) {
      slot.resources.erase(found);
    }
  }
  const auto gate = regionGates_.find(request.regionId);
  if (gate != regionGates_.end() && gate->second.exclusiveOwner == 0 &&
      gate->second.activeKeyOwners == 0 && gate->second.exclusiveWaiters.empty()) {
    regionGates_.erase(gate);
  }
}

std::vector<uint64_t> NodeLatchManager::GrantReadyLocked() {
  std::vector<uint64_t> candidates;
  candidates.reserve(requests_.size());
  for (const auto& item : requests_) {
    if (!item.second.acquired) candidates.push_back(item.first);
  }
  std::sort(candidates.begin(), candidates.end());

  std::vector<uint64_t> ready;
  for (uint64_t commandId : candidates) {
    auto found = requests_.find(commandId);
    if (found != requests_.end() && TryGrantLocked(&found->second)) ready.push_back(commandId);
  }
  return ready;
}

std::vector<uint64_t> NodeLatchManager::Release(uint64_t commandId) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = requests_.find(commandId);
  if (found == requests_.end()) return {};
  LockRequest request = found->second;

  RegionGate& gate = regionGates_[request.regionId];
  if (request.acquired) {
    if (request.mode == TxnLatchMode::RegionExclusive) {
      if (gate.exclusiveOwner == commandId) gate.exclusiveOwner = 0;
    } else {
      for (const ResourceId& resource : request.resources) {
        ResourceState& state = slots_[SlotIndex(resource)].resources[resource];
        if (state.owner == commandId) state.owner = 0;
      }
      if (gate.activeKeyOwners > 0) --gate.activeKeyOwners;
    }
    if (stats_.held > 0) --stats_.held;
  } else {
    FinishWaitLocked(&request);
    if (request.mode == TxnLatchMode::RegionExclusive) {
      RemoveWaiterLocked(&gate.exclusiveWaiters, commandId);
    } else {
      for (const ResourceId& resource : request.resources) {
        RemoveWaiterLocked(&slots_[SlotIndex(resource)].resources[resource].waiters, commandId);
      }
    }
  }
  requests_.erase(found);
  CleanupResourcesLocked(request);
  return GrantReadyLocked();
}

std::vector<uint64_t> NodeLatchManager::Cancel(uint64_t commandId) {
  return Release(commandId);
}

bool NodeLatchManager::Holds(uint64_t commandId) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = requests_.find(commandId);
  return found != requests_.end() && found->second.acquired;
}

NodeLatchManager::Stats NodeLatchManager::GetStats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stats_;
}

size_t NodeTxnScheduler::RequestKeyHash::operator()(const RequestKey& key) const {
  size_t seed = std::hash<int>{}(key.regionId);
  seed ^= std::hash<std::string>{}(key.clientId) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
  seed ^= std::hash<int>{}(key.requestId) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
  return seed;
}

NodeTxnScheduler::NodeTxnScheduler(size_t latchSlots, size_t workerThreads, size_t queueCapacity,
                                   size_t maxPendingTasks)
    : latches_(latchSlots),
      workerPool_(workerThreads, queueCapacity),
      maxPendingTasks_(maxPendingTasks),
      deadlineThread_(&NodeTxnScheduler::DeadlineLoop, this) {
  if (maxPendingTasks_ == 0) throw std::invalid_argument("scheduler max pending tasks must be positive");
}

NodeTxnScheduler::~NodeTxnScheduler() {
  stopping_.store(true, std::memory_order_release);
  deadlineChanged_.notify_all();
  if (deadlineThread_.joinable()) deadlineThread_.join();
}

void NodeTxnScheduler::RegisterRegion(const std::shared_ptr<TxnRegionExecutor>& region) {
  if (!region) throw std::invalid_argument("cannot register a null transaction Region");
  std::lock_guard<std::mutex> lock(mutex_);
  regions_[region->TxnRegionId()] = region;
}

std::shared_ptr<TxnRegionExecutor> NodeTxnScheduler::FindRegion(int regionId) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = regions_.find(regionId);
  return found == regions_.end() ? nullptr : found->second.lock();
}

std::vector<std::shared_ptr<TxnRegionExecutor>> NodeTxnScheduler::Regions() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::shared_ptr<TxnRegionExecutor>> result;
  result.reserve(regions_.size());
  for (const auto& pair : regions_) {
    if (auto region = pair.second.lock()) {
      result.push_back(region);
    }
  }
  return result;
}

NodeTxnScheduler::RequestKey NodeTxnScheduler::MakeRequestKey(const TxnCommand& command) {
  return {command.regionId, command.clientId, command.requestId};
}

TxnScheduleResult NodeTxnScheduler::WrongLeaderResult(bool timedOut) {
  TxnScheduleResult result;
  result.status = ErrWrongLeader;
  result.responseTimedOut = timedOut;
  return result;
}

TxnScheduleResult NodeTxnScheduler::StatusResult(TxnStatus status) {
  TxnScheduleResult result;
  result.status = std::to_string(static_cast<int>(status));
  return result;
}

TxnScheduleResult NodeTxnScheduler::PreparedResult(const PreparedMvccWrite& prepared) {
  TxnScheduleResult result = StatusResult(prepared.status);
  result.readStatus = prepared.readStatus;
  result.value = prepared.readValue;
  result.valueCommitTs = prepared.readCommitTs;
  result.txnRecordStatus = prepared.txnRecordStatus;
  return result;
}

Op NodeTxnScheduler::BuildOp(const TxnCommand& command, const PreparedMvccWrite* prepared) {
  Op op;
  switch (command.type) {
    case TxnCommandType::Prewrite: op.Operation = "TxnPreparedPrewrite"; break;
    case TxnCommandType::Commit: op.Operation = "TxnPreparedCommit"; break;
    case TxnCommandType::Rollback: op.Operation = "TxnPreparedRollback"; break;
    case TxnCommandType::PessimisticLock: op.Operation = "TxnPreparedPessimisticLock"; break;
    case TxnCommandType::CheckTxnStatus: op.Operation = "TxnPreparedCheckStatus"; break;
    case TxnCommandType::ResolveLock: op.Operation = "TxnPreparedResolveLock"; break;
    case TxnCommandType::GarbageCollect: op.Operation = "TxnGarbageCollect"; break;
  }
  op.Key = command.key;
  op.ClientId = command.clientId;
  op.RequestId = command.requestId;
  if (prepared != nullptr) {
    op.Value = prepared->Serialize();
  } else {
    TxnOpPayload payload;
    payload.startTs = command.safePointTs;
    op.Value = payload.asString();
  }
  return op;
}

void NodeTxnScheduler::Schedule(TxnCommand command, Completion completion) {
  if (!completion) return;
  if (command.keys.empty() && command.latchMode == TxnLatchMode::Keys && !command.key.empty()) {
    command.keys.push_back(command.key);
  }
  if (command.deadline == std::chrono::steady_clock::time_point{}) {
    command.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(CONSENSUS_TIMEOUT);
  }

  std::shared_ptr<TxnRegionExecutor> region = FindRegion(command.regionId);
  if (!region || !region->IsTxnLeader()) {
    rejected_.fetch_add(1, std::memory_order_relaxed);
    completion(WrongLeaderResult());
    return;
  }

  const RequestKey requestKey = MakeRequestKey(command);
  uint64_t commandId = 0;
  bool overloaded = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto duplicate = requests_.find(requestKey);
    if (duplicate != requests_.end()) {
      const auto task = tasks_.find(duplicate->second);
      if (task != tasks_.end()) {
        task->second->responses.push_back({std::move(completion), command.deadline, false});
        deadlineChanged_.notify_one();
        return;
      }
      requests_.erase(duplicate);
    }
    if (tasks_.size() >= maxPendingTasks_) {
      rejected_.fetch_add(1, std::memory_order_relaxed);
      overloaded = true;
    } else {
      commandId = nextCommandId_.fetch_add(1, std::memory_order_relaxed) + 1;
      auto task = std::make_shared<TaskContext>();
      task->commandId = commandId;
      task->requestKey = requestKey;
      task->command = std::move(command);
      task->region = region;
      task->createdAt = std::chrono::steady_clock::now();
      task->responses.push_back({std::move(completion), task->command.deadline, false});
      tasks_[commandId] = task;
      requests_[requestKey] = commandId;
    }
  }
  // Completion may re-enter the scheduler or run an RPC callback, so never
  // invoke it while holding the pending-task table mutex.
  if (overloaded) {
    completion(WrongLeaderResult());
    return;
  }

  accepted_.fetch_add(1, std::memory_order_relaxed);
  std::shared_ptr<TaskContext> task;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    task = tasks_.at(commandId);
  }
  const bool acquired = latches_.Acquire(commandId, task->command.regionId, task->command.keys,
                                         task->command.latchMode);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = tasks_.find(commandId);
    if (found != tasks_.end()) {
      found->second->state = acquired ? TxnTaskState::Queued : TxnTaskState::WaitingLatch;
    }
  }
  deadlineChanged_.notify_one();
  if (acquired) Dispatch(commandId);
}

void NodeTxnScheduler::Dispatch(uint64_t commandId) {
  if (workerPool_.TrySchedule([this, commandId]() { Execute(commandId); })) return;
  rejected_.fetch_add(1, std::memory_order_relaxed);
  FinishBeforeProposal(commandId, WrongLeaderResult());
}

void NodeTxnScheduler::DispatchAll(const std::vector<uint64_t>& commandIds) {
  for (uint64_t commandId : commandIds) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto found = tasks_.find(commandId);
      if (found == tasks_.end()) continue;
      found->second->state = TxnTaskState::Queued;
    }
    Dispatch(commandId);
  }
}

void NodeTxnScheduler::Execute(uint64_t commandId) {
  std::shared_ptr<TaskContext> task;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = tasks_.find(commandId);
    if (found == tasks_.end()) return;
    task = found->second;
    task->state = TxnTaskState::Preparing;
  }

  if (std::chrono::steady_clock::now() >= task->command.deadline) {
    FinishBeforeProposal(commandId, WrongLeaderResult(true));
    return;
  }
  std::shared_ptr<TxnRegionExecutor> region = task->region.lock();
  if (!region || !region->IsTxnLeader()) {
    FinishBeforeProposal(commandId, WrongLeaderResult());
    return;
  }

  PreparedMvccWrite prepared;
  const bool needsPrepare = task->command.type != TxnCommandType::GarbageCollect;
  if (needsPrepare) {
    const auto prepareStarted = std::chrono::steady_clock::now();
    prepared = region->PrepareTxn(task->command);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto found = tasks_.find(commandId);
      if (found != tasks_.end()) found->second->preparedResponse = prepared;
    }
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - prepareStarted)
                            .count();
    if (micros > 0) prepareMicros_.fetch_add(static_cast<uint64_t>(micros), std::memory_order_relaxed);
    if (prepared.status != TxnStatus::Ok) {
      if (prepared.status == TxnStatus::LockConflict || prepared.status == TxnStatus::WriteConflict) {
        prepareConflicts_.fetch_add(1, std::memory_order_relaxed);
      }
      FinishBeforeProposal(commandId, PreparedResult(prepared));
      return;
    }
    if (!prepared.HasChanges()) {
      FinishBeforeProposal(commandId, PreparedResult(prepared));
      return;
    }
  }

  if (std::chrono::steady_clock::now() >= task->command.deadline || !region->IsTxnLeader()) {
    FinishBeforeProposal(commandId, WrongLeaderResult(true));
    return;
  }

  const Op op = BuildOp(task->command, needsPrepare ? &prepared : nullptr);
  int raftIndex = -1;
  if (!region->ProposeTxn(op, &raftIndex)) {
    FinishBeforeProposal(commandId, WrongLeaderResult());
    return;
  }
  raftProposals_.fetch_add(1, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = tasks_.find(commandId);
    if (found == tasks_.end()) return;  // A very fast Apply already completed it.
    found->second->proposed = true;
    found->second->raftIndex = raftIndex;
    found->second->proposedAt = std::chrono::steady_clock::now();
    found->second->state = TxnTaskState::Proposed;
  }
}

void NodeTxnScheduler::FinishBeforeProposal(uint64_t commandId, const TxnScheduleResult& result) {
  std::vector<Completion> completions;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = tasks_.find(commandId);
    if (found == tasks_.end()) return;
    found->second->state = TxnTaskState::Failed;
    for (WaitingResponse& response : found->second->responses) {
      if (!response.completed) {
        response.completed = true;
        completions.push_back(std::move(response.completion));
      }
    }
    requests_.erase(found->second->requestKey);
    tasks_.erase(found);
  }
  const std::vector<uint64_t> ready = latches_.Release(commandId);
  for (Completion& completion : completions) completion(result);
  DispatchAll(ready);
}

void NodeTxnScheduler::OnApplied(int regionId, const Op& appliedOp, int raftIndex) {
  const RequestKey requestKey{regionId, appliedOp.ClientId, appliedOp.RequestId};
  std::vector<Completion> completions;
  TxnScheduleResult result;
  uint64_t commandId = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto request = requests_.find(requestKey);
    if (request == requests_.end()) {
      lateApplies_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    commandId = request->second;
    const auto found = tasks_.find(commandId);
    if (found == tasks_.end()) {
      requests_.erase(request);
      lateApplies_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    std::shared_ptr<TaskContext> task = found->second;
    task->state = TxnTaskState::Applied;
    result.status = appliedOp.Status;
    result.raftIndex = raftIndex;
    result.applied = true;
    result.readStatus = task->preparedResponse.readStatus;
    result.value = task->preparedResponse.readValue;
    result.valueCommitTs = task->preparedResponse.readCommitTs;
    result.txnRecordStatus = task->preparedResponse.txnRecordStatus;
    if (task->command.type == TxnCommandType::GarbageCollect) {
      try {
        result.removedCount = static_cast<uint64_t>(std::stoull(appliedOp.Status));
        result.status = std::to_string(static_cast<int>(TxnStatus::Ok));
      } catch (...) {
        result.status = std::to_string(static_cast<int>(TxnStatus::StorageError));
      }
    }
    if (task->proposedAt != std::chrono::steady_clock::time_point{}) {
      const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::steady_clock::now() - task->proposedAt)
                              .count();
      if (micros > 0) applyWaitMicros_.fetch_add(static_cast<uint64_t>(micros), std::memory_order_relaxed);
    }
    for (WaitingResponse& response : task->responses) {
      if (!response.completed) {
        response.completed = true;
        completions.push_back(std::move(response.completion));
      }
    }
    requests_.erase(request);
    tasks_.erase(found);
  }

  applied_.fetch_add(1, std::memory_order_relaxed);
  const std::vector<uint64_t> ready = latches_.Release(commandId);
  for (Completion& completion : completions) completion(result);
  DispatchAll(ready);
}

void NodeTxnScheduler::DeadlineLoop() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stopping_.load(std::memory_order_acquire)) {
    deadlineChanged_.wait_for(lock, std::chrono::milliseconds(10));
    if (stopping_.load(std::memory_order_acquire)) break;
    const auto now = std::chrono::steady_clock::now();
    std::vector<Completion> expired;
    for (auto& item : tasks_) {
      for (WaitingResponse& response : item.second->responses) {
        if (!response.completed && response.deadline <= now) {
          response.completed = true;
          expired.push_back(std::move(response.completion));
          responseTimeouts_.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
    lock.unlock();
    const TxnScheduleResult timeout = WrongLeaderResult(true);
    for (Completion& completion : expired) completion(timeout);
    lock.lock();
  }
}

void NodeTxnScheduler::OnRegionRemoved(int regionId) {
  std::vector<uint64_t> toFinish;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    regions_.erase(regionId);
    for (const auto& item : tasks_) {
      if (item.second->command.regionId == regionId) toFinish.push_back(item.first);
    }
  }
  for (uint64_t commandId : toFinish) FinishBeforeProposal(commandId, WrongLeaderResult());
}

NodeTxnScheduler::Stats NodeTxnScheduler::GetStats() const {
  Stats stats;
  stats.latches = latches_.GetStats();
  stats.accepted = LoadRelaxed(accepted_);
  stats.rejected = LoadRelaxed(rejected_);
  stats.prepareConflicts = LoadRelaxed(prepareConflicts_);
  stats.raftProposals = LoadRelaxed(raftProposals_);
  stats.applied = LoadRelaxed(applied_);
  stats.responseTimeouts = LoadRelaxed(responseTimeouts_);
  stats.lateApplies = LoadRelaxed(lateApplies_);
  stats.prepareMicros = LoadRelaxed(prepareMicros_);
  stats.applyWaitMicros = LoadRelaxed(applyWaitMicros_);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stats.pendingTasks = tasks_.size();
  }
  stats.workerThreads = workerPool_.workers();
  stats.queuedWorkers = workerPool_.queued();
  stats.activeWorkers = workerPool_.active();
  stats.workerRejections = workerPool_.rejected();
  return stats;
}
