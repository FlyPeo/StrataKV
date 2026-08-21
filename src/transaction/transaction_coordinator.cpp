// Transaction subsystem: transaction lifecycle and write buffering.
#include "transaction_coordinator.h"

#include <algorithm>

// 事务对象把用户的读写操作收集成一个可提交的局部上下文。
Transaction::Transaction(uint64_t startTs) : startTs_(startTs) {}

// 返回事务的全局开始时间戳，用于 MVCC 快照读取和冲突判断。
uint64_t Transaction::StartTs() const { return startTs_; }

// 记录一次写入操作，并推动事务在全局提交流程中选定 primary key。
void Transaction::Put(const std::string& key, const std::string& value) {
  EnsurePrimaryKey(key);
  mutations_[key] = TxnMutation{value, false};
}

// 记录一次删除操作，供后续 prewrite/commit 阶段落到各个分片。
void Transaction::Delete(const std::string& key) {
  EnsurePrimaryKey(key);
  mutations_[key] = TxnMutation{"", true};
}

// 记录悲观锁持有的 key，方便提交失败时统一回滚和释放。
void Transaction::TrackPessimisticLock(const std::string& key) {
  EnsurePrimaryKey(key);
  if (std::find(pessimisticLocks_.begin(), pessimisticLocks_.end(), key) == pessimisticLocks_.end()) {
    pessimisticLocks_.push_back(key);
  }
}

void Transaction::TrackUncertainPessimisticLock(const std::string& key) {
  if (std::find(uncertainPessimisticLocks_.begin(), uncertainPessimisticLocks_.end(), key) ==
      uncertainPessimisticLocks_.end()) {
    uncertainPessimisticLocks_.push_back(key);
  }
}

void Transaction::ConfirmPessimisticLock(const std::string& key, uint64_t forUpdateTs) {
  uncertainPessimisticLocks_.erase(
      std::remove(uncertainPessimisticLocks_.begin(), uncertainPessimisticLocks_.end(), key),
      uncertainPessimisticLocks_.end());
  TrackPessimisticLock(key);
  ObserveForUpdateTs(forUpdateTs);
}

void Transaction::ForgetPessimisticLock(const std::string& key) {
  pessimisticLocks_.erase(std::remove(pessimisticLocks_.begin(), pessimisticLocks_.end(), key),
                          pessimisticLocks_.end());
  uncertainPessimisticLocks_.erase(
      std::remove(uncertainPessimisticLocks_.begin(), uncertainPessimisticLocks_.end(), key),
      uncertainPessimisticLocks_.end());
}

void Transaction::ClearPessimisticLocks() {
  pessimisticLocks_.clear();
  uncertainPessimisticLocks_.clear();
}

// 暴露事务收集到的 mutation 集合，供协调器执行预写和提交。
const std::unordered_map<std::string, TxnMutation>& Transaction::Mutations() const { return mutations_; }

// 暴露事务持有的悲观锁列表，供协调器在异常分支中清理。
const std::vector<std::string>& Transaction::PessimisticLocks() const { return pessimisticLocks_; }

const std::vector<std::string>& Transaction::UncertainPessimisticLocks() const {
  return uncertainPessimisticLocks_;
}

// 返回事务的 primary key，用作两阶段提交里的首要落点。
const std::string& Transaction::PrimaryKey() const { return primaryKey_; }

std::string Transaction::ProposedPrimaryKey(const std::string& key) const {
  return primaryKey_.empty() ? key : primaryKey_;
}

uint64_t Transaction::MaxForUpdateTs() const { return maxForUpdateTs_; }

void Transaction::ObserveForUpdateTs(uint64_t forUpdateTs) {
  maxForUpdateTs_ = std::max(maxForUpdateTs_, forUpdateTs);
}

TransactionState Transaction::State() const { return state_; }

bool Transaction::IsActive() const { return state_ == TransactionState::Active; }

void Transaction::MarkAbortOnly() {
  if (state_ == TransactionState::Active) state_ = TransactionState::AbortOnly;
}

void Transaction::MarkCleanupPending() {
  if (state_ == TransactionState::Active || state_ == TransactionState::AbortOnly) {
    state_ = TransactionState::CleanupPending;
  }
}

void Transaction::MarkResultUnknown() {
  if (state_ != TransactionState::Finished) state_ = TransactionState::ResultUnknown;
}

void Transaction::MarkFinished() { state_ = TransactionState::Finished; }

// 首次写入时确定 primary key，让整个事务在全局提交链路里有稳定锚点。
void Transaction::EnsurePrimaryKey(const std::string& key) {
  if (primaryKey_.empty()) {
    primaryKey_ = key;
  }
}
