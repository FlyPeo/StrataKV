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

// 暴露事务收集到的 mutation 集合，供协调器执行预写和提交。
const std::unordered_map<std::string, TxnMutation>& Transaction::Mutations() const { return mutations_; }

// 暴露事务持有的悲观锁列表，供协调器在异常分支中清理。
const std::vector<std::string>& Transaction::PessimisticLocks() const { return pessimisticLocks_; }

// 返回事务的 primary key，用作两阶段提交里的首要落点。
const std::string& Transaction::PrimaryKey() const { return primaryKey_; }

// 首次写入时确定 primary key，让整个事务在全局提交链路里有稳定锚点。
void Transaction::EnsurePrimaryKey(const std::string& key) {
  if (primaryKey_.empty()) {
    primaryKey_ = key;
  }
}
