#ifndef STRATAKV_TRANSACTION_TRANSACTION_COORDINATOR_H
#define STRATAKV_TRANSACTION_TRANSACTION_COORDINATOR_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// TxnOptions 用来控制一次事务提交时的运行参数，
// 例如锁的 TTL，影响过期锁多久后会被后台回收。
struct TxnOptions {
  // 锁在系统中允许保留的最长时间，超时后会进入恢复/回滚流程。
  // 该值必须明显长于故障切换和排队后的最坏提交延迟。过短的 TTL 会让
  // 仍在提交的事务被后台扫描器误判为过期；当前远端锁尚未实现心跳续租，
  // 因此给本地高负载和 Raft 快照预留两分钟。
  uint64_t lockTtlMs = 120000;
  // 第一阶段不支持锁心跳，因此事务超过该时限后只能回滚重试。
  uint64_t transactionTimeoutMs = 60000;
  // 单次协调器操作的本地端到端预算。跨 RPC 仅传剩余毫秒数。
  uint64_t rpcBudgetMs = 5000;
  // 显式测试 project token，用于受控 2PC failpoint barrier。
  std::string testProjectToken;
};

enum class TransactionState {
  Active,
  AbortOnly,
  CleanupPending,
  ResultUnknown,
  Finished,
};

// TxnMutation 描述事务里对某个 key 的单次变更，
// 它是协调器把用户操作落成 MVCC 预写记录的中间格式。
struct TxnMutation {
  // 变更要写入的值；删除操作时该字段为空。
  std::string value;
  // 标记这次 mutation 是否是删除操作。
  bool isDelete = false;
};

// Transaction 保存一次事务在协调器侧的局部状态，
// 包括 startTs、mutation 列表以及悲观锁记录。
class Transaction {
 public:
  // 创建一个带有全局开始时间戳的事务句柄。
  explicit Transaction(uint64_t startTs);

  // 返回事务的开始时间戳，用于 MVCC 读和写冲突判断。
  uint64_t StartTs() const;
  // 记录一次写入操作，并在需要时把该 key 设为 primary key。
  void Put(const std::string& key, const std::string& value);
  // 记录一次删除操作，并在需要时把该 key 设为 primary key。
  void Delete(const std::string& key);
  // 记录一次悲观锁申请结果，供提交或回滚阶段统一收尾。
  void TrackPessimisticLock(const std::string& key);
  // 请求发出但结果尚未确定时先记录 key，避免响应丢失后遗漏清理。
  void TrackUncertainPessimisticLock(const std::string& key);
  // 确认锁成功后从 uncertain 集合迁移到 confirmed 集合并固定 Primary。
  void ConfirmPessimisticLock(const std::string& key, uint64_t forUpdateTs);
  // 已确认请求没有留下锁，或清理成功后移除相应记录。
  void ForgetPessimisticLock(const std::string& key);
  void ClearPessimisticLocks();
  // 取出事务收集到的所有 mutation，供提交阶段预写到各个 shard。
  const std::unordered_map<std::string, TxnMutation>& Mutations() const;
  // 取出事务持有的悲观锁列表，供协调器在提交失败时回滚。
  const std::vector<std::string>& PessimisticLocks() const;
  const std::vector<std::string>& UncertainPessimisticLocks() const;
  // 返回当前事务选定的 primary key，决定提交链路里谁先落盘。
  const std::string& PrimaryKey() const;
  std::string ProposedPrimaryKey(const std::string& key) const;
  uint64_t MaxForUpdateTs() const;
  void ObserveForUpdateTs(uint64_t forUpdateTs);

  TransactionState State() const;
  bool IsActive() const;
  void MarkAbortOnly();
  void MarkCleanupPending();
  void MarkResultUnknown();
  void MarkFinished();

 private:
  // 在首次写入时设定 primary key，保证一个事务有稳定的提交锚点。
  void EnsurePrimaryKey(const std::string& key);

  // 事务的开始时间戳，是 MVCC 可见性判断的核心。
  uint64_t startTs_;
  // 事务积累的写操作集合。
  std::unordered_map<std::string, TxnMutation> mutations_;
  // 事务期间持有的悲观锁 key 集合。
  std::vector<std::string> pessimisticLocks_;
  // 已发出但无法确认是否 Apply 的锁请求；回滚必须覆盖这些 key。
  std::vector<std::string> uncertainPessimisticLocks_;
  // 当前事务选定的 primary key。
  std::string primaryKey_;
  uint64_t maxForUpdateTs_ = 0;
  TransactionState state_ = TransactionState::Active;
};

#endif  // STRATAKV_TRANSACTION_TRANSACTION_COORDINATOR_H
