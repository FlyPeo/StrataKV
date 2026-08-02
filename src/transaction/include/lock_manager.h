#ifndef STRATAKV_TRANSACTION_LOCK_MANAGER_H
#define STRATAKV_TRANSACTION_LOCK_MANAGER_H

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <mutex>

#include "mvcc_storage.h"

// LockManager 负责在后台持续扫描过期锁，
// 并把这些锁交给回滚/清理逻辑处理，是事务恢复链路的自动化执行器。
class LockManager {
 public:
  // 当发现一个过期锁时，交给外部回滚函数处理该 key 对应的事务状态。
  using RollbackFn = std::function<void(const std::string&, const MvccLock&)>;

  // 构造锁管理器，并指定扫描哪个存储以及采用什么方式回滚过期锁。
  LockManager(std::shared_ptr<MvccStorage> storage, RollbackFn rollbackFn,
              std::chrono::milliseconds checkInterval = std::chrono::milliseconds(200));
  // 停止后台线程，并确保退出前完成资源回收。
  ~LockManager();

  // 启动后台扫描循环，让系统自动发现并处理过期锁。
  void Start();
  // 主动停止扫描循环，通常在协调器关闭或服务退出时调用。
  void Stop();
  // 执行一次过期锁扫描，供后台线程或手动触发使用。
  void ScanOnce();

 private:
  // 被管理的 MVCC 存储实例，扫描范围来自这里。
  std::shared_ptr<MvccStorage> storage_;
  // 处理过期锁时的回滚回调，决定锁被发现后如何收尾。
  RollbackFn rollbackFn_;
  // 后台扫描的时间间隔，控制锁清理频率和开销。
  std::chrono::milliseconds checkInterval_;
  // 标记后台扫描线程是否已经停止。
  std::atomic<bool> stopped_;
  // 负责周期性扫描过期锁的后台线程。
  std::thread worker_;
  // 保护对worker_的启动和停止。
  std::mutex stateMutex_;
};

#endif  // STRATAKV_TRANSACTION_LOCK_MANAGER_H
