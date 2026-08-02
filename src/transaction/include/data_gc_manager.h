#ifndef STRATAKV_TRANSACTION_DATA_GC_MANAGER_H
#define STRATAKV_TRANSACTION_DATA_GC_MANAGER_H

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "mvcc_storage.h"
#include "timestamp_oracle.h"

// DataGcManager 负责在后台定期推进 MVCC 多版本数据的垃圾回收。
// 它通过定期记录物理时间和逻辑时间戳(TSO)的映射关系，
// 来安全地计算并执行指定保留时间外的历史版本清理。
class DataGcManager {
 public:
  // 构造函数：指定依赖的存储、TSO发号器，以及数据的安全保留时长和检查间隔。
  // 默认保留 10000 毫秒（10秒）方便测试。
  DataGcManager(std::shared_ptr<MvccStorage> storage, std::shared_ptr<TimestampOracle> tso,
                std::chrono::milliseconds gcRetention = std::chrono::milliseconds(10000),
                std::chrono::milliseconds checkInterval = std::chrono::milliseconds(1000));
  
  ~DataGcManager();

  // 启动后台 GC 推进线程
  void Start();
  // 停止后台线程
  void Stop();

 private:
  // 后台扫描循环执行的方法
  void ScanOnce();

  std::shared_ptr<MvccStorage> storage_;
  std::shared_ptr<TimestampOracle> tso_;

  std::chrono::milliseconds gcRetention_;
  std::chrono::milliseconds checkInterval_;

  std::atomic<bool> stopped_;
  std::thread worker_;
  std::mutex stateMutex_;

  // 记录物理时间和逻辑 TSO 的映射队列。
  // pair.first = 物理时间(毫秒), pair.second = 对应的逻辑 TSO。
  std::deque<std::pair<uint64_t, uint64_t>> timeToTsQueue_;
};

#endif  // STRATAKV_TRANSACTION_DATA_GC_MANAGER_H
