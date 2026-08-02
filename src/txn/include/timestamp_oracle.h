#ifndef STRATAKV_TIMESTAMP_ORACLE_H
#define STRATAKV_TIMESTAMP_ORACLE_H

#include <atomic>   // 提供无锁原子变量，用于并发分配时间戳
#include <cstdint>  // 提供 uint64_t 这类固定宽度整数类型
#include <mutex>    // 提供互斥锁，保护分段刷新逻辑

// TimestampOracle 负责给事务系统分配单调递增的全局时间戳。
// 它用“分段预分配”的方式减少锁竞争：大部分 Next() 直接走原子递增，
// 只有跨段时才进入互斥保护的刷新路径。
class TimestampOracle {
 public:
  // 构造一个时间戳发生器；segmentSize 决定每次预留多少个时间戳。
  explicit TimestampOracle(uint64_t segmentSize = 4096);

  // 申请一个新的时间戳，返回值保证单调递增。
  uint64_t Next();
  // 读取当前已分配到的位置，但不会消耗新的时间戳。
  uint64_t Peek() const;
  // 观察到外部系统已有更大的时间戳时，推进本地水位线。
  void Observe(uint64_t ts);

 private:
  // 在持有 mutex_ 时刷新当前时间戳段边界。
  void RefillLocked(uint64_t observed);

  // 每次刷新时预留的时间戳数量，保证至少为 1。
  const uint64_t segmentSize_;
  // 下一个可分配的时间戳，Next() 主要依赖这个原子变量。
  std::atomic<uint64_t> nextTs_;
  // 当前段的上界，超过该值时需要进入锁保护的刷新路径。
  std::atomic<uint64_t> segmentLimit_;
  // 持久化或记忆的最大时间戳，用于确保刷新后的水位线不回退。
  uint64_t persistedMaxTs_;
  // 保护分段刷新和外部观察更新的互斥锁。
  mutable std::mutex mutex_;
};

#endif  // STRATAKV_TIMESTAMP_ORACLE_H
