
#ifndef UTIL_H
#define UTIL_H

#include <cstring>
#include <condition_variable>  // pthread_condition_t
#include <functional>
#include <iostream>
#include <mutex>  // pthread_mutex_t
#include <queue>
#include <random>
#include <sstream>
#include <thread>
#include "config.h"

template <class F>
class DeferClass {
 public:
  DeferClass(F&& f) : m_func(std::forward<F>(f)) {}
  DeferClass(const F& f) : m_func(f) {}
  ~DeferClass() { m_func(); }

  DeferClass(const DeferClass& e) = delete;
  DeferClass& operator=(const DeferClass& e) = delete;

 private:
  F m_func;
};

#define _CONCAT(a, b) a##b
#define _MAKE_DEFER_(line) DeferClass _CONCAT(defer_placeholder, line) = [&]()

#undef DEFER
#define DEFER _MAKE_DEFER_(__LINE__)

void DPrintf(const char* format, ...);

void myAssert(bool condition, std::string message = "Assertion failed!");

template <typename... Args>
std::string format(const char* format_str, Args... args) {
    int size_s = std::snprintf(nullptr, 0, format_str, args...) + 1; // "\0"
    if (size_s <= 0) { throw std::runtime_error("Error during formatting."); }
    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, format_str, args...);
    return std::string(buf.data(), buf.data() + size - 1);  // remove '\0'
}

std::chrono::steady_clock::time_point now();

std::chrono::milliseconds getRandomizedElectionTimeout();
void sleepNMilliseconds(int N);

// Thread-safe blocking queue shared by Raft and the Region state machine.
template <typename T>
class LockQueue {
 public:
  // 多个worker线程都会写日志queue
  void Push(const T& data) {
    std::lock_guard<std::mutex> lock(m_mutex);  //使用lock_gurad，即RAII的思想保证锁正确释放
    m_queue.push(data);
    m_condvariable.notify_one();
  }

  // 一个线程读日志queue，写日志文件
  T Pop() {
    std::unique_lock<std::mutex> lock(m_mutex);
    while (m_queue.empty()) {
      // 日志队列为空，线程进入wait状态
      m_condvariable.wait(lock);  //这里用unique_lock是因为lock_guard不支持解锁，而unique_lock支持
    }
    T data = m_queue.front();
    m_queue.pop();
    return data;
  }

  bool timeOutPop(int timeout, T* ResData)  // 添加一个超时时间参数，默认为 50 毫秒
  {
    std::unique_lock<std::mutex> lock(m_mutex);

    // 获取当前时间点，并计算出超时时刻
    auto now = std::chrono::steady_clock::now();
    auto timeout_time = now + std::chrono::milliseconds(timeout);

    // 在超时之前，不断检查队列是否为空
    while (m_queue.empty()) {
      // 如果已经超时了，就返回一个空对象
      if (m_condvariable.wait_until(lock, timeout_time) == std::cv_status::timeout) {
        return false;
      } else {
        continue;
      }
    }

    T data = m_queue.front();
    m_queue.pop();
    *ResData = data;
    return true;
  }

  std::vector<T> PopBatch(int max_batch_size) {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::vector<T> batch;
    if (m_queue.empty()) {
      return batch;
    }
    batch.reserve(max_batch_size);
    while (!m_queue.empty() && batch.size() < max_batch_size) {
      batch.push_back(m_queue.front());
      m_queue.pop();
    }
    return batch;
  }

 private:
  std::queue<T> m_queue;
  std::mutex m_mutex;
  std::condition_variable m_condvariable;
};
struct TxnOpPayload {
  std::string value;
  std::string primaryKey;
  uint64_t startTs = 0;
  uint64_t commitTs = 0;
  uint64_t ttlMs = 0;
  bool isDelete = false;

  std::string asString() const {
    std::string out;
    uint32_t len = value.size();
    out.append(reinterpret_cast<const char*>(&len), sizeof(len));
    out.append(value);
    len = primaryKey.size();
    out.append(reinterpret_cast<const char*>(&len), sizeof(len));
    out.append(primaryKey);
    out.append(reinterpret_cast<const char*>(&startTs), sizeof(startTs));
    out.append(reinterpret_cast<const char*>(&commitTs), sizeof(commitTs));
    out.append(reinterpret_cast<const char*>(&ttlMs), sizeof(ttlMs));
    out.append(reinterpret_cast<const char*>(&isDelete), sizeof(isDelete));
    return out;
  }

  bool parseFromString(const std::string& in) {
    size_t offset = 0;
    if (offset + sizeof(uint32_t) > in.size()) return false;
    uint32_t len;
    std::memcpy(&len, in.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    if (offset + len > in.size()) return false;
    value.assign(in.data() + offset, len);
    offset += len;

    if (offset + sizeof(uint32_t) > in.size()) return false;
    std::memcpy(&len, in.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    if (offset + len > in.size()) return false;
    primaryKey.assign(in.data() + offset, len);
    offset += len;

    if (offset + sizeof(uint64_t) > in.size()) return false;
    std::memcpy(&startTs, in.data() + offset, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    if (offset + sizeof(uint64_t) > in.size()) return false;
    std::memcpy(&commitTs, in.data() + offset, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    if (offset + sizeof(uint64_t) > in.size()) return false;
    std::memcpy(&ttlMs, in.data() + offset, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    if (offset + sizeof(bool) > in.size()) return false;
    std::memcpy(&isDelete, in.data() + offset, sizeof(bool));
    offset += sizeof(bool);

    return true;
  }
};

// Command envelope replicated through Raft.
class Op {
 public:
  std::string Operation;  // "Get" "Put" "Append"
  std::string Key;
  std::string Value;
  std::string ClientId;  //客户端号码
  int RequestId;         //客户端号码请求的Request的序列号，为了保证线性一致性
                         // IfDuplicate bool // Duplicate command can't be applied twice , but only for PUT and APPEND
  std::string Status;    // Transaction status returned from apply thread

 public:
  // Encode length-prefixed fields so command values may contain arbitrary bytes.
  std::string asString() const {
    std::string out;
    uint32_t len = Operation.size();
    out.append(reinterpret_cast<const char*>(&len), sizeof(len));
    out.append(Operation);

    len = Key.size();
    out.append(reinterpret_cast<const char*>(&len), sizeof(len));
    out.append(Key);

    len = Value.size();
    out.append(reinterpret_cast<const char*>(&len), sizeof(len));
    out.append(Value);

    len = ClientId.size();
    out.append(reinterpret_cast<const char*>(&len), sizeof(len));
    out.append(ClientId);

    out.append(reinterpret_cast<const char*>(&RequestId), sizeof(RequestId));

    len = Status.size();
    out.append(reinterpret_cast<const char*>(&len), sizeof(len));
    out.append(Status);
    return out;
  }

  bool parseFromString(const std::string& str) {
    size_t offset = 0;
    uint32_t len;

    if (offset + sizeof(uint32_t) > str.size()) return false;
    std::memcpy(&len, str.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    if (offset + len > str.size()) return false;
    Operation.assign(str.data() + offset, len);
    offset += len;

    if (offset + sizeof(uint32_t) > str.size()) return false;
    std::memcpy(&len, str.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    if (offset + len > str.size()) return false;
    Key.assign(str.data() + offset, len);
    offset += len;

    if (offset + sizeof(uint32_t) > str.size()) return false;
    std::memcpy(&len, str.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    if (offset + len > str.size()) return false;
    Value.assign(str.data() + offset, len);
    offset += len;

    if (offset + sizeof(uint32_t) > str.size()) return false;
    std::memcpy(&len, str.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    if (offset + len > str.size()) return false;
    ClientId.assign(str.data() + offset, len);
    offset += len;

    if (offset + sizeof(int) > str.size()) return false;
    std::memcpy(&RequestId, str.data() + offset, sizeof(int));
    offset += sizeof(int);

    if (offset + sizeof(uint32_t) > str.size()) return false;
    std::memcpy(&len, str.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    if (offset + len > str.size()) return false;
    Status.assign(str.data() + offset, len);
    offset += len;

    return true;
  }

 public:
  friend std::ostream& operator<<(std::ostream& os, const Op& obj) {
    os << "[MyClass:Operation{" + obj.Operation + "},Key{" + obj.Key + "},Value{" + obj.Value + "},ClientId{" +
              obj.ClientId + "},RequestId{" + std::to_string(obj.RequestId) + "},Status{" + obj.Status + "}";  // 在这里实现自定义的输出格式
    return os;
  }


};

///////////////////////////////////////////////kvserver reply err to clerk

const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";

#endif  //  UTIL_H
