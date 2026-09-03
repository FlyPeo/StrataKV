#include "util.h"
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <cstdlib>

void myAssert(bool condition, std::string message) {
  if (!condition) {
    std::cerr << "Error: " << message << std::endl;
    std::exit(EXIT_FAILURE);
  }
}

// Raft election/heartbeat and queue timeouts are elapsed-time decisions. A
// wall-clock correction must never trigger an early election or extend a
// leader lease, so all callers use the monotonic clock.
std::chrono::steady_clock::time_point now() { return std::chrono::steady_clock::now(); }

std::chrono::milliseconds getRandomizedElectionTimeout() {
  std::random_device rd;
  std::mt19937 rng(rd());
  std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime);

  return std::chrono::milliseconds(dist(rng));
}

void sleepNMilliseconds(int N) { std::this_thread::sleep_for(std::chrono::milliseconds(N)); };

void DPrintf(const char *format, ...) {
  if (Debug) {
    // 获取当前的日期，然后取日志信息，写入相应的日志文件当中 a+
    time_t now = time(nullptr);
    tm *nowtm = localtime(&now);
    va_list args;
    va_start(args, format);
    std::printf("[%d-%d-%d-%d-%d-%d] ", nowtm->tm_year + 1900, nowtm->tm_mon + 1, nowtm->tm_mday, nowtm->tm_hour,
                nowtm->tm_min, nowtm->tm_sec);
    std::vprintf(format, args);
    std::printf("\n");
    va_end(args);
  }
}
