#include "timestamp_oracle.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>

namespace {
std::runtime_error SystemError(const std::string& action) {
  return std::runtime_error(action + ": " + std::strerror(errno));
}

void WriteAll(int fd, const std::string& data) {
  size_t written = 0;
  while (written < data.size()) {
    const ssize_t count = write(fd, data.data() + written, data.size() - written);
    if (count < 0) {
      if (errno == EINTR) continue;
      throw SystemError("write TSO state");
    }
    written += static_cast<size_t>(count);
  }
}

}  // namespace

uint64_t HlcTimestamp::PhysicalMs(uint64_t timestamp) { return timestamp >> kLogicalBits; }

uint32_t HlcTimestamp::Logical(uint64_t timestamp) {
  return static_cast<uint32_t>(timestamp & kLogicalMask);
}

uint64_t HlcTimestamp::Compose(uint64_t physicalMs, uint32_t logical) {
  if (physicalMs > kMaxPhysicalMs) throw std::overflow_error("HLC physical time overflow");
  if (logical > kLogicalMask) throw std::overflow_error("HLC logical time overflow");
  return (physicalMs << kLogicalBits) | logical;
}

uint64_t HlcTimestamp::WallClockMs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count());
}

PersistentTimestampOracle::PersistentTimestampOracle(std::string statePath, uint64_t segmentSize,
                                                       Clock clock)
    : statePath_(std::move(statePath)),
      segmentSize_(segmentSize == 0 ? 1 : segmentSize),
      clock_(std::move(clock)) {
  if (statePath_.empty()) throw std::invalid_argument("TSO state path is empty");
  if (!clock_) throw std::invalid_argument("TSO clock is empty");

  const std::filesystem::path parent = std::filesystem::path(statePath_).parent_path();
  if (!parent.empty()) std::filesystem::create_directories(parent);

  const std::string lockPath = statePath_ + ".lock";
  lockFd_ = open(lockPath.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (lockFd_ < 0) throw SystemError("open TSO lock file");
  if (flock(lockFd_, LOCK_EX | LOCK_NB) != 0) {
    close(lockFd_);
    lockFd_ = -1;
    throw std::runtime_error("TSO state is already owned by another process: " + statePath_);
  }

  const PersistedState persisted = ReadPersistedState();
  segmentLimit_ = persisted.limit;
  if (segmentLimit_ == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("TSO timestamp space is exhausted");
  }
  nextTs_ = std::max(segmentLimit_ + 1, HlcTimestamp::Compose(clock_(), 0));
  if (persisted.legacy) {
    // Publish the first HLC reservation before any migrated timestamp can be
    // observed. A crash can only create a gap, never a repeated timestamp.
    ReserveThroughLocked(nextTs_);
  }
}

PersistentTimestampOracle::~PersistentTimestampOracle() {
  if (lockFd_ >= 0) close(lockFd_);
}

uint64_t PersistentTimestampOracle::Next() {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t candidate = NextCandidateLocked();
  if (candidate == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("TSO timestamp space is exhausted");
  }
  ReserveThroughLocked(candidate);
  nextTs_ = candidate + 1;
  return candidate;
}

uint64_t PersistentTimestampOracle::Peek() {
  std::lock_guard<std::mutex> lock(mutex_);
  return NextCandidateLocked();
}

void PersistentTimestampOracle::Observe(uint64_t ts) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (ts < nextTs_) return;
  if (ts == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("observed maximum TSO timestamp");
  }
  ReserveThroughLocked(ts + 1);
  nextTs_ = ts + 1;
}

PersistentTimestampOracle::PersistedState PersistentTimestampOracle::ReadPersistedState() const {
  if (!std::filesystem::exists(statePath_)) return {};

  std::ifstream input(statePath_);
  std::string first;
  if (!(input >> first)) throw std::runtime_error("invalid TSO state file: " + statePath_);

  PersistedState state;
  state.exists = true;
  if (first == "v2") {
    if (!(input >> state.limit)) {
      throw std::runtime_error("invalid TSO v2 state file: " + statePath_);
    }
    std::string trailing;
    if (input >> trailing) {
      throw std::runtime_error("trailing data in TSO v2 state file: " + statePath_);
    }
    return state;
  }
  if (!first.empty() && first.front() == 'v') {
    throw std::runtime_error("unsupported TSO state version in: " + statePath_);
  }

  size_t consumed = 0;
  uint64_t limit = 0;
  try {
    limit = std::stoull(first, &consumed);
  } catch (const std::exception&) {
    throw std::runtime_error("invalid legacy TSO state file: " + statePath_);
  }
  if (consumed != first.size()) {
    throw std::runtime_error("invalid legacy TSO state file: " + statePath_);
  }
  std::string trailing;
  if (input >> trailing) {
    throw std::runtime_error("trailing data in legacy TSO state file: " + statePath_);
  }
  state.limit = limit;
  state.legacy = true;
  return state;
}

uint64_t PersistentTimestampOracle::NextCandidateLocked() const {
  return std::max(nextTs_, HlcTimestamp::Compose(clock_(), 0));
}

void PersistentTimestampOracle::ReserveThroughLocked(uint64_t required) {
  if (required <= segmentLimit_) return;

  const uint64_t extra = segmentSize_ - 1;
  if (required > std::numeric_limits<uint64_t>::max() - extra) {
    throw std::overflow_error("TSO timestamp space is exhausted");
  }
  const uint64_t newLimit = required + extra;
  PersistLimitLocked(newLimit);
  segmentLimit_ = newLimit;
}

void PersistentTimestampOracle::PersistLimitLocked(uint64_t limit) {
  const std::string temporary = statePath_ + ".tmp." + std::to_string(getpid());
  int fd = open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0644);
  if (fd < 0) throw SystemError("open temporary TSO state");

  try {
    WriteAll(fd, "v2 " + std::to_string(limit) + "\n");
    if (fsync(fd) != 0) throw SystemError("fsync temporary TSO state");
    if (close(fd) != 0) {
      fd = -1;
      throw SystemError("close temporary TSO state");
    }
    fd = -1;
    if (rename(temporary.c_str(), statePath_.c_str()) != 0) throw SystemError("replace TSO state");

    const std::filesystem::path parentPath = std::filesystem::path(statePath_).parent_path();
    const std::string parent = parentPath.empty() ? "." : parentPath.string();
    const int directoryFd = open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directoryFd < 0) throw SystemError("open TSO state directory");
    const int syncResult = fsync(directoryFd);
    const int syncErrno = errno;
    close(directoryFd);
    if (syncResult != 0) {
      errno = syncErrno;
      throw SystemError("fsync TSO state directory");
    }
  } catch (...) {
    if (fd >= 0) close(fd);
    unlink(temporary.c_str());
    throw;
  }
}
