#include "timestamp_oracle.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cerrno>
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

PersistentTimestampOracle::PersistentTimestampOracle(std::string statePath, uint64_t segmentSize)
    : statePath_(std::move(statePath)), segmentSize_(segmentSize == 0 ? 1 : segmentSize) {
  if (statePath_.empty()) throw std::invalid_argument("TSO state path is empty");

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

  segmentLimit_ = ReadPersistedLimit();
  if (segmentLimit_ == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("TSO timestamp space is exhausted");
  }
  nextTs_ = segmentLimit_ + 1;
}

PersistentTimestampOracle::~PersistentTimestampOracle() {
  if (lockFd_ >= 0) close(lockFd_);
}

uint64_t PersistentTimestampOracle::Next() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (nextTs_ == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("TSO timestamp space is exhausted");
  }
  ReserveThroughLocked(nextTs_);
  return nextTs_++;
}

uint64_t PersistentTimestampOracle::Peek() {
  std::lock_guard<std::mutex> lock(mutex_);
  return nextTs_;
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

uint64_t PersistentTimestampOracle::ReadPersistedLimit() const {
  if (!std::filesystem::exists(statePath_)) return 0;

  std::ifstream input(statePath_);
  uint64_t limit = 0;
  if (!(input >> limit)) throw std::runtime_error("invalid TSO state file: " + statePath_);
  return limit;
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
    WriteAll(fd, std::to_string(limit) + "\n");
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
