#include <atomic>
#include <cerrno>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>

#include <pulsar/iomanager.hpp>
#include <pulsar/sync.hpp>

namespace {

using pulsar::FiberMutex;
using pulsar::FiberSemaphore;
using pulsar::IOManager;

int Fail(std::atomic<int>& failures, const char* message) {
  std::cerr << "stratakv-test-fiber-sync: " << message << std::endl;
  ++failures;
  return -1;
}

}  // namespace

int main() {
  IOManager iom(1, false, "fiber-sync-check");
  FiberMutex mutex;
  FiberMutex timeout_mutex;
  FiberMutex cancelled_mutex;
  FiberSemaphore mutex_locked;
  FiberSemaphore timeout_locked;
  std::atomic<int> failures{0};
  std::atomic<int> completed{0};
  auto finished = std::make_shared<std::promise<void>>();
  std::future<void> result = finished->get_future();

  auto complete = [&]() {
    if (completed.fetch_add(1) + 1 == 3) finished->set_value();
  };

  // A contending Fiber must yield while A holds the lock; A can therefore run
  // its timer and release the lock on a single worker thread.
  iom.scheduler([&]() {
    if (mutex.lock() != 0) {
      Fail(failures, "holder failed to lock mutex");
    } else {
      mutex_locked.signal();
      usleep(20 * 1000);
      if (mutex.unlock() != 0) Fail(failures, "holder failed to unlock mutex");
    }
  });
  iom.scheduler([&]() {
    if (mutex_locked.wait() != 0 || mutex.lock() != 0) {
      Fail(failures, "contender did not resume after mutex unlock");
    } else if (mutex.unlock() != 0) {
      Fail(failures, "contender failed to unlock mutex");
    }
    complete();
  });

  // A timed wait should resume the Fiber with ETIMEDOUT, not block the worker.
  iom.scheduler([&]() {
    if (timeout_mutex.lock() != 0) {
      Fail(failures, "timeout holder failed to lock");
    } else {
      timeout_locked.signal();
      usleep(60 * 1000);
      timeout_mutex.unlock();
    }
  });
  iom.scheduler([&]() {
    if (timeout_locked.wait() != 0) {
      Fail(failures, "timeout contender setup failed");
    } else if (timeout_mutex.lock(10) != -1 || errno != ETIMEDOUT) {
      Fail(failures, "mutex timeout did not return ETIMEDOUT");
    }
    complete();
  });

  // Cancel explicitly wakes a queued Fiber with the supplied error.
  iom.scheduler([&]() {
    if (cancelled_mutex.lock() != 0) {
      Fail(failures, "cancel holder failed to lock");
    } else {
      usleep(60 * 1000);
      cancelled_mutex.unlock();
    }
  });
  iom.scheduler([&]() {
    if (cancelled_mutex.lock() != -1 || errno != ECANCELED) {
      Fail(failures, "cancelled waiter did not return ECANCELED");
    }
    complete();
  });
  // It is queued after the waiter because tasks execute FIFO on this one
  // worker.  The waiter has yielded by the time this callback runs.
  iom.scheduler([&]() {
    if (cancelled_mutex.cancelWaiters(ECANCELED) != 1) {
      Fail(failures, "cancel did not find exactly one waiter");
    }
  });

  int exit_code = 0;
  if (result.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
    std::cerr << "stratakv-test-fiber-sync: timed out" << std::endl;
    exit_code = 1;
  }
  // Drain all timer callbacks and Fiber tasks before the synchronization
  // objects declared above are destroyed.
  iom.stop();
  const bool passed = exit_code == 0 && failures.load() == 0;
  std::cout << "correctness=" << (passed ? "PASS" : "FAIL") << std::endl;
  return passed ? 0 : 1;
}
