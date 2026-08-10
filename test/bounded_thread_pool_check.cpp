#include <atomic>
#include <chrono>
#include <future>
#include <iostream>

#include "bounded_thread_pool.h"

int main() {
  BoundedThreadPool pool(1, 1);
  std::promise<void> releaseFirst;
  std::shared_future<void> releaseSignal = releaseFirst.get_future().share();
  std::promise<void> firstStarted;
  std::promise<void> secondCompleted;

  if (!pool.TrySchedule([&]() {
        firstStarted.set_value();
        releaseSignal.wait();
      })) {
    std::cerr << "first task was rejected" << std::endl;
    return 1;
  }
  firstStarted.get_future().wait();
  if (!pool.TrySchedule([&]() { secondCompleted.set_value(); })) {
    std::cerr << "queued task was rejected" << std::endl;
    return 1;
  }
  if (pool.TrySchedule([]() {})) {
    std::cerr << "task beyond queue capacity was accepted" << std::endl;
    return 1;
  }
  if (pool.active() != 1 || pool.queued() != 1 || pool.rejected() != 1) {
    std::cerr << "executor metrics do not reflect bounded overload" << std::endl;
    return 1;
  }

  releaseFirst.set_value();
  if (secondCompleted.get_future().wait_for(std::chrono::seconds(2)) !=
      std::future_status::ready) {
    std::cerr << "queued task did not run" << std::endl;
    return 1;
  }

  auto answer = pool.Submit([]() { return 42; });
  if (answer.get() != 42) {
    std::cerr << "Submit did not return its task result" << std::endl;
    return 1;
  }
  std::cout << "correctness=PASS" << std::endl;
  return 0;
}
