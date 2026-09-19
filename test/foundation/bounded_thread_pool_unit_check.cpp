/*
 * 测试目标：验证 BoundedThreadPool 在容量耗尽时提供确定的背压，而不是无限接收任务。
 * 测试策略：使用单 worker、单队列槽，阻塞首个任务、排队第二个任务并提交第三个任务
 *           制造满载，再释放阻塞任务并通过 Submit 获取返回值。
 * 测试规模：固定 1 个 worker、1 个队列槽，共尝试提交 4 个任务，其中 3 个应执行、
 *           1 个应因队列满被拒绝。
 * 验证内容：确认前两个任务被接收、超容量任务被拒绝，active/queued/rejected 指标正确，
 *           排队任务最终执行且 future 返回预期结果。
 */
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
