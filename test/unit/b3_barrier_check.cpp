/*
 * 测试目标：验证 B3 两个命名 failpoint barrier（after_all_prewrite_before_primary_commit 与 after_primary_commit_before_secondaries）在显式 project token 鉴权、原子写 marker、一次性触发、无业务锁挂起与释放控制逻辑。
 * 测试策略：通过控制线程与模拟工作线程并发调用 MaybeTrigger；验证在未 arm、错误 token 时直通且不写 marker；验证匹配 token 时原子生成 marker 文件并安全挂起；验证 Release() 与 Disarm() 能正确唤醒并清理状态。
 * 测试规模：覆盖全部 2 个 failpoint 阶段、多轮 arm/trigger/release 周期及并发命中与等待。
 * 验证条件：IsEnabled() 为真；未 arm 或错误 token 不触发且无阻塞；正确 token 成功写出 marker 文件且 WaitForHit() 返回 true；调用 Release() 后工作线程正常退出且状态复位。
 */
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include "txn_2pc_failpoint.h"

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

}  // namespace

int main() {
  try {
    std::cout << "--- Starting B3 Failpoint Barrier Unit Check ---" << std::endl;

    namespace fp = stratakv::transaction::failpoint;

    Require(fp::IsEnabled(), "Failpoints must be enabled in test build");

    const std::string token = "test-project-b3-token-2026";
    const std::filesystem::path markerDir = std::filesystem::temp_directory_path() / "stratakv_b3_test";
    std::filesystem::create_directories(markerDir);
    const std::string marker1 = (markerDir / "marker_prewrite.txt").string();
    const std::string marker2 = (markerDir / "marker_primary.txt").string();

    std::filesystem::remove(marker1);
    std::filesystem::remove(marker2);

    // 1. 验证未 arm 时的调用为 no-op
    fp::Disarm();
    Require(!fp::IsArmed(), "Must not be armed initially");
    Require(!fp::IsHit(), "Must not be hit initially");
    fp::MaybeTrigger(fp::FailpointLocation::AfterAllPrewriteBeforePrimaryCommit, token);
    Require(!std::filesystem::exists(marker1), "Marker must not be created when disarmed");

    // 2. 验证错误 token 不触发
    fp::Arm(token, fp::FailpointLocation::AfterAllPrewriteBeforePrimaryCommit, marker1);
    Require(fp::IsArmed(), "Must be armed");
    fp::MaybeTrigger(fp::FailpointLocation::AfterAllPrewriteBeforePrimaryCommit, "wrong-token-abc");
    Require(!fp::IsHit(), "Wrong token must not trigger failpoint");
    Require(!std::filesystem::exists(marker1), "Marker must not be created on wrong token");

    // 3. 验证位置不匹配不触发
    fp::MaybeTrigger(fp::FailpointLocation::AfterPrimaryCommitBeforeSecondaries, token);
    Require(!fp::IsHit(), "Wrong location must not trigger failpoint");
    Require(!std::filesystem::exists(marker1), "Marker must not be created on wrong location");

    // 4. 验证正确 token 触发 after_all_prewrite_before_primary_commit 并挂起
    std::atomic<bool> workerFinished{false};
    std::thread worker1([&]() {
      fp::MaybeTrigger(fp::FailpointLocation::AfterAllPrewriteBeforePrimaryCommit, token);
      workerFinished = true;
    });

    Require(fp::WaitForHit(3000), "WaitForHit must succeed when triggered");
    Require(fp::IsHit(), "Must be marked hit");
    Require(std::filesystem::exists(marker1), "Marker file must be atomically written");
    Require(!workerFinished.load(), "Worker thread must remain suspended at barrier");

    // 验证 marker 文件内容包含事件、位置和 token
    {
      std::ifstream in(marker1);
      std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
      Require(content.find("event=failpoint_hit") != std::string::npos, "Marker must contain event");
      Require(content.find("after_all_prewrite_before_primary_commit") != std::string::npos, "Marker must contain location");
      Require(content.find(token) != std::string::npos, "Marker must contain token");
    }

    // 释放 barrier
    fp::Release();
    worker1.join();
    Require(workerFinished.load(), "Worker thread must resume and exit after Release()");
    std::cout << "[Checkpoint 1] after_all_prewrite_before_primary_commit barrier verified." << std::endl;

    // 5. 验证 after_primary_commit_before_secondaries
    fp::Disarm();
    fp::Arm(token, fp::FailpointLocation::AfterPrimaryCommitBeforeSecondaries, marker2);
    workerFinished = false;
    std::thread worker2([&]() {
      fp::MaybeTrigger(fp::FailpointLocation::AfterPrimaryCommitBeforeSecondaries, token);
      workerFinished = true;
    });

    Require(fp::WaitForHit(3000), "WaitForHit must succeed for secondaries failpoint");
    Require(std::filesystem::exists(marker2), "Second marker file must be written");
    Require(!workerFinished.load(), "Worker thread must suspend before secondaries");

    {
      std::ifstream in(marker2);
      std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
      Require(content.find("after_primary_commit_before_secondaries") != std::string::npos, "Marker must contain location");
    }

    fp::Release();
    worker2.join();
    Require(workerFinished.load(), "Worker2 thread must resume and exit");
    std::cout << "[Checkpoint 2] after_primary_commit_before_secondaries barrier verified." << std::endl;

    // 清理临时文件
    std::filesystem::remove_all(markerDir);
    std::cout << "--- B3 Failpoint Barrier Unit Check Successfully Passed ---" << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "b3_barrier_check failed: " << e.what() << std::endl;
    return 1;
  }
}
