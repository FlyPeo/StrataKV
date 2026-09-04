/*
 * 测试目标：验证 TiKV 风格 Raft Log GC 的软阈值、条数/大小硬阈值和安全压缩边界。
 * 测试策略：向纯 GC 决策器输入可控的 snapshot、applied、replicated、日志条数和字节数，
 *           分别覆盖正常 Follower、落后 Follower、强制回收和没有已 Apply 日志的情况。
 * 测试规模：固定 8 个决策场景，默认阈值为 50 条、196608 条、192 MiB。
 * 验证内容：软 GC 不越过最慢复制位置，硬 GC 不越过已 Apply 位置，未 Apply 日志永不回收。
 */
#include <iostream>
#include <stdexcept>

#include "raft_log_gc.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RaftLogGcState State(int truncated, int applied, int replicated, uint64_t count,
                     uint64_t bytes) {
  return RaftLogGcState{truncated, applied, replicated, count, bytes};
}

}  // namespace

int main() {
  try {
    const RaftLogGcConfig config;

    const auto belowSoft = EvaluateRaftLogGc(config, State(100, 149, 149, 49, 4096));
    Require(!belowSoft.ShouldGc(), "49 reclaimable entries must stay below the soft threshold");

    const auto soft = EvaluateRaftLogGc(config, State(100, 150, 150, 50, 4096));
    Require(soft.ShouldGc() && !soft.Forced() && soft.softThreshold &&
                soft.compactIndex == 150 && soft.reclaimableCount == 50,
            "50 replicated entries must trigger soft GC");

    const auto lagging = EvaluateRaftLogGc(config, State(100, 200, 149, 100, 8192));
    Require(!lagging.ShouldGc(), "soft GC must not strand a follower below the threshold");

    const auto countForced =
        EvaluateRaftLogGc(config, State(100, 200, 110, config.countLimit, 8192));
    Require(countForced.ShouldGc() && countForced.Forced() && countForced.countLimit &&
                countForced.compactIndex == 200,
            "count limit must force GC through the applied index");

    const auto sizeForced = EvaluateRaftLogGc(
        config, State(100, 175, 110, 75, config.sizeLimitBytes));
    Require(sizeForced.ShouldGc() && sizeForced.Forced() && sizeForced.sizeLimit &&
                sizeForced.compactIndex == 175,
            "size limit must force GC through the applied index");

    const auto noApplied =
        EvaluateRaftLogGc(config, State(100, 100, 100, config.countLimit, config.sizeLimitBytes));
    Require(!noApplied.ShouldGc(), "hard limits must not compact an unapplied log");

    const auto restoringSnapshot =
        EvaluateRaftLogGc(config, State(100, 90, 100, config.countLimit, config.sizeLimitBytes));
    Require(!restoringSnapshot.ShouldGc(),
            "state machine behind the snapshot boundary must not trigger GC");

    const auto clampedReplication =
        EvaluateRaftLogGc(config, State(100, 160, 1000, 60, 4096));
    Require(clampedReplication.ShouldGc() && clampedReplication.compactIndex == 160,
            "replicated index must be clamped to the applied boundary");

    std::cout << "PASS raft_log_gc_soft_threshold\n"
                 "PASS raft_log_gc_lagging_follower\n"
                 "PASS raft_log_gc_count_limit\n"
                 "PASS raft_log_gc_size_limit\n"
                 "PASS raft_log_gc_applied_boundary\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "raft log GC check failed: " << error.what() << '\n';
    return 1;
  }
}
