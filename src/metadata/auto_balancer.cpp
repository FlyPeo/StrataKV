#include "auto_balancer.h"

#include <chrono>

namespace {

uint64_t CurrentTimeMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

}  // namespace

AutoBalancer::AutoBalancer(std::shared_ptr<MetadataClient> metadataClient,
                           LeaderCheck isLeader, ViewProvider viewProvider,
                           CommandSubmitter submitCommand)
    : metadataClient_(std::move(metadataClient)),
      isLeader_(std::move(isLeader)),
      viewProvider_(std::move(viewProvider)),
      submitCommand_(std::move(submitCommand)),
      coordinator_(
          std::make_unique<TopologyOperationCoordinator>(metadataClient_)) {}

AutoBalancer::~AutoBalancer() { Stop(); }

void AutoBalancer::Start() {
  if (running_.exchange(true)) return;
  stopRequested_.store(false);
  evaluationThread_ = std::thread(&AutoBalancer::EvaluationLoop, this);
  executorThread_ = std::thread(&AutoBalancer::ExecutorLoop, this);
}

void AutoBalancer::Stop() {
  if (!running_.exchange(false)) return;
  stopRequested_.store(true);
  cv_.notify_all();
  if (evaluationThread_.joinable()) evaluationThread_.join();
  if (executorThread_.joinable()) executorThread_.join();
}

void AutoBalancer::EvaluateOnce() {
  {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.evaluationsRun++;
  }

  if (!isLeader_ || !isLeader_()) {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.evaluationsSkippedNonLeader++;
    return;
  }

  if (!viewProvider_) return;
  const auto view = viewProvider_();
  if (!view) return;

  if (!view->balancerConfig.enabled() || view->balancerConfig.paused()) {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.evaluationsSkippedDisabled++;
    return;
  }

  ClusterSchedulingSnapshot snapshot;
  snapshot.metadata = view;
  snapshot.nowMs = CurrentTimeMs();

  const auto planned = planner_.Plan(snapshot);
  if (planned.plans.empty()) return;

  const auto& top = planned.plans.front();
  metadataRpcProtocol::MetadataCommand cmd;
  cmd.set_mutationid("autobalance:" + top.operation.operatorid());
  cmd.set_expectedrevision(view->revision);
  *cmd.mutable_createschedulingoperator()->mutable_operator_() = top.operation;

  if (submitCommand_) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(5000);
    const auto result = submitCommand_(cmd, deadline);
    if (result.error() == metadataRpcProtocol::METADATA_OK) {
      std::lock_guard<std::mutex> lock(metricsMutex_);
      metrics_.operatorsCreated++;
    }
  }
}

void AutoBalancer::ReconcileOnce() {
  if (!isLeader_ || !isLeader_()) return;
  if (!viewProvider_) return;
  const auto view = viewProvider_();
  if (!view) return;

  for (const auto& item : view->schedulingOperators) {
    const auto& op = item.second;
    if (op.phase() == metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED ||
        op.phase() == metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED ||
        op.phase() == metadataRpcProtocol::SCHEDULING_OPERATOR_FAILED) {
      continue;
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(10000);
    coordinator_->ReconcileOperator(
        op, deadline, [this]() { return !isLeader_() || stopRequested_.load(); });

    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.operatorsReconciled++;
    break;  // Conservative serialization: process one operator per tick
  }
}

void AutoBalancer::EvaluationLoop() {
  while (!stopRequested_.load(std::memory_order_acquire)) {
    EvaluateOnce();
    std::unique_lock<std::mutex> lock(cvMutex_);
    cv_.wait_for(lock, std::chrono::milliseconds(1000), [this] {
      return stopRequested_.load(std::memory_order_acquire);
    });
  }
}

void AutoBalancer::ExecutorLoop() {
  while (!stopRequested_.load(std::memory_order_acquire)) {
    ReconcileOnce();
    std::unique_lock<std::mutex> lock(cvMutex_);
    cv_.wait_for(lock, std::chrono::milliseconds(200), [this] {
      return stopRequested_.load(std::memory_order_acquire);
    });
  }
}

AutoBalancerMetrics AutoBalancer::Metrics() const {
  std::lock_guard<std::mutex> lock(metricsMutex_);
  return metrics_;
}

