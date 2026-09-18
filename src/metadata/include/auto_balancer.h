#ifndef STRATAKV_METADATA_AUTO_BALANCER_H
#define STRATAKV_METADATA_AUTO_BALANCER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "auto_balancer_planner.h"
#include "metadata_client.h"
#include "metadata_rpc.pb.h"
#include "metadata_state_machine.h"
#include "topology_operation_coordinator.h"

struct AutoBalancerMetrics {
  uint64_t evaluationsRun = 0;
  uint64_t operatorsCreated = 0;
  uint64_t operatorsReconciled = 0;
  uint64_t evaluationsSkippedNonLeader = 0;
  uint64_t evaluationsSkippedDisabled = 0;
};

class AutoBalancer {
 public:
  using LeaderCheck = std::function<bool()>;
  using ViewProvider = std::function<std::shared_ptr<const MetadataView>()>;
  using CommandSubmitter = std::function<metadataRpcProtocol::MetadataCommandResult(
      const metadataRpcProtocol::MetadataCommand&,
      std::chrono::steady_clock::time_point)>;

  AutoBalancer(std::shared_ptr<MetadataClient> metadataClient,
               LeaderCheck isLeader, ViewProvider viewProvider,
               CommandSubmitter submitCommand);
  ~AutoBalancer();

  void Start();
  void Stop();
  bool IsRunning() const { return running_.load(std::memory_order_acquire); }

  void EvaluateOnce();
  void ReconcileOnce();

  AutoBalancerMetrics Metrics() const;
  TopologyOperationCoordinator* Coordinator() { return coordinator_.get(); }

 private:
  void EvaluationLoop();
  void ExecutorLoop();

  std::shared_ptr<MetadataClient> metadataClient_;
  LeaderCheck isLeader_;
  ViewProvider viewProvider_;
  CommandSubmitter submitCommand_;

  AutoBalancerPlanner planner_;
  std::unique_ptr<TopologyOperationCoordinator> coordinator_;

  std::atomic<bool> running_{false};
  std::atomic<bool> stopRequested_{false};

  mutable std::mutex metricsMutex_;
  AutoBalancerMetrics metrics_;

  std::mutex cvMutex_;
  std::condition_variable cv_;
  std::thread evaluationThread_;
  std::thread executorThread_;
};

#endif  // STRATAKV_METADATA_AUTO_BALANCER_H

