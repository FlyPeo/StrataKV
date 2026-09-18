#ifndef STRATAKV_SERVER_STORE_HEARTBEAT_REPORTER_H
#define STRATAKV_SERVER_STORE_HEARTBEAT_REPORTER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "metadata_client.h"
#include "metadata_rpc.pb.h"
#include "region_registry.h"

struct StoreHeartbeatReporterConfig {
  uint64_t storeId = 0;
  std::string metadataEndpoints;
  std::chrono::milliseconds interval{1000};
  std::chrono::milliseconds timeout{5000};
  uint64_t capacityBytes = 1024ULL * 1024 * 1024 * 1024;  // 1 TB default
  uint64_t availableBytes = 768ULL * 1024 * 1024 * 1024;  // 768 GB default
};

struct HeartbeatReporterMetrics {
  uint64_t samplesCollected = 0;
  uint64_t heartbeatsSent = 0;
  uint64_t heartbeatsSucceeded = 0;
  uint64_t heartbeatsFailed = 0;
  uint64_t retriesCoalesced = 0;
  uint64_t duplicateRetries = 0;
};

class StoreHeartbeatReporter {
 public:
  using CustomSender = std::function<metadataRpcProtocol::MetadataCommandResult(
      const metadataRpcProtocol::StoreHeartbeat&)>;
  using TelemetryCollector =
      std::function<std::vector<metadataRpcProtocol::RegionSchedulingObservation>()>;

  StoreHeartbeatReporter(StoreHeartbeatReporterConfig config,
                         const RegionRegistry* registry,
                         std::shared_ptr<MetadataClient> metadataClient = nullptr);
  ~StoreHeartbeatReporter();

  void Start();
  void Stop();
  bool IsRunning() const { return running_.load(std::memory_order_acquire); }

  // Snapshot generation without holding registry or peer locks during RPC
  metadataRpcProtocol::StoreHeartbeat CollectHeartbeat();

  // Test seams
  void SetCustomSender(CustomSender sender) { customSender_ = std::move(sender); }
  void SetTelemetryCollector(TelemetryCollector collector) {
    customCollector_ = std::move(collector);
  }
  void SetCapacity(uint64_t capacity, uint64_t available) {
    std::lock_guard<std::mutex> lock(metricsMutex_);
    config_.capacityBytes = capacity;
    config_.availableBytes = available;
  }
  void RecordStoreRequest(uint64_t count = 1) {
    totalRequests_.fetch_add(count, std::memory_order_relaxed);
  }
  void TriggerOnce(bool collectNewSample = true);  // Deterministic execution seam
  HeartbeatReporterMetrics Metrics() const;

 private:
  void WorkerLoop();
  bool SendHeartbeat(const metadataRpcProtocol::StoreHeartbeat& heartbeat);

  StoreHeartbeatReporterConfig config_;
  const RegionRegistry* registry_;
  std::shared_ptr<MetadataClient> metadataClient_;
  CustomSender customSender_;
  TelemetryCollector customCollector_;

  std::atomic<bool> running_{false};
  std::atomic<bool> stopRequested_{false};
  std::atomic<uint64_t> sequence_{0};
  std::atomic<uint64_t> totalRequests_{0};

  // Coalesced pending slot: at most one pending sample waiting to be sent/retried
  std::mutex pendingMutex_;
  std::unique_ptr<metadataRpcProtocol::StoreHeartbeat> pendingHeartbeat_;
  bool lastSendFailed_ = false;

  mutable std::mutex metricsMutex_;
  HeartbeatReporterMetrics metrics_;

  std::mutex cvMutex_;
  std::condition_variable cv_;
  std::thread workerThread_;
};

#endif  // STRATAKV_SERVER_STORE_HEARTBEAT_REPORTER_H
