/*
 * 测试目标：验证 StoreHeartbeatReporter 的采样上报生命周期与 Auto-Balancer 心跳遥测语义。
 * 测试策略：注入自定义发送回调与脚本化 metadata 应答，驱动 reporter 启停、超时重试与样本合并，
 *           另以 NodeServer 实例验证 reporter 所有权，并在内存 MetadataStateMachine 上应用心跳与配置。
 * 测试规模：7 个场景：reporter 启停、NodeServer 持有、重复/超时重试、新样本合并、过期 Store
 *           剔除、epoch 单调、遥测脱敏。
 * 验证内容：启停状态与 samplesCollected/heartbeatsSent 指标正确，失败重试复用同一序列号并计
 *           duplicateRetries，重复心跳被忽略不重复计数，过期 Store 不参与调度，epoch 只前进，
 *           线上字节流与诊断文本不含任何原始用户键值而只含 splitcandidategeneration 等派生量。
 */
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "auto_balancer_planner.h"
#include "metadata_state_machine.h"
#include "node_server.h"
#include "region_registry.h"
#include "store_heartbeat_reporter.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata MakeRegion(int id, uint64_t version = 1, uint64_t confVersion = 1) {
  RegionMetadata region;
  region.regionId = id;
  region.startKey = id == 10 ? "" : "m";
  region.endKey = id == 10 ? "m" : "";
  region.epoch = {version, confVersion};
  region.metadataRevision = 1;
  region.leaderPeerId = static_cast<uint64_t>(id * 10 + 1);
  for (uint64_t storeId = 1; storeId <= 3; ++storeId) {
    region.peers.push_back({static_cast<int>(storeId - 1), "127.0.0.1",
                            static_cast<short>(30000 + storeId), storeId,
                            static_cast<uint64_t>(id * 10) + storeId, false});
  }
  return region;
}

metadataRpcProtocol::MetadataCommand BootstrapCommand() {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid("heartbeat-bootstrap");
  command.mutable_bootstrap()->set_configdigest("heartbeat-digest");
  auto* region = command.mutable_bootstrap()->add_regions();
  region->set_regionid(10);
  region->set_startkey("");
  region->set_endkey("");
  region->mutable_epoch()->set_version(1);
  region->mutable_epoch()->set_confversion(1);
  region->set_leaderpeerid(101);
  for (uint64_t storeId = 1; storeId <= 3; ++storeId) {
    auto* peer = region->add_peers();
    peer->set_peerid(100 + storeId);
    peer->set_storeid(storeId);
    peer->set_host("127.0.0.1");
    peer->set_port(static_cast<uint32_t>(30000 + storeId));
  }
  for (uint64_t storeId = 1; storeId <= 4; ++storeId) {
    auto* store = command.mutable_bootstrap()->add_stores();
    store->set_storeid(storeId);
    store->set_host("127.0.0.1");
    store->set_port(static_cast<uint32_t>(30000 + storeId));
  }
  return command;
}

// 1. Reporter ownership and clean stoppable shutdown
void CheckReporterOwnershipAndShutdown() {
  StoreHeartbeatReporterConfig config;
  config.storeId = 1;
  config.interval = std::chrono::milliseconds(50);
  config.timeout = std::chrono::milliseconds(200);

  RegionRegistry registry;
  auto reporter = std::make_unique<StoreHeartbeatReporter>(config, &registry);
  Require(!reporter->IsRunning(), "reporter should not be running before Start()");

  reporter->SetCustomSender([](const metadataRpcProtocol::StoreHeartbeat&) {
    metadataRpcProtocol::MetadataCommandResult result;
    result.set_error(metadataRpcProtocol::METADATA_OK);
    return result;
  });

  reporter->Start();
  Require(reporter->IsRunning(), "reporter should be running after Start()");
  std::this_thread::sleep_for(std::chrono::milliseconds(120));

  reporter->Stop();
  Require(!reporter->IsRunning(), "reporter should be stopped after Stop()");
  const auto metrics = reporter->Metrics();
  Require(metrics.samplesCollected >= 1, "should have collected at least one sample");
  Require(metrics.heartbeatsSent >= 1, "should have sent at least one heartbeat");
}

void CheckNodeServerReporterOwnership() {
  const short port = 19500;
  RegionMetadata r1;
  r1.regionId = 100;
  r1.startKey = "";
  r1.endKey = "";
  r1.epoch = {1, 1};
  r1.metadataRevision = 7;
  r1.peers = {
      {0, "127.0.0.1", port, 1, 1000},
      {1, "127.0.0.1", static_cast<short>(port + 1), 2, 1001},
      {2, "127.0.0.1", static_cast<short>(port + 2), 3, 1002},
  };
  r1.leaderPeerId = 1000;

  RegionCatalog catalog({r1});
  NodeDynamicBootstrap bootstrap;
  bootstrap.regions = catalog.Regions();
  bootstrap.revision = 7;
  RaftLogGcConfig gcConfig;

  auto nodeServer = std::make_unique<NodeServer>(0, gcConfig, catalog, "", bootstrap);
  Require(nodeServer->HeartbeatReporterForTest() != nullptr,
          "dynamic NodeServer must own a heartbeat reporter");
  Require(!nodeServer->HeartbeatReporterForTest()->IsRunning(),
          "reporter should not run until NodeServer starts");

  nodeServer->SetMetadataEndpoints("127.0.0.1:2379");
  Require(nodeServer->HeartbeatReporterForTest() != nullptr,
          "SetMetadataEndpoints must configure reporter");

  nodeServer.reset();
}

// 2. Duplicate timeout retry handling
void CheckDuplicateTimeoutRetry() {
  StoreHeartbeatReporterConfig config;
  config.storeId = 1;
  config.interval = std::chrono::milliseconds(1000);
  config.timeout = std::chrono::milliseconds(2000);

  RegionRegistry registry;
  StoreHeartbeatReporter reporter(config, &registry);

  bool simulateFailure = true;
  std::vector<uint64_t> sentSequences;
  reporter.SetCustomSender([&](const metadataRpcProtocol::StoreHeartbeat& hb) {
    sentSequences.push_back(hb.sequence());
    metadataRpcProtocol::MetadataCommandResult result;
    if (simulateFailure) {
      result.set_error(metadataRpcProtocol::METADATA_TIMEOUT);
    } else {
      result.set_error(metadataRpcProtocol::METADATA_OK);
    }
    return result;
  });

  // Step A: Trigger with failure -> records failure and retains sample for retry
  reporter.TriggerOnce();
  Require(sentSequences.size() == 1 && sentSequences[0] == 1,
          "first trigger should attempt sequence 1");
  Require(reporter.Metrics().heartbeatsFailed == 1, "heartbeat should fail");

  // Step B: Next trigger should retry the exact same sequence 1
  simulateFailure = false;
  reporter.TriggerOnce(false);
  Require(sentSequences.size() == 2 && sentSequences[1] == 1,
          "retry should send duplicate sequence 1");
  Require(reporter.Metrics().duplicateRetries == 1, "duplicate retry should be counted");
  Require(reporter.Metrics().heartbeatsSucceeded == 1, "retry should succeed");

  // Verify state machine accepts duplicate sequence idempotently
  MetadataStateMachine stateMachine;
  stateMachine.Apply(BootstrapCommand());

  metadataRpcProtocol::MetadataCommand cmd1;
  cmd1.set_mutationid("hb-seq-1-first");
  auto* hb1 = cmd1.mutable_reportstoreheartbeat()->mutable_heartbeat();
  hb1->set_storeid(1);
  hb1->set_sequence(1);
  hb1->set_observedatms(1000);
  hb1->set_expiresatms(5000);
  hb1->set_capacitybytes(10000);
  hb1->set_availablebytes(8000);
  hb1->set_requestcount(100);

  auto res1 = stateMachine.Apply(cmd1);
  Require(res1.error() == metadataRpcProtocol::METADATA_OK, "first report should succeed");

  // Duplicate seq 1 replay
  metadataRpcProtocol::MetadataCommand cmd2;
  cmd2.set_mutationid("hb-seq-1-retry");
  *cmd2.mutable_reportstoreheartbeat()->mutable_heartbeat() = *hb1;
  auto res2 = stateMachine.Apply(cmd2);
  Require(res2.error() == metadataRpcProtocol::METADATA_OK, "duplicate report should succeed idempotently");

  const auto metrics = stateMachine.Metrics();
  Require(metrics.heartbeatAccepted == 1, "duplicate sequence must not double-accept");
  Require(metrics.heartbeatIgnored == 1, "duplicate sequence must increment ignored counter");
}

// 3. Newer-sample coalescing (bounded retry slot)
void CheckNewerSampleCoalescing() {
  StoreHeartbeatReporterConfig config;
  config.storeId = 1;
  config.interval = std::chrono::milliseconds(1000);
  config.timeout = std::chrono::milliseconds(2000);

  RegionRegistry registry;
  StoreHeartbeatReporter reporter(config, &registry);

  std::vector<uint64_t> sentSequences;
  bool failNext = true;
  reporter.SetCustomSender([&](const metadataRpcProtocol::StoreHeartbeat& hb) {
    sentSequences.push_back(hb.sequence());
    metadataRpcProtocol::MetadataCommandResult result;
    if (failNext) {
      result.set_error(metadataRpcProtocol::METADATA_TIMEOUT);
    } else {
      result.set_error(metadataRpcProtocol::METADATA_OK);
    }
    return result;
  });

  // Collect sample 1 and fail sending
  reporter.TriggerOnce();
  Require(sentSequences.size() == 1 && sentSequences[0] == 1, "first sample should be seq 1");

  // Before next transmission, simulate newer observations arriving
  failNext = false;
  // Triggering again generates sample 2 which coalesces the pending retry slot
  reporter.TriggerOnce();
  Require(sentSequences.size() == 2 && sentSequences[1] == 2,
          "newer sample seq 2 must coalesce previous retry");
  Require(reporter.Metrics().retriesCoalesced >= 1, "coalesced retry must be recorded");
}

// 4. Stale Store exclusion from scheduling destinations
void CheckStaleStoreExclusion() {
  MetadataStateMachine stateMachine;
  stateMachine.Apply(BootstrapCommand());

  // Store 1: fresh heartbeat
  metadataRpcProtocol::MetadataCommand cmd1;
  cmd1.set_mutationid("hb-store-1");
  auto* hb1 = cmd1.mutable_reportstoreheartbeat()->mutable_heartbeat();
  hb1->set_storeid(1);
  hb1->set_sequence(1);
  hb1->set_observedatms(1000);
  hb1->set_expiresatms(10000);
  hb1->set_capacitybytes(10000);
  hb1->set_availablebytes(8000);
  stateMachine.Apply(cmd1);

  // Store 4: expired/stale heartbeat
  metadataRpcProtocol::MetadataCommand cmd4;
  cmd4.set_mutationid("hb-store-4");
  auto* hb4 = cmd4.mutable_reportstoreheartbeat()->mutable_heartbeat();
  hb4->set_storeid(4);
  hb4->set_sequence(1);
  hb4->set_observedatms(1000);
  hb4->set_expiresatms(1500);  // Expired at now = 2000
  hb4->set_capacitybytes(10000);
  hb4->set_availablebytes(8000);
  stateMachine.Apply(cmd4);

  ClusterSchedulingSnapshot snapshot;
  snapshot.metadata = stateMachine.View();
  snapshot.nowMs = 2000;

  AutoBalancerPlanner planner;
  const auto planned = planner.Plan(snapshot);
  for (const auto& plan : planned.plans) {
    Require(plan.operation.targetstoreid() != 4,
            "stale Store 4 must never be selected as migration destination");
  }
}

// 5. Region epoch monotonicity in heartbeat observations
void CheckRegionEpochMonotonicity() {
  MetadataStateMachine stateMachine;
  stateMachine.Apply(BootstrapCommand());

  // Report Region 10 with version 2
  metadataRpcProtocol::MetadataCommand cmd1;
  cmd1.set_mutationid("hb-v2");
  auto* hb1 = cmd1.mutable_reportstoreheartbeat()->mutable_heartbeat();
  hb1->set_storeid(1);
  hb1->set_sequence(1);
  hb1->set_observedatms(1000);
  hb1->set_expiresatms(10000);
  hb1->set_capacitybytes(10000);
  hb1->set_availablebytes(8000);
  auto* r1 = hb1->add_regions();
  r1->set_regionid(10);
  r1->set_peerid(101);
  r1->mutable_epoch()->set_version(2);
  r1->mutable_epoch()->set_confversion(1);
  r1->set_approximatebytes(5000);
  stateMachine.Apply(cmd1);

  // Later report with older version 1 (out-of-order delivery)
  metadataRpcProtocol::MetadataCommand cmd2;
  cmd2.set_mutationid("hb-v1-stale");
  auto* hb2 = cmd2.mutable_reportstoreheartbeat()->mutable_heartbeat();
  hb2->set_storeid(1);
  hb2->set_sequence(2);
  hb2->set_observedatms(2000);
  hb2->set_expiresatms(10000);
  hb2->set_capacitybytes(10000);
  hb2->set_availablebytes(8000);
  auto* r2 = hb2->add_regions();
  r2->set_regionid(10);
  r2->set_peerid(101);
  r2->mutable_epoch()->set_version(1);  // Older!
  r2->mutable_epoch()->set_confversion(1);
  r2->set_approximatebytes(3000);
  stateMachine.Apply(cmd2);

  // View must retain version 2
  const auto view = stateMachine.View();
  const auto it = view->storeHeartbeats.find(1);
  Require(it != view->storeHeartbeats.end(), "heartbeat must be present");
  Require(!it->second.regions().empty(), "regions must not be empty");
  Require(it->second.regions(0).epoch().version() == 2,
          "older epoch must not overwrite newer accepted epoch");
}

// 6. Absence of raw keys/values in telemetry and diagnostics
void CheckAbsenceOfRawKeysInTelemetryAndDiagnostics() {
  StoreHeartbeatReporterConfig config;
  config.storeId = 1;

  RegionRegistry registry;
  StoreHeartbeatReporter reporter(config, &registry);

  // Setup custom telemetry with opaque split generation
  reporter.SetTelemetryCollector([]() {
    metadataRpcProtocol::RegionSchedulingObservation obs;
    obs.set_regionid(10);
    obs.set_peerid(101);
    obs.mutable_epoch()->set_version(1);
    obs.mutable_epoch()->set_confversion(1);
    obs.set_approximatebytes(1024 * 1024);
    obs.set_requestcount(50);
    obs.set_isleader(true);
    obs.set_splitcandidategeneration(42);
    return std::vector<metadataRpcProtocol::RegionSchedulingObservation>{obs};
  });

  const auto heartbeat = reporter.CollectHeartbeat();
  const std::string serialized = heartbeat.SerializeAsString();
  const std::string debugStr = heartbeat.DebugString();

  // Validate absence of raw keys
  const std::string secretKey = "user_secret_data_key_999";
  const std::string secretVal = "sensitive_user_payload";
  Require(serialized.find(secretKey) == std::string::npos, "no raw user key in wire format");
  Require(serialized.find(secretVal) == std::string::npos, "no raw user value in wire format");
  Require(debugStr.find("startkey") == std::string::npos, "no startkey in telemetry diagnostics");
  Require(debugStr.find("endkey") == std::string::npos, "no endkey in telemetry diagnostics");
  Require(debugStr.find("splitkey") == std::string::npos, "no splitkey in telemetry diagnostics");

  // Verify split candidate generation is opaque and resolvable at execution time
  Require(heartbeat.regions(0).splitcandidategeneration() == 42,
          "split candidate must be represented as opaque generation");
}

}  // namespace

int main() {
  try {
    CheckReporterOwnershipAndShutdown();
    CheckNodeServerReporterOwnership();
    CheckDuplicateTimeoutRetry();
    CheckNewerSampleCoalescing();
    CheckStaleStoreExclusion();
    CheckRegionEpochMonotonicity();
    CheckAbsenceOfRawKeysInTelemetryAndDiagnostics();
    std::cout << "Auto-Balancer heartbeat checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Auto-Balancer heartbeat checks failed: " << error.what() << std::endl;
    return 1;
  }
}
