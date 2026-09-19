/*
 * 测试目标：验证动态模式下节点 bootstrap：一份校验通过的 metadata revision 必须初始化所有本地 peer。
 * 测试策略：在 mkdtemp 临时目录用真实 RegionPeer/NodeServer 驱动 BootstrapNodeRegions，注入缺失
 *           Region、混合 revision、抛异常的 peer 工厂等失败输入，再检查动态模式的 Raft 传输围栏与结构化日志。
 * 测试规模：7 个场景：完整引导、缺失 Region、revision 混用、peer 故障、完整集合才发布、
 *           动态模式 header/epoch 围栏、结构化诊断。
 * 验证内容：只有完整一致的 revision 才构建并发布全部 peer（报告携带 revision 且 summary 完整），
 *           任何失败都让 registry 不发布且失败记录定位到 Region 与组件；无 header 与陈旧 epoch 的
 *           Raft 消息分别以 MISSING_HEADER/EPOCH_NOT_MATCH 结构化拒绝并分开计数，日志含 Region/epoch/revision 标识。
 */
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include "kv_server_rpc.pb.h"
#include "node_bootstrap.h"
#include "node_server.h"
#include "raft_rpc.pb.h"
#include "region.pb.h"
#include "region_metadata.h"
#include "structured_region_log.h"
#include "util.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Descriptor(int id, const char* start, const char* end, uint64_t basePeerId,
                          short localPort, uint64_t revision) {
  RegionMetadata descriptor;
  descriptor.regionId = id;
  descriptor.startKey = start;
  descriptor.endKey = end;
  descriptor.epoch = {1, 1};
  descriptor.metadataRevision = revision;
  descriptor.peers = {
      {0, "127.0.0.1", localPort, 1, basePeerId},
      {1, "127.0.0.1", static_cast<short>(localPort + 1), 2, basePeerId + 1},
      {2, "127.0.0.1", static_cast<short>(localPort + 2), 3, basePeerId + 2},
  };
  descriptor.leaderPeerId = basePeerId;
  return descriptor;
}

RegionCatalog TwoRegionCatalog(int firstId, uint64_t basePeerId, short localPort,
                               uint64_t revision) {
  return RegionCatalog({Descriptor(firstId, "", "m", basePeerId, localPort, revision),
                        Descriptor(firstId + 10, "m", "", basePeerId + 10, localPort, revision)});
}

class SyncDone final : public google::protobuf::Closure {
 public:
  void Run() override { finished = true; }
  bool finished = false;
};

NodeBootstrapReport RunBootstrap(int nodeId, const RegionCatalog& catalog,
                                const std::vector<RegionMetadata>& regions, uint64_t revision,
                                const RegionPeerFactory& factory) {
  return BootstrapNodeRegions(nodeId, catalog, regions, revision, factory);
}

RegionPeerFactory ThrowingFactory() {
  return [](const RegionMetadata&, size_t) -> std::shared_ptr<RegionPeer> {
    throw std::runtime_error("rocksdb open failed");
  };
}

RegionPeerFactory RealFactory(const std::shared_ptr<NodeTxnScheduler>& scheduler) {
  return [scheduler](const RegionMetadata& descriptor, size_t peerIndex) {
    return std::make_shared<RegionPeer>(0, descriptor, static_cast<int>(peerIndex),
                                        RaftLogGcConfig{}, scheduler);
  };
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-node-bootstrap-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  const auto originalDirectory = std::filesystem::current_path();
  std::filesystem::current_path(temporaryDirectory);
  try {
    // A complete metadata revision of the local assignment initializes every
    // assigned peer exactly once.
    {
      const auto catalog = TwoRegionCatalog(10, 1000000, 25010, 5);
      std::vector<RegionMetadata> metadata = {
          Descriptor(10, "", "m", 1000000, 25010, 5),
          Descriptor(20, "m", "", 1000010, 25010, 5),
      };
      auto scheduler = std::make_shared<NodeTxnScheduler>();
      const auto report = RunBootstrap(
          0, catalog, metadata, 5,
          [&scheduler](const RegionMetadata& descriptor, size_t peerIndex) {
            return std::make_shared<RegionPeer>(0, descriptor, static_cast<int>(peerIndex),
                                                RaftLogGcConfig{}, scheduler);
          });
      Require(report.ok(), "a consistent metadata revision must bootstrap every peer");
      Require(report.peers.size() == 2, "both assigned Regions must initialize");
      Require(report.revision == 5, "the report must carry the bootstrap revision");
      Require(report.Summary().find("initialized_regions=2") != std::string::npos,
              "the report summary must describe the published set");
    }

    // A Region missing from the metadata revision aborts the whole bootstrap.
    {
      const auto catalog = TwoRegionCatalog(40, 4000000, 25110, 9);
      std::vector<RegionMetadata> metadata = {Descriptor(40, "", "m", 4000000, 25110, 9)};
      auto scheduler = std::make_shared<NodeTxnScheduler>();
      const auto report = RunBootstrap(0, catalog, metadata, 9, RealFactory(scheduler));
      Require(!report.ok() && report.peers.empty(),
              "a missing Region must leave nothing to publish");
      Require(report.failures.size() == 1 && report.failures.front().regionId == 50,
              "the failure must name the Region that is absent");
      Require(report.failures.front().component == "metadata",
              "a metadata gap must be attributed to the metadata component");
    }

    // A descriptor from another revision cannot join one bootstrap view.
    {
      const auto catalog = TwoRegionCatalog(70, 7000000, 25210, 3);
      std::vector<RegionMetadata> metadata = {
          Descriptor(70, "", "m", 7000000, 25210, 3),
          Descriptor(80, "m", "", 7000010, 25210, 4),
      };
      auto scheduler = std::make_shared<NodeTxnScheduler>();
      const auto report = RunBootstrap(0, catalog, metadata, 3, RealFactory(scheduler));
      Require(!report.ok() && report.peers.empty(), "mixed revisions must abort bootstrap");
      Require(!report.failures.empty() && report.failures.front().regionId == 80 &&
                  report.failures.front().component == "metadata",
              "the stale revision must be reported against its Region");
    }

    // One RocksDB/Raft failure is reported with its component and Region.
    {
      const auto catalog = TwoRegionCatalog(100, 10000000, 25310, 11);
      std::vector<RegionMetadata> metadata = {
          Descriptor(100, "", "m", 10000000, 25310, 11),
          Descriptor(110, "m", "", 10000010, 25310, 11),
      };
      const auto report = RunBootstrap(0, catalog, metadata, 11, ThrowingFactory());
      Require(!report.ok() && report.peers.empty(), "a peer failure must publish nothing");
      Require(report.failures.size() == 2 &&
                  report.failures.front().component == "peer" &&
                  report.failures.front().message == "rocksdb open failed",
              "peer failures must carry the underlying component error");
    }

    // NodeServer dynamic bootstrap publishes only a complete peer set.
    {
      const auto catalog = TwoRegionCatalog(130, 13000000, 25410, 21);
      NodeDynamicBootstrap bootstrap;
      bootstrap.revision = 21;
      bootstrap.regions = {Descriptor(130, "", "m", 13000000, 25410, 21),
                           Descriptor(140, "m", "", 13000010, 25410, 21)};
      NodeServer server(0, RaftLogGcConfig{}, catalog, "", bootstrap);
      Require(server.PeersForTest().size() == 2, "dynamic bootstrap must build every peer");
      for (const auto& peer : server.PeersForTest()) {
        Require(peer->Descriptor().metadataRevision == 21,
                "dynamic peers must carry the bootstrap metadata revision");
        peer->MarkServing();
      }
      server.PublishInitialRegistryForTest();
      Require(server.RegistryMetrics().revision == 21 &&
                  server.RegistryMetrics().serving == 2,
              "the complete registry must be published at the bootstrap revision");
    }

    bool dynamicBootstrapFailed = false;
    std::string failureMessage;
    try {
      const auto catalog = TwoRegionCatalog(160, 16000000, 25510, 31);
      NodeDynamicBootstrap bootstrap;
      bootstrap.revision = 31;
      bootstrap.regions = {Descriptor(160, "", "m", 16000000, 25510, 31)};
      NodeServer server(0, RaftLogGcConfig{}, catalog, "", bootstrap);
    } catch (const std::exception& error) {
      dynamicBootstrapFailed = true;
      failureMessage = error.what();
    }
    Require(dynamicBootstrapFailed, "an incomplete dynamic bootstrap must fail startup");
    Require(failureMessage.find("region_id=170") != std::string::npos,
            "startup must report the Region that failed to initialize");

    // Dynamic mode fences Raft transport: identity and epoch are mandatory.
    {
      const auto catalog = TwoRegionCatalog(190, 19000000, 25610, 41);
      NodeDynamicBootstrap bootstrap;
      bootstrap.revision = 41;
      bootstrap.regions = {Descriptor(190, "", "m", 19000000, 25610, 41),
                           Descriptor(200, "m", "", 19000010, 25610, 41)};
      NodeServer server(0, RaftLogGcConfig{}, catalog, "", bootstrap);
      for (const auto& peer : server.PeersForTest()) peer->MarkServing();
      server.PublishInitialRegistryForTest();
      RaftServiceDispatcher raftDispatcher(&server);

      raftRpcProctoc::AppendEntriesArgs headerlessAppend;
      headerlessAppend.set_regionid(190);
      headerlessAppend.set_term(3);
      headerlessAppend.set_leaderid(1);
      raftRpcProctoc::AppendEntriesReply headerlessReply;
      SyncDone headerlessDone;
      raftDispatcher.AppendEntries(nullptr, &headerlessAppend, &headerlessReply,
                                   &headerlessDone);
      Require(headerlessDone.finished && !headerlessReply.success(),
              "dynamic mode must reject Raft messages without a peer header");
      Require(headerlessReply.header().error().code() ==
                  stratakv::region::REGION_ERROR_MISSING_HEADER,
              "a headerless Raft message must report a structured missing-header error");
      Require(server.RegistryMetrics().headerRejects >= 1,
              "header rejections must be counted for operators");

      raftRpcProctoc::AppendEntriesArgs staleAppend;
      staleAppend.set_regionid(190);
      staleAppend.set_term(3);
      staleAppend.set_leaderid(1);
      auto* header = staleAppend.mutable_header();
      header->set_regionid(190);
      header->set_frompeerid(19000001);
      header->set_topeerid(19000000);
      header->mutable_epoch()->set_version(1);
      header->mutable_epoch()->set_confversion(2);
      raftRpcProctoc::AppendEntriesReply staleReply;
      SyncDone staleDone;
      raftDispatcher.AppendEntries(nullptr, &staleAppend, &staleReply, &staleDone);
      Require(staleDone.finished && !staleReply.success() &&
                  staleReply.header().error().code() ==
                      stratakv::region::REGION_ERROR_EPOCH_NOT_MATCH,
              "stale-epoch Raft messages must be fenced before Raft execution");
      Require(server.RegistryMetrics().epochRejects == 1,
              "epoch rejections must be counted separately from header rejections");
    }

    // Structured diagnostics identify the Region and epoch without payloads.
    {
      const auto line = FormatRegionLog({"region_request_rejected", 7, 19000000, 1, 2, 3, 41,
                                         "REGION_ERROR_EPOCH_NOT_MATCH", "dispatch", "stale"});
      Require(line.find("region_id=7") != std::string::npos &&
                  line.find("epoch=2.3") != std::string::npos &&
                  line.find("metadata_revision=41") != std::string::npos &&
                  line.find("error_code=REGION_ERROR_EPOCH_NOT_MATCH") != std::string::npos,
              "structured logs must carry Region, peer, epoch and revision identifiers");
    }

    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);
    std::cout << "Node dynamic bootstrap checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::filesystem::current_path(originalDirectory);
    std::cerr << "Node dynamic bootstrap checks failed: " << error.what() << std::endl;
    return 1;
  }
}
