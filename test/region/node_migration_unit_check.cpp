/*
 * 测试目标：验证副本迁移的 metadata 两阶段协议，以及目标节点 learner 的挂载与退役清理。
 * 测试策略：用内存 MetadataStateMachine 驱动 prepare/commit/cancel move-peer，并用快照恢复模拟
 *           metadata Leader 在阶段间崩溃；再用真实 NodeServer 挂载 learner peer 并走完整退役路径。
 * 测试规模：2 组场景：metadata 迁移协议（prepare/并发拒绝/崩溃恢复/commit/晋升前取消/源重启）、
 *           节点挂载与清理（MountMigrationPeer/RetireMigrationPeer）。
 * 验证内容：prepare 分配全新 learner peer ID 并持久化在途标记、可重放且并发 move 被拒，commit
 *           原子用 voter 替换源 peer 并推进 conf_version，晋升前取消保持原 quorum；目标 learner
 *           原子发布进 registry 与调度器，退役后排空保留请求并物理删除 RocksDB 目录。
 */
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include "metadata_state_machine.h"
#include "node_server.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

RegionMetadata Region(int id, std::string start, std::string end, uint64_t revision,
                      std::vector<RegionPeerLocation> peers) {
  RegionMetadata region;
  region.regionId = id;
  region.startKey = std::move(start);
  region.endKey = std::move(end);
  region.epoch = {1, 1};
  region.metadataRevision = revision;
  region.peers = std::move(peers);
  region.leaderPeerId = region.peers.front().peerId;
  return region;
}

metadataRpcProtocol::MetadataCommand BootstrapMetadata() {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid("migration-bootstrap");
  command.set_expectedrevision(0);
  command.mutable_bootstrap()->set_configdigest("migration-test");

  RegionMetadata region = Region(
      10, "", "", 1,
      {{0, "127.0.0.1", 28101, 1, 101, false},
       {1, "127.0.0.1", 28102, 2, 102, false},
       {2, "127.0.0.1", 28103, 3, 103, false}});
  *command.mutable_bootstrap()->add_regions() = ToProtoRegion(region);
  for (uint64_t storeId = 1; storeId <= 4; ++storeId) {
    auto* store = command.mutable_bootstrap()->add_stores();
    store->set_storeid(storeId);
    store->set_host("127.0.0.1");
    store->set_port(static_cast<uint32_t>(28100 + storeId));
  }
  return command;
}

void CheckMetadataMigration() {
  MetadataStateMachine state;
  Require(state.Apply(BootstrapMetadata()).error() == metadataRpcProtocol::METADATA_OK,
          "migration metadata bootstrap must succeed");

  metadataRpcProtocol::MetadataCommand prepare;
  prepare.set_mutationid("move-10-1-to-4");
  prepare.set_expectedrevision(1);
  prepare.mutable_preparemovepeer()->set_regionid(10);
  prepare.mutable_preparemovepeer()->set_fromstoreid(1);
  prepare.mutable_preparemovepeer()->set_tostoreid(4);
  prepare.mutable_preparemovepeer()->mutable_expectedepoch()->set_version(1);
  prepare.mutable_preparemovepeer()->mutable_expectedepoch()->set_confversion(1);
  const auto prepared = state.Apply(prepare);
  Require(prepared.error() == metadataRpcProtocol::METADATA_OK && prepared.revision() == 2,
          "prepare move must durably advance metadata revision");
  Require(prepared.has_migration() && prepared.migration().targetpeer().peerid() > 103 &&
              prepared.migration().targetpeer().islearner(),
          "prepare move must allocate a fresh learner peer ID");
  Require(prepared.regions_size() == 1 && prepared.regions(0).peers_size() == 4,
          "prepare result must carry a mountable descriptor including the learner");
  Require(state.LookupMigration(10).has_value(),
          "in-flight migration must be visible in the published metadata view");
  Require(state.Apply(prepare).SerializeAsString() == prepared.SerializeAsString(),
          "prepare move replay must be idempotent");

  metadataRpcProtocol::MetadataCommand concurrent = prepare;
  concurrent.set_mutationid("move-10-concurrent");
  concurrent.set_expectedrevision(2);
  Require(state.Apply(concurrent).error() == metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
          "a second in-flight move for the Region must be rejected");

  MetadataStateMachine restored;
  std::string error;
  Require(restored.Restore(state.Snapshot(), &error) && restored.LookupMigration(10).has_value(),
          "metadata snapshot must preserve in-flight migration state");

  metadataRpcProtocol::MetadataCommand commit;
  commit.set_mutationid("move-10-commit");
  commit.set_expectedrevision(2);
  commit.mutable_commitmovepeer()->set_regionid(10);
  commit.mutable_commitmovepeer()->set_newpeerid(prepared.migration().targetpeer().peerid());
  commit.mutable_commitmovepeer()->mutable_expectedepoch()->set_version(1);
  commit.mutable_commitmovepeer()->mutable_expectedepoch()->set_confversion(1);
  // Applying the commit after restoring the prepared snapshot models a
  // metadata leader crash/failover between migration phases.
  const auto committed = restored.Apply(commit);
  Require(committed.error() == metadataRpcProtocol::METADATA_OK && committed.revision() == 3,
          "commit move must atomically publish the new topology");
  Require(committed.migration().phase() == stratakv::region::REPLICA_MIGRATION_COMPLETE &&
              !restored.LookupMigration(10).has_value(),
          "commit move must complete and clear the in-flight marker");
  const RegionMetadata published = *restored.LookupRegion(10);
  Require(published.epoch.confVersion == 4 && published.peers.size() == 3 &&
              published.FindPeer(101) == nullptr &&
              published.FindPeer(prepared.migration().targetpeer().peerid()) != nullptr &&
              !published.FindPeer(prepared.migration().targetpeer().peerid())->isLearner,
          "committed topology must replace the source with a voter and advance conf_version");
  Require(restored.Apply(commit).SerializeAsString() == committed.SerializeAsString(),
          "commit move replay must be idempotent");

  // A target failure before promotion cancels safely: the original voter set
  // remains authoritative, while the allocated peer ID is never published.
  MetadataStateMachine cancelledState;
  Require(cancelledState.Apply(BootstrapMetadata()).error() == metadataRpcProtocol::METADATA_OK,
          "cancellation bootstrap must succeed");
  const auto cancelPrepared = cancelledState.Apply(prepare);
  metadataRpcProtocol::MetadataCommand cancel;
  cancel.set_mutationid("move-10-cancel");
  cancel.set_expectedrevision(cancelPrepared.revision());
  cancel.mutable_cancelmovepeer()->set_regionid(10);
  cancel.mutable_cancelmovepeer()->set_newpeerid(
      cancelPrepared.migration().targetpeer().peerid());
  const auto cancelled = cancelledState.Apply(cancel);
  Require(cancelled.error() == metadataRpcProtocol::METADATA_OK &&
              cancelled.migration().phase() ==
                  stratakv::region::REPLICA_MIGRATION_CANCELLED &&
              !cancelledState.LookupMigration(10).has_value(),
          "target timeout cancellation must clear durable in-flight state");
  const RegionMetadata unchanged = *cancelledState.LookupRegion(10);
  Require(unchanged.FindPeer(101) != nullptr && unchanged.peers.size() == 3 &&
              unchanged.epoch.confVersion == 1,
          "pre-promotion cancellation must leave the original quorum untouched");

  // A source restart after committed removal reconstructs topology without
  // the retired peer, so it cannot rejoin elections or heartbeat handling.
  MetadataStateMachine afterSourceRestart;
  Require(afterSourceRestart.Restore(restored.Snapshot(), &error),
          "committed migration metadata must survive source-node restart");
  Require(afterSourceRestart.LookupRegion(10)->FindPeer(101) == nullptr,
          "restarted source must remain excluded from the committed peer set");
}

void CheckNodeMountAndCleanup() {
  RegionMetadata migrating = Region(
      10, "", "m", 1,
      {{0, "127.0.0.1", 28201, 1, 1001, false},
       {2, "127.0.0.1", 28203, 3, 1003, false}});
  RegionMetadata resident = Region(
      20, "m", "", 1,
      {{1, "127.0.0.1", 28202, 2, 2002, false}});
  RegionCatalog catalog({migrating, resident});
  NodeDynamicBootstrap bootstrap;
  bootstrap.revision = 1;
  bootstrap.regions = {migrating, resident};
  NodeServer target(1, RaftLogGcConfig{}, catalog, "", bootstrap);
  for (const auto& peer : target.PeersForTest()) peer->MarkServing();
  target.PublishInitialRegistryForTest();

  RegionMetadata prepared = migrating;
  prepared.metadataRevision = 2;
  prepared.peers.push_back({1, "127.0.0.1", 28202, 2, 1004, true});
  auto mounted = target.MountMigrationPeer(prepared, 1004, false);
  const std::string storagePath = mounted->StoragePath();
  Require(std::filesystem::exists(storagePath),
          "target mount must initialize the learner RocksDB directory");
  {
    auto handle = target.RegistryForTest().Lookup(10);
    Require(handle && handle->LocalPeerDescriptor().peerId == 1004 &&
                handle->LocalPeerDescriptor().isLearner,
            "target learner must be atomically published in RegionRegistry");
  }
  bool schedulerHasLearner = false;
  for (const auto& region : target.TxnSchedulerForTest()->Regions()) {
    if (region->TxnRegionId() == 10) schedulerHasLearner = true;
  }
  Require(schedulerHasLearner, "mounted learner must be registered with the node scheduler");

  mounted.reset();
  const auto retired = target.RetireMigrationPeer(
      10, 1004, 3, std::chrono::steady_clock::now() + std::chrono::seconds(2));
  Require(retired.found && retired.drained,
          "removed peer must unregister and drain all retained requests");
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (std::filesystem::exists(storagePath) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  Require(!std::filesystem::exists(storagePath),
          "retired peer must close RocksDB and physically delete its directory");
  Require(!target.RegistryForTest().Lookup(10),
          "retired Region must no longer be routable on the source node");
}

}  // namespace

int main() {
  char temporaryTemplate[] = "/tmp/stratakv-node-migration-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  if (temporaryDirectory == nullptr) return 1;
  const auto originalDirectory = std::filesystem::current_path();
  std::filesystem::current_path(temporaryDirectory);
  try {
    CheckMetadataMigration();
    CheckNodeMountAndCleanup();
    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);
    std::cout << "Node replica migration checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::filesystem::current_path(originalDirectory);
    std::filesystem::remove_all(temporaryDirectory);
    std::cerr << "Node replica migration checks failed: " << error.what() << std::endl;
    return 1;
  }
}
