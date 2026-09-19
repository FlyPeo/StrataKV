/*
 * 测试目标：验证 MetadataStateMachine 的 bootstrap 去重、replaceRegions、两阶段在线 Split 与快照持久化。
 * 测试策略：对内存状态机直接应用脚本化 metadata 命令，按 prepare→commit 驱动分裂，注入陈旧
 *           revision、非法键与中断的快照暂存文件，逐条检查接受/拒绝结果与发布的视图。
 * 测试规模：5 组场景：bootstrap 与去重、替换与 ID 分配、分裂两阶段、快照恢复、中断的快照暂存。
 * 验证内容：重复 bootstrap/mutation 幂等且计 dedup hits，非法替换返回 INVALID_ARGUMENT 且不推进
 *           revision，prepare 分配超过 ID 高水位的子 Region 且可重放、in-flight 二次 prepare 被拒，
 *           commit 收缩父 Region 并发布子 Region 且同样幂等，截断快照拒绝且不发布部分状态，
 *           未提交的暂存快照在崩溃后不得复活为已发布视图。
 */
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "metadata_state_machine.h"
#include "persister.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

stratakv::region::RegionDescriptor Region(uint64_t id, std::string start, std::string end,
                                          uint64_t version, uint64_t peerId,
                                          uint64_t storeId) {
  stratakv::region::RegionDescriptor region;
  region.set_regionid(id);
  region.set_startkey(std::move(start));
  region.set_endkey(std::move(end));
  region.mutable_epoch()->set_version(version);
  region.mutable_epoch()->set_confversion(1);
  region.set_leaderpeerid(peerId);
  auto* peer = region.add_peers();
  peer->set_peerid(peerId);
  peer->set_storeid(storeId);
  peer->set_host("127.0.0.1");
  peer->set_port(static_cast<uint32_t>(26000 + storeId));
  return region;
}

metadataRpcProtocol::MetadataCommand Bootstrap() {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid("bootstrap-1");
  command.set_expectedrevision(0);
  command.mutable_bootstrap()->set_configdigest("catalog-digest");
  *command.mutable_bootstrap()->add_regions() = Region(10, "", "m", 1, 101, 1);
  *command.mutable_bootstrap()->add_regions() = Region(20, "m", "", 1, 201, 2);
  for (uint64_t storeId = 1; storeId <= 2; ++storeId) {
    auto* store = command.mutable_bootstrap()->add_stores();
    store->set_storeid(storeId);
    store->set_host("127.0.0.1");
    store->set_port(static_cast<uint32_t>(26000 + storeId));
  }
  return command;
}

void CheckBootstrapAndDedup() {
  MetadataStateMachine state;
  auto command = Bootstrap();
  const auto first = state.Apply(command);
  Require(first.error() == metadataRpcProtocol::METADATA_OK && first.revision() == 1,
          "bootstrap must publish revision one");
  const auto duplicate = state.Apply(command);
  Require(duplicate.SerializeAsString() == first.SerializeAsString(),
          "duplicate mutation must return its original result");
  Require(state.Metrics().dedupHits == 1, "duplicate must increment dedup metric");
  Require(state.LookupKey("a")->regionId == 10 && state.LookupKey("m")->regionId == 20,
          "key lookup must honor half-open boundaries");

  command.set_mutationid("different-bootstrap");
  Require(state.Apply(command).error() == metadataRpcProtocol::METADATA_ALREADY_BOOTSTRAPPED,
          "second bootstrap must fail");
}

void CheckReplacementAndAllocation() {
  MetadataStateMachine state;
  Require(state.Apply(Bootstrap()).error() == metadataRpcProtocol::METADATA_OK,
          "bootstrap must succeed");

  metadataRpcProtocol::MetadataCommand replace;
  replace.set_mutationid("split-shape");
  replace.set_expectedrevision(1);
  replace.mutable_replaceregions()->add_removeregionids(10);
  *replace.mutable_replaceregions()->add_addregions() = Region(11, "", "g", 2, 111, 1);
  *replace.mutable_replaceregions()->add_addregions() = Region(12, "g", "m", 2, 121, 1);
  const auto replaced = state.Apply(replace);
  Require(replaced.error() == metadataRpcProtocol::METADATA_OK && replaced.revision() == 2,
          "valid replacement must advance revision");
  Require(state.LookupKey("g")->regionId == 12, "new split boundary must route right");

  metadataRpcProtocol::MetadataCommand invalid;
  invalid.set_mutationid("gap");
  invalid.set_expectedrevision(2);
  invalid.mutable_replaceregions()->add_removeregionids(11);
  *invalid.mutable_replaceregions()->add_addregions() = Region(13, "", "f", 3, 131, 1);
  Require(state.Apply(invalid).error() == metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
          "replacement with a gap must fail");
  Require(state.View()->revision == 2, "invalid replacement must not advance revision");

  metadataRpcProtocol::MetadataCommand allocate;
  allocate.set_mutationid("allocate-regions");
  allocate.set_expectedrevision(2);
  allocate.mutable_allocateids()->set_namespace_(metadataRpcProtocol::ID_NAMESPACE_REGION);
  allocate.mutable_allocateids()->set_count(3);
  const auto allocated = state.Apply(allocate);
  Require(allocated.error() == metadataRpcProtocol::METADATA_OK &&
              allocated.firstallocatedid() > 20 && allocated.allocatedcount() == 3,
          "allocation must start beyond descriptor high-water");
  Require(state.Apply(allocate).firstallocatedid() == allocated.firstallocatedid(),
          "allocation retry must return the same range");
}


void CheckSplitPhases() {
  MetadataStateMachine state;
  Require(state.Apply(Bootstrap()).error() == metadataRpcProtocol::METADATA_OK,
          "bootstrap must succeed");

  // Rejections on an unsplit Region leave the topology untouched.
  metadataRpcProtocol::MetadataCommand badKey;
  badKey.set_mutationid("split-bad-key");
  badKey.set_expectedrevision(1);
  badKey.mutable_preparesplit()->set_regionid(10);
  badKey.mutable_preparesplit()->set_splitkey("");
  badKey.mutable_preparesplit()->mutable_expectedepoch()->set_version(1);
  badKey.mutable_preparesplit()->mutable_expectedepoch()->set_confversion(1);
  Require(state.Apply(badKey).error() == metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
          "boundary split key must be rejected");
  metadataRpcProtocol::MetadataCommand stale;
  stale.set_mutationid("split-stale-epoch");
  stale.set_expectedrevision(1);
  stale.mutable_preparesplit()->set_regionid(10);
  stale.mutable_preparesplit()->set_splitkey("d");
  stale.mutable_preparesplit()->mutable_expectedepoch()->set_version(9);
  stale.mutable_preparesplit()->mutable_expectedepoch()->set_confversion(1);
  Require(state.Apply(stale).error() == metadataRpcProtocol::METADATA_REVISION_MISMATCH,
          "stale expected epoch must be rejected");
  Require(state.LookupKey("d")->regionId == 10,
          "rejected splits must not change topology");

  // Phase 1: prepare marks the parent and allocates the child beyond the
  // descriptor high-water marks.
  metadataRpcProtocol::MetadataCommand prepare;
  prepare.set_mutationid("split-prepare-1");
  prepare.set_expectedrevision(1);
  prepare.mutable_preparesplit()->set_regionid(10);
  prepare.mutable_preparesplit()->set_splitkey("d");
  prepare.mutable_preparesplit()->mutable_expectedepoch()->set_version(1);
  prepare.mutable_preparesplit()->mutable_expectedepoch()->set_confversion(1);
  const auto prepared = state.Apply(prepare);
  Require(prepared.error() == metadataRpcProtocol::METADATA_OK && prepared.revision() == 2,
          "split prepare must advance revision");
  Require(prepared.regions_size() == 1 && prepared.regions(0).has_splitpending(),
          "prepare must return the marked parent");
  const auto& child = prepared.regions(0).splitpending().child();
  Require(child.regionid() > 20 && child.startkey() == "d" && child.endkey() == "m",
          "child must be allocated beyond the high-water and own the right half");
  Require(child.epoch().version() == 1 && child.peers_size() == 1,
          "child must start with a fresh epoch and mirrored placement");
  Require(state.Apply(prepare).SerializeAsString() == prepared.SerializeAsString(),
          "prepare replay must return the original result");

  // A second prepare on an in-flight split is rejected.
  metadataRpcProtocol::MetadataCommand second = prepare;
  second.set_mutationid("split-prepare-2");
  second.set_expectedrevision(2);
  Require(state.Apply(second).error() == metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
          "a second prepare on an in-flight split must fail");

  // Phase 2: commit shrinks the parent, publishes the child, clears the mark.
  metadataRpcProtocol::MetadataCommand commit;
  commit.set_mutationid("split-commit-1");
  commit.set_expectedrevision(2);
  commit.mutable_commitsplit()->set_regionid(10);
  const auto committed = state.Apply(commit);
  Require(committed.error() == metadataRpcProtocol::METADATA_OK && committed.revision() == 3,
          "split commit must advance revision");
  Require(committed.regions_size() == 2, "commit must return parent and child");
  bool sawShrunken = false;
  bool sawChild = false;
  for (const auto& region : committed.regions()) {
    if (region.regionid() == 10) {
      sawShrunken = region.endkey() == "d" && region.epoch().version() == 2 &&
                    !region.has_splitpending();
    }
    if (region.regionid() == static_cast<uint64_t>(child.regionid())) {
      sawChild = region.startkey() == "d" && region.endkey() == "m" &&
                 region.metadatarevision() == 3;
    }
  }
  Require(sawShrunken && sawChild, "commit must shrink the parent and stamp the child");
  Require(state.LookupKey("a")->regionId == 10 &&
              state.LookupKey("d")->regionId == static_cast<int>(child.regionid()),
          "post-split routing must honor the new boundary");
  Require(state.Apply(commit).regions_size() == 2,
          "commit replay must return the original result");

  // Commit without a prepared split is rejected.
  metadataRpcProtocol::MetadataCommand unprepared;
  unprepared.set_mutationid("split-commit-none");
  unprepared.set_expectedrevision(3);
  unprepared.mutable_commitsplit()->set_regionid(20);
  Require(state.Apply(unprepared).error() == metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
          "commit without prepare must fail");
}

void CheckSnapshotRecovery() {
  MetadataStateMachine state;
  Require(state.Apply(Bootstrap()).error() == metadataRpcProtocol::METADATA_OK,
          "bootstrap must succeed");
  const std::string snapshot = state.Snapshot();

  MetadataStateMachine restored;
  std::string error;
  Require(restored.Restore(snapshot, &error), "valid snapshot must restore");
  Require(restored.View()->revision == 1 && restored.LookupRegion(20).has_value(),
          "restored state must expose committed topology");
  Require(restored.Apply(Bootstrap()).error() == metadataRpcProtocol::METADATA_OK,
          "restored dedup result must remain idempotent");

  MetadataStateMachine corrupt;
  Require(!corrupt.Restore(snapshot.substr(0, snapshot.size() / 2), &error),
          "truncated snapshot must fail");
  Require(corrupt.View()->revision == 0, "failed restore must not publish partial state");
}

void CheckInterruptedSnapshotStaging() {
  char temporaryTemplate[] = "/tmp/stratakv-metadata-snapshot-test-XXXXXX";
  const char* temporaryDirectory = mkdtemp(temporaryTemplate);
  Require(temporaryDirectory != nullptr, "cannot create snapshot test directory");
  const std::filesystem::path directory(temporaryDirectory);
  std::string staged;
  {
    Persister persister("metadata-stage", directory.string());
    persister.Save("raft-before", "snapshot-before");
    Require(persister.StageSnapshot("snapshot-uncommitted", &staged),
            "snapshot staging must succeed");
    Require(std::filesystem::exists(staged), "staged snapshot file must exist");
  }

  {
    Persister recovered("metadata-stage", directory.string());
    Require(!std::filesystem::exists(staged),
            "restart must discard an interrupted staged snapshot");
    Require(recovered.ReadRaftState() == "raft-before" &&
                recovered.ReadSnapshot() == "snapshot-before",
            "interrupted staging must preserve the committed state pair");
    Require(recovered.StageSnapshot("snapshot-after", &staged),
            "second snapshot staging must succeed");
    Require(recovered.CommitStagedSnapshot("raft-after", staged),
            "staged snapshot commit must succeed");
    Require(recovered.ReadRaftState() == "raft-after" &&
                recovered.ReadSnapshot() == "snapshot-after",
            "committed staged snapshot must publish both files");
  }
  std::filesystem::remove_all(directory);
}

}  // namespace

int main() {
  try {
    CheckBootstrapAndDedup();
    CheckReplacementAndAllocation();
    CheckSplitPhases();
    CheckSnapshotRecovery();
    CheckInterruptedSnapshotStaging();
    std::cout << "Metadata state-machine checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Metadata state-machine checks failed: " << error.what() << std::endl;
    return 1;
  }
}
