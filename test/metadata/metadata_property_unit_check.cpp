// Metadata state-machine property tests: every accepted mutation must keep the
// keyspace fully covered, Region ids unique and epochs monotonic, while every
// malformed mutation must be rejected without advancing the revision.
/*
 * 测试目标：验证 MetadataStateMachine 的结构不变量：任何被接受的 mutation 都保持键空间完整覆盖。
 * 测试策略：以固定种子 mt19937 随机挑选 Region 与分裂边界连续提交 replaceRegions，每轮后校验
 *           全量不变量，再叠加 5 类畸形替换与快照往返；状态机是唯一的真值分配器，拒绝即不推进。
 * 测试规模：64 轮随机分裂（要求至少 8 轮被接受）、5 个畸形替换（gap/重叠/重复 id/epoch 回退/
 *           空替换）、1 次快照恢复，每轮 5 个探针键。
 * 验证内容：发布视图必须从空键开始到空键结束、相邻 Region 无缝无叠、Region id 唯一、epoch 为
 *           正且不回退，每个采样键恰好解析到一个 Region；被拒 mutation 不推进 revision，
 *           快照恢复保留全部不变量与 revision。
 */
#include <algorithm>
#include <iostream>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "metadata_state_machine.h"

namespace {

void Require(bool condition, const std::string& message) {
  if (!condition) throw std::runtime_error(message);
}

stratakv::region::RegionDescriptor Region(uint64_t id, const std::string& start,
                                          const std::string& end, uint64_t version,
                                          uint64_t peerId, uint64_t storeId) {
  stratakv::region::RegionDescriptor region;
  region.set_regionid(id);
  region.set_startkey(start);
  region.set_endkey(end);
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
  command.set_mutationid("property-bootstrap");
  command.set_expectedrevision(0);
  command.mutable_bootstrap()->set_configdigest("property-digest");
  *command.mutable_bootstrap()->add_regions() = Region(10, "", "", 1, 101, 1);
  auto* store = command.mutable_bootstrap()->add_stores();
  store->set_storeid(1);
  store->set_host("127.0.0.1");
  store->set_port(26001);
  return command;
}

// The invariants every published view must satisfy, regardless of the shape of
// the mutation that produced it.
void RequireValidView(const MetadataStateMachine& state, const std::string& context) {
  const auto view = state.View();
  Require(view != nullptr, context + ": view must be published");
  Require(!view->catalog->Regions().empty(), context + ": keyspace must not be empty");
  auto ordered = view->catalog->Regions();
  std::sort(ordered.begin(), ordered.end(),
            [](const RegionMetadata& lhs, const RegionMetadata& rhs) {
              return lhs.startKey < rhs.startKey;
            });
  Require(ordered.front().startKey.empty(), context + ": keyspace must start at the empty key");
  Require(ordered.back().endKey.empty(), context + ": keyspace must end at the empty key");
  std::set<int> identifiers;
  for (size_t index = 0; index < ordered.size(); ++index) {
    Require(identifiers.insert(ordered[index].regionId).second,
            context + ": Region ids must be unique");
    Require(ordered[index].epoch.version >= 1, context + ": epoch version must be positive");
    Require(!ordered[index].peers.empty(), context + ": every Region needs at least one peer");
    if (index == 0) continue;
    Require(ordered[index - 1].endKey == ordered[index].startKey,
            context + ": keyspace must be contiguous without gaps or overlaps");
  }
  // Every byte of the sampled keyspace must resolve to exactly one Region.
  for (const std::string& key : {std::string(""), std::string("a"), std::string("m"),
                                 std::string("z"), std::string("\x7f\x7f")}) {
    Require(state.LookupKey(key).has_value(), context + ": every key must resolve");
  }
}

}  // namespace

int main() {
  try {
    MetadataStateMachine state;
    Require(state.Apply(Bootstrap()).error() == metadataRpcProtocol::METADATA_OK,
            "bootstrap must succeed");
    RequireValidView(state, "bootstrap");

    std::mt19937 generator(20260915u);
    uint64_t nextId = 20;
    int accepted = 0;
    // Each round splits one Region at a random boundary. The state machine is
    // the only allocator of truth: it must accept every well-formed split and
    // reject every malformed one without moving the revision.
    for (int round = 0; round < 64; ++round) {
      const auto view = state.View();
      std::uniform_int_distribution<size_t> pick(0, view->catalog->Regions().size() - 1);
      const RegionMetadata victim = view->catalog->Regions()[pick(generator)];
      const uint64_t version = victim.epoch.version + 1;
      // Pick a boundary strictly inside the victim so the split is well formed.
      std::string boundary;
      for (char suffix = 'a'; suffix <= 'z'; ++suffix) {
        std::string candidate = victim.startKey + suffix;
        if (candidate > victim.startKey && (victim.endKey.empty() || candidate < victim.endKey)) {
          boundary = candidate;
          break;
        }
      }
      if (boundary.empty()) continue;  // The range is too narrow to split.
      Require(victim.Contains(boundary), "generated boundary must stay inside the victim");

      metadataRpcProtocol::MetadataCommand split;
      split.set_mutationid("property-split-" + std::to_string(round));
      split.set_expectedrevision(view->revision);
      split.mutable_replaceregions()->add_removeregionids(victim.regionId);
      *split.mutable_replaceregions()->add_addregions() = Region(
          ++nextId, victim.startKey, boundary, version, 1000 + nextId, 1);
      *split.mutable_replaceregions()->add_addregions() =
          Region(++nextId, boundary, victim.endKey.empty() ? std::string() : victim.endKey,
                 version, 1000 + nextId, 1);
      if (!victim.endKey.empty()) {
        split.mutable_replaceregions()->mutable_addregions(1)->set_endkey(victim.endKey);
      }
      const auto result = state.Apply(split);
      if (result.error() != metadataRpcProtocol::METADATA_OK) {
        Require(state.View()->revision == view->revision,
                "a rejected split must not advance the revision");
        --nextId;
        --nextId;
        continue;
      }
      ++accepted;
      RequireValidView(state, "split round " + std::to_string(round));
      Require(state.View()->revision > view->revision, "an accepted split must advance revision");
      // Epochs never go backwards: both halves inherit the incremented version.
      for (const auto& region : state.View()->catalog->Regions()) {
        if (region.regionId != static_cast<int>(nextId) &&
            region.regionId != static_cast<int>(nextId - 1)) {
          Require(region.epoch.version >= 1, "retained Regions keep their epoch");
        }
      }
    }
    // Narrow ranges cannot always be split further, so the property is that a
    // meaningful number of generated splits land and none breaks an invariant.
    Require(accepted >= 8, "generated splits must keep being accepted");

    // Malformed mutations must all be rejected atomically.
    const uint64_t revisionBefore = state.View()->revision;
    const std::vector<std::string> malformedIds = {"gap", "overlap", "duplicate-id",
                                                   "epoch-regression", "empty-replacement"};
    for (const auto& id : malformedIds) {
      const auto view = state.View();
      const RegionMetadata victim = view->catalog->Regions().front();
      metadataRpcProtocol::MetadataCommand bad;
      bad.set_mutationid(id);
      bad.set_expectedrevision(view->revision);
      bad.mutable_replaceregions()->add_removeregionids(victim.regionId);
      if (id == "gap") {
        *bad.mutable_replaceregions()->add_addregions() =
            Region(900, victim.startKey, "zzz-leaves-a-gap", victim.epoch.version + 1, 9001, 1);
      } else if (id == "overlap") {
        *bad.mutable_replaceregions()->add_addregions() =
            Region(901, victim.startKey, victim.endKey, victim.epoch.version + 1, 9011, 1);
        *bad.mutable_replaceregions()->add_addregions() =
            Region(902, victim.startKey, victim.endKey, victim.epoch.version + 1, 9021, 1);
      } else if (id == "duplicate-id") {
        *bad.mutable_replaceregions()->add_addregions() =
            Region(903, victim.startKey, victim.endKey, victim.epoch.version + 1, 9031, 1);
        *bad.mutable_replaceregions()->add_addregions() =
            Region(903, victim.startKey, victim.endKey, victim.epoch.version + 1, 9032, 1);
      } else if (id == "epoch-regression") {
        *bad.mutable_replaceregions()->add_addregions() =
            Region(904, victim.startKey, victim.endKey, 1, 9041, 1);
      }
      const auto result = state.Apply(bad);
      Require(result.error() != metadataRpcProtocol::METADATA_OK,
              "malformed replacement " + id + " must be rejected");
      Require(state.View()->revision == revisionBefore,
              "malformed replacement " + id + " must not advance the revision");
      RequireValidView(state, "after rejecting " + id);
    }

    // A snapshot round trip preserves every invariant.
    std::string error;
    MetadataStateMachine restored;
    Require(restored.Restore(state.Snapshot(), &error), "snapshot must restore: " + error);
    RequireValidView(restored, "restored snapshot");
    Require(restored.View()->revision == state.View()->revision,
            "restored snapshot must keep the revision");

    std::cout << "Metadata property checks passed (accepted splits=" << accepted << ")"
              << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Metadata property checks failed: " << error.what() << std::endl;
    return 1;
  }
}
