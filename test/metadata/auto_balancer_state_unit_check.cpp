/*
 * 测试目标：验证 MetadataStateMachine 中 Auto-Balancer 心跳、配置与调度算子的状态机语义。
 * 测试策略：直接向内存状态机应用脚本化命令（bootstrap/heartbeat/configure/创建算子/阶段迁移/
 *           取消），并用快照恢复模拟 metadata Leader 崩溃后在新 Leader 上继续推进同一算子。
 * 测试规模：3 组场景：心跳与配置、算子生命周期与恢复、旧版本快照向后兼容。
 * 验证内容：重复心跳按序列幂等去重、过期样本被忽略且 Region epoch 单调前进，算子重复提交幂等、
 *           竞争阶段迁移返回 CONFLICT，恢复出的快照能继续推进算子直至 success 并可重复取消，
 *           旧格式快照恢复后 balancer 配置安全默认关闭。
 */
#include <iostream>
#include <stdexcept>
#include <string>

#include "metadata_state_machine.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

metadataRpcProtocol::MetadataCommand Bootstrap() {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid("balancer-bootstrap");
  command.mutable_bootstrap()->set_configdigest("balancer-state-test");
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
    peer->set_port(static_cast<uint32_t>(29000 + storeId));
  }
  for (uint64_t storeId = 1; storeId <= 4; ++storeId) {
    auto* store = command.mutable_bootstrap()->add_stores();
    store->set_storeid(storeId);
    store->set_host("127.0.0.1");
    store->set_port(static_cast<uint32_t>(29000 + storeId));
  }
  return command;
}

metadataRpcProtocol::MetadataCommand Heartbeat(std::string mutationId, uint64_t storeId,
                                               uint64_t sequence, uint64_t regionVersion = 1) {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid(std::move(mutationId));
  command.set_expectedrevision(1);
  auto* heartbeat = command.mutable_reportstoreheartbeat()->mutable_heartbeat();
  heartbeat->set_storeid(storeId);
  heartbeat->set_sequence(sequence);
  heartbeat->set_observedatms(1000 + sequence);
  heartbeat->set_expiresatms(10000 + sequence);
  heartbeat->set_capacitybytes(1024 * 1024);
  heartbeat->set_availablebytes(768 * 1024);
  heartbeat->set_requestcount(sequence * 10);
  if (storeId <= 3) {
    auto* region = heartbeat->add_regions();
    region->set_regionid(10);
    region->set_peerid(100 + storeId);
    region->mutable_epoch()->set_version(regionVersion);
    region->mutable_epoch()->set_confversion(1);
    region->set_approximatebytes(128 * 1024);
    region->set_requestcount(sequence * 5);
    region->set_isleader(storeId == 1);
    region->set_splitcandidategeneration(7);
  }
  return command;
}

metadataRpcProtocol::AutoBalancerConfig EnabledConfig(uint64_t version) {
  metadataRpcProtocol::AutoBalancerConfig config;
  config.set_enabled(true);
  config.set_version(version);
  config.set_evaluationintervalms(1000);
  config.set_heartbeattimeoutms(5000);
  config.set_minimumfreebytes(1024);
  config.set_maximumdiskusedratio(0.90);
  config.set_replicationfactor(3);
  config.set_imbalancethreshold(0.10);
  config.set_splitsizebytes(1024 * 1024);
  config.set_splitrequestthreshold(100);
  config.set_splitconsecutivewindows(2);
  config.set_regioncooldownms(10000);
  config.set_maximumactiveoperators(2);
  config.set_maximumactiveperstore(1);
  return config;
}

metadataRpcProtocol::MetadataCommand Configure(std::string id,
                                               metadataRpcProtocol::AutoBalancerConfig config) {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid(std::move(id));
  command.set_expectedrevision(1);
  *command.mutable_updateautobalancerconfig()->mutable_config() = std::move(config);
  return command;
}

metadataRpcProtocol::MetadataCommand MoveOperator(std::string mutationId,
                                                  std::string operatorId,
                                                  uint64_t regionId = 10) {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid(std::move(mutationId));
  command.set_expectedrevision(1);
  auto* operation = command.mutable_createschedulingoperator()->mutable_operator_();
  operation->set_operatorid(std::move(operatorId));
  operation->set_type(metadataRpcProtocol::SCHEDULING_OPERATOR_MOVE_PEER);
  operation->set_regionid(regionId);
  operation->set_sourcerevision(1);
  operation->mutable_regionepoch()->set_version(1);
  operation->mutable_regionepoch()->set_confversion(1);
  operation->set_sourcestoreid(1);
  operation->set_targetstoreid(4);
  operation->set_phase(metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING);
  operation->set_createdatms(2000);
  operation->set_updatedatms(2000);
  operation->set_deadlinems(20000);
  return command;
}

metadataRpcProtocol::MetadataCommand Transition(
    std::string mutationId, const std::string& operatorId,
    metadataRpcProtocol::SchedulingOperatorPhase expected,
    metadataRpcProtocol::SchedulingOperatorPhase next, uint32_t attempts,
    bool irreversible = false) {
  metadataRpcProtocol::MetadataCommand command;
  command.set_mutationid(std::move(mutationId));
  command.set_expectedrevision(1);
  auto* update = command.mutable_updateschedulingoperator();
  update->set_operatorid(operatorId);
  update->set_expectedphase(expected);
  update->set_phase(next);
  update->set_attempts(attempts);
  update->set_irreversible(irreversible);
  update->set_updatedatms(3000 + attempts);
  return command;
}

void CheckHeartbeatAndConfiguration() {
  MetadataStateMachine state;
  Require(state.Apply(Bootstrap()).error() == metadataRpcProtocol::METADATA_OK,
          "bootstrap must succeed");

  const auto fresh = state.Apply(Heartbeat("heartbeat-4-1", 4, 1));
  Require(fresh.error() == metadataRpcProtocol::METADATA_OK && fresh.revision() == 1 &&
              fresh.heartbeat().sequence() == 1,
          "fresh heartbeat must be accepted without changing topology revision");
  const auto duplicate = state.Apply(Heartbeat("heartbeat-4-duplicate", 4, 1));
  Require(duplicate.error() == metadataRpcProtocol::METADATA_OK &&
              duplicate.heartbeat().sequence() == 1,
          "duplicate sequence must be acknowledged idempotently");

  Require(state.Apply(Heartbeat("heartbeat-1-new", 1, 2, 2)).error() ==
              metadataRpcProtocol::METADATA_OK,
          "new Region observation must be accepted");
  const auto reordered = state.Apply(Heartbeat("heartbeat-1-old-epoch", 1, 3, 1));
  Require(reordered.heartbeat().regions(0).epoch().version() == 2,
          "new heartbeat must not roll back a Region epoch");
  Require(state.Metrics().heartbeatAccepted == 3 && state.Metrics().heartbeatIgnored == 1,
          "heartbeat metrics must distinguish accepted and ignored reports");

  auto invalid = EnabledConfig(2);
  invalid.set_heartbeattimeoutms(500);
  Require(state.Apply(Configure("config-invalid", invalid)).error() ==
              metadataRpcProtocol::METADATA_INVALID_ARGUMENT,
          "invalid timing configuration must be rejected");
  const auto configured = state.Apply(Configure("config-enabled", EnabledConfig(2)));
  Require(configured.error() == metadataRpcProtocol::METADATA_OK &&
              configured.balancerconfig().enabled() &&
              state.View()->balancerConfig.version() == 2,
          "valid enabled configuration must be published");
}

void CheckOperatorLifecycleAndRecovery() {
  MetadataStateMachine state;
  Require(state.Apply(Bootstrap()).error() == metadataRpcProtocol::METADATA_OK,
          "bootstrap must succeed");
  Require(state.Apply(Heartbeat("heartbeat-target", 4, 1)).error() ==
              metadataRpcProtocol::METADATA_OK,
          "target heartbeat must be accepted");
  Require(state.Apply(Configure("config-enabled", EnabledConfig(2))).error() ==
              metadataRpcProtocol::METADATA_OK,
          "balancer must be enabled");

  auto create = MoveOperator("operator-create", "move-10-1-4");
  const auto admitted = state.Apply(create);
  Require(admitted.error() == metadataRpcProtocol::METADATA_OK &&
              admitted.operator_().phase() ==
                  metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING &&
              state.View()->activeOperatorByRegion.at(10) == "move-10-1-4",
          "safe operator must be atomically admitted");
  auto replay = MoveOperator("operator-create-after-lost-response", "move-10-1-4");
  Require(state.Apply(replay).operator_().SerializeAsString() ==
              admitted.operator_().SerializeAsString(),
          "same operator identity must survive response-loss replay");
  auto racing = MoveOperator("operator-race", "move-10-racing");
  Require(state.Apply(racing).error() == metadataRpcProtocol::METADATA_CONFLICT,
          "a second active operator for one Region must lose admission");

  MetadataStateMachine restored;
  std::string error;
  Require(restored.Restore(state.Snapshot(), &error),
          "snapshot with scheduling state must restore");
  Require(restored.View()->storeHeartbeats.count(4) == 1 &&
              restored.View()->balancerConfig.enabled() &&
              restored.LookupSchedulingOperator("move-10-1-4").has_value(),
          "snapshot must preserve heartbeat, configuration and operator state");

  Require(restored.Apply(Transition("operator-dispatch", "move-10-1-4",
                                    metadataRpcProtocol::SCHEDULING_OPERATOR_PENDING,
                                    metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING,
                                    1))
              .error() == metadataRpcProtocol::METADATA_OK,
          "pending operator must enter dispatching");
  Require(restored.Apply(Transition("operator-wait", "move-10-1-4",
                                    metadataRpcProtocol::SCHEDULING_OPERATOR_DISPATCHING,
                                    metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING,
                                    1, true))
              .operator_()
              .irreversible(),
          "irreversible milestone must be durable");

  metadataRpcProtocol::MetadataCommand lateCancel;
  lateCancel.set_mutationid("operator-late-cancel");
  lateCancel.set_expectedrevision(1);
  lateCancel.mutable_cancelschedulingoperator()->set_operatorid("move-10-1-4");
  lateCancel.mutable_cancelschedulingoperator()->set_updatedatms(4000);
  Require(restored.Apply(lateCancel).error() == metadataRpcProtocol::METADATA_CONFLICT,
          "cancellation after irreversible progress must be rejected");
  Require(restored.Apply(Transition("operator-success", "move-10-1-4",
                                    metadataRpcProtocol::SCHEDULING_OPERATOR_WAITING,
                                    metadataRpcProtocol::SCHEDULING_OPERATOR_SUCCEEDED,
                                    2, true))
              .error() == metadataRpcProtocol::METADATA_OK &&
              restored.View()->activeOperatorByRegion.empty(),
          "terminal transition must release the Region admission slot");

  auto second = MoveOperator("operator-second", "move-10-second");
  Require(restored.Apply(second).error() == metadataRpcProtocol::METADATA_OK,
          "Region must accept new work after a terminal operator");
  metadataRpcProtocol::MetadataCommand cancel;
  cancel.set_mutationid("operator-cancel-pending");
  cancel.set_expectedrevision(1);
  cancel.mutable_cancelschedulingoperator()->set_operatorid("move-10-second");
  cancel.mutable_cancelschedulingoperator()->set_updatedatms(5000);
  const auto cancelled = restored.Apply(cancel);
  Require(cancelled.error() == metadataRpcProtocol::METADATA_OK &&
              cancelled.operator_().phase() ==
                  metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED &&
              restored.View()->activeOperatorByRegion.empty(),
          "pending cancellation must be immediately terminal and release admission");
  auto repeatCancel = cancel;
  repeatCancel.set_mutationid("operator-cancel-repeat");
  Require(restored.Apply(repeatCancel).operator_().phase() ==
              metadataRpcProtocol::SCHEDULING_OPERATOR_CANCELLED,
          "cancellation replay with a new request identity must be idempotent");
}

void CheckBackwardCompatibleSnapshot() {
  const auto bootstrap = Bootstrap();
  metadataRpcProtocol::MetadataSnapshot old;
  old.set_formatversion(1);
  old.set_revision(1);
  old.set_regionidhighwater(10);
  old.set_peeridhighwater(103);
  old.set_storeidhighwater(4);
  old.set_bootstrapdigest("old-format");
  for (const auto& region : bootstrap.bootstrap().regions()) {
    *old.add_regions() = region;
    old.mutable_regions(old.regions_size() - 1)->set_metadatarevision(1);
  }
  for (const auto& store : bootstrap.bootstrap().stores()) *old.add_stores() = store;
  MetadataStateMachine restored;
  std::string bytes;
  std::string error;
  Require(old.SerializeToString(&bytes), "old snapshot must serialize");
  if (!restored.Restore(bytes, &error)) {
    throw std::runtime_error("snapshot without Auto-Balancer fields must remain restorable: " +
                             error);
  }
  Require(!restored.View()->balancerConfig.enabled() &&
              restored.View()->balancerConfig.version() == 1,
          "old snapshot must receive safe disabled defaults");
}

}  // namespace

int main() {
  try {
    CheckHeartbeatAndConfiguration();
    CheckOperatorLifecycleAndRecovery();
    CheckBackwardCompatibleSnapshot();
    std::cout << "Auto-Balancer state checks passed" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Auto-Balancer state checks failed: " << error.what() << std::endl;
    return 1;
  }
}
