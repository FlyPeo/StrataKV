#include "metadata_service.h"

#include <algorithm>
#include <chrono>
#include <limits>

namespace {

std::chrono::steady_clock::time_point Deadline(uint64_t timeoutMs) {
  constexpr uint64_t kDefaultTimeoutMs = 3000;
  const uint64_t bounded =
      std::min<uint64_t>(timeoutMs == 0 ? kDefaultTimeoutMs : timeoutMs,
                         static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
  return std::chrono::steady_clock::now() + std::chrono::milliseconds(bounded);
}

template <typename Reply>
void SetReadError(Reply* reply, const std::exception& error,
                  metadataRpcProtocol::MetadataErrorCode code, int leaderId) {
  reply->set_error(code);
  reply->set_message(error.what());
  reply->set_leaderid(leaderId);
}

}  // namespace

void MetadataService::Mutate(google::protobuf::RpcController*,
                             const metadataRpcProtocol::MutateRequest* request,
                             metadataRpcProtocol::MutateReply* response,
                             google::protobuf::Closure* done) {
  try {
    *response->mutable_result() = node_->Mutate(request->command(), Deadline(request->timeoutms()));
  } catch (const MetadataNotLeaderError& error) {
    response->mutable_result()->set_error(metadataRpcProtocol::METADATA_NOT_LEADER);
    response->mutable_result()->set_message(error.what());
    response->mutable_result()->set_revision(node_->LocalView()->revision);
  } catch (const std::exception& error) {
    response->mutable_result()->set_error(metadataRpcProtocol::METADATA_UNAVAILABLE);
    response->mutable_result()->set_message(error.what());
    response->mutable_result()->set_revision(node_->LocalView()->revision);
  }
  response->set_leaderid(node_->Status().isLeader ? node_->NodeId() : -1);
  done->Run();
}

void MetadataService::LookupKey(google::protobuf::RpcController*,
                                const metadataRpcProtocol::LookupKeyRequest* request,
                                metadataRpcProtocol::RegionReply* response,
                                google::protobuf::Closure* done) {
  try {
    const auto view = node_->LinearizableView(Deadline(request->timeoutms()));
    response->set_revision(view->revision);
    if (!view->catalog) {
      response->set_error(metadataRpcProtocol::METADATA_NOT_FOUND);
      response->set_message("metadata cluster is not bootstrapped");
    } else {
      try {
        *response->mutable_region() = ToProtoRegion(view->catalog->FindByKey(request->key()));
        response->mutable_region()->set_metadatarevision(view->revision);
        response->set_error(metadataRpcProtocol::METADATA_OK);
      } catch (const std::out_of_range&) {
        response->set_error(metadataRpcProtocol::METADATA_NOT_FOUND);
        response->set_message("key is outside the configured keyspace");
      }
    }
  } catch (const MetadataNotLeaderError& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_NOT_LEADER, -1);
  } catch (const MetadataTimeoutError& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_TIMEOUT, -1);
  } catch (const std::exception& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_UNAVAILABLE, -1);
  }
  if (response->leaderid() == 0 && !node_->Status().isLeader) response->set_leaderid(-1);
  done->Run();
}

void MetadataService::LookupRegion(google::protobuf::RpcController*,
                                   const metadataRpcProtocol::LookupRegionRequest* request,
                                   metadataRpcProtocol::RegionReply* response,
                                   google::protobuf::Closure* done) {
  try {
    const auto view = node_->LinearizableView(Deadline(request->timeoutms()));
    response->set_revision(view->revision);
    if (!view->catalog ||
        request->regionid() > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
      response->set_error(metadataRpcProtocol::METADATA_NOT_FOUND);
    } else {
      try {
        *response->mutable_region() =
            ToProtoRegion(view->catalog->FindById(static_cast<int>(request->regionid())));
        response->mutable_region()->set_metadatarevision(view->revision);
        response->set_error(metadataRpcProtocol::METADATA_OK);
      } catch (const std::out_of_range&) {
        response->set_error(metadataRpcProtocol::METADATA_NOT_FOUND);
        response->set_message("Region does not exist");
      }
    }
  } catch (const MetadataNotLeaderError& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_NOT_LEADER, -1);
  } catch (const MetadataTimeoutError& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_TIMEOUT, -1);
  } catch (const std::exception& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_UNAVAILABLE, -1);
  }
  done->Run();
}

void MetadataService::ScanRegions(google::protobuf::RpcController*,
                                  const metadataRpcProtocol::ScanRegionsRequest* request,
                                  metadataRpcProtocol::ScanRegionsReply* response,
                                  google::protobuf::Closure* done) {
  try {
    const auto view = node_->LinearizableView(Deadline(request->timeoutms()));
    response->set_revision(view->revision);
    if (!view->catalog) {
      response->set_error(metadataRpcProtocol::METADATA_NOT_FOUND);
      response->set_message("metadata cluster is not bootstrapped");
    } else {
      const auto& regions = view->catalog->Regions();
      auto found = std::find_if(regions.begin(), regions.end(), [&](const RegionMetadata& region) {
        return region.Contains(request->startkey()) ||
               !RegionBytewiseLess(region.startKey, request->startkey());
      });
      const size_t limit = request->limit() == 0 ? 256 : request->limit();
      for (; found != regions.end() &&
             static_cast<size_t>(response->regions_size()) < limit; ++found) {
        auto* descriptor = response->add_regions();
        *descriptor = ToProtoRegion(*found);
        descriptor->set_metadatarevision(view->revision);
      }
      response->set_error(metadataRpcProtocol::METADATA_OK);
    }
  } catch (const MetadataNotLeaderError& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_NOT_LEADER, -1);
  } catch (const MetadataTimeoutError& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_TIMEOUT, -1);
  } catch (const std::exception& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_UNAVAILABLE, -1);
  }
  done->Run();
}

void MetadataService::Status(google::protobuf::RpcController*,
                             const metadataRpcProtocol::MetadataStatusRequest*,
                             metadataRpcProtocol::MetadataStatusReply* response,
                             google::protobuf::Closure* done) {
  const Raft::NodeStatus status = node_->Status();
  const auto view = node_->LocalView();
  const MetadataStateMetrics metrics = node_->StateMetrics();
  response->set_nodeid(node_->NodeId());
  response->set_term(status.term);
  response->set_isleader(status.isLeader);
  response->set_revision(view->revision);
  response->set_commitindex(status.commitIndex);
  response->set_appliedindex(status.lastApplied);
  response->set_regioncount(view->catalog ? view->catalog->Regions().size() : 0);
  response->set_storecount(view->stores.size());
  response->set_validationrejects(metrics.validationRejects);
  response->set_deduphits(metrics.dedupHits);
  response->set_snapshotcount(metrics.snapshotCount);
  response->set_heartbeataccepted(metrics.heartbeatAccepted);
  response->set_heartbeatignored(metrics.heartbeatIgnored);
  response->set_operatoradmissions(metrics.operatorAdmissions);
  response->set_activeoperatorcount(view->activeOperatorByRegion.size());
  response->set_balancerenabled(view->balancerConfig.enabled());
  response->set_balancerpaused(view->balancerConfig.paused());
  done->Run();
}

void MetadataService::BalancerStatus(
    google::protobuf::RpcController*,
    const metadataRpcProtocol::BalancerStatusRequest* request,
    metadataRpcProtocol::BalancerStatusReply* response,
    google::protobuf::Closure* done) {
  try {
    const auto view = node_->LinearizableView(Deadline(request->timeoutms()));
    response->set_error(metadataRpcProtocol::METADATA_OK);
    response->set_revision(view->revision);
    response->set_leaderid(node_->NodeId());
    *response->mutable_config() = view->balancerConfig;
    std::vector<uint64_t> storeIds;
    storeIds.reserve(view->storeHeartbeats.size());
    for (const auto& item : view->storeHeartbeats) storeIds.push_back(item.first);
    std::sort(storeIds.begin(), storeIds.end());
    for (uint64_t storeId : storeIds) {
      *response->add_storeheartbeats() = view->storeHeartbeats.at(storeId);
    }
    std::vector<std::string> operatorIds;
    operatorIds.reserve(view->schedulingOperators.size());
    for (const auto& item : view->schedulingOperators) operatorIds.push_back(item.first);
    std::sort(operatorIds.begin(), operatorIds.end());
    for (const auto& operatorId : operatorIds) {
      *response->add_operators() = view->schedulingOperators.at(operatorId);
    }
  } catch (const MetadataNotLeaderError& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_NOT_LEADER, -1);
  } catch (const MetadataTimeoutError& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_TIMEOUT, -1);
  } catch (const std::exception& error) {
    SetReadError(response, error, metadataRpcProtocol::METADATA_UNAVAILABLE, -1);
  }
  done->Run();
}
