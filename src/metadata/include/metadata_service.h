#ifndef STRATAKV_METADATA_METADATA_SERVICE_H
#define STRATAKV_METADATA_METADATA_SERVICE_H

#include <memory>

#include "metadata_consensus.h"
#include "metadata_rpc.pb.h"

class MetadataService final : public metadataRpcProtocol::metadataRpc {
 public:
  explicit MetadataService(std::shared_ptr<MetadataConsensusNode> node)
      : node_(std::move(node)) {}

  void Mutate(google::protobuf::RpcController*,
              const metadataRpcProtocol::MutateRequest*,
              metadataRpcProtocol::MutateReply*,
              google::protobuf::Closure*) override;
  void LookupKey(google::protobuf::RpcController*,
                 const metadataRpcProtocol::LookupKeyRequest*,
                 metadataRpcProtocol::RegionReply*,
                 google::protobuf::Closure*) override;
  void LookupRegion(google::protobuf::RpcController*,
                    const metadataRpcProtocol::LookupRegionRequest*,
                    metadataRpcProtocol::RegionReply*,
                    google::protobuf::Closure*) override;
  void ScanRegions(google::protobuf::RpcController*,
                   const metadataRpcProtocol::ScanRegionsRequest*,
                   metadataRpcProtocol::ScanRegionsReply*,
                   google::protobuf::Closure*) override;
  void Status(google::protobuf::RpcController*,
              const metadataRpcProtocol::MetadataStatusRequest*,
              metadataRpcProtocol::MetadataStatusReply*,
              google::protobuf::Closure*) override;
  void BalancerStatus(google::protobuf::RpcController*,
                      const metadataRpcProtocol::BalancerStatusRequest*,
                      metadataRpcProtocol::BalancerStatusReply*,
                      google::protobuf::Closure*) override;

 private:
  std::shared_ptr<MetadataConsensusNode> node_;
};

#endif  // STRATAKV_METADATA_METADATA_SERVICE_H
