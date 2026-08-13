#ifndef STRATAKV_TSO_TSO_SERVICE_H
#define STRATAKV_TSO_TSO_SERVICE_H

#include <memory>
#include <utility>

#include "tso_consensus.h"
#include "tso_rpc.pb.h"

class TsoService final : public tsoRpcProtocol::timestampOracleRpc {
 public:
  explicit TsoService(std::shared_ptr<TsoConsensusNode> oracle) : oracle_(std::move(oracle)) {}

  void Next(google::protobuf::RpcController*, const tsoRpcProtocol::TimestampRequest*,
            tsoRpcProtocol::TimestampReply*, google::protobuf::Closure*) override;
  void Peek(google::protobuf::RpcController*, const tsoRpcProtocol::TimestampRequest*,
            tsoRpcProtocol::TimestampReply*, google::protobuf::Closure*) override;
  void Observe(google::protobuf::RpcController*, const tsoRpcProtocol::ObserveTimestampRequest*,
               tsoRpcProtocol::ObserveTimestampReply*, google::protobuf::Closure*) override;
  void Status(google::protobuf::RpcController*, const tsoRpcProtocol::TimestampStatusRequest*,
              tsoRpcProtocol::TimestampStatusReply*, google::protobuf::Closure*) override;

 private:
  std::shared_ptr<TsoConsensusNode> oracle_;
};

#endif  // STRATAKV_TSO_TSO_SERVICE_H
