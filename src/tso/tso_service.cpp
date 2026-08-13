#include "tso_service.h"

#include <exception>

void TsoService::Next(google::protobuf::RpcController*, const tsoRpcProtocol::TimestampRequest*,
                      tsoRpcProtocol::TimestampReply* response, google::protobuf::Closure* done) {
  try {
    response->set_timestamp(oracle_->Next());
  } catch (const TsoNotLeaderError& error) {
    response->set_notleader(true);
    response->set_err(error.what());
  } catch (const std::exception& error) {
    response->set_err(error.what());
  }
  done->Run();
}

void TsoService::Peek(google::protobuf::RpcController*, const tsoRpcProtocol::TimestampRequest*,
                      tsoRpcProtocol::TimestampReply* response, google::protobuf::Closure* done) {
  try {
    response->set_timestamp(oracle_->Peek());
  } catch (const TsoNotLeaderError& error) {
    response->set_notleader(true);
    response->set_err(error.what());
  } catch (const std::exception& error) {
    response->set_err(error.what());
  }
  done->Run();
}

void TsoService::Observe(google::protobuf::RpcController*, const tsoRpcProtocol::ObserveTimestampRequest* request,
                         tsoRpcProtocol::ObserveTimestampReply* response, google::protobuf::Closure* done) {
  try {
    oracle_->Observe(request->timestamp());
  } catch (const TsoNotLeaderError& error) {
    response->set_notleader(true);
    response->set_err(error.what());
  } catch (const std::exception& error) {
    response->set_err(error.what());
  }
  done->Run();
}

void TsoService::Status(google::protobuf::RpcController*, const tsoRpcProtocol::TimestampStatusRequest*,
                        tsoRpcProtocol::TimestampStatusReply* response, google::protobuf::Closure* done) {
  const Raft::NodeStatus status = oracle_->Status();
  response->set_nodeid(oracle_->NodeId());
  response->set_term(status.term);
  response->set_isleader(status.isLeader);
  response->set_highwater(oracle_->HighWater());
  done->Run();
}
