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
  response->set_protocolversion(TsoConsensusNode::kProtocolVersion);
  response->set_hybridlogical(true);
  response->set_rangeproposalcount(oracle_->RangeProposalCount());
  response->set_rangereservationcount(oracle_->RangeReservationCount());
  response->set_timestampallocationcount(oracle_->TimestampAllocationCount());
  response->set_rangesize(oracle_->RangeSize());
  response->set_nexttimestamp(oracle_->NextTimestamp());
  response->set_activerangehighwater(oracle_->ActiveRangeHighWater());
  response->set_fencevalid(oracle_->HasValidFence());
  const uint64_t reservations = oracle_->RangeReservationCount();
  response->set_averagetimestampsperreservation(
      reservations == 0 ? 0.0
                        : static_cast<double>(oracle_->TimestampAllocationCount()) /
                              static_cast<double>(reservations));
  done->Run();
}
