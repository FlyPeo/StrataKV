#include "rpc_provider.h"
#include <arpa/inet.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <fstream>
#include <string>
#include <rpc_header.pb.h>
#include "util.h"

namespace {
constexpr uint32_t kMaxRpcHeaderBytes = 64 * 1024;
constexpr uint32_t kMaxRpcRequestBytes = 64 * 1024 * 1024;
// Raft and the transaction service currently share one listener. Transaction
// handlers synchronously wait for a Raft apply result, so the worker pool must
// retain capacity for AppendEntries/RequestVote while client requests are
// waiting. The reliability workload uses 16 concurrent transaction requests,
// so matching the pool to 16 can still occupy every worker with handlers that
// are waiting for Raft. Keep another 16 workers available for consensus RPCs.
constexpr int kRpcWorkerThreads = 32;

enum class VarintParseResult {
  Complete,
  Incomplete,
  Invalid,
};

VarintParseResult DecodeVarint32(const char* data, size_t size, uint32_t* value, size_t* bytes_consumed) {
  uint32_t result = 0;
  const size_t limit = std::min(size, static_cast<size_t>(5));
  for (size_t i = 0; i < limit; ++i) {
    const uint8_t byte = static_cast<uint8_t>(data[i]);
    if (i == 4 && ((byte & 0x80) != 0 || (byte & 0xF0) != 0)) {
      return VarintParseResult::Invalid;
    }

    result |= static_cast<uint32_t>(byte & 0x7F) << (7 * i);
    if ((byte & 0x80) == 0) {
      *value = result;
      *bytes_consumed = i + 1;
      return VarintParseResult::Complete;
    }
  }
  return size >= 5 ? VarintParseResult::Invalid : VarintParseResult::Incomplete;
}
}  // namespace

// Register a Protobuf service and cache descriptors for request dispatch.
void RpcProvider::NotifyService(google::protobuf::Service *service) {
  ServiceInfo service_info;

  const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
  std::string service_name = pserviceDesc->name();
  int methodCnt = pserviceDesc->method_count();

  std::cout << "service_name:" << service_name << std::endl;

  for (int i = 0; i < methodCnt; ++i) {
    const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
    std::string method_name = pmethodDesc->name();
    service_info.m_methodMap.insert({method_name, pmethodDesc});
  }
  service_info.m_service = service;
  m_serviceMap.insert({service_name, service_info});
}

// Start the RPC listener and enter the owning Muduo event loop.
void RpcProvider::Run(int nodeIndex, short port) {
  (void)nodeIndex;
  std::string ip = "0.0.0.0";
  muduo::net::InetAddress address(ip, port);
  m_muduo_server = std::make_shared<muduo::net::TcpServer>(&m_eventLoop, address, "RpcProvider");

  m_muduo_server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
  m_muduo_server->setMessageCallback(
      std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

  m_muduo_server->setThreadNum(kRpcWorkerThreads);

  std::cout << "RpcProvider start service at ip:" << ip << " port:" << port << std::endl;

  m_muduo_server->start();
  m_eventLoop.loop();
}

// 新的socket连接回调
void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr &conn) {
  // 如果是新连接就什么都不干，即正常的接收连接即可
  if (!conn->connected()) {
    // 和rpc client的连接断开了
    conn->shutdown();
  }
}

// Request wire format:
// [varint32 header_size][serialized RpcHeader][serialized request arguments].
void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buffer, muduo::Timestamp) {
  while (true) {
    const char *frame = buffer->peek();
    const size_t readable = buffer->readableBytes();

    uint32_t header_size = 0;
    size_t header_prefix_size = 0;
    const VarintParseResult prefix_result = DecodeVarint32(frame, readable, &header_size, &header_prefix_size);
    if (prefix_result == VarintParseResult::Incomplete) {
      return;
    }
    if (prefix_result == VarintParseResult::Invalid || header_size == 0 || header_size > kMaxRpcHeaderBytes) {
      std::cerr << "invalid rpc header length" << std::endl;
      buffer->retrieveAll();
      conn->shutdown();
      return;
    }

    const size_t header_end = header_prefix_size + header_size;
    if (readable < header_end) {
      return;
    }

    RPC::RpcHeader rpc_header;
    if (!rpc_header.ParseFromArray(frame + header_prefix_size, header_size)) {
      std::cerr << "rpc header parse error" << std::endl;
      buffer->retrieveAll();
      conn->shutdown();
      return;
    }

    const uint32_t args_size = rpc_header.args_size();
    if (args_size > kMaxRpcRequestBytes || args_size > kMaxRpcRequestBytes - header_end) {
      std::cerr << "rpc request too large" << std::endl;
      buffer->retrieveAll();
      conn->shutdown();
      return;
    }

    const size_t frame_size = header_end + args_size;
    if (readable < frame_size) {
      return;
    }

    const std::string service_name = rpc_header.service_name();
    const std::string method_name = rpc_header.method_name();
    const char *args_data = frame + header_end;

    auto service_it = m_serviceMap.find(service_name);
    if (service_it == m_serviceMap.end()) {
      std::cerr << "rpc service not found: " << service_name << std::endl;
      buffer->retrieve(frame_size);
      continue;
    }

    auto method_it = service_it->second.m_methodMap.find(method_name);
    if (method_it == service_it->second.m_methodMap.end()) {
      std::cerr << "rpc method not found: " << service_name << "." << method_name << std::endl;
      buffer->retrieve(frame_size);
      continue;
    }

    google::protobuf::Service *service = service_it->second.m_service;
    const google::protobuf::MethodDescriptor *method = method_it->second;
    std::unique_ptr<google::protobuf::Message> request(service->GetRequestPrototype(method).New());
    if (!request->ParseFromArray(args_data, args_size)) {
      std::cerr << "rpc request parse error" << std::endl;
      buffer->retrieve(frame_size);
      continue;
    }

    google::protobuf::Message *response = service->GetResponsePrototype(method).New();
    // The callback can outlive OnMessage because transaction writes complete
    // asynchronously after Raft apply. Store a TcpConnectionPtr by value in
    // the closure; storing `const TcpConnectionPtr&` leaves a dangling
    // reference as soon as OnMessage returns.
    google::protobuf::Closure *done =
        google::protobuf::NewCallback<RpcProvider, muduo::net::TcpConnectionPtr, google::protobuf::Message *>(
            this, &RpcProvider::SendRpcResponse, conn, response);

    // Consume exactly one frame before invoking user code. This leaves incomplete
    // bytes buffered and lets the loop dispatch any coalesced frames.
    buffer->retrieve(frame_size);
    service->CallMethod(method, nullptr, request.get(), response, done);
  }
}

// Closure的回调操作，用于序列化rpc的响应和网络发送,发送响应回去
void RpcProvider::SendRpcResponse(muduo::net::TcpConnectionPtr conn, google::protobuf::Message *response) {
  std::string response_str;
  if (response->SerializeToString(&response_str))  // response进行序列化
  {
    uint32_t len = htonl(response_str.size());
    std::string send_str;
    send_str.append(reinterpret_cast<char*>(&len), 4);
    send_str.append(response_str);
    // 序列化成功后，通过网络把rpc方法执行的结果发送会rpc的调用方
    // Transaction callbacks may now run on the node scheduler or a Region
    // apply thread. Marshal the response back to the connection's EventLoop
    // and keep both the connection and frame alive until that loop sends it.
    // The bundled Muduo cross-thread path captures a raw TcpConnection pointer,
    // which is unsafe when an RPC timeout closes the socket concurrently.
    conn->getLoop()->runInLoop([conn, frame = std::move(send_str)]() {
      if (conn->connected()) conn->send(frame);
    });
  } else {
    std::cout << "serialize response_str error!" << std::endl;
  }
  delete response;
}

RpcProvider::~RpcProvider() {
  std::cout << "[func - RpcProvider::~RpcProvider()]: ip和port信息：" << m_muduo_server->ipPort() << std::endl;
  m_eventLoop.quit();
}
