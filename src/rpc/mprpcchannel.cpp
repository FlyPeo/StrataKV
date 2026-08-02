#include "mprpcchannel.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "mprpccontroller.h"
#include "rpcheader.pb.h"
#include "util.h"

namespace {
// Keep a small amount of endpoint concurrency. A single connection turns all
// requests for one Region into a head-of-line queue; after a node restart one
// slow response can then consume the retry budget of every waiting request.
// Failed sockets are removed independently and reconnect attempts have no
// second time-based backoff layer, so the pool remains recoverable.
constexpr size_t kConnectionPoolSize = 4;
constexpr uint32_t kMaxRpcResponseBytes = 64 * 1024 * 1024;
constexpr std::chrono::milliseconds kConnectTimeout{500};
// The server-side consensus wait is 500 ms. Five seconds leaves ample room for
// a snapshot while preventing one dead socket from blocking a Region mutation
// queue for 30 seconds.
constexpr std::chrono::milliseconds kIoTimeout{5000};
bool SetSocketTimeouts(int fd, std::string* errMsg) {
  const timeval timeout{static_cast<time_t>(kIoTimeout.count() / 1000),
                        static_cast<suseconds_t>((kIoTimeout.count() % 1000) * 1000)};
  if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == -1 ||
      setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == -1) {
    *errMsg = "set socket timeout error! errno:" + std::to_string(errno);
    return false;
  }
  return true;
}
}

/*
header_size + service_name method_name args_size + args
*/
// 所有通过stub代理对象调用的rpc方法，都会走到这里了，
// 统一通过rpcChannel来调用方法
// 统一做rpc方法调用的数据数据序列化和网络发送
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                              google::protobuf::RpcController* controller, const google::protobuf::Message* request,
                              google::protobuf::Message* response, google::protobuf::Closure* done) {
  const google::protobuf::ServiceDescriptor* sd = method->service();
  std::string service_name = sd->name();
  std::string method_name = method->name();

  uint32_t args_size = 0;
  std::string args_str;
  if (request->SerializeToString(&args_str)) {
    args_size = args_str.size();
  } else {
    controller->SetFailed("serialize request error!");
    return;
  }

  RPC::RpcHeader rpcHeader;
  rpcHeader.set_service_name(service_name);
  rpcHeader.set_method_name(method_name);
  rpcHeader.set_args_size(args_size);

  std::string rpc_header_str;
  if (!rpcHeader.SerializeToString(&rpc_header_str)) {
    controller->SetFailed("serialize rpc header error!");
    return;
  }

  std::string send_rpc_str;
  send_rpc_str.reserve(rpc_header_str.size() + args_str.size() + 8);
  {
    google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
    google::protobuf::io::CodedOutputStream coded_output(&string_output);
    coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));
    coded_output.WriteString(rpc_header_str);
  }
  send_rpc_str += args_str;

  Connection& connection = PickConnection();
  std::lock_guard<std::mutex> lock(connection.mutex);
  if (!EnsureConnected(connection, controller)) {
    return;
  }
  if (!SendAll(connection, send_rpc_str, controller)) {
    return;
  }

  uint32_t resp_len = 0;
  int recv_size = recv(connection.fd, &resp_len, sizeof(resp_len), MSG_WAITALL);
  if (recv_size != static_cast<int>(sizeof(resp_len))) {
    char errtxt[512] = {0};
    snprintf(errtxt, sizeof(errtxt), "recv len error! recv_size:%d errno:%d", recv_size, errno);
    MarkConnectionFailure(connection);
    controller->SetFailed(errtxt);
    return;
  }

  resp_len = ntohl(resp_len);
  if (resp_len > kMaxRpcResponseBytes) {
    MarkConnectionFailure(connection);
    controller->SetFailed("rpc response too large!");
    return;
  }

  std::vector<char> recv_buf(resp_len);
  int total_read = 0;
  while (total_read < static_cast<int>(resp_len)) {
    int n = recv(connection.fd, recv_buf.data() + total_read, resp_len - total_read, 0);
    if (n <= 0) {
      break;
    }
    total_read += n;
  }

  if (total_read < static_cast<int>(resp_len)) {
    MarkConnectionFailure(connection);
    controller->SetFailed("recv body incomplete!");
    return;
  }

  if (!response->ParseFromArray(recv_buf.data(), resp_len)) {
    MarkConnectionFailure(connection);
    controller->SetFailed("parse error!");
    return;
  }

}

bool MprpcChannel::newConnect(int* clientFd, const char* ip, uint16_t port, std::string* errMsg) {
  *clientFd = -1;

  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* result = nullptr;
  const int resolve_result = getaddrinfo(ip, nullptr, &hints, &result);
  if (resolve_result != 0 || result == nullptr) {
    *errMsg = "resolve host error for " + std::string(ip) + ": " + gai_strerror(resolve_result);
    return false;
  }

  sockaddr_in server_addr = *reinterpret_cast<sockaddr_in*>(result->ai_addr);
  server_addr.sin_port = htons(port);
  freeaddrinfo(result);

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    char errtxt[512] = {0};
    snprintf(errtxt, sizeof(errtxt), "create socket error! errno:%d", errno);
    *clientFd = -1;
    *errMsg = errtxt;
    return false;
  }

  const int original_flags = fcntl(fd, F_GETFL, 0);
  if (original_flags == -1 || fcntl(fd, F_SETFL, original_flags | O_NONBLOCK) == -1) {
    char errtxt[512] = {0};
    snprintf(errtxt, sizeof(errtxt), "set nonblocking connect error! errno:%d", errno);
    close(fd);
    *errMsg = errtxt;
    return false;
  }

  int connect_result = connect(fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr));
  if (connect_result == -1 && errno == EINPROGRESS) {
    pollfd poll_fd{fd, POLLOUT, 0};
    const int poll_result = poll(&poll_fd, 1, static_cast<int>(kConnectTimeout.count()));
    if (poll_result > 0) {
      int socket_error = 0;
      socklen_t socket_error_len = sizeof(socket_error);
      if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len) == -1 || socket_error != 0) {
        errno = socket_error == 0 ? errno : socket_error;
        connect_result = -1;
      } else {
        connect_result = 0;
      }
    } else {
      if (poll_result == 0) {
        errno = ETIMEDOUT;
      }
      connect_result = -1;
    }
  }

  if (connect_result == -1) {
    const int connect_errno = errno;
    close(fd);
    char errtxt[512] = {0};
    snprintf(errtxt, sizeof(errtxt), "connect fail! errno:%d", connect_errno);
    *errMsg = errtxt;
    return false;
  }

  if (fcntl(fd, F_SETFL, original_flags) == -1) {
    char errtxt[512] = {0};
    snprintf(errtxt, sizeof(errtxt), "restore blocking mode error! errno:%d", errno);
    close(fd);
    *errMsg = errtxt;
    return false;
  }
  if (!SetSocketTimeouts(fd, errMsg)) {
    close(fd);
    return false;
  }
  *clientFd = fd;
  return true;
}

MprpcChannel::Connection& MprpcChannel::PickConnection() {
  size_t index = m_nextConnection.fetch_add(1, std::memory_order_relaxed) % m_connections.size();
  return *m_connections[index];
}

bool MprpcChannel::EnsureConnected(Connection& connection, google::protobuf::RpcController* controller) {
  const uint64_t endpointGeneration = m_connectionGeneration.load(std::memory_order_acquire);
  if (connection.generation != endpointGeneration) {
    CloseConnection(connection);
    connection.generation = endpointGeneration;
  }

  if (connection.fd != -1) {
    return true;
  }

  if (!TryStartReconnect(controller)) {
    return false;
  }

  std::string errMsg;
  if (!newConnect(&connection.fd, m_ip.c_str(), m_port, &errMsg)) {
    FinishReconnectAttempt();
    DPrintf("[func-MprpcChannel::CallMethod]连接ip：{%s} port{%d}失败", m_ip.c_str(), m_port);
    controller->SetFailed(errMsg);
    return false;
  }
  FinishReconnectAttempt();
  DPrintf("[func-MprpcChannel::CallMethod]连接ip：{%s} port{%d}成功", m_ip.c_str(), m_port);
  return true;
}

bool MprpcChannel::SendAll(Connection& connection, const std::string& data, google::protobuf::RpcController* controller) {
  size_t total_sent = 0;
  while (total_sent < data.size()) {
    ssize_t n = send(connection.fd, data.data() + total_sent, data.size() - total_sent, MSG_NOSIGNAL);
    if (n > 0) {
      total_sent += static_cast<size_t>(n);
      continue;
    }

    char errtxt[512] = {0};
    snprintf(errtxt, sizeof(errtxt), "send error! sent:%zu errno:%d", total_sent, errno);
    // A reconnect followed by resending the whole frame can duplicate a write
    // whose bytes already reached the remote peer. Return an unknown-result
    // failure and let the Raft/transaction layer apply its own retry semantics.
    MarkConnectionFailure(connection);
    controller->SetFailed(errtxt);
    return false;
  }
  return true;
}

void MprpcChannel::CloseConnection(Connection& connection) {
  if (connection.fd != -1) {
    close(connection.fd);
    connection.fd = -1;
  }
}

void MprpcChannel::MarkConnectionFailure(Connection& connection) {
  CloseConnection(connection);

  // The other sockets in this endpoint's pool were established against the
  // same process generation and may be half-open after a restart. Invalidate
  // them as one generation. A stale concurrent failure cannot invalidate a
  // connection generation that has already moved on.
  uint64_t failedGeneration = connection.generation;
  m_connectionGeneration.compare_exchange_strong(
      failedGeneration, failedGeneration + 1,
      std::memory_order_acq_rel, std::memory_order_acquire);

}

bool MprpcChannel::TryStartReconnect(google::protobuf::RpcController* controller) {
  std::lock_guard<std::mutex> lock(m_reconnectMutex);
  if (m_reconnectInProgress) {
    controller->SetFailed("rpc reconnect already in progress");
    return false;
  }
  m_reconnectInProgress = true;
  return true;
}

void MprpcChannel::FinishReconnectAttempt() {
  std::lock_guard<std::mutex> lock(m_reconnectMutex);
  m_reconnectInProgress = false;
}

MprpcChannel::MprpcChannel(std::string ip, short port, bool connectNow) : m_ip(std::move(ip)), m_port(port) {
  m_connections.reserve(kConnectionPoolSize);
  for (size_t i = 0; i < kConnectionPoolSize; ++i) {
    m_connections.emplace_back(std::make_unique<Connection>());
  }

  if (!connectNow) {
    return;
  }

  Connection& connection = *m_connections[0];
  std::lock_guard<std::mutex> lock(connection.mutex);
  connection.generation = m_connectionGeneration.load(std::memory_order_acquire);
  std::string errMsg;
  if (!newConnect(&connection.fd, m_ip.c_str(), m_port, &errMsg)) {
    MarkConnectionFailure(connection);
    std::cerr << "rpc preconnect failed for " << m_ip << ':' << m_port << ": " << errMsg << std::endl;
  }
}

MprpcChannel::~MprpcChannel() {
  for (auto& connection : m_connections) {
    std::lock_guard<std::mutex> lock(connection->mutex);
    CloseConnection(*connection);
  }
}
