#ifndef STRATAKV_RPC_MPRPC_CHANNEL_H
#define STRATAKV_RPC_MPRPC_CHANNEL_H

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// 真正负责发送和接受的前后处理工作
//  如消息的组织方式，向哪个节点发送等等
class MprpcChannel : public google::protobuf::RpcChannel {
 public:
  // 所有通过stub代理对象调用的rpc方法，都走到这里了，统一做rpc方法调用的数据数据序列化和网络发送 那一步
  void CallMethod(const google::protobuf::MethodDescriptor *method, google::protobuf::RpcController *controller,
                  const google::protobuf::Message *request, google::protobuf::Message *response,
                  google::protobuf::Closure *done) override;
  MprpcChannel(std::string ip, short port, bool connectNow);
  ~MprpcChannel() override;

 private:
  struct Connection {
    std::mutex mutex;
    int fd = -1;
    uint64_t generation = 0;
  };

  std::vector<std::unique_ptr<Connection>> m_connections;
  std::atomic<size_t> m_nextConnection{0};
  // One transport failure invalidates the complete endpoint generation. Other
  // pool entries notice this lazily while holding their own mutex, avoiding
  // cross-connection lock ordering and stale sockets after a node restart.
  std::atomic<uint64_t> m_connectionGeneration{1};
  // Shared by every pooled connection to the same endpoint. It serializes the
  // short connect handshake only; retry timing belongs to the Raft/MVCC layer.
  std::mutex m_reconnectMutex;
  bool m_reconnectInProgress = false;
  const std::string m_ip;  // 保存ip和端口，如果断了可以尝试重连
  const uint16_t m_port;
  /// @brief 连接ip和端口,并设置m_clientFd
  /// @param ip ip地址，本机字节序
  /// @param port 端口，本机字节序
  /// @return 成功返回空字符串，否则返回失败信息
  bool newConnect(int *clientFd, const char *ip, uint16_t port, std::string *errMsg);
  Connection &PickConnection();
  bool EnsureConnected(Connection &connection, google::protobuf::RpcController *controller);
  bool SendAll(Connection &connection, const std::string &data, google::protobuf::RpcController *controller);
  void CloseConnection(Connection &connection);
  void MarkConnectionFailure(Connection &connection);
  bool TryStartReconnect(google::protobuf::RpcController *controller);
  void FinishReconnectAttempt();
};

#endif  // STRATAKV_RPC_MPRPC_CHANNEL_H
