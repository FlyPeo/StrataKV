#pragma once  // StrataKV RPC service provider.
#include <google/protobuf/descriptor.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/TcpServer.h>
#include <functional>
#include <string>
#include <unordered_map>
#include "google/protobuf/service.h"

// Protobuf service registry and Muduo-based RPC listener.
class RpcProvider {
 public:
  void NotifyService(google::protobuf::Service *service);

  void Run(int nodeIndex, short port);

 private:
  // 组合EventLoop
  muduo::net::EventLoop m_eventLoop;
  std::shared_ptr<muduo::net::TcpServer> m_muduo_server;

  // service服务类型信息
  struct ServiceInfo {
    google::protobuf::Service *m_service;                                                     // 保存服务对象
    std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> m_methodMap;  // 保存服务方法
  };
  // 存储注册成功的服务对象和其服务方法的所有信息
  std::unordered_map<std::string, ServiceInfo> m_serviceMap;

  // 新的socket连接回调
  void OnConnection(const muduo::net::TcpConnectionPtr &);
  // 已建立连接用户的读写事件回调
  void OnMessage(const muduo::net::TcpConnectionPtr &, muduo::net::Buffer *, muduo::Timestamp);
  // Closure的回调操作，用于序列化rpc的响应和网络发送
  void SendRpcResponse(muduo::net::TcpConnectionPtr, google::protobuf::Message *);

 public:
  ~RpcProvider();
};
