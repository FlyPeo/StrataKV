#include "rpcprovider.h"
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
#include "rpcheader.pb.h"
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

/*
service_name =>  service描述
                        =》 service* 记录服务对象
                        method_name  =>  method方法对象
json   protobuf
*/
// 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
// 只是简单的把服务描述符和方法描述符全部保存在本地而已
// todo 待修改 要把本机开启的ip和端口写在文件里面
void RpcProvider::NotifyService(google::protobuf::Service *service) {
  ServiceInfo service_info;

  // 获取了服务对象的描述信息
  const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
  // 获取服务的名字
  std::string service_name = pserviceDesc->name();
  // 获取服务对象service的方法的数量
  int methodCnt = pserviceDesc->method_count();

  std::cout << "service_name:" << service_name << std::endl;

  for (int i = 0; i < methodCnt; ++i) {
    // 获取了服务对象指定下标的服务方法的描述（抽象描述） UserService   Login
    const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
    std::string method_name = pmethodDesc->name();
    service_info.m_methodMap.insert({method_name, pmethodDesc});
  }
  service_info.m_service = service;
  m_serviceMap.insert({service_name, service_info});
}

// 启动rpc服务节点，开始提供rpc远程网络调用服务
void RpcProvider::Run(int nodeIndex, short port) {
  // 绑定到所有接口，确保外部或本地都可以访问
  std::string ip = "0.0.0.0";
  
  // 写入文件 "test.conf" 的逻辑已被 raftKvDB.cpp 接管，此处为了防止冲突和数据重复，不再写入
  // std::string node = "node" + std::to_string(nodeIndex);
  // std::ofstream outfile;
  // outfile.open("test.conf", std::ios::app);  //打开文件并追加写入
  // if (!outfile.is_open()) {
  //   std::cout << "打开文件失败！" << std::endl;
  //   exit(EXIT_FAILURE);
  // }
  // outfile << node + "ip=" + ip << std::endl;
  // outfile << node + "port=" + std::to_string(port) << std::endl;
  // outfile.close();

  //创建服务器
  muduo::net::InetAddress address(ip, port);

  // 创建TcpServer对象
  m_muduo_server = std::make_shared<muduo::net::TcpServer>(&m_eventLoop, address, "RpcProvider");

  // 绑定连接回调和消息读写回调方法  分离了网络代码和业务代码
  /*
  bind的作用：
  如果不使用std::bind将回调函数和TcpConnection对象绑定起来，那么在回调函数中就无法直接访问和修改TcpConnection对象的状态。因为回调函数是作为一个独立的函数被调用的，它没有当前对象的上下文信息（即this指针），也就无法直接访问当前对象的状态。
  如果要在回调函数中访问和修改TcpConnection对象的状态，需要通过参数的形式将当前对象的指针传递进去，并且保证回调函数在当前对象的上下文环境中被调用。这种方式比较复杂，容易出错，也不便于代码的编写和维护。因此，使用std::bind将回调函数和TcpConnection对象绑定起来，可以更加方便、直观地访问和修改对象的状态，同时也可以避免一些常见的错误。
  */
  m_muduo_server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
  m_muduo_server->setMessageCallback(
      std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

  // 设置muduo库的线程数量
  m_muduo_server->setThreadNum(kRpcWorkerThreads);

  // rpc服务端准备启动，打印信息
  std::cout << "RpcProvider start service at ip:" << ip << " port:" << port << std::endl;

  // 启动网络服务
  m_muduo_server->start();
  m_eventLoop.loop();
  /*
  这段代码是在启动网络服务和事件循环，其中server是一个TcpServer对象，m_eventLoop是一个EventLoop对象。

首先调用server.start()函数启动网络服务。在Muduo库中，TcpServer类封装了底层网络操作，包括TCP连接的建立和关闭、接收客户端数据、发送数据给客户端等等。通过调用TcpServer对象的start函数，可以启动底层网络服务并监听客户端连接的到来。

接下来调用m_eventLoop.loop()函数启动事件循环。在Muduo库中，EventLoop类封装了事件循环的核心逻辑，包括定时器、IO事件、信号等等。通过调用EventLoop对象的loop函数，可以启动事件循环，等待事件的到来并处理事件。

在这段代码中，首先启动网络服务，然后进入事件循环阶段，等待并处理各种事件。网络服务和事件循环是两个相对独立的模块，它们的启动顺序和调用方式都是确定的。启动网络服务通常是在事件循环之前，因为网络服务是事件循环的基础。启动事件循环则是整个应用程序的核心，所有的事件都在事件循环中被处理。
  */
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
    google::protobuf::Closure *done =
        google::protobuf::NewCallback<RpcProvider, const muduo::net::TcpConnectionPtr &, google::protobuf::Message *>(
            this, &RpcProvider::SendRpcResponse, conn, response);

    // Consume exactly one frame before invoking user code. This leaves incomplete
    // bytes buffered and lets the loop dispatch any coalesced frames.
    buffer->retrieve(frame_size);
    service->CallMethod(method, nullptr, request.get(), response, done);
  }
}

// Closure的回调操作，用于序列化rpc的响应和网络发送,发送响应回去
void RpcProvider::SendRpcResponse(const muduo::net::TcpConnectionPtr &conn, google::protobuf::Message *response) {
  std::string response_str;
  if (response->SerializeToString(&response_str))  // response进行序列化
  {
    uint32_t len = htonl(response_str.size());
    std::string send_str;
    send_str.append(reinterpret_cast<char*>(&len), 4);
    send_str.append(response_str);
    // 序列化成功后，通过网络把rpc方法执行的结果发送会rpc的调用方
    conn->send(send_str);
  } else {
    std::cout << "serialize response_str error!" << std::endl;
  }
  //    conn->shutdown(); // 模拟http的短链接服务，由rpcprovider主动断开连接  //改为长连接，不主动断开
  
  delete response;
}

RpcProvider::~RpcProvider() {
  std::cout << "[func - RpcProvider::~RpcProvider()]: ip和port信息：" << m_muduo_server->ipPort() << std::endl;
  m_eventLoop.quit();
  //    m_muduo_server.   怎么没有stop函数，奇奇怪怪，看csdn上面的教程也没有要停止，甚至上面那个都没有
}
