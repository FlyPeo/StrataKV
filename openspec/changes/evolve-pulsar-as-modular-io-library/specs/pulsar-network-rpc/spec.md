## Purpose

提供可独立使用的 Pulsar TCP 网络接口与可选 Protobuf RPC 能力，使新应用能在协程中处理网络等待，并能与 StrataKV 现有 RPC v1 对端互通而不搬入数据库业务代码。

## ADDED Requirements

### Requirement: 显式的协程 TCP 接口
网络模块 MUST 提供连接、监听、接收、发送和关闭能力；在 Pulsar Worker 内等待 socket 就绪时 MUST 只挂起当前 Fiber，并为超时、连接关闭和部分读写给出明确结果。

#### Scenario: 多连接并发读写
- **WHEN** 多个连接中的一个等待数据而另一个已有可读数据
- **THEN** 等待中的 Fiber 不阻塞另一连接的处理

#### Scenario: 超时与对端关闭
- **WHEN** 读写在期限前未完成或对端关闭连接
- **THEN** 操作返回可区分的超时或关闭结果，等待者只完成一次

### Requirement: 有界资源与安全关闭
网络模块 MUST 限制待处理连接、缓冲数据和请求数量；停止服务时 MUST 拒绝新请求，并在关闭 fd、事件就绪和定时器到期并发发生时保证每个等待操作最多完成一次。

#### Scenario: 过载
- **WHEN** 到达配置的连接或待处理请求上限
- **THEN** 新请求被明确拒绝或施加背压，服务不会无界增加 Fiber 与缓冲占用

#### Scenario: 停止期间事件竞争
- **WHEN** 服务停止与 socket 就绪/超时同时发生
- **THEN** 操作终止或完成一次，且不会恢复已经销毁的 Fiber

### Requirement: 与业务解耦的 Protobuf RPC
RPC 扩展 MUST 接受通用 Protobuf 服务和消息，不要求或生成 StrataKV 的 Raft、KV、TSO 业务类型；同步业务处理 MUST 有有界的阻塞执行通道，不能占用所有 I/O Worker。

#### Scenario: 独立应用注册服务
- **WHEN** 一个不含 StrataKV 代码的应用注册 Protobuf 服务并发送请求
- **THEN** 服务能收到请求并返回响应，安装和链接过程不需要 StrataKV 源码

#### Scenario: 处理函数阻塞
- **WHEN** 服务处理函数进行同步等待
- **THEN** 等待在有界阻塞执行通道中发生，I/O Worker 继续处理其他连接

### Requirement: RPC v1 线协议互通
RPC 扩展 MUST 能选择与当前 StrataKV RPC v1 相同的请求和响应帧格式，并保留每条连接至多一个在途请求的语义；未知结果的部分发送 MUST 不由传输层自动重发。

#### Scenario: 新客户端调用旧服务端
- **WHEN** Pulsar RPC v1 客户端调用现有 StrataKV RpcProvider
- **THEN** 请求按旧协议被解析，响应按旧协议返回

#### Scenario: 旧客户端调用新服务端
- **WHEN** 现有 MprpcChannel 调用 Pulsar RPC v1 服务端
- **THEN** 客户端无需修改即可解析响应

#### Scenario: 部分发送后断线
- **WHEN** 请求已有部分字节发送且连接断开
- **THEN** 调用方收到结果未知的失败；传输层不自动重放可能产生重复写入的请求

### Requirement: 故障与恢复边界
RPC 扩展 MUST 对帧大小、超时、连接失效和异步响应生命周期执行有界处理；应用层 MUST 能区分传输失败与业务结果，并自行决定 leader 变化后的重试策略。

#### Scenario: 半包与非法长度
- **WHEN** 请求或响应分段到达、合并到达或声明的长度超出上限
- **THEN** 合法帧仅在完整后被分发，非法帧被拒绝且不会无界分配内存

#### Scenario: leader 变化或对端重启
- **WHEN** 请求因 leader 变化或对端重启失败
- **THEN** 连接被失效，调用方得到明确的失败结果，并可通过新的调用按业务策略重试

#### Scenario: 异步回调晚于断连
- **WHEN** 服务回调在客户端断开或服务停止后才完成
- **THEN** 响应对象被安全释放，不向失效连接发送数据或访问已销毁的服务状态
