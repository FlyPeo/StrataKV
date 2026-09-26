## Why

Pulsar 已有独立构建、安装、Fiber、调度、epoll/Timer 与 socket Hook，但其公开定位仍是实验性协程运行时；StrataKV 的 RPC 则由独立的 Muduo 服务端和同步客户端承担。要把 Pulsar 发展为可复用的并发与 I/O 库，应先建立清晰的模块边界和可选网络/RPC 能力，同时保护现有 StrataKV 路径的功能与性能。

## What Changes

- 明确 Pulsar 第一阶段的产品定位：面向 Linux 的模块化协程与网络 I/O 库。保留现有 `Pulsar::pulsar` 构建目标、公开 API、默认选项及运行行为。
- 增加可选的网络模块，提供基于现有 IOManager 的显式 TCP 客户端/服务端接口；现有 socket Hook 路径保持可用。
- 增加可选的 Protobuf RPC 模块，复用通用帧格式与服务分发概念，但不引入 StrataKV 的 Raft/KV/TSO 消息或业务策略；提供与当前 StrataKV RPC v1 线协议的互通验证。
- 将新增模块单独导出、安装和测试；StrataKV 的默认 RPC 提供者、同步 Channel、线程池、SDK、Raft 与事务执行路径维持现状。新模块仅供独立示例和显式选择的测试使用。
- 记录能力边界、来源与许可核查状态，发布前完成许可核对；建立同机、同配置、可复跑的功能及性能门槛。

本变更不包含 io_uring、文件系统、HTTP/TLS、跨平台移植、StrataKV 生产 RPC 切换，也不宣称 Pulsar 已具备 PhotonLibOS 的完整功能或生产成熟度。没有计划中的破坏性 API 变更。

## Capabilities

### New Capabilities

- `pulsar-modular-distribution`: 独立消费、可选模块、依赖隔离、旧目标兼容和默认路径回归门槛。
- `pulsar-network-rpc`: 基于 Pulsar 的显式网络接口、可选 Protobuf RPC，以及现有 RPC v1 协议互通。

### Modified Capabilities

无。现有 StrataKV 功能的要求和默认行为不变。

## Impact

- **涉及范围**：`src/pulsar` 独立子模块、新增模块的构建/示例/测试，以及可选的 StrataKV 互通测试。现有 `src/rpc`、服务入口和业务 `.proto` 不迁移。
- **兼容性**：默认构建和链接仍使用 `Pulsar::pulsar` 与 `stratakv_rpc`；RPC v1 请求/响应字节格式保持兼容。新模块不得在未显式链接时改变现有二进制的运行路径。
- **正确性风险**：fd 关闭与事件就绪/超时竞态、Fiber 重复恢复、半包/粘包、异步响应对象生命周期，以及同步业务处理占满 Worker。设计与测试须逐项覆盖；同步业务处理继续使用有界 pthread 池。
- **性能预期与门槛**：默认路径没有新增热路径或后台线程。实施前冻结当前代码与测试环境的基线；任何不得不触及现有核心的改动必须通过同机 A/B，覆盖 Fiber 切换、调度、Hook echo 和 StrataKV 代表性 RPC/事务负载，不能仅凭跨机器历史数字判断“零回退”。
- **回滚**：不链接或关闭可选模块即恢复原有构建和 RPC 路径；StrataKV 生产流量不在本变更中切换。
