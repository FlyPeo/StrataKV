## Purpose

让 Pulsar 作为独立安装的 Linux 并发与网络 I/O 库供其他工程选用，同时保证现有 Pulsar 核心调用者和 StrataKV 默认构建不因新增模块而改变运行行为。

## ADDED Requirements

### Requirement: 兼容的独立核心库
Pulsar MUST 保持现有 `Pulsar::pulsar` 目标、公开头文件、默认构建选项和 Fiber/Scheduler/IOManager/Hook 的可观察行为，并支持独立构建、安装和下游 `find_package` 使用。

#### Scenario: 旧使用者继续构建
- **WHEN** 一个现有项目只链接 `Pulsar::pulsar` 并按既有方式创建 Fiber、调度任务和等待 socket
- **THEN** 该项目无需修改源码即可构建，运行结果与新增模块前一致

### Requirement: 可选模块与依赖隔离
网络和 RPC 扩展 MUST 通过独立目标显式启用；未启用时核心库 MUST 不要求 Protobuf、Muduo 或 StrataKV 源码、消息定义及配置。

#### Scenario: 仅安装核心库
- **WHEN** 下游只配置和安装 Pulsar 核心目标
- **THEN** 安装成功且不拉入网络/RPC 扩展的可选依赖

#### Scenario: 显式使用 RPC 模块
- **WHEN** 下游显式启用并链接 RPC 扩展
- **THEN** 构建系统只为该扩展声明和检查其所需依赖，核心目标保持可独立使用

### Requirement: StrataKV 默认路径不变
本阶段的 StrataKV 默认构建 MUST 继续使用当前 `stratakv_rpc`、Muduo RpcProvider、同步 Channel 与既有 RPC 线程池；新增 Pulsar 模块 MUST 不在默认请求路径启动后台线程、接管端口或增加热路径操作。

#### Scenario: 默认启动节点
- **WHEN** 未选择 Pulsar 网络/RPC 实验目标而启动 StrataKV 节点
- **THEN** 监听端口、服务分发、客户端调用和线程所有权与当前版本一致

### Requirement: 可复核的回归门槛
合入前 MUST 固定基线代码版本、机器、编译配置和负载，并对已有正确性测试以及 Fiber 切换、调度、Hook echo 和 StrataKV 代表性 RPC/事务基准执行同机前后对照；可重复的功能失败或性能回退 MUST 阻止合入。

#### Scenario: 检测到回退
- **WHEN** 相同环境的重复测试显示现有路径有可重复的错误或性能退化
- **THEN** 变更不得作为“无回退”发布，并须修复或回滚后重测

#### Scenario: 发布能力说明
- **WHEN** 用户阅读 Pulsar 的安装与能力文档
- **THEN** 文档明确区分已验证能力、可选实验模块、平台限制和待核对的来源/许可状态
