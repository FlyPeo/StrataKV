# Proposal: make-balancer-default-on

## Why

自动调度器(自动分裂/副本修复/均衡)默认禁用是初版保守取舍;内部单机部署场景下,每次新集群都要手动 `balancer-enable`,而忘记开启的集群得不到自动管理。产品定位明确为"默认自动管理,需要时手动暂停"。

## What Changes

- `DefaultAutoBalancerConfig()` 的 `enabled` 默认值 `false → true`:**仅影响新集群首次引导**;存量集群的配置随快照持久化,保持各自显式设置,不受影响。
- leader-only、幂等重放、不可逆点保护、暂停/禁用命令等运行时安全语义全部不变;`balancer-disable/pause` 仍是运维退出通道。
- spec `automatic-region-scheduling` 的"opt-in"需求改为"默认开启、可显式禁用"。
- 版本 1.2.0;deploy/README 与 QUICKSTART 中"默认关闭"表述同步。

**Non-Goals**:不改评估窗口/阈值等默认参数;不改静态模式行为(静态拓扑仍无调度器)。

## Capabilities

### New Capabilities
(无)

### Modified Capabilities
- `automatic-region-scheduling`:默认状态从 disabled 改为 enabled(存量集群不受影响的持久化语义 + 显式禁用场景)。

## Impact

- 代码:`src/metadata/metadata_state_machine.cpp` 一行默认值;测试:`test/metadata/auto_balancer_state_unit_check.cpp` 默认禁用断言翻转;文档:deploy/README、QUICKSTART;版本号。
- 兼容性:存量动态集群零影响(快照持久化);新集群从引导起自动调度,风险=自动分裂阈值 512MiB 与 operator 并发上限 1 不变,行为可预期。
- 回滚:revert 单 commit 即回到默认禁用;已引导集群的 enabled 配置可随时 `balancer-disable`。
