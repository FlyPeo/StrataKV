# Design: make-balancer-default-on

## Context

默认值唯一来源是 `DefaultAutoBalancerConfig()`(metadata_state_machine.cpp:63);`balancerConfig_` 随元数据快照持久化(保存于 :1063,恢复于 :1139),因此改默认值只作用于"首次引导、无既有配置"的新集群。

## Decisions

### D1. 一行改默认,不改持久化路径

`config.set_enabled(false) → true`。存量集群恢复快照时覆盖默认值,天然满足"保持显式选择";无需迁移逻辑。

### D2. 测试断言翻转而非删除

`auto_balancer_state_unit_check.cpp` 中"默认禁用"断言翻转为"默认开启",保留"显式配置覆盖默认"与"快照恢复保持选择"的断言——它们现在恰好验证新语义的存量兼容。

## Risks / Trade-offs

- [新集群在无人知晓的情况下开始自动调度] → 默认参数保守(512MiB 分裂阈值、operator 并发 1、冷却 60s);QUICKSTART/deploy README 明示默认开启与关闭命令。
- [升级误解为"被自动打开了"] → 持久化保证存量不变;发布说明写明。

## Migration Plan

代码+测试+文档+版本 1.2.0;回滚 revert 单 commit。

## Open Questions
(无)
