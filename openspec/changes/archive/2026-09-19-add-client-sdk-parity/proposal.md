# Proposal: add-client-sdk-parity

## Why

`stratakv-client` 目前只暴露单键 get/put/del/list,SDK 的核心能力(多键跨分片原子写、悲观锁读、批量锁、事务状态查询、客户端指标)在客户端上无法使用,用户必须写 C++ 才能表达完整业务操作,产品能力不对齐。

## What Changes

- 客户端新增**事务会话**:`begin [lockTtlMs]` 开启,`put/del/get` 在会话内暂存/读己之写,`commit`/`rollback` 收尾;退出时未提交事务自动回滚并提示。
- 客户端新增**锁操作命令**:`get-for-update <k>`、`batch-get-for-update <k>...`、`lock-keys <k>...`,要求处于会话中(避免锁泄漏到 TTL)。
- 客户端新增 **`multi` 一次性命令**:`multi put k1 v1 [k2 v2 ...]`、`multi del k1 [k2 ...]`,一个事务跨分片原子提交(语法糖,自动重试)。
- 客户端新增 **`scan <start> <end> [limit]`**(原始范围)、**`txn-status`**(当前事务状态查询)、**`metrics`**(客户端指标)。
- 发布包版本升至 **1.1.0**;QUICKSTART 命令文档同步。

**Non-Goals**:不在客户端增加脚本/编程能力;不改变 SDK 与服务端行为;`list` 语义保持不变。

## Capabilities

### New Capabilities

(无)

### Modified Capabilities

- `client-tool`:命令面扩展至 SDK 全量能力;新增事务会话与锁操作的行为契约。

## Impact

- 仅 `src/client/client_main.cpp`(会话状态机与命令分发)、`deploy/QUICKSTART.md`、根 CMakeLists 版本号。
- 无协议/服务端/SDK 变更;回滚 = revert 单文件与文档,版本号回落。
