# Design: add-client-sdk-parity

## Context

SDK 门面(client.h)已提供全部能力;客户端是纯命令分发层,无服务端/协议改动。

## Decisions

### D1. 会话模型:单活动事务 + 状态机

`std::shared_ptr<stratakv::Transaction> session_` 非空即会话中。`begin` 在已有会话时报错;`commit/rollback` 结束会话。会话内 `put/del` 暂存(SDK 语义),`get` 走 `Client::Get`(SDK 自带 read-your-writes:先查本地暂存)。会话内 `scan/list` 使用会话事务的快照(暂存写不体现在 list 中,帮助文本注明)。EOF/quit 时活动事务自动 rollback 并打印提示,避免锁滞留到 TTL。

### D2. 锁操作要求会话

`get-for-update/batch-get-for-update/lock-keys` 操作会话事务;无会话时拒绝并提示 `begin`。理由:这些操作会获取悲观锁,自动提交式的"一次性"使用会在出错时把锁留到 TTL;会话边界让锁生命周期显式。

### D3. multi = 自动提交糖

`multi put/del` 开临时事务、全部暂存、Commit 复用既有 RunWithRetry(冲突退避重试);参数校验失败(put 奇数参数/无键)在开事务前拒绝。与 `begin` 会话互不影响(要求无活动会话时才可用,避免嵌套歧义)。

### D4. 版本 1.1.0

`project(StrataKV VERSION 1.1.0)`;打包脚本自动取版本号,产物名随之变为 `stratakv-1.1.0-linux-x64.tar.gz`,旧 1.0.0 包不受影响。

## Risks / Trade-offs

- [会话内 list 不含暂存写,与 get 行为不一致] → 帮助与 QUICKSTART 明示;实现 read-your-writes 的范围合并超出本次范围。
- [begin 后忘记 commit 靠退出回滚兜底] → 退出自动回滚 + 提示;不引入会话超时(锁本身有 TTL)。

## Migration Plan

单文件改动 + 文档;回滚 = revert。

## Open Questions

(无)
