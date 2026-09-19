# Tasks: add-client-sdk-parity

## 1. 客户端命令面

- [x] 1.1 重写 `src/client/client_main.cpp` 命令分发:事务会话状态机(begin/commit/rollback、会话内 put/del 暂存与 read-your-writes get、EOF 自动回滚)、锁操作(get-for-update/batch-get-for-update/lock-keys,要求会话)、`multi put/del` 一次性原子写、`scan <start> <end> [limit]`、`txn-status`、`metrics`;帮助文本与错误提示同步。验证:构建通过;对运行中集群管道式验证:跨 Region multi 原子提交、会话暂存+read-your-writes+rollback 丢弃、get-for-update 会话内外行为、scan limit、metrics/txn-status 输出、无会话 commit 报错。

## 2. 版本与发布

- [x] 2.1 版本升至 1.1.0,QUICKSTART 命令表更新,完整打包(验收门在新客户端上回归)+ 源码树 up/verify 回归。验证:打包 rc=0 且验收门通过;两次打包幂等;`openspec validate add-client-sdk-parity` 通过;git status 无无关脏文件。
