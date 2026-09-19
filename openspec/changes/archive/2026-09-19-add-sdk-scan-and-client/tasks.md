# Tasks: add-sdk-scan-and-client

## 1. 范围扫描存储与传输链

- [x] 1.1 proto 与 node 侧:`kv_server_rpc.proto` 新增 `TxnScanArgs/TxnScanReply` 与 `rpc TxnScan`;`mvcc_storage.{h,cpp}` 基类实现快照范围读(逐键复用 Get 可见性规则,limit 截断,冲突即整批 LockConflict);`raft/region_peer.{h,cpp}` 新增 `TxnScan`(OwnsKey 起点校验、范围内裁剪、LinearizableReadBarrier、IsLeaderInTerm 复核);`node_server.cpp` 增加 `DISPATCH_KV(TxnScan,...)` 与 `RequestKeys(TxnScanArgs)`。验证:Release 构建(含 proto 代码生成)通过;新增 `test/sdk/sdk_scan_unit_check.cpp` 用进程内 `MvccStorage` 覆盖:跨"分片"有序聚合、limit 截断+续读无缝、快照后提交不可见、锁冲突整批可重试、删除键不出现;单测通过。

- [x] 1.2 SDK 侧:`raft_mvcc_storage.{h,cpp}` 远程桩 `Scan`(镜像 TxnGet 的 static/dynamic 双路径与错误分类);`shard_router.{h,cpp}` 新增 `RegionsInRange(start,end)`;`distributed_transaction_coordinator.{h,cpp}` 实现 `Scan`(逐 Region 裁剪聚合、limit 全局计账);`sdk/client.{h,cpp}` 暴露 `ScanResult Scan(txn, start, end, limit)`。验证:单测通过(与 1.1 同一用例文件覆盖协调器聚合);对运行中集群用管道方式验证跨 Region 扫描输出与逐键 Get 一致。

## 2. 正式客户端工具

- [x] 2.1 `src/client/client_main.cpp` 实现 `stratakv-client`:常驻连接,`get/put/del/list [prefix]/quit`,put/del 带退避重试,list 前缀转区间,支持交互与管道;根 CMakeLists 增加 `stratakv-client` 目标并纳入 install。验证:对运行中集群人工+管道各跑一轮:put/get 回读、前缀 list 过滤正确、删除后 list 不出现、未知命令有提示。

## 3. 发布包布局与验收门

- [x] 3.1 server/client 分离:`deploy/stratakv-package` 按 `server/`(bin+scripts+config)与 `client/`(bin+examples)装包,`lib/include/cmake/docs/VERSION` 留根;`deploy/stratakv-server`/`stratakvctl` 发行包模式改用 `server/bin` 与 `server/runtime`(源码树模式不变);验收门改用 `client/bin/stratakv-client` 完成 put/get/list 断言(保留 admin 冒烟)。验证:完整打包退出 0,解压检查布局归属正确,包内全部二进制 ldd 解析到包内 lib,验收门含 client list 断言。

## 4. 文档与回归

- [x] 4.1 文档与端到端回归:QUICKSTART 与 bin/README 更新新布局、client 用法与已知限制;连续两次 `deploy/stratakv-package` 均通过;源码树 `up/verify` 回归;`git status` 复核未触碰无关脏文件。验证:两次打包 rc=0 且无残留;源码树集群验证通过;`openspec validate add-sdk-scan-and-client` 通过。
