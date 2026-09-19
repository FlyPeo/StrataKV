# Proposal: add-sdk-scan-and-client

## Why

SDK 目前只有按键读写(`Get/Put/Delete`),没有范围扫描,导致:基于 SDK 的常驻客户端程序无法 `list` 数据;实际使用中大家退回 `stratakv-admin list`(原始调试扫描,吐出 MVCC 内部键、无快照一致性),这不是面向业务的读取接口。同时 /tmp 下的临时客户端程序无法进入项目构建与发布包,且发布包布局中服务端与客户端混在同一个 `bin/`,产品边界不清。

## What Changes

- 全栈新增快照一致的范围扫描:**proto**(`TxnScanArgs/TxnScanReply`,携带 `StartKey/EndKey/ReadTs/Limit`)→ **MvccStorage** 基类快照范围读(与 `Get` 同一可见性规则)→ **RegionPeer::TxnScan**(线性一致读屏障 + 任期校验,镜像 `TxnGet`)→ **RaftMvccStorage::Scan**(SDK 远程桩,镜像 `TxnGet` 收发)→ **ShardRouter** 范围路由(按 Region 边界裁剪、有序遍历)→ **协调器 Scan**(逐 Region 聚合,limit 跨 Region 计账)→ **Client::Scan** 公共门面。
- 新增正式客户端工具 `stratakv-client`(源码 `src/client/`,CMake 目标,install 进包):常驻连接,命令 `get/put/del/list [prefix]/quit`,支持交互与管道两种用法;取代 /tmp 临时程序。
- 发布包布局服务端/客户端分离:`server/`(bin、scripts、runtime、config)与 `client/`(bin、examples)子目录;`lib/include/cmake/docs/VERSION` 留在包根;`stratakv-server`/`stratakvctl` 兼容新布局;SDK 消费契约(包根作 `CMAKE_PREFIX_PATH`)不变。
- 打包验收门扩展:除集群健康与 admin put/get 外,必须用 `stratakv-client` 完成 put/get/list 回读断言。
- QUICKSTART 与 bin/README 文档同步。

**Goals**:业务侧可在事务快照下跨分片枚举数据;客户端工具随包分发;包内服务端/客户端一目了然。

**Non-Goals**:不做分页游标 token(limit 即截断,续读用最后 key+1);不做服务端谓词过滤;不改变 admin 的调试定位用途;不做反向扫描。

## Capabilities

### New Capabilities

- `sdk-range-scan`:范围扫描的全栈行为契约——快照一致性、Region 裁剪与跨 Region 聚合、limit 语义、锁冲突语义(与 Get 一致,整扫描返回可重试冲突)、边界(空 endKey=无界、startKey 不存在的键)。
- `client-tool`:`stratakv-client` 的命令行为契约——常驻连接、四条命令加 quit、前缀 list、交互与管道、错误输出。

### Modified Capabilities

- `release-packaging`:发布包目录布局变更为 server/client 分离;包内容新增 `stratakv-client`;验收门新增 client list 断言。

## Impact

- **受影响代码**:`kv_server_rpc.proto`(新消息与 rpc)、`mvcc_storage.{h,cpp}`(基类 Scan)、`raft/region_peer.{h,cpp}`(TxnScan 处理)、`node_server.cpp`(分发 + RequestKeys)、`raft_mvcc_storage.{h,cpp}`(远程桩)、`shard_router.{h,cpp}`(范围路由)、`distributed_transaction_coordinator.{h,cpp}`、`sdk/client.{h,cpp}`(门面)、新增 `src/client/client_main.cpp`、根 CMakeLists、`deploy/stratakv-package`、`deploy/stratakv-server`、`deploy/stratakvctl`、QUICKSTART、bin/README、新增 `test/sdk/sdk_scan_unit_check.cpp`。
- **兼容性**:wire 协议纯增量(新 rpc/消息),新旧版本混布时旧节点对 `TxnScan` 返回不可用错误,SDK 已有重试/错误通道可表达;现有命令、参数、数据路径零变化;包布局变化仅影响发行包内部组织,解压使用方式在 QUICKSTART 中体现。
- **回滚策略**:各层增量独立可 revert;wire 新增消息不破坏既有消息;包布局回滚 = revert 打包脚本与 install 规则,已分发旧布局包继续可用。
