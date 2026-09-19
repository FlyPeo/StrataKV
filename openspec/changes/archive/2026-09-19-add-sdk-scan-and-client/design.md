# Design: add-sdk-scan-and-client

## Context

读路径现状(已核实):SDK `Get` = 协调器把 `Route(key)->Get(key, readTs)` 打到 `RaftMvccStorage`(SDK 侧远程桩)→ `TxnGet` RPC → `KvServiceDispatcher`(node_server 宏分发)→ `RegionPeer::TxnGet`(线性一致读屏障 `LinearizableReadBarrier` + 本地 `MvccStorage::Get` 快照读 + `IsLeaderInTerm` 任期复核)。现有 `List` RPC 是原始前缀扫描(无 ReadTs、吐内部键),仅供 admin 调试,不复用。

`MvccStorage::Get` 可见性规则(Scan 逐键复用):键上有 startTs ≤ readTs 的未完成锁 → LockConflict;writes_ 中 ≤ readTs 的最新非 Rollback/Lock 记录,Delete→NotFound,否则 `engine_->Get(DataKey(key, write.startTs))`。

## Goals / Non-Goals

目标:一次 Scan 调用 = 一个事务快照下、任意 Region 数、有序、可 limit 的用户键枚举;`stratakv-client` 成为包内正式客户端;包布局 server/client 分离。

非目标:分页游标 token(续读 = last key 的直接后继)、谓词下推、反向扫描、快照隔离级别的修改。

## Decisions

### D1. Scan 语义:冲突即失败,不做静默跳过

Range 内任一键存在快照下未完成锁 → 整次 Scan 返回 `LockConflict`(retryable),与 `Get` 单键语义一致。备选"跳过冲突键继续"被否:静默缺键违背快照读契约,调用方无法区分"不存在"与"暂不可见"。冲突由既有锁解析器按 TTL 推进,上层按既有 retryable 重试。

### D2. 实现路径:新增 `TxnScan` RPC,镜像 TxnGet 全链

- **proto**(`kv_server_rpc.proto`):`TxnScanArgs{StartKey, EndKey, ReadTs, Limit, RegionId, Header}` / `TxnScanReply{Err, repeated KeyValue Entries, Header}`(复用既有 `KeyValue`);service 块加 `rpc TxnScan`。纯增量,旧节点收到新 rpc 返回不可用,SDK 侧映射 retryable。
- **MvccStorage 基类**(`mvcc_storage.{h,cpp}`):`virtual TxnStatus Scan(startKey, endKey, readTs, limit, std::vector<std::pair<std::string,std::string>>*)`,在基类直接实现(遍历有序 `writes_`,逐键执行 Get 同款可见性判定,limit 截断)。node 侧本地存储与单机/测试用法同享。锁检查先于逐键读,命中冲突即整批返回。
- **RegionPeer::TxnScan**(`raft/region_peer.{h,cpp}`):镜像 `TxnGet`——范围起点 `OwnsKey` 校验(SDK 保证起点落在本 Region),终点在本 Region 边界内裁剪;`LinearizableReadBarrier` → `m_mvccStorage->Scan` → `IsLeaderInTerm` 复核。读不走 Raft 提案,一致性由读屏障保证(与 Get 相同级别)。
- **node 分发**:`DISPATCH_KV(TxnScan, TxnScanArgs, TxnScanReply)` + `RequestKeys(const TxnScanArgs&)` 返回 `{startKey}` 供头部校验。
- **RaftMvccStorage::Scan**(远程桩):镜像 `TxnGet` 的 static/dynamic(sender_)两条发送路径与错误分类。

### D3. 范围路由:ShardRouter 新增 RegionsInRange

`ShardRouter::RegionsInRange(start, end)` 沿 cache 路由表从 start 所在 Region 起按序遍历至 end(空 end=无界),返回 `vector<RegionRoute>`(metadata+storage)。协调器逐 Region 调 `Scan(裁剪后区间)`,区间 = `[max(region.start,start), region.end 为空 ? end : min(region.end,end))`,顺序天然有序无需排序;limit 全局计账,取尽即停。Region epoch 失配/分裂竞态由 Route 层既有刷新与重试语义处理,Scan 不自建重试。

### D4. 客户端工具 stratakv-client:门面薄封装,命令面最小

`src/client/client_main.cpp`,链接 `stratakv_core + stratakv_rpc`(与 admin 同款)。参数与 admin 一致的拓扑约定:`argv[1]=regions.conf`、`argv[2]=tsoEndpoints`,动态模式支持 `STRATAKV_TOPOLOGY_MODE/METADATA_ENDPOINTS` 环境变量。命令:`get/put/del/list [prefix]/quit`;`list` 前缀转区间(prefix, prefix↑)(末字节进位,全 0xFF 回退为无界);put/del 走带退避的重试提交。每命令独立事务;连接常驻。不复制 admin 的内部运维命令(split/balancer 等)——客户端只做业务读写。

### D5. 包布局:server/ client/ 分离,SDK 契约不动

```
stratakv-<v>-linux-x64/
├── server/  bin(node|tso|meta|admin) scripts(stratakv-server|ctl) runtime/ config/
├── client/  bin(stratakv-client) examples/
├── lib/ include/ cmake/      ← 包根不动,消费契约(CMAKE_PREFIX_PATH=包根)不变
├── docs/QUICKSTART.md  VERSION
```

- rpath:server 与 client 的 bin 均为 `$ORIGIN/../../lib`(install 后由打包脚本移动,打包脚本对包内 ELF 统一修正 rpath 或 CMake install 前缀布局调整——**采用 install 到 `server/` 与 `client/` 子前缀再合并**:cmake `--install` 分两次,`bin` 类目标装到 `server`,`stratakv-client` 单独 install 到 `client`,其余照旧,天然获得正确 rpath,无移动无 patch)。
- `stratakv-server`:发行包模式下 `bin_dir=<pkg>/server/bin`、`runtime_root=<pkg>/server/runtime`;模式检测不变(scripts 父目录无 CMakeLists)。源码树模式零变化。
- stratakv-package 验收门改用 `client/bin/stratakv-client` 做 put/get/`list` 断言(保留 admin 冒烟)。

## Risks / Trade-offs

- [Scan 冲突即整批失败,大范围扫在写热点下成功率高] → 文档明示重试;list 前缀缩小范围是调用方手段;不做服务端跳过(语义优先)。
- [无界 list 对大库是全量遍历] → limit 参数暴露在 Scan;客户端 MVP 不分页,标注已知限制。
- [TxnScan 在旧版本节点上不可用] → 混布窗口内 SDK Scan 报 retryable unavailable;内部发版同步升级,不为此加版本协商。
- [布局重构破坏既有脚本引用路径] → stratakv-server 双模式路径常量集中一处;验收门在真包上回归 up/verify/put/get/list。
- [Proto 增量导致 ABI 不兼容需全量重编] → 内部包本就整包发布,构建门禁覆盖。

## Migration Plan

实现顺序:proto → 存储 → peer → 桩 → 路由 → 协调器 → 门面 → 客户端工具 → 单测 → CMake/打包/文档。回滚:wire 与代码增量可独立 revert;包布局回滚不影响已发旧包。

## Open Questions

(无)
