# StrataKV 本地部署指南

StrataKV 只使用 Linux/WSL 本地进程部署。部署脚本启动三个 `stratakv-node`、三个
`stratakv-tso` 控制层成员；`--topology-mode dynamic` 时额外启动三成员元数据集群
（`stratakv-meta`）。不需要容器运行时。客户端一律通过原生 C++ SDK 直连数据节点，
不存在独立的网关进程。

构建 Fiber 运行时需要 Boost.Context（Ubuntu/WSL 安装
`libboost-context-dev`）。默认后端是原生 `fcontext`，未启用
`BOOST_USE_UCONTEXT`。

## 启动

在项目根目录执行：

```bash
# 静态拓扑：三个 Region 来自 regions.conf
bash deploy/stratakv-server up \
  --project my-db \
  --nodes 3 \
  --replicas 3 \
  --tso-port 26300

# 动态拓扑：额外启动三成员元数据集群，支持在线分裂与副本迁移
bash deploy/stratakv-server up \
  --project my-db \
  --nodes 3 \
  --replicas 3 \
  --tso-port 26300 \
  --topology-mode dynamic
```

结果：

- 自动构建程序到 `bin/`；
- 静态模式启动 `node-0..2`、`tso-0..2`；动态模式再加 `meta-0..2`；
- 静态模式创建 Region 100、101、102（各三个 Raft 副本）；动态模式由元数据
  bootstrap 同一静态目录，之后可通过 `stratakv-admin` 在线分裂；
- 把 PID、日志、配置和 RocksDB 数据放入 `deploy/runtime/my-db/`。

同一台机器目前只能启动一套使用默认 Raft 端口的集群。另一个项目如需同时
运行，必须先扩展脚本使其支持自定义 Raft 端口。

### 动态拓扑与在线分裂

动态模式下集群支持：

- **在线 Region 分裂**：`bin/stratakv-admin split-region ...` 在指定 split key
  处一分为二，期间业务持续服务；
- **动态路由**：SDK 按元数据 revision 缓存拓扑，epoch 变化时自动刷新；
- **在线副本迁移**：Learner 追平后 Promote，源副本 drain 后退役。

一键体验分裂韧性压测（自动拉起动态集群、施压、触发分裂、审计、出报告）：

```bash
bash deploy/stratakv-split-benchmark
```

### Raft 日志压缩

存储 Region 默认每 3 秒检查一次 Raft Log GC。至少有 50 条已经 Apply 且已复制到
所有 Follower 的日志时执行软 GC；日志达到 196608 条或近似大小达到 192 MiB 时，
强制压缩到本机已 Apply 的位置。落后到压缩边界之前的 Follower 会通过
`InstallSnapshot` 追赶。

```bash
bash deploy/stratakv-server up \
  --project my-db \
  --raft-log-gc-threshold 10000 \
  --raft-log-gc-count-limit 196608 \
  --raft-log-gc-size-limit 201326592 \
  --raft-log-gc-tick-interval-ms 3000
```

`raft-log-gc-size-limit` 使用字节；默认 192 MiB，按 256 MiB Region 目标上限的
四分之三设置。运行值会保存在项目的 `runtime.conf` 中，旧项目中的
`max_raft_state` 会作为大小硬阈值继续读取。

## 状态与验证

```bash
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server verify --project my-db
```

正常结果是全部受管进程均为 `running`（静态模式六个，动态模式九个），TSO 显示
`leaders=1`，三个 Region 均为 `leaders=1`，`verify` 输出 `Verification passed`。

## 进程、端口和文件

| 项目 | 位置或端口 |
| --- | --- |
| 所有可执行文件 | `bin/` |
| 运行配置 | `deploy/runtime/my-db/runtime.conf` |
| Region 配置 | `deploy/runtime/my-db/regions.conf` |
| PID | `deploy/runtime/my-db/pids/` |
| 日志 | `deploy/runtime/my-db/logs/` |
| 节点数据 | `deploy/runtime/my-db/node-N/run_data/` |
| TSO 水位与 Raft 数据 | `deploy/runtime/my-db/tso-N/` |
| 元数据集群数据（动态模式） | `deploy/runtime/my-db/metadata-v1/` |
| TSO control plane | `127.0.0.1:26300`～`26302` |
| 元数据 RPC（动态模式） | `127.0.0.1:26580`～`26582` |
| node-0 shared RPC | `127.0.0.1:26200` |
| node-1 shared RPC | `127.0.0.1:26201` |
| node-2 shared RPC | `127.0.0.1:26202` |

每个物理节点只有一个 `NodeServer`/`RpcProvider`。该节点上所有 Region 的 KV 与
Raft RPC 共享节点端口，并通过 `RegionId` 路由到各自的 `RegionPeer`。
三个 TSO 成员运行独立 Raft Group，只有 Leader 分配时间戳；SDK 会在 Leader
故障后切换到新 Leader。停止项目不会删除水位、Raft 日志或快照。

查看日志：

```bash
bash deploy/stratakv-server logs --project my-db
bash deploy/stratakv-server logs --project my-db --node node-1
tail -f deploy/runtime/my-db/logs/node-0.log
```

重启 TSO 成员并验证自动选主：

```bash
bash deploy/stratakv-server restart-tso --project my-db --node tso-0
bash deploy/stratakv-server verify --project my-db
```

## 重建与故障演练

修改源码后停止、重建并恢复原数据：

```bash
bash deploy/stratakv-server rebuild --project my-db
```

只重启一个节点：

```bash
bash deploy/stratakv-server restart-node --project my-db --node node-2
```

## 停止、恢复和删除

停止所有进程并保留数据：

```bash
bash deploy/stratakv-server down --project my-db
```

恢复：

```bash
bash deploy/stratakv-server up --project my-db --no-build
```

停止进程并永久删除该项目数据：

```bash
bash deploy/stratakv-server reset --project my-db
```

`reset` 只接受由字母、数字、下划线或短横线组成的项目名，并只删除
`deploy/runtime/<project>/`。执行前应确认数据不再需要。

## 自动可靠性测试

```bash
bash deploy/stratakv-reliability run \
  --transactions 10000 \
  --workers 16
```

脚本使用独立的 `reliability-db` 本地目录，运行时重启一个 Leader 所在节点，
逐笔验证三 Region 原子性，再重启整个本地集群验证 RocksDB 持久化。完整参数可通过
`bash deploy/stratakv-reliability --help` 查看。

Raft Log GC 的开关 A/B、三档数据规模、节点重启和 Follower Snapshot 追赶使用：

```bash
bash deploy/stratakv-raft-log-gc-benchmark \
  --result-dir test-results/performance/raft-log-gc-$(date +%Y%m%d-%H%M%S)
```

每档先 load，再对固定键集测量三轮。原始数据和报告统一写入指定目录；脚本退出时会
停止并删除自己创建的三个临时项目。

## 常见问题

端口占用：

```bash
ss -ltnp | grep -E ':(2620[0-2]|2630[0-2]|2640[0-2]|2658[0-2])\b'
```

进程启动失败时先执行：

```bash
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server logs --project my-db
```

如果上一次被强制中断，先用 `down` 清理受管进程，再重新 `up`。数据不会被
`down` 删除。
