# StrataKV 本地部署指南

StrataKV 只使用 Linux/WSL 本地进程部署。部署脚本会启动三个
`stratakv-node`、三个 `stratakv-tso` 控制层成员和一个 `stratakv-gateway`，不需要容器运行时。

## 启动

在项目根目录执行：

```bash
bash deploy/stratakv-server up \
  --project my-db \
  --nodes 3 \
  --replicas 3 \
  --gateway-port 8080 \
  --tso-port 26300
```

结果：

- 自动构建程序到 `bin/`；
- 启动 `node-0`、`node-1`、`node-2`、`tso-0`、`tso-1`、`tso-2` 和 `gateway`；
- 创建 Region 100、101、102，每个 Region 有三个 Raft 副本；
- 把 PID、日志、配置和 RocksDB 数据放入
  `deploy/runtime/my-db/`；
- Gateway 在 `http://127.0.0.1:8080` 提供服务。

同一台机器目前只能启动一套使用默认 Raft 端口的集群。另一个项目如需同时
运行，必须先扩展脚本使其支持自定义 Raft 端口。

### Gateway 运行模式

Gateway 默认使用 `thread` 模式。高连接扇入或需要严格约束线程数时，
可显式启用 Pulsar Fiber 网络运行时：

```bash
bash deploy/stratakv-server up \
  --project my-db \
  --gateway-runtime fiber \
  --gateway-workers 4 \
  --gateway-request-workers 16
```

Fiber 只负责 HTTP socket 的 accept/read/wait/write。SDK、2PC、同步 RPC、
Raft 和 RocksDB 仍在原生线程或有界线程池中执行；请求队列满时
返回 HTTP 503。Fiber 模式的主要目标是给连接和线程资源设置上界，
不保证在所有负载下比 `thread` 模式更快；生产取舍前应使用目标负载做 A/B。

## 状态与验证

```bash
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server verify --project my-db
curl -fsS http://127.0.0.1:8080/healthz
```

正常结果是七个进程均为 `running`，TSO 显示 `leaders=1`，三个 Region 均为 `leaders=1`，
`verify` 输出 `Verification passed`。

## 使用客户端

单条命令：

```bash
bash deploy/stratakv-client put --project my-db customer:42 alice
bash deploy/stratakv-client get --project my-db customer:42
bash deploy/stratakv-client delete --project my-db customer:42
```

交互事务：

```bash
bash deploy/stratakv-client shell --project my-db
```

进入后可以执行：

```text
begin
put apple:1 value-a
put hello:1 value-h
put zoo:1 value-z
commit
quit
```

批量命令：

```bash
bash deploy/stratakv-client batch --project my-db deploy/examples/bulk-demo.txn
```

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
| Gateway | `127.0.0.1:8080` |
| TSO control plane | `127.0.0.1:26300`～`26302` |
| node-0 shared RPC | `127.0.0.1:26200` |
| node-1 shared RPC | `127.0.0.1:26201` |
| node-2 shared RPC | `127.0.0.1:26202` |

每个物理节点只有一个 `NodeServer`/`RpcProvider`。三个 Region 的 KV 与
Raft RPC 共享该节点端口，并通过 `RegionId` 路由到各自的 `RegionPeer`。
三个 TSO 成员运行独立 Raft Group，只有 Leader 分配时间戳；Gateway/SDK 会在 Leader
故障后切换到新 Leader。停止项目不会删除水位、Raft 日志或快照。

查看日志：

```bash
bash deploy/stratakv-server logs --project my-db
bash deploy/stratakv-server logs --project my-db --node node-1
tail -f deploy/runtime/my-db/logs/gateway.log
```

重启 TSO 成员并验证自动选主：

```bash
bash deploy/stratakv-server restart-tso --project my-db --node tso-0
bash deploy/stratakv-server verify --project my-db
```

查看 Gateway 指标：

```bash
curl -fsS http://127.0.0.1:8080/metrics
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

## 常见问题

端口占用：

```bash
ss -ltnp | grep -E ':(8080|2620[0-2]|2630[0-2]|2640[0-2])\b'
```

进程启动失败时先执行：

```bash
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server logs --project my-db
```

如果上一次被强制中断，先用 `down` 清理受管进程，再重新 `up`。数据不会被
`down` 删除。
