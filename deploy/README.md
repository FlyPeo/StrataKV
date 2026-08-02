# StrataKV 本地部署指南

StrataKV 只使用 Linux/WSL 本地进程部署。部署脚本会启动三个
`stratakv-node` 和一个 `stratakv-gateway`，不需要容器运行时。

## 启动

在项目根目录执行：

```bash
bash deploy/stratakv-server up \
  --project my-db \
  --nodes 3 \
  --replicas 3 \
  --gateway-port 8080
```

结果：

- 自动构建程序到 `bin/`；
- 启动 `node-0`、`node-1`、`node-2` 和 `gateway`；
- 创建 Region 100、101、102，每个 Region 有三个 Raft 副本；
- 把 PID、日志、配置和 RocksDB 数据放入
  `deploy/runtime/my-db/`；
- Gateway 在 `http://127.0.0.1:8080` 提供服务。

同一台机器目前只能启动一套使用默认 Raft 端口的集群。另一个项目如需同时
运行，必须先扩展脚本使其支持自定义 Raft 端口。

## 状态与验证

```bash
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server verify --project my-db
curl -fsS http://127.0.0.1:8080/healthz
```

正常结果是四个进程均为 `running`，三个 Region 均为 `leaders=1`，
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
| Gateway | `127.0.0.1:8080` |
| Region 100 | `127.0.0.1:26200-26202` |
| Region 101 | `127.0.0.1:26300-26302` |
| Region 102 | `127.0.0.1:26400-26402` |

查看日志：

```bash
bash deploy/stratakv-server logs --project my-db
bash deploy/stratakv-server logs --project my-db --node node-1
tail -f deploy/runtime/my-db/logs/gateway.log
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
逐笔验证三 Region 原子性，再重启整个本地集群验证 RocksDB 持久化。详情见
[可靠性测试文档](../docs/reliability-testing.md)。

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
