# StrataKV

StrataKV 是一个用于学习、开发和可靠性验证的分布式事务 KV 系统。项目将
Raft 多副本、MVCC、静态 Region 路由和 Primary-First 2PC 封装为 HTTP
Transaction Gateway、命令行客户端和 C++ SDK。

当前版本定位为 **Developer Edition**：适合本地部署、功能演示、源码学习
和故障测试，不应直接承载生产数据。

StrataKV 只在 Linux/WSL 上以本地进程运行，不使用 Docker、Compose 或 Helm。

## 1. 系统能力与结构

```text
业务程序 / stratakv-client
          |
          v
  HTTP Transaction Gateway
          |
          v
静态 Region 路由 + MVCC + 2PC
          |
          v
3 个本地节点 / 每个 Region 3 个 Raft 副本 / RocksDB
```

主要能力：

- 三个固定 Key 范围 Region：`["", "h")`、`["h", "p")`、`["p", +∞)`；
- 每个 Region 使用三个 Raft 副本；
- MVCC、乐观事务、悲观锁和 Primary-First 2PC；
- HTTP/JSON Gateway、业务 CLI、管理工具和 C++ SDK；
- 本地进程启停、节点重启、日志、状态检查和可靠性测试；
- RocksDB 持久化，`skiplist` 仅用于开发测试。

## 2. 快速开始

所有命令均在项目根目录执行。

Pulsar 以 Git submodule 形式提供，首次获取源码时请使用：

```bash
git clone --recursive https://github.com/FlyPeo/StrataKV.git
cd StrataKV
```

如果已经执行普通 `git clone`，补充初始化依赖：

```bash
git submodule update --init --recursive
```

### 2.1 安装依赖

推荐 Ubuntu 24.04 或兼容 Linux/WSL，使用 GCC/G++ 13+ 和 CMake 3.22+。

```bash
sudo apt update
sudo apt install -y \
  build-essential \
  cmake \
  curl \
  gdb \
  protobuf-compiler \
  libprotobuf-dev \
  librocksdb-dev \
  libboost-serialization-dev
```

还需要 Muduo 的以下库：

```text
libmuduo_net
libmuduo_base
```

如果发行版没有 Muduo 开发包，需要单独构建安装。检查环境：

```bash
cmake --version
g++ --version
protoc --version
ldconfig -p | grep -E 'muduo_(net|base)|rocksdb|protobuf'
```

### 2.2 构建程序

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

构建后，所有程序统一位于 `bin/`，静态库位于 `lib/`。

### 2.3 启动本地集群

```bash
bash deploy/stratakv-server up \
  --project my-db \
  --nodes 3 \
  --replicas 3 \
  --gateway-port 8080
```

该命令会自动构建并启动：

- `node-0`、`node-1`、`node-2`；
- 一个 `stratakv-gateway`；
- Region 100、101、102，每个 Region 三个 Raft 副本。

如果 `bin/` 中已有可用程序，可以跳过构建：

```bash
bash deploy/stratakv-server up --project my-db --no-build
```

运行配置、PID、日志和 RocksDB 数据保存在：

```text
deploy/runtime/my-db/
```

### 2.4 检查集群

```bash
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server verify --project my-db
curl -fsS http://127.0.0.1:8080/healthz
```

正常结果：

- `node-0`、`node-1`、`node-2` 和 Gateway 均为 `running`；
- Region 100、101、102 均为 `leaders=1`；
- `verify` 输出 `Verification passed`；
- 健康接口返回 `{"status":"ok"}`。

### 2.5 读写数据

```bash
bash deploy/stratakv-client put --project my-db customer:42 alice
bash deploy/stratakv-client get --project my-db customer:42
bash deploy/stratakv-client delete --project my-db customer:42
```

预期结果：

```text
OK key=customer:42
alice
OK deleted=customer:42
```

### 2.6 执行跨 Region 事务

```bash
bash deploy/stratakv-client shell --project my-db
```

在交互界面输入：

```text
begin
put apple:1 value-a
put hello:1 value-h
put zoo:1 value-z
commit
quit
```

三个 Key 分别落入三个 Region，并作为同一事务提交。

### 2.7 停止、恢复或删除

停止进程并保留数据：

```bash
bash deploy/stratakv-server down --project my-db
```

使用原数据恢复：

```bash
bash deploy/stratakv-server up --project my-db --no-build
```

永久删除该项目的配置、日志和数据库：

```bash
bash deploy/stratakv-server reset --project my-db
```

`reset` 不可恢复；只想停止服务时使用 `down`。

## 3. 可执行程序

| 程序 | 用途 |
| --- | --- |
| `bin/stratakv-node` | 数据节点，承载 Region、Raft 副本和 RocksDB |
| `bin/stratakv-gateway` | 面向业务的 HTTP/JSON 事务入口 |
| `bin/stratakv-client` | 业务 CLI，支持单条命令和交互事务 |
| `bin/stratakv-admin` | 直连 Region 的开发与运维工具 |
| `bin/stratakv-test-fiber-sync` | 协程同步组件测试 |
| `bin/stratakv-test-reliability` | 批量事务、原子性和持久化验证 |

## 4. 开发构建

### 4.1 Release

用于日常运行和性能测试：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

### 4.2 Debug

用于断点、堆栈和变量检查：

```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j"$(nproc)"
```

### 4.3 AddressSanitizer

用于排查越界和 use-after-free：

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DSTRATAKV_ENABLE_ASAN=ON
cmake --build build-asan -j"$(nproc)"
```

不同构建目录的程序都会输出到同一个 `bin/`。不要在集群运行时切换构建类型
或覆盖可执行文件；应先停止受管进程：

```bash
bash deploy/stratakv-server down --project my-db
```

### 4.4 单独构建目标

```bash
cmake --build build --target stratakv-node
cmake --build build --target stratakv-gateway
cmake --build build --target stratakv-client
cmake --build build --target stratakv-admin
cmake --build build --target stratakv_sdk
cmake --build build --target stratakv-test-fiber-sync
cmake --build build --target stratakv-test-reliability
```

查看全部目标：

```bash
cmake --build build --target help
```

修改源码后，重建并恢复原有数据：

```bash
bash deploy/stratakv-server rebuild --project my-db
```

## 5. 构建产物与项目目录

| 位置 | 内容 | 数据属性 |
| --- | --- | --- |
| `bin/` | 所有可执行程序 | 可重新构建，通常保留 |
| `lib/` | C++ SDK 和内部静态库 | 可重新构建，SDK 使用时保留 |
| `src/` | Raft、RPC、MVCC、事务、Gateway 和客户端源码 | 必须保留 |
| `build*/` | CMake 缓存、对象文件和测试元数据 | 可重新生成 |
| `deploy/` | 本地部署、客户端和可靠性测试脚本 | 必须保留 |
| `deploy/runtime/` | 配置、PID、日志、Raft 状态和 RocksDB | 删除会丢失项目数据 |
| `test/` | 测试源码 | 建议保留 |
| `test-results/` | 自动测试报告 | 可重新生成，删除会失去历史报告 |

每个运行项目使用独立目录：

```text
deploy/runtime/<project>/
├── runtime.conf
├── regions.conf
├── pids/
├── logs/
├── gateway/
├── node-0/run_data/
├── node-1/run_data/
└── node-2/run_data/
```

`node-N/run_data/` 包含 RocksDB、Raft 状态和快照，不能当作普通构建缓存
删除。

## 6. 常用运维操作

```bash
# 查看状态和验证集群
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server verify --project my-db

# 查看全部日志或指定节点日志
bash deploy/stratakv-server logs --project my-db
bash deploy/stratakv-server logs --project my-db --node node-0

# 查看 Gateway 指标
curl -fsS http://127.0.0.1:8080/metrics

# 只重启一个物理节点
bash deploy/stratakv-server restart-node --project my-db --node node-2

# 查看节点本地数据
bash deploy/stratakv-server local-data \
  --project my-db \
  --node node-0 \
  --prefix customer:
```

同一台机器目前只能运行一套使用默认 Raft 端口的集群。Gateway 端口可以通过
`--gateway-port` 修改，但 Raft 端口仍是固定的：

| 服务 | 地址 |
| --- | --- |
| Gateway | `127.0.0.1:8080` |
| Region 100 peers | `127.0.0.1:26200-26202` |
| Region 101 peers | `127.0.0.1:26300-26302` |
| Region 102 peers | `127.0.0.1:26400-26402` |

## 7. 客户端与 Gateway

批量执行命令文件：

```bash
bash deploy/stratakv-client batch \
  --project my-db \
  deploy/examples/bulk-demo.txn
```

常用接口：

| 接口 | 地址 |
| --- | --- |
| Gateway | `http://127.0.0.1:8080` |
| 健康检查 | `http://127.0.0.1:8080/healthz` |
| Prometheus 指标 | `http://127.0.0.1:8080/metrics` |

Gateway 提供事务创建、读写、提交、回滚、健康检查和 Prometheus 指标接口。

## 8. 测试

### 8.1 协程同步测试

```bash
./bin/stratakv-test-fiber-sync
ctest --test-dir build --output-on-failure
```

### 8.2 本地集群冒烟测试

```bash
STRATAKV_PROJECT=my-db bash deploy/stratakvctl verify
STRATAKV_PROJECT=my-db bash deploy/stratakvctl gateway-smoke
```

### 8.3 自动可靠性测试

```bash
bash deploy/stratakv-reliability run \
  --transactions 10000 \
  --workers 16
```

测试会：

1. 启动独立三节点集群；
2. 并发写入跨三个 Region 的事务；
3. 在负载期间重启一个 Leader 所在节点；
4. 验证每笔事务的三个 Key；
5. 重启全部本地进程；
6. 再次验证 RocksDB 和 Raft 持久化数据。

报告生成到：

```text
test-results/reliability/<run-id>/
```

成功必须同时满足：

```text
availability_failures=0
safety_violations=0
verification_failures=0
RELIABILITY PASS
result=PASS
```

详细参数可通过 `bash deploy/stratakv-reliability --help` 查看。

## 9. 调试

先查看受管进程状态和日志：

```bash
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server logs --project my-db
tail -f deploy/runtime/my-db/logs/gateway.log
```

使用 GDB：

```bash
gdb ./bin/stratakv-node
```

常用命令：

```gdb
run <arguments>
info threads
thread apply all bt
quit
```

检查端口：

```bash
ss -ltnp | grep -E ':(8080|2620[0-2]|2630[0-2]|2640[0-2])\b'
```

## 10. 清理与重新配置

只清理当前构建目标：

```bash
cmake --build build --target clean
```

使用新的构建目录重新配置：

```bash
cmake -S . -B build-clean -DCMAKE_BUILD_TYPE=Release
cmake --build build-clean -j"$(nproc)"
ctest --test-dir build-clean --output-on-failure
```

可重新生成：

```text
build*/
test-results/
```

删除 `test-results/` 会失去历史报告。删除 `deploy/runtime/` 会丢失数据库，
必须先确认其中没有需要保留的数据。

建议长期保留：

```text
bin/
lib/
src/
deploy/（runtime 除外）
test/
```

## 11. 常见问题

| 现象 | 检查与处理 |
| --- | --- |
| `muduo_net` 或 `muduo_base` 找不到 | 安装 Muduo，并用 `ldconfig -p` 检查 |
| `docker: command not found` | 当前脚本不使用 Docker；请执行 `deploy/stratakv-server` |
| Gateway 端口被占用 | 使用 `ss -ltnp` 查找，或指定其他 `--gateway-port` |
| 上次运行被强制中断 | 执行 `down` 清理受管进程；数据会保留 |
| 修改源码后仍运行旧逻辑 | 先执行 `down`，再执行 `rebuild` |
| 可靠性测试没有进度 | 检查 Leader、RPC timeout、快照和 runtime 日志 |
| WSL 出现 EGL/图形错误 | 当前没有 Qt 客户端，不需要图形栈 |

## 12. 相关项目与文档

| 文档 | 内容 |
| --- | --- |
| [本地部署指南](deploy/README.md) | 完整进程参数、端口、日志和数据目录 |
| [Pulsar](https://github.com/FlyPeo/Pulsar) | 用户态有栈协程、M:N 调度、epoll 与 Hook I/O 运行时 |

## 13. 当前边界

- 业务程序应使用 Gateway 或 C++ SDK，不应依赖内部 protobuf RPC；
- Region 为静态配置，不支持动态 split/merge；
- 暂不支持认证、TLS、备份恢复、在线成员变更和多租户隔离；
- 高负载下的快照、选举稳定性和长尾延迟仍需继续优化；
- 使用前应自行备份重要数据。
