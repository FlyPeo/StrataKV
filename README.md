# StrataKV

StrataKV 是一个面向学习、开发和可靠性验证的分布式事务 KV 系统。它使用
Raft 维护 Region 内的多副本一致性，在其上实现 MVCC、静态 Region 路由和
Primary-First 2PC，并提供 HTTP/JSON Gateway、命令行客户端和 C++ SDK。

当前版本定位为 **Developer Edition**：适合 Linux/WSL 本地部署、功能演示、
源码学习和故障测试，不应直接承载生产数据。

## 1. 架构与能力

```text
业务程序 / stratakv-client / C++ SDK
                  |
                  v
        HTTP Transaction Gateway
                  |
                  v
       静态 Region 路由 + MVCC + 2PC
                  |
                  v
      3 个本地节点 × 每个 Region 3 个 Raft 副本
                  |
                  v
               RocksDB
```

当前已实现：

- 三个固定 Key 范围 Region：`["", "h")`、`["h", "p")`、`["p", +∞)`；
- 每个 Region 三个 Raft 副本；
- RocksDB 本地持久化、Raft 状态持久化和快照；
- MVCC、乐观事务、悲观锁和 Primary-First 2PC；
- HTTP/JSON Gateway、业务 CLI、管理工具和 C++ SDK；
- 本地进程管理、节点重启、状态检查和可靠性测试。

StrataKV 当前不使用 Docker、Compose、Kubernetes 或 Helm。

## 2. 快速开始

### 2.1 环境要求

必需环境：

- Linux 或 WSL2；
- 支持 C++17 的 GCC/Clang；
- CMake 3.22 或更高版本；
- 下表列出的直接依赖。

以下环境已经实际用于构建：Ubuntu 24.04 / WSL2、GCC 13.3、CMake 3.22+。

#### 直接链接依赖

| 依赖 | CMake/链接名称 | 项目中的用途 | 获取方式 |
| --- | --- | --- | --- |
| Pulsar | `Pulsar::pulsar` | Fiber、调度器、epoll、Timer 和 Hook I/O | Git submodule：`src/pulsar` |
| Protobuf | `find_package(Protobuf REQUIRED)`、`${Protobuf_LIBRARIES}` | Raft/KV RPC 消息、Service、Stub 和反射分发 | `libprotobuf-dev`；修改 `.proto` 时还需要 `protoc` |
| RocksDB | `rocksdb` | 唯一的本地 KV 持久化引擎、WriteBatch 和前缀扫描 | `librocksdb-dev` |
| Boost.Serialization | `boost_serialization` | Raft 状态、快照和 RocksDB 快照数据的序列化 | `libboost-serialization-dev` |
| Muduo | `muduo_net`、`muduo_base` | TCP Server、EventLoop、连接与 Buffer | 安装 Muduo 头文件及 `libmuduo_net/base` |
| POSIX Threads | `pthread` | 节点、Gateway、SDK 和测试的多线程执行 | Linux 系统线程库 |
| Dynamic Loader | `dl` | Pulsar 使用 `dlsym` 解析被 Hook 的原始系统调用 | Linux `libdl` |

Pulsar 是仓库内的子模块，其自身不再引入其他第三方 C++ 库。C++ 标准库、
Linux socket/epoll、文件系统和进程接口属于系统能力，不是额外仓库依赖。

Pulsar 在 StrataKV 中的边界是“可选 Gateway HTTP 网络运行时”。
它不承载 SDK、2PC、同步 RPC、Raft 或 RocksDB 执行；默认 Gateway
仍使用 `thread` 模式。

#### 构建与运行工具

| 工具 | 是否必需 | 用途 |
| --- | --- | --- |
| C++17 编译器、CMake、Make/Ninja | 构建必需 | 配置和编译所有目标 |
| Bash、GNU coreutils、util-linux | 本地部署必需 | 脚本、`setsid`、`taskset`、`lscpu` 等 |
| curl | 本地部署必需 | Gateway 健康检查、指标和可靠性测试 |
| iproute2 (`ss`) | 排障可选 | 检查端口占用 |
| GDB | 调试可选 | 线程、堆栈和崩溃分析 |

当前 CMake 只编码了 CMake 3.22 和 C++17 要求，没有为 RocksDB、Muduo、
Boost 设置项目自定义的最低版本；README 因此不声明未经验证的版本下限。

Ubuntu 24.04 可先安装发行版依赖：

```bash
sudo apt update
sudo apt install -y \
  build-essential \
  cmake \
  curl \
  util-linux \
  iproute2 \
  protobuf-compiler \
  libprotobuf-dev \
  librocksdb-dev \
  libboost-serialization-dev
```

项目还需要 `libmuduo_net` 和 `libmuduo_base`。如果系统没有 Muduo 开发包，
需要先从 Muduo 源码构建并安装。GDB 仅用于调试，不是编译必需依赖。

检查主要工具和动态库：

```bash
cmake --version
c++ --version
protoc --version
ldconfig -p | grep -E 'muduo_(net|base)|rocksdb|protobuf'
```

如果 Muduo 只安装了静态库，它不会出现在 `ldconfig -p` 中；此时应确认
链接器搜索路径中存在 `libmuduo_net.a` 和 `libmuduo_base.a`。

### 2.2 获取源码

Pulsar 作为 Git submodule 位于 `src/pulsar`：

```bash
git clone --recursive https://github.com/FlyPeo/StrataKV.git
cd StrataKV
```

如果已经使用普通方式克隆：

```bash
git submodule update --init --recursive
```

### 2.3 构建与自动测试

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

构建产物输出到源码目录中的 `bin/` 和 `lib/`。当前 CTest 注册了
Fiber 同步原语和有界线程池正确性测试；集群可靠性测试需要单独
运行，见第 7 节。

### 2.4 启动三节点本地集群

```bash
bash deploy/stratakv-server up \
  --project my-db \
  --nodes 3 \
  --replicas 3 \
  --gateway-port 8080
```

该命令会自动构建并启动三个 `stratakv-node` 和一个
`stratakv-gateway`，创建 Region 100、101、102。运行配置、PID、日志和
数据库保存在 `deploy/runtime/my-db/`。

Gateway 默认使用延迟更稳定的 `thread` 模式。线程配额严格或需要承接大量
并发连接时，可以显式启用有界 Fiber worker：

```bash
bash deploy/stratakv-server up \
  --project my-db \
  --gateway-runtime fiber \
  --gateway-workers 4 \
  --gateway-request-workers 16
```

Fiber 模式只用协程处理 HTTP socket 收发；SDK、同步 RPC 和事务逻辑
会进入 16 个有界原生请求线程，2PC 的跨 Region 工作再进入独立的
8 线程执行池。请求队列满时 Gateway 返回 503，不会无界堆积。这个
模式的确定收益是高连接扇入时线程数有上界，不承诺所有负载都有
更高吞吐或更低 P99；选择前应使用目标负载做 A/B。实测和取舍见
[优化记录](docs/优化记录/优化.md#2026-08-10pulsar-协程运行时接入与-ab-取舍)。

如果已经完成构建，可以跳过构建步骤：

```bash
bash deploy/stratakv-server up --project my-db --gateway-port 8080 --no-build
```

### 2.5 检查集群

```bash
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server verify --project my-db
curl -fsS http://127.0.0.1:8080/healthz
```

正常情况下：

- `node-0`、`node-1`、`node-2` 和 Gateway 均为 `running`；
- Region 100、101、102 均显示 `leaders=1`；
- `verify` 输出 `Verification passed`；
- 健康接口返回 `{"status":"ok"}`。

### 2.6 读写数据

```bash
bash deploy/stratakv-client put --project my-db customer:42 alice
bash deploy/stratakv-client get --project my-db customer:42
bash deploy/stratakv-client delete --project my-db customer:42
```

预期输出：

```text
OK key=customer:42
alice
OK deleted=customer:42
```

### 2.7 执行跨 Region 事务

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

三个 Key 分别路由到 Region 100、101、102，并作为同一事务提交。

### 2.8 停止或删除

停止进程并保留数据：

```bash
bash deploy/stratakv-server down --project my-db
```

使用原数据重新启动：

```bash
bash deploy/stratakv-server up --project my-db --gateway-port 8080 --no-build
```

永久删除该项目的运行配置、日志和数据库：

```bash
bash deploy/stratakv-server reset --project my-db
```

`reset` 不可恢复；只想停止服务时必须使用 `down`。

## 3. 程序与源码布局

### 3.1 可执行程序

| 程序 | 用途 |
| --- | --- |
| `bin/stratakv-node` | 数据节点，承载 Region、Raft 副本和 RocksDB |
| `bin/stratakv-gateway` | 面向业务的 HTTP/JSON 事务入口 |
| `bin/stratakv-client` | 业务 CLI，支持单条命令和交互事务 |
| `bin/stratakv-admin` | 直连内部 RPC 的开发与运维工具 |
| `bin/stratakv-test-fiber-sync` | Pulsar 同步原语正确性测试 |
| `bin/stratakv-test-bounded-thread-pool` | 有界线程池与过载背压正确性测试 |
| `bin/stratakv-test-fiber-benchmark` | Pulsar 性能与压力基准 |
| `bin/stratakv-test-reliability` | 集群事务与持久化验证负载 |

### 3.2 源码目录

| 目录 | 职责 |
| --- | --- |
| `src/storage` | RocksDB 适配与存储抽象 |
| `src/raft` | Raft 共识、状态机和 Raft 持久化 |
| `src/transaction` | MVCC、锁、时间戳、路由和 2PC |
| `src/rpc` | 自研 Protobuf RPC 传输与服务分发 |
| `src/proto` | Raft 与 KV 的 Protobuf 契约及生成代码 |
| `src/server` | 存储节点入口 |
| `src/admin` | 内部管理工具入口 |
| `src/sdk` | C++ 事务客户端 SDK |
| `src/gateway` | HTTP/JSON Gateway 入口 |
| `src/cli` | 业务命令行客户端入口 |
| `src/common` | 公共配置和工具 |
| `src/pulsar` | Pulsar 协程运行时子模块 |
| `test` | 正确性、基准和可靠性测试源码 |
| `deploy` | 本地部署、客户端和测试脚本 |

## 4. 开发构建

### Release

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

### Debug

```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j"$(nproc)"
```

### AddressSanitizer

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DSTRATAKV_ENABLE_ASAN=ON
cmake --build build-asan -j"$(nproc)"
```

所有构建目录都会把最终程序写入同一个 `bin/`，静态库写入同一个 `lib/`。
不要在集群运行时切换构建类型或覆盖正在运行的程序；应先执行：

```bash
bash deploy/stratakv-server down --project my-db
```

常用单独目标：

```bash
cmake --build build --target stratakv-node
cmake --build build --target stratakv-gateway
cmake --build build --target stratakv-client
cmake --build build --target stratakv-admin
cmake --build build --target stratakv_sdk
cmake --build build --target stratakv-test-fiber-sync
cmake --build build --target stratakv-test-bounded-thread-pool
cmake --build build --target stratakv-test-fiber-benchmark
cmake --build build --target stratakv-test-reliability
```

修改源码后停止进程、重新构建并用原数据启动：

```bash
bash deploy/stratakv-server rebuild --project my-db
```

## 5. 客户端与 SDK

批量执行事务命令：

```bash
bash deploy/stratakv-client batch \
  --project my-db \
  deploy/examples/bulk-demo.txn
```

Gateway 常用地址：

| 接口 | 地址 |
| --- | --- |
| Gateway | `http://127.0.0.1:8080` |
| 健康检查 | `http://127.0.0.1:8080/healthz` |
| Prometheus 指标 | `http://127.0.0.1:8080/metrics` |

C++ SDK 的入口为 `stratakv/client.h`：

```cpp
#include <stratakv/client.h>

auto client = stratakv::Client::Connect("deploy/runtime/my-db/regions.conf");
auto txn = client->Begin();
auto put = client->Put(txn, "apple:1", "value-a");
if (put.ok()) {
  auto commit = client->Commit(txn);
}
```

在通过 `add_subdirectory` 引入 StrataKV 的 CMake 工程中，可链接
`stratakv_sdk`。业务程序不应直接依赖 `src/proto` 中的内部 RPC。

## 6. 运维与数据目录

```bash
# 状态与一致性检查
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server verify --project my-db

# 日志
bash deploy/stratakv-server logs --project my-db
bash deploy/stratakv-server logs --project my-db --node node-0

# 重启一个物理节点
bash deploy/stratakv-server restart-node --project my-db --node node-2

# 查看指定节点的本地数据
bash deploy/stratakv-server local-data \
  --project my-db \
  --node node-0 \
  --prefix customer:
```

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

`node-N/run_data/` 包含 RocksDB、Raft 状态和快照，不是构建缓存。

当前 Raft 端口固定，因此同一台机器不能同时运行两套本地集群。修改
`--gateway-port` 只会改变 Gateway 端口，不会改变 Raft 端口：

| 服务 | 默认地址 |
| --- | --- |
| Gateway | `127.0.0.1:8080` |
| node-0 shared RPC | `127.0.0.1:26200` |
| node-1 shared RPC | `127.0.0.1:26201` |
| node-2 shared RPC | `127.0.0.1:26202` |

三个 Region 在同一物理节点上共享一个 `NodeServer`、监听端口和 Muduo
`RpcProvider`；KV 与 Raft 请求通过协议中的 `RegionId` 分发到对应的轻量
`RegionPeer`。Region Peer 仍分别维护 Raft、MVCC、快照和 RocksDB 数据目录。

## 7. 测试与基准

### 7.1 CTest

```bash
ctest --test-dir build --output-on-failure
```

当前 CTest 自动执行 Pulsar Fiber 同步和有界线程池测试。集群测试
不会自动注册到 CTest，避免在普通构建过程中启动服务和写入持久化数据。

### 7.2 集群冒烟测试

```bash
STRATAKV_PROJECT=my-db bash deploy/stratakvctl verify
STRATAKV_PROJECT=my-db bash deploy/stratakvctl gateway-smoke
```

### 7.3 自动可靠性测试

可靠性脚本会启动独立项目、执行跨 Region 事务、重启一个承载 Leader 的节点，
然后重启整套集群验证持久化：

```bash
bash deploy/stratakv-reliability run \
  --transactions 10000 \
  --workers 16
```

由于 Raft 端口固定，运行前必须先停止其他 StrataKV 本地集群。测试报告位于：

```text
test-results/reliability/<run-id>/
```

成功结果同时包含：

```text
availability_failures=0
safety_violations=0
verification_failures=0
RELIABILITY PASS
result=PASS
```

完整参数：

```bash
bash deploy/stratakv-reliability --help
```

### 7.4 Pulsar 基准

运行完整的多轮协程基准：

```bash
bash deploy/stratakv-fiber-benchmark
```

该脚本会构建 Release 目标并把环境信息和原始结果写入
`test-results/fiber/<run-id>/`。性能结果必须连同机器、构建类型、负载参数和
原始输出一起解释。

## 8. 清理规则

可安全重新生成、且不应提交到 Git：

```text
build*/
bin/
lib/
test-results/
```

必须长期保留并提交：

```text
CMakeLists.txt
README.md
.gitmodules
src/
test/
deploy/（deploy/runtime/ 除外）
```

`deploy/runtime/` 是本地数据库与运行状态。删除它会丢失数据，不能按普通构建
缓存处理。

## 9. 常见问题

| 现象 | 检查与处理 |
| --- | --- |
| `muduo_net` 或 `muduo_base` 找不到 | 确认 Muduo 已安装，并检查动态库或静态库搜索路径 |
| `src/pulsar` 为空 | 执行 `git submodule update --init --recursive` |
| Gateway 端口被占用 | 使用 `ss -ltnp` 检查，或指定其他 `--gateway-port` |
| Raft 端口被占用 | 先停止同机运行的其他 StrataKV 集群 |
| 上次运行被强制中断 | 执行 `down` 清理受管进程；该命令不会删除数据 |
| 修改源码后仍运行旧逻辑 | 先 `down`，再执行 `rebuild` |
| 可靠性测试没有进度 | 检查 Leader、RPC timeout 和 `test-results` 中的运行日志 |

查看完整部署说明：[deploy/README.md](deploy/README.md)。

## 10. 当前边界

- Region 为静态配置，不支持动态 split/merge；
- 不支持认证、TLS、备份恢复、在线成员变更和多租户隔离；
- 高负载下的快照、选举稳定性和长尾延迟仍需继续验证；
- Gateway 与 C++ SDK 是业务入口，内部 Protobuf RPC 不承诺兼容性；
- 使用前应自行备份重要数据。

## 11. 相关项目与许可

- [Pulsar](https://github.com/FlyPeo/Pulsar)：用户态有栈协程、M:N 调度、
  epoll 和 Hook I/O 运行时。

本仓库当前未附带开源许可证；在添加明确许可证前，不默认授予复制、修改或
再分发权利。
