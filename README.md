# StrataKV

StrataKV 是一个面向学习、开发和可靠性验证的分布式事务 KV 系统。它使用
Raft 维护 Region 内的多副本一致性，在其上实现 MVCC、Region 路由和
Primary-First 2PC，业务程序通过原生 C++ SDK 直连数据节点读写。

项目分两个阶段演进：v1 以静态三 Region（`regions.conf`）跑通跨 Region
事务主链路，但 Region 边界固定、无 epoch、不能分裂或迁移副本；这些边界
驱动了 v2 的动态拓扑——三副本元数据控制面（`stratakv-meta`）、在线
Region 分裂（崩溃安全 Checkpoint 物化）、SDK 动态路由与 Epoch 自愈、
Learner 副本迁移和自动均衡调度框架（执行注入点待接线，当前以手动
split 为主）。两种拓扑通过 `--topology-mode static|dynamic` 显式选择。

当前版本定位为 **Developer Edition**：适合 Linux/WSL 本地部署、功能演示、
源码学习和故障测试，不应直接承载生产数据。

## 1. 架构与能力

```text
   业务程序 / C++ SDK（RegionCache 动态路由 + Epoch 自愈）
                  |
                  v
 [动态模式] stratakv-meta 元数据集群 ×3（拓扑权威：分裂/迁移/调度）
                  |
                  v
     Region 路由（静态固定 / 动态 epoch）+ MVCC + 2PC
                  |
                  v
      3 个本地节点 × 每个 Region 3 个 Raft 副本
                  |
                  v
               RocksDB
```

当前已实现：

- Region 路由：静态模式固定为 `["", "h")`、`["h", "p")`、`["p", +∞)`，
  动态模式由元数据集群权威维护并支持在线分裂；
- 每个 Region 三个 Raft 副本，动态模式支持 Learner 副本在线迁移；
- RocksDB 本地持久化、Raft 状态持久化和快照；
- MVCC、乐观事务、悲观锁和 Primary-First 2PC；
- 原生 C++ SDK（RegionCache 动态路由、Epoch 自愈）、`stratakv-admin`
  运维工具和三副本元数据控制面；
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

| 依赖                | CMake/链接名称                                                 | 项目中的用途                                                           | 获取方式                                                 |
| ------------------- | -------------------------------------------------------------- | ---------------------------------------------------------------------- | -------------------------------------------------------- |
| Pulsar              | `Pulsar::pulsar`                                             | Fiber、per-worker deque/work stealing 调度器、epoll、Timer 和 Hook I/O | Git submodule：`src/pulsar`                            |
| Boost.Context       | `Boost::context`                                             | Pulsar Fiber 的原生`fcontext` 上下文切换                             | `libboost-context-dev`                                 |
| Protobuf            | `find_package(Protobuf REQUIRED)`、`${Protobuf_LIBRARIES}` | Raft/KV RPC 消息、Service、Stub 和反射分发                             | `libprotobuf-dev`；修改 `.proto` 时还需要 `protoc` |
| RocksDB             | `rocksdb`                                                    | 唯一的本地 KV 持久化引擎、WriteBatch 和前缀扫描                        | `librocksdb-dev`                                       |
| Boost.Serialization | `boost_serialization`                                        | Raft 状态、快照和 RocksDB 快照数据的序列化                             | `libboost-serialization-dev`                           |
| Muduo               | `muduo_net`、`muduo_base`                                  | TCP Server、EventLoop、连接与 Buffer                                   | 安装 Muduo 头文件及`libmuduo_net/base`                 |
| POSIX Threads       | `pthread`                                                    | 节点、元数据、SDK 和测试的多线程执行                                  | Linux 系统线程库                                         |
| Dynamic Loader      | `dl`                                                         | Pulsar 使用`dlsym` 解析被 Hook 的原始系统调用                        | Linux`libdl`                                           |

Pulsar 是仓库内的子模块，直接依赖 Boost.Context；默认使用原生 `fcontext`，
不会配置 `BOOST_USE_UCONTEXT`。C++ 标准库、Linux socket/epoll、文件系统和
进程接口属于系统能力，不是额外仓库依赖。

Pulsar 曾作为历史 HTTP 接入层的可选网络运行时，该层已随架构演进移除。
Pulsar 是独立的协程运行时，从未承载 SDK、2PC、同步 RPC、Raft 或
RocksDB 执行。

#### 构建与运行工具

| 工具                            | 是否必需     | 用途                                        |
| ------------------------------- | ------------ | ------------------------------------------- |
| C++17 编译器、CMake、Make/Ninja | 构建必需     | 配置和编译所有目标                          |
| Bash、GNU coreutils、util-linux | 本地部署必需 | 脚本、`setsid`、`taskset`、`lscpu` 等 |
| curl                            | 可靠性测试必需 | 可靠性测试脚本的前置依赖检查                |
| iproute2 (`ss`)               | 排障可选     | 检查端口占用                                |
| GDB                             | 调试可选     | 线程、堆栈和崩溃分析                        |

当前 CMake 要求 Boost.Context 1.61 或更新版本；没有为 RocksDB、Muduo
设置项目自定义的最低版本，README 因此不声明未经验证的版本下限。

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
  libboost-context-dev \
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
cmake --preset release
cmake --build --preset release -j"$(nproc)"
ctest --preset release
```

所有 CMake 中间文件统一位于 `build/`，并按项目和配置分类；例如主项目的
Release、Debug 和 ASan 构建分别位于 `build/stratakv/release`、
`build/stratakv/debug` 和 `build/stratakv/asan`。最终产物输出到源码目录中的
`bin/` 和 `lib/`。当前 CTest 注册了 48 项测试，
覆盖 Fiber 上下文/同步、Scheduler work stealing 与线程亲和、有界线程池、栈池
与回调 Fiber 缓存复用、事务调度、TSO、TimestampOracle、事务协调器、性能
测试体系（A1–A4/B3/C1/C2）和 SDK contract；集群可靠性测试需要单独
运行，见第 7 节。

### 2.4 启动三节点本地集群

```bash
bash deploy/stratakv-server up \
  --project my-db \
  --nodes 3 \
  --replicas 3
```

该命令会自动构建并启动三个 `stratakv-node` 和三个 `stratakv-tso`，按
`regions.conf` 创建 Region 100、101、102。运行配置、PID、日志和
数据库保存在 `deploy/runtime/my-db/`。如果已经完成构建，可以追加
`--no-build` 跳过编译：

```bash
bash deploy/stratakv-server up --project my-db --no-build
```

默认 `static` 模式适合验证跨 Region 事务主链路；需要元数据控制面、
在线分裂与副本迁移时，追加 `--topology-mode dynamic --metadata-port
26580` 启动，详见 2.4.1。

#### 2.4.1 动态拓扑模式（元数据路由）

默认 `static` 模式保持既有行为：Region 边界来自 `regions.conf`，SDK
与节点启动时一次性加载，任何路由故障折叠为 `ErrWrongLeader` /
`StorageError`。`dynamic` 模式引入三副本元数据控制面
（`stratakv-meta`）作为 Region 拓扑的权威来源：

```bash
bash deploy/stratakv-server up \
  --project my-db \
  --topology-mode dynamic \
  --metadata-port 26580
```

启动顺序：TSO → 三副本 metadata（用生成的 `regions.conf`
幂等 bootstrap 一次）→ 三个存储节点（从同一元数据修订版引导全部
RegionPeer，任一失败则整体不发布并报告失败 Region）。
SDK 从元数据扫描引导 RegionCache，
之后稳态请求不再访问元数据；缓存过期或收到 epoch 类错误时按
调用方 deadline 刷新并重试。

- `--topology-mode static|dynamic`（默认 static）：显式选择，元数据
  不可用时绝不静默回落到 `regions.conf`。
- `--metadata-port N`（默认 26580）：三个 metadata 成员占用
  N、N+1、N+2。
- SDK 侧等价参数：`Client::Connect(ConnectionOptions)` 的
  `topologyMode` / `metadataEndpoints` / `metadataTimeoutMs`。
- admin CLI（`stratakv-admin`）侧等价环境变量：`STRATAKV_TOPOLOGY_MODE`、
  `STRATAKV_METADATA_ENDPOINTS`、`STRATAKV_METADATA_TIMEOUT_MS`。

结构化路由错误（`RegionResponseHeader.Error`）区分
`NOT_LEADER`（只换 Leader 提示）、`EPOCH_NOT_MATCH` /
`REGION_NOT_FOUND` / `PEER_NOT_FOUND` / `KEY_NOT_IN_REGION`
（刷新路由后重试）与存储类失败（不下沉到路由层）。回滚规则：
在尚未发生过任何动态拓扑变更（分裂/成员变更）之前，可以直接停掉
metadata 并用 static 模式重启全部进程；Region RocksDB 与 TSO 状态
不受影响。相关性能与正确性证据见第 7 节与本变更的 OpenSpec 记录。

#### 2.4.2 动态 Region 分裂（Crash-Safe Region Split）

动态模式支持在集群运行时对指定 Region 执行在线分裂：

```bash
# 配置元数据端点环境变量（或由启动脚本自动注入）
export STRATAKV_METADATA_ENDPOINTS="127.0.0.1:26580,127.0.0.1:26581,127.0.0.1:26582"

# 将 Region 100 在 split_key "b" 处分裂（父 Region 保留 ["", "b")，子 Region 拥有 ["b", "h")）
bin/stratakv-admin split-region 100 b
```

- **前置条件与约束**：
  - 仅在 `dynamic` 拓扑模式下可用；命令执行前会强制校验 `STRATAKV_METADATA_ENDPOINTS`。
  - `split_key` 必须严格位于该 Region 的半开区间 `(start_key, end_key)` 内部。
  - 父 Region 当前不能存在尚未完成的 In-Flight 分裂。
  - **静态回滚窗口永久关闭**：一旦集群成功发生过任一次 Region 分裂，因新增子 Region 的元数据与数据目录脱离了静态 `regions.conf` 的固定定义，**永久不可再回退为 `static` 模式启动**。
- **两阶段元数据与本地阶段定义**：
  - 元数据两阶段提交：阶段一 `PrepareSplit` 分配全局唯一的子 Region/Peer ID 并标记父 Region `splitPending`；阶段二 `CommitSplit` 原子替换拓扑（父区间收缩且 `epoch.version + 1`，子区间新增且 epoch 为 `{1, 1}`）。
  - 副本本地五阶段状态机（`RegionLocalState`）：
    - `0: NotStarted`：分裂尚未开始；
    - `1: Checkpointing`：父 Region RocksDB 打快照克隆至子 generation 目录（`.pending` 临时目录）；
    - `2: ChildReady`：Checkpoint 完成并原子 rename，写入持久化阶段；
    - `3: ParentShrunk`：父 Region 内存描述符与 epoch 原子收缩，并后台执行有界 `DeleteRange` 清理右半区间数据；
    - `4: Complete`：本地子 RegionPeer 启动并经 `RegionRegistry` 原子发布，父子均处于服务状态。
- **前滚恢复原则（Forward-Progress Recovery）**：
  - 分裂在 Raft 日志中以 `AdminSplit` 保证各副本在相同日志位点确定性生效。
  - 节点崩溃重启时遵循**仅向前推进、不回滚已提交分裂**原则：
    - 若持久化状态处于 `Checkpointing`，废弃 `.pending` 残留目录并重新 checkpoint；
    - 若持久化状态处于 `ChildReady`，直接跳过 checkpoint 进入父区间收缩与子节点拉起；
    - 若持久化状态为 `Complete`，重放 AdminSplit 时判定为幂等 no-op，避免重复清理数据或创建重复 Peer。
- **可观测性与安全规范**：
  - 指标与日志严禁打印任何业务原始 key 或 value；日志与指标中仅输出 `RegionId`、`peer_id`、`epoch`、`revision` 与 `phase`。
  - 分裂过程中路由层若遇到旧 epoch 请求，返回携带最新拓扑的 `EPOCH_NOT_MATCH`，驱动客户端 SDK 执行缓存更新并在边界跨越时自动重分组（Regroup），事务保持原 `start_ts`、`primary_key` 与 `commit_ts` 语义不丢失。

#### 2.4.3 在线副本迁移（Online Replica Migration）

动态模式可将一个 Region 的副本从源 Store 在线迁移到目标 Store：

```bash
export STRATAKV_TOPOLOGY_MODE=dynamic
export STRATAKV_METADATA_ENDPOINTS="127.0.0.1:26580,127.0.0.1:26581,127.0.0.1:26582"

# 将 Region 100 在 Store 1 上的副本迁移到 Store 4
bin/stratakv-admin move-peer 100 1 4
```

命令会依次输出 `PreparingTarget`、`CatchingUp`、`Promoted`、
`RetiringSource`、`Complete` 阶段，并报告 `region_id`、`peer_id`、
`store_id`、epoch、日志落后量和已传输字节数。可通过
`STRATAKV_MIGRATION_TIMEOUT_MS` 设置总超时（默认 120 秒）。

- MetaServer 用持久化 peer ID 高水位分配全新 ID，并用 mutation ID 保证
  Prepare/Commit/Cancel 在网络超时或换主后的幂等重放。
- 新副本先以不参与选举和提交 quorum 的 Learner 挂载；保留日志时走
  AppendEntries，跨越 GC 边界时走 256 KiB 分块 InstallSnapshot；落后量
  不超过 100 条后才提拔为 Voter。
- 同一 Region 只允许一笔未 Apply 的 ConfChange。AddLearner、
  PromoteLearner、RemovePeer 分别推进一次 `epoch.conf_version`，最终元数据
  发布与数据面的 epoch 保持一致。
- 被移除副本先从 RegionRegistry 原子注销并排空在途请求，再停止并 join
  Raft/Apply/GC 后台线程、关闭 RocksDB，最后异步删除本地目录。
- 客户端收到 `EPOCH_NOT_MATCH` 会刷新 RegionCache；`NOT_LEADER` 若指向缓存
  中尚不存在的新 peer，也会按拓扑变化刷新后重试。请求 ID、事务
  `start_ts`/`commit_ts` 在重试中保持不变。
- 提拔前目标故障会移除临时 Learner 并提交 `CancelMovePeer`，原 voter quorum
  不变；提拔后若协调器退出，可用相同命令参数命中幂等状态并继续前滚。

迁移只支持 `dynamic` 模式。执行过成员变更后，不应再用旧的静态
`regions.conf` 启动该集群。

#### 2.4.4 自动均衡与自动分裂调度器（Auto-Balancer & Auto-Split Scheduler）

在动态模式下，StrataKV 提供了由控制面 Metadata Leader 驱动的自动调度器，支持实时监控各 Store/Region 心跳与容量，自动执行副本安全修复、容量倾斜均衡以及热点/大 Region 的自动分裂：

```bash
export STRATAKV_TOPOLOGY_MODE=dynamic
export STRATAKV_METADATA_ENDPOINTS="127.0.0.1:26580,127.0.0.1:26581,127.0.0.1:26582"

# 查看当前自动均衡器状态、配置、节点心跳及活跃算子
bin/stratakv-admin balancer-status

# 启用 / 禁用自动调度器（默认处于禁用状态）
bin/stratakv-admin balancer-enable
bin/stratakv-admin balancer-disable

# 暂停 / 恢复自动调度评估（暂停期间允许在途算子安全完成，不再接纳新算子）
bin/stratakv-admin balancer-pause
bin/stratakv-admin balancer-resume

# 演练模式：基于当前元数据与心跳快照输出确定性的动作推荐与拒绝原因，不产生任何副作用
bin/stratakv-admin balancer-dry-run

# 取消在途调度算子（仅允许在提拔不可逆点之前取消）
bin/stratakv-admin balancer-cancel <operator_id>
```

- **心跳遥测与安全规范**：
  - 各 `NodeServer` 定期汇总本地 Store 容量及各 Region 副本大小，心跳协议与日志严禁携带任何原始 key/value，仅传输 `splitCandidateGeneration` 与容量计数。
  - 心跳报告支持有界合并与按序列号单调递增，过时样本自动丢弃。
- **确定性规划与迟滞保护**：
  - 调度策略优先执行**副本安全修复（SafetyRepair）**：若副本所在 Store 离线或心跳超时，自动挑选健康节点迁移副本，确保法定人数安全；
  - 自动均衡（Balance）受 `imbalanceThreshold` 迟滞约束，避免微小抖动造成频繁搬迁；支持单个 Region 冷却时间（`regionCooldownMs`）和并发限额（`maximumActiveOperators`, `maximumActivePerStore`）；
  - 自动分裂（AutomaticSplit）需满足连续多个评估窗口（`splitConsecutiveWindows`）持续超标，避免瞬时流量尖刺误触发分裂。
- **不可逆点与主从切换恢复**：
  - 调度算子经历 `PENDING -> DISPATCHING -> WAITING -> SUCCEEDED` 严格单向状态推进；
  - 在 Learner 提拔为 Voter 前属于可逆阶段，支持安全取消；一旦跨越不可逆点（`irreversible = true`），拒绝取消并强制前滚，杜绝配置回滚导致的脑裂；
  - Leader 切换后，新 Leader 从已提交的元数据日志重构在途算子并接管驱动，旧 Leader 自动停止派发新工作。

### 2.5 检查集群

```bash
bash deploy/stratakv-server status --project my-db
bash deploy/stratakv-server verify --project my-db
```

正常情况下：

- `node-0`～`node-2` 与 `tso-0`～`tso-2` 均为 `running`（动态模式下
  `meta-0`～`meta-2` 由同一脚本拉起并管理）；
- TSO control plane 显示 `leaders=1`；
- Region 100、101、102 均显示 `leaders=1`；
- `verify` 输出 `Verification passed`。

### 2.6 读写与事务验证

读写入口是原生 C++ SDK（见第 5 节），没有独立的业务 CLI。跨 Region
事务主链路——三个 Key 分别路由到 Region 100、101、102 并作为同一事务
提交——由 SDK contract 测试与下述压测端到端覆盖：

```bash
# 动态分片分裂韧性压测：拉起动态集群、持续事务流量、在线分裂并校验零丢失
bash deploy/stratakv-split-benchmark

# 性能与正确性套件（interview-smoke 配置）
bash deploy/stratakv-performance all --profile interview-smoke
```

开发与运维命令（`put`/`get`、`split-region`、`move-peer`、`balancer-*`）
由 `bin/stratakv-admin` 直连内部 RPC 提供；各测试脚本的职责见
[`test/README.md`](test/README.md)。

### 2.7 停止或删除

停止进程并保留数据：

```bash
bash deploy/stratakv-server down --project my-db
```

使用原数据重新启动：

```bash
bash deploy/stratakv-server up --project my-db --no-build
```

永久删除该项目的运行配置、日志和数据库：

```bash
bash deploy/stratakv-server reset --project my-db
```

`reset` 不可恢复；只想停止服务时必须使用 `down`。

## 3. 程序与源码布局

### 3.1 可执行程序

| 程序                                      | 用途                                                                |
| ----------------------------------------- | ------------------------------------------------------------------- |
| `bin/stratakv-node`                     | 数据节点，承载 Region、Raft 副本和 RocksDB                          |
| `bin/stratakv-tso`                      | TSO 控制层成员，以 Raft 提交 high-water range 并在有效 fence 内发号 |
| `bin/stratakv-meta`                     | 元数据控制面成员，维护 Region 拓扑权威与调度元信息                  |
| `bin/stratakv-admin`                    | 直连内部 RPC 的开发与运维工具                                       |
| `bin/stratakv-test-fiber-sync`          | Pulsar 同步原语正确性测试                                           |
| `bin/stratakv-test-bounded-thread-pool` | 有界线程池与过载背压正确性测试                                      |
| `bin/stratakv-test-tso`                 | 多客户端全局时间戳与崩溃重启测试                                    |
| `bin/stratakv-test-tso-range-benchmark` | range=1/4096 的三成员 TSO 同条件 A/B                                |
| `bin/stratakv-test-fiber-benchmark`     | Pulsar 性能与压力基准                                               |
| `bin/stratakv-test-reliability`         | 集群事务与持久化验证负载                                            |

### 3.2 源码目录

| 目录                | 职责                                  |
| ------------------- | ------------------------------------- |
| `src/storage`     | RocksDB 适配与存储抽象                |
| `src/raft`        | Raft 共识、状态机和 Raft 持久化       |
| `src/transaction` | MVCC、锁、时间戳、路由和 2PC          |
| `src/rpc`         | 自研 Protobuf RPC 传输与服务分发      |
| `src/proto`       | Raft 与 KV 的 Protobuf 契约及生成代码 |
| `src/server`      | 存储节点入口                          |
| `src/tso`         | TSO 控制层入口与 Raft 发号            |
| `src/metadata`    | 元数据控制面：拓扑权威、分裂与迁移    |
| `src/admin`       | 内部管理工具入口                      |
| `src/sdk`         | C++ 事务客户端 SDK                    |
| `src/common`      | 公共配置和工具                        |
| `src/pulsar`      | Pulsar 协程运行时子模块               |
| `test`            | 正确性、基准和可靠性测试源码          |
| `deploy`          | 本地部署、运维和测试脚本              |

## 4. 开发构建

### Release

```bash
cmake --preset release
cmake --build --preset release -j"$(nproc)"
```

### Debug

```bash
cmake --preset debug
cmake --build --preset debug -j"$(nproc)"
```

### AddressSanitizer

```bash
cmake --preset asan
cmake --build --preset asan -j"$(nproc)"
```

所有新构建都应使用上述 Preset，避免在仓库根目录新增 `build-*`。各配置仍会把
最终程序写入同一个 `bin/`，静态库写入同一个 `lib/`。
不要在集群运行时切换构建类型或覆盖正在运行的程序；应先执行：

```bash
bash deploy/stratakv-server down --project my-db
```

常用单独目标：

```bash
cmake --build --preset release --target stratakv-node
cmake --build --preset release --target stratakv-tso
cmake --build --preset release --target stratakv-meta
cmake --build --preset release --target stratakv-admin
cmake --build --preset release --target stratakv_sdk
cmake --build --preset release --target stratakv-test-fiber-sync
cmake --build --preset release --target stratakv-test-bounded-thread-pool
cmake --build --preset release --target stratakv-test-tso
cmake --build --preset release --target stratakv-test-tso-range-benchmark
cmake --build --preset release --target stratakv-test-fiber-benchmark
cmake --build --preset release --target stratakv-test-reliability
```

修改源码后停止进程、重新构建并用原数据启动：

```bash
bash deploy/stratakv-server rebuild --project my-db
```

## 5. C++ SDK

C++ SDK 的入口为 `stratakv/client.h`：

```cpp
#include <stratakv/client.h>

stratakv::ConnectionOptions options;
options.regionConfigPath = "deploy/runtime/my-db/regions.conf";
options.tsoEndpoints =
    "127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302";
auto client = stratakv::Client::Connect(options);
auto txn = client->Begin();
auto put = client->Put(txn, "apple:1", "value-a");
if (put.ok()) {
  auto commit = client->Commit(txn);
}
```

SDK 不在客户端本地发号；`Begin` 和 `Commit` 使用的时间戳都来自 TSO Leader。
SDK 缓存最近成功的 Leader，并在连接失败或收到 `NotLeader` 时轮询三个控制层
端点。动态模式改用 `topologyMode = TopologyMode::kDynamic` 与
`metadataEndpoints`（如 `127.0.0.1:26580,127.0.0.1:26581,127.0.0.1:26582`）：
SDK 从元数据集群扫描引导 RegionCache，稳态请求直连数据节点，收到
`EPOCH_NOT_MATCH` 等 epoch 类错误时刷新缓存并在新拓扑上重试。

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

# 重启一个 TSO 成员；若它是 Leader，另外两个成员会自动选主
bash deploy/stratakv-server restart-tso --project my-db --node tso-0

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
├── meta-0/            （动态模式）
├── meta-1/            （动态模式）
├── meta-2/            （动态模式）
├── tso-0/{tso.state,run_data/}
├── tso-1/{tso.state,run_data/}
├── tso-2/{tso.state,run_data/}
├── node-0/run_data/
├── node-1/run_data/
└── node-2/run_data/
```

`node-N/run_data/` 包含 RocksDB、Raft 状态和快照，不是构建缓存。
`tso-N/tso.state` 只保存该成员的 crash-safe candidate floor；集群权威 high-water
位于 `tso-N/run_data/` 的 Raft 日志和快照。停止集群时二者都必须与节点数据一起保留。

数据节点的 shared RPC 端口固定，因此同一台机器不能同时运行两套本地集群；
`--tso-port` 与 `--metadata-port` 只改变对应控制面的起始端口：

| 服务                       | 默认地址                       |
| -------------------------- | ------------------------------ |
| TSO control plane          | `127.0.0.1:26300`～`26302` |
| 元数据集群（动态模式）     | `127.0.0.1:26580`～`26582` |
| node-0 shared RPC          | `127.0.0.1:26200`            |
| node-1 shared RPC          | `127.0.0.1:26201`            |
| node-2 shared RPC          | `127.0.0.1:26202`            |

三个 Region 在同一物理节点上共享一个 `NodeServer`、监听端口和 Muduo
`RpcProvider`；KV 与 Raft 请求通过协议中的 `RegionId` 分发到对应的轻量
`RegionPeer`。Region Peer 仍分别维护 Raft、MVCC、快照和 RocksDB 数据目录。
三个 `stratakv-tso` 进程组成独立的控制层 Raft Group，同一时刻只有 Leader 发号；
Raft 一次提交一段 high-water range，Leader 在 150 ms 多数派 fence 有效且区间未耗尽
时以原子递增发号。Leader 故障后新 Leader 跳过旧活动区间；失去 quorum 后即使本地
仍有余额也停止服务。该控制层只负责全局时间戳，不参与 Region 路由或调度。

## 7. 测试与基准

### 7.1 CTest

```bash
ctest --preset release
```

当前 CTest 自动执行 Pulsar Fiber 切换/reset/异常边界、Fiber 同步、Scheduler work stealing/线程亲和、有界线程池、事务调度，以及 TSO 多 range 并发、
Observe、stale/crashed Leader、未提交 reservation、少数派 fence、Snapshot 和全控制层重启测试。集群测试
不会自动注册到 CTest，避免在普通构建过程中启动服务和写入持久化数据。每个测试
文件的职责和运行方式见 [`test/README.md`](test/README.md)。

### 7.2 集群冒烟与一键压测

```bash
STRATAKV_PROJECT=my-db bash deploy/stratakvctl verify

# 动态分片分裂韧性压测：拉起 3 Metadata + 3 TSO + 3 Node 的动态集群，
# 稳态注入在线 Region 分裂，度量 QPS 抖动与尾延迟并校验零丢失
bash deploy/stratakv-split-benchmark

# 性能与正确性套件；--profile 支持 interview-smoke（默认）、
# interview-full 与 interview-full-10pct
bash deploy/stratakv-performance all --profile interview-smoke
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

### 7.4 TSO range 基准

```bash
cmake --build --preset release --target stratakv-test-tso-range-benchmark
./bin/stratakv-test-tso-range-benchmark \
  --requests 2048 --clients 8 --warmup 128 \
  --baseline-range 1 --optimized-range 4096
```

该自包含基准为每组启动三个临时 TSO 进程，只改变 range size，并输出吞吐、
P50/P95/P99、Raft proposal 数和实际 allocations/reservation。正式数据与边界见
`Docs/8-性能报告/2026-09-03-TSO-Range预留优化.md`。

部署时可通过 `--tso-range-size N` 修改默认的 4096；一般只在 A/B 或诊断时调整。

### 7.5 Pulsar 基准

运行完整的多轮协程基准：

```bash
bash deploy/stratakv-fiber-benchmark
```

该脚本会构建 Release 目标并把环境信息和原始结果写入
`test-results/fiber/<run-id>/`。性能结果必须连同机器、构建类型、负载参数和
原始输出一起解释。2026-08-28 的 ucontext 迁移前基线与 Boost.Context/fcontext
迁移后对比见 `Docs/8-性能报告/2026-08-28-Boost.Context迁移.md`。

## 8. 清理规则

可安全重新生成、且不应提交到 Git：

```text
build/
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

| 现象                                   | 检查与处理                                               |
| -------------------------------------- | -------------------------------------------------------- |
| `muduo_net` 或 `muduo_base` 找不到 | 确认 Muduo 已安装，并检查动态库或静态库搜索路径          |
| `src/pulsar` 为空                    | 执行`git submodule update --init --recursive`          |
| Raft 端口被占用                        | 先停止同机运行的其他 StrataKV 集群                       |
| 上次运行被强制中断                     | 执行`down` 清理受管进程；该命令不会删除数据            |
| 修改源码后仍运行旧逻辑                 | 先`down`，再执行 `rebuild`                           |
| 可靠性测试没有进度                     | 检查 Leader、RPC timeout 和`test-results` 中的运行日志 |

查看完整部署说明：[deploy/README.md](deploy/README.md)。

## 10. 当前边界

- 静态模式 Region 固定三段，不能分裂、无 epoch、无副本迁移；动态模式的
  自动调度器执行注入点尚待接线，分裂/迁移当前以手动命令为主；
- 不支持认证、TLS、备份恢复和多租户隔离；
- 高负载下的快照、选举稳定性和长尾延迟仍需继续验证；
- C++ SDK 是唯一业务入口，内部 Protobuf RPC 不承诺兼容性；
- 使用前应自行备份重要数据。

## 11. 相关项目与许可

- [Pulsar](https://github.com/FlyPeo/Pulsar)：用户态有栈协程、每 Worker deque、
  work stealing、M:N 调度、epoll 和 Hook I/O 运行时。

本仓库当前未附带开源许可证；在添加明确许可证前，不默认授予复制、修改或
再分发权利。
