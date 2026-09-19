# StrataKV 发布包快速上手

本包是自包含的二进制发行包:不需要源码、不需要 C++ 工具链、不需要手工安装
Boost/RocksDB/protobuf 等第三方库——运行所需的共享库已随包附带。

**平台要求**:glibc 兼容的 x86_64 Linux,与打包机同系列或更新的系统
(例如打包机是 Ubuntu 22.04,则目标机为 Ubuntu 22.04 及以上)。

## 三步启动

```bash
tar xzf stratakv-<version>-linux-x64.tar.gz
cd stratakv-<version>-linux-x64

# 1. 启动单机集群(3 数据节点 + 3 TSO 成员)
scripts/stratakv-server up --project my-db

# 2. 验证健康(3 TSO 成员、3 节点、9 Region 副本、4 个 Raft Leader)
scripts/stratakv-server verify --project my-db

# 3. 读写一条数据
STRATAKV_REGION_CONFIG=runtime/my-db/regions.conf \
  bin/stratakv-admin put hello world
STRATAKV_REGION_CONFIG=runtime/my-db/regions.conf \
  bin/stratakv-admin get hello
```

其他命令:`status`、`logs --project my-db [--node node-1]`、
`down --project my-db`(停服保数据)、`reset --project my-db`(停服删数据)。
完整说明见包内 `scripts/stratakv-server --help`。

## 用 C++ SDK 读写

包内含静态库 `lib/`、头文件 `include/` 与 CMake 包配置,在自己的工程里:

```cmake
find_package(stratakv REQUIRED)
target_link_libraries(my_app PRIVATE stratakv::sdk)
```

配置构建时把包根加入搜索路径:

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/stratakv-<version>-linux-x64
```

`examples/` 里有一个完整的可构建示例(事务 put + get 回读),配置运行时
参数后即可验证连通性:

```bash
cmake -S examples -B /tmp/example-build -DCMAKE_PREFIX_PATH="$PWD"
cmake --build /tmp/example-build
/tmp/example-build/stratakv_sdk_example \
  runtime/my-db/regions.conf 127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302
```

SDK 核心用法(事务型 API):

```cpp
#include <stratakv/client.h>

stratakv::ConnectionOptions options;
options.topologyMode = stratakv::TopologyMode::kStatic;
options.regionConfigPath = "runtime/my-db/regions.conf";
options.tsoEndpoints = "127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302";
auto client = stratakv::Client::Connect(options);

auto tx = client->Begin();
client->Put(tx, "key", "value");
client->Commit(tx);
```

## 端口与数据位置

| 项目 | 位置或端口 |
| --- | --- |
| 运行时数据 | `<包根>/runtime/<project>/` |
| 节点共享 RPC | `127.0.0.1:26200`–`26202` |
| TSO 控制面 | `127.0.0.1:26300`–`26302`(可用 `--tso-port` 调整) |

## 已知限制

- 同一台机器同一时刻只能运行一套使用默认节点端口(26200–26202)的集群。
- 包目录需要可写(runtime 数据、PID、日志写在其 `runtime/` 下);如需只读
  安装,请把整个包目录拷贝到可写位置再运行。
- 发布包内没有源码,`rebuild` 不可用;升级请解压新版本包。数据目录结构
  与源码树部署一致,可直接迁移。
- 动态拓扑(`--topology-mode dynamic`)会额外使用 26580–26582 端口。
