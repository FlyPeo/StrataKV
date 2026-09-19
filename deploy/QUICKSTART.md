# StrataKV 发布包快速上手

本包是自包含的二进制发行包:不需要源码、不需要 C++ 工具链、不需要手工安装
Boost/RocksDB/protobuf 等第三方库——运行所需的共享库已随包附带。

**平台要求**:glibc 兼容的 x86_64 Linux,与打包机同系列或更新的系统
(例如打包机是 Ubuntu 22.04,则目标机为 Ubuntu 22.04 及以上)。

## 包内布局:服务端与客户端分开放

```
stratakv-<version>-linux-x64/
├── server/                    ← 服务端:启动/管理集群
│   ├── bin/                   ← stratakv-node / tso / meta / admin
│   ├── scripts/               ← stratakv-server(启停入口)、stratakvctl
│   ├── config/                ← 拓扑配置模板
│   └── runtime/<project>/     ← 运行时数据(PID、日志、RocksDB)
├── client/                    ← 客户端:访问集群
│   └── bin/stratakv-client    ← 常驻命令行客户端
├── lib/  include/  cmake/     ← C++ SDK(静态库、头文件、包配置)
├── docs/QUICKSTART.md
└── VERSION
```

## 三步启动服务端

```bash
tar xzf stratakv-<version>-linux-x64.tar.gz
cd stratakv-<version>-linux-x64

# 1. 启动单机集群(3 数据节点 + 3 TSO 成员)
server/scripts/stratakv-server up --project my-db

# 2. 验证健康(3 TSO 成员、3 节点、9 Region 副本、4 个 Raft Leader)
server/scripts/stratakv-server verify --project my-db
```

服务端管理命令:`status`、`logs --project my-db [--node node-1]`、
`down --project my-db`(停服保数据)、`reset --project my-db`(停服删数据)。
完整说明见 `server/scripts/stratakv-server --help`。

## 客户端:常驻读写

```bash
client/bin/stratakv-client \
  server/runtime/my-db/regions.conf \
  127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302
```

连接一次、常驻复用。命令面与 SDK 能力对齐:

```
put <k> <v>                 单键写(自动提交,跨 Region 原子,冲突自动重试)
get <k>                     读
del <k>                     删
list [prefix]               列出(前缀过滤,快照一致性)
scan <start> <end> [limit]  原始范围扫描,空参数 = 无界
begin [lockTtlMs]           开启事务会话;之后 put/del 暂存、get 读己之写
commit / rollback           提交(跨 Region 原子)/ 丢弃;退出自动回滚
get-for-update <k>          悲观锁读(要求会话)
batch-get-for-update <k>..  批量锁读(要求会话)
lock-keys <k>...            只锁键不读值(要求会话)
multi put <k> <v> [...]     多键一个事务原子提交(跨分片,语法糖)
multi del <k> [...]         多键原子删除
txn-status                  查询会话事务状态
metrics                     客户端路由/重试指标
quit                        退出
```

也支持脚本式管道输入:

```bash
printf 'put hello world\nget hello\nlist\nquit\n' | \
  client/bin/stratakv-client server/runtime/my-db/regions.conf \
  127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302
```

运维排查(原始 Region 视角,含内部键)仍用 `server/bin/stratakv-admin`,
业务读写一律用 `client/bin/stratakv-client`。

## 用 C++ SDK 读写

包根含静态库 `lib/`、头文件 `include/` 与 CMake 包配置,在自己的工程里:

```cmake
find_package(stratakv REQUIRED)
target_link_libraries(my_app PRIVATE stratakv::sdk)
```

配置构建时把包根加入搜索路径:

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/stratakv-<version>-linux-x64
```

`client/examples/` 里有一个完整的可构建示例(事务 put + get 回读):

```bash
cmake -S client/examples -B /tmp/example-build -DCMAKE_PREFIX_PATH="$PWD"
cmake --build /tmp/example-build
/tmp/example-build/stratakv_sdk_example \
  server/runtime/my-db/regions.conf 127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302
```

SDK 事务核心用法(范围扫描同样可用,`Client::Scan(tx, start, end, limit)`):

```cpp
#include <stratakv/client.h>

stratakv::ConnectionOptions options;
options.topologyMode = stratakv::TopologyMode::kStatic;
options.regionConfigPath = "server/runtime/my-db/regions.conf";
options.tsoEndpoints = "127.0.0.1:26300,127.0.0.1:26301,127.0.0.1:26302";
auto client = stratakv::Client::Connect(options);

auto tx = client->Begin();
client->Put(tx, "key", "value");
client->Commit(tx);
```

## 端口与数据位置

| 项目 | 位置或端口 |
| --- | --- |
| 运行时数据 | `<包根>/server/runtime/<project>/` |
| 节点共享 RPC | `127.0.0.1:26200`–`26202` |
| TSO 控制面 | `127.0.0.1:26300`–`26302`(可用 `--tso-port` 调整) |

## 已知限制

- 同一台机器同一时刻只能运行一套使用默认节点端口(26200–26202)的集群。
- 包目录需要可写(server/runtime 写运行数据);只读安装请整体拷贝到可写位置。
- 发布包内没有源码,`rebuild` 不可用;升级请解压新版本包。数据目录结构
  与源码树部署一致,可直接迁移。
- `list` 是快照范围扫描:遇到范围内未完成事务的锁会整体返回可重试冲突,
  稍后重试即可;大库全量 list 请改用前缀分批。
- 动态拓扑(`--topology-mode dynamic`)会额外使用 26580–26582 端口;新集群的
  自动调度器(自动分裂/副本修复/均衡)**默认开启**,无需 `balancer-enable`;
  不想被自动管理时 `server/bin/stratakv-admin balancer-disable`(需设置
  `STRATAKV_TOPOLOGY_MODE=dynamic` 与 `STRATAKV_METADATA_ENDPOINTS`),该选择
  随集群持久化。
