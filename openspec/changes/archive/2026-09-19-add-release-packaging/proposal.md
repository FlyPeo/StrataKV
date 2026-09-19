# Proposal: add-release-packaging

## Why

StrataKV 目前只能以"源码树 + 现场构建"的形态部署:`deploy/stratakv-server` 依赖源码根目录现场执行 CMake 构建,且二进制动态链接手工安装到 `/lib` 的 `librocksdb.so.6.11` 等系统库。内部同事在只有发布产物的机器上既无法部署集群,也无法把 C++ SDK 接进自己的程序。

本 change 面向内部使用场景,产出自包含的二进制发布包:采用**方案 B(捆绑运行时依赖)**,把第三方 `.so` 一起打进包内并用 `$ORIGIN` rpath 定位,使发布包在 glibc 兼容的 x86_64 Linux 上解压即用。部署目标为单机多进程,客户端仅提供 C++ SDK。

**Goals**

- 一条命令产出 `stratakv-<version>-linux-x64.tar.gz`:解压后无需源码和工具链即可 `up`/`verify` 集群,并用 SDK 编写客户端程序。
- 第三方运行时依赖(Boost.Context、RocksDB、protobuf 等)随包分发,目标机器不再需要手工安装 `/lib/librocksdb.so.6.11`。

**Non-Goals**

- 不做 deb/rpm、容器镜像、云产品或包仓库分发。
- 不做跨多台物理服务器的编排部署。
- 不新增独立 CLI 客户端或 HTTP 网关;`stratakv-admin` 维持运维工具定位。
- 不改变节点、Region、Raft、事务、MVCC 的任何运行时行为。

## What Changes

- CMake 打包基础:为项目引入语义化版本号;完善 `install()` 布局,输出到发行包目录(`bin/`、`lib/`、`include/`、`cmake/`、`config/`、`examples/`、`docs/`);生成 `stratakvConfig.cmake` 包配置,使 SDK 消费者通过 `find_package(stratakv)` + `stratakv::sdk` 自动获得全部静态库与传递链接依赖。
- 新增打包脚本 `deploy/stratakv-package`:Release 构建 → `cmake --install` 到 `dist/stratakv-<version>-linux-x64/` → 捆绑第三方运行时 `.so`(boost_context、boost_serialization、protobuf、rocksdb 及其传递依赖如 snappy、gflags、lz4、zstd 等)并为包内可执行文件设置 `$ORIGIN/../lib` rpath → 产出 `stratakv-<version>-linux-x64.tar.gz`。
- `deploy/stratakv-server` 解耦源码树:检测发行包布局时跳过构建步骤(发行包内 `--no-build` 语义成为默认),源码树内的现有构建行为保持不变。
- 新增 `QUICKSTART.md`(解压 → `up` → `verify` → SDK 写入三步走)和 SDK 链接示例 `examples/`(源码 + CMakeLists,演示 `find_package(stratakv)` 用法)。
- 发布验收:打包后在干净的临时目录解压发布包,完成 `up → verify → stratakv-admin put/get → down` 全流程,并从包外目录用示例程序编译链接 SDK 成功。

## Capabilities

### New Capabilities

- `release-packaging`:发布包的目录布局、自包含运行时依赖(rpath)、打包脚本行为、发行包模式下 `stratakv-server` 的免构建运行、SDK 消费契约(`find_package`/`stratakv::sdk`)与发布包验收要求。

### Modified Capabilities

(无——现有 specs 均描述运行时行为,本 change 不改变任何运行时需求。)

## Impact

- **受影响代码**:`CMakeLists.txt`(版本号、install 规则、rpath、包配置)、新增 `deploy/stratakv-package`、`deploy/stratakv-server`(新增发行包模式分支)、新增 `QUICKSTART.md` 与 `examples/`。
- **包内容物新增依赖**:捆绑 `libboost_context.so.1.74`、`libboost_serialization.so.1.74`、`libprotobuf.so.23`、`librocksdb.so.6.11` 及其传递 `.so`;目标机器仍需 glibc 兼容的 x86_64 Linux(打包机同系或更新即可)。
- **兼容性**:源码树内开发流程完全不变(`up` 默认仍自动构建);`stratakv-server` 现有参数与行为向后兼容,仅新增发行包检测分支;运行时二进制行为与性能不受影响。
- **回滚策略**:本 change 不触及运行时逻辑;CMake install 增量、打包脚本与文档可独立 git revert,已发布的 tar 包是纯产物,回滚只需停止使用。`stratakv-server` 的发行包分支在源码树内不激活,回归风险限于打包流程本身。
