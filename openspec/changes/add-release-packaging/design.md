# Design: add-release-packaging

## Context

见 proposal.md 的 Why。现有事实约束:

- 四个服务端二进制 + `libstratakv_sdk.a`(PUBLIC 依赖 `stratakv_core`、`stratakv_rpc`)已由根 CMakeLists 构建;`install()` 规则已有雏形(bin 四件 + `stratakv_sdk.a` + SDK 头),但没有版本号、没有包配置、没有第三方依赖处理。
- `deploy/stratakv-server` 从 `deploy_dir/..` 推导源码根并现场 CMake 构建;运行时数据固定在 `<源码根>/deploy/runtime/<project>/`。
- 实测 `ldd bin/stratakv-node`:非基础第三方 `.so` 共 8+ 个,其中 `librocksdb.so.6.11` 位于 `/lib`(手工安装,apt 不可得),boost 1.74 / protobuf 23 来自系统 apt。
- `src/sdk/include/stratakv/client.h` 只依赖标准库头(干净);重活都在 `.a` 的传递链接里。

本 change 不触及任何运行时并发与数据路径代码。

## Goals / Non-Goals

设计层目标:发行包在**无源码、无手工安装第三方库**的 glibc 兼容 x86_64 Linux 上可部署、可运行、可被 SDK 消费;打包流程可重复、失败要响亮。

设计层非目标:不做符号裁剪/stripping 策略(内部使用,保留符号便于排障)、不做跨发行版兼容矩阵、不引入 CPack(见 D5 的理由)。

## Decisions

### D1. 依赖收集:从构建产物推导 ldd 闭包,而非手工清单

打包脚本对包内每个 ELF 执行 `ldd`,按"基础库排除表"收集非基础 `.so` 到包内 `lib/`。排除表只含 glibc/编译运行时基础件:`libc`、`libm`、`libpthread`、`librt`、`libdl`、`ld-linux`、`linux-vdso`、`libstdc++`、`libgcc_s`。

- 备选:手工维护白名单。拒绝:RocksDB 的传递依赖(snappy/gflags/zstd/lz4/bz2/z)容易漏,依赖升级后清单腐烂,且与 spec 的"从构建产物推导"要求相悖。
- `libstdc++` 归入基础库:目标机 glibc 兼容则 libstdc++ 亦兼容,且替换系统 libstdc++ 反而有风险。

### D2. rpath:链接期 `INSTALL_RPATH = $ORIGIN/../lib`,不做打包后 ELF 改写

CMake 设 `CMAKE_INSTALL_RPATH` 为 `$ORIGIN/../lib`,只在 install 产物上生效,build tree 二进制不受影响。不使用 patchelf 事后改写,避免额外工具依赖和二次修改 ELF 的风险。

对 SDK 消费者:导出的 `stratakv::sdk` target 附加 `INTERFACE_LINK_OPTIONS` 中的 `-Wl,-rpath,$<INSTALL_PREFIX>/lib` 与 `INTERFACE_LINK_DIRECTORIES`,使消费者程序链接静态库后运行时同样从包内 `lib/` 解析第三方 `.so`,零枚举、零环境变量。

- 备选 a:要求消费者设置 `LD_LIBRARY_PATH`。拒绝:每个使用点都要配环境,examples 验收和真实使用都会踩坑。
- 备选 b:静态链接全部第三方库。拒绝:RocksDB 静态化需连带全量压缩库静态链,构建系统改动大,超出本 change 范围。
- 风险:向消费者注入 rpath 属于"有观点"的接口。内部使用可接受,风险区已记录,必要时消费者可自行覆盖。

### D3. 部署脚本双模式:自位置检测,发行包内默认免构建

`stratakv-server` 以脚本自身位置判定模式:若脚本所在目录的上级存在 `CMakeLists.txt` 即源码树模式(现状不变,默认自动构建);否则为发行包模式,跳过构建,直接使用包内 `bin/`。发行包内显式请求构建应报"发布包不含源码"的可执行错误;`--no-build` 在发行包内接受但为 no-op,保持参数兼容。

发行包布局(与源码树 `deploy/` 相对结构同构,脚本改动最小):

```
stratakv-<v>-linux-x64/
├── bin/                        # node / tso / meta / admin
├── lib/                        # 我方 .a + 捆绑第三方 .so
├── include/stratakv/client.h
├── cmake/                      # stratakvConfig.cmake + 导出 targets
├── scripts/                    # stratakv-server、stratakvctl
├── config/                     # regions.conf、cluster.conf 模板
├── examples/                   # sdk_example.cpp + CMakeLists.txt
├── docs/QUICKSTART.md
└── VERSION
```

发行包模式的运行时数据目录为 `<包根>/runtime/<project>/`(`scripts/../runtime/`),与源码树的 `deploy/runtime/<project>/` 结构逐项一致(PID/日志/regions.conf/RocksDB 数据),因此数据目录可直接在包与源码树之间搬移。包目录假定可写(内部使用,风险区记录)。

### D4. CMake 包配置:install(EXPORT) + 手写 stratakvConfig.cmake

- 项目版本:`project(... VERSION 1.0.0)` 作为首个发布包版本;归档名与包内 `VERSION` 文件同源于此。
- 导出:`install(TARGETS stratakv_sdk stratakv_core stratakv_rpc EXPORT stratakvTargets ARCHIVE DESTINATION lib)`,头文件整目录安装;`stratakvConfig.cmake` 模板 include `stratakvTargets.cmake` 并附加 D2 的接口链接目录/rpath 选项。消费者侧只需 `find_package(stratakv)` + `stratakv::sdk`。
- 备选:仅装 `stratakv_sdk.a` 让消费者自己找 core/rpc。拒绝:PUBLIC 传递依赖要求消费者枚举,违背 spec 的消费契约。

### D5. 打包脚本 `deploy/stratakv-package`:POSIX sh,内建验收门,不用 CPack

CPack 能产 tar.gz 但表达不了 ldd 闭包收集、验收门、失败日志保留这些流程控制,引入的抽象收益为负。脚本为纯 POSIX sh,与现有 deploy 脚本同风格。流程:

1. Release 构建(`cmake --preset release`,幂等,先清 `dist/<包名>`);
2. `cmake --install --prefix dist/stratakv-<v>-linux-x64/`;
3. D1 依赖收集 → 包内 `lib/`,`readelf -d` 抽查 RPATH 生效;
4. `tar czf` 产出归档;
5. **验收门**:临时目录解压 → `stratakv-server up`(独立项目名 `stratakv-package-acceptance`,避免污染用户项目)→ `verify` 等健康 → `stratakv-admin` put/get 回读 → 包外目录用 `-DCMAKE_PREFIX_PATH=<包>` 构建 examples 并连集群读写 → `down`;
6. 清理:无论成败,停止验收集群、删除临时项目;失败时保留验收日志目录并在 stderr 打印路径。

任何一步失败即退出非零且不产出"可发布"结论。

## Risks / Trade-offs

- [目标机 glibc 版本低于打包机时无法运行] → QUICKSTART 声明平台要求(与打包机同系或更新);acceptance 在本机执行,跨机风险明确记录,不做兼容矩阵。
- [ldd 闭包漏收 dlopen 式依赖] → 排除表收紧(宁多收勿漏收);验收门的真实 put/get 会触发 RocksDB 压缩路径,dlopen 类问题在此暴露。
- [向 SDK 消费者注入 rpath 的接口观点] → 内部使用接受;rpath 可被消费者追加的 rpath 与 `LD_LIBRARY_PATH` 叠加覆盖,不锁死。
- [源码树部署行为回归] → 源码树分支代码路径不动,仅新增检测分支;任务中包含源码树 `up/verify` 回归项。
- [包目录被只读挂载导致 runtime 数据无法写入] → 记录为已知限制(QUICKSTART 注明),需要时以整体搬移包目录或未来增加 `--runtime-root` 解决,本 change 不扩参数。

## Migration Plan

无数据迁移、无运行时行为变更。落地顺序:CMake 版本与 install/export → 脚本双模式 → 打包脚本与 examples/QUICKSTART → 验收门跑通。回滚:全部增量可独立 `git revert`;已分发的 tar 包为纯产物,停用即回滚;包内 runtime 数据与源码树结构同构,可随时回到源码树模式继续使用既有数据目录。

## Open Questions

(无——版本号取 1.0.0 为首个发布版本,若需调整属参数级改动,不影响本设计。)
