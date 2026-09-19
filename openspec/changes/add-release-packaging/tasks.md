# Tasks: add-release-packaging

## 1. CMake 打包基础

- [x] 1.1 引入项目版本号并设置安装期 rpath:`project(... VERSION 1.0.0)`,`CMAKE_INSTALL_RPATH` 设为 `$ORIGIN/../lib`(仅影响 install 产物)。验证:Release 构建后执行 `cmake --install --prefix /tmp/pkg-check`,`readelf -d /tmp/pkg-check/bin/stratakv-node` 显示 RPATH 指向 `$ORIGIN/../lib`,而 build tree 内二进制不含该 RPATH。

- [x] 1.2 完善 install 布局与 CMake 包配置:安装全部静态库(`stratakv_sdk`、`stratakv_core`、`stratakv_rpc`)经 `EXPORT stratakvTargets`,SDK 公共头整目录安装,新增 `stratakvConfig.cmake` 模板(include 导出 targets,附加 `$<INSTALL_PREFIX>/lib` 链接目录与 rpath 接口选项)。验证:在包外临时目录用最小 consumer `CMakeLists.txt` 执行 `find_package(stratakv)` + 链接 `stratakv::sdk`,CMake 配置成功且导出 target 携带预期 INTERFACE 属性。

## 2. 部署脚本发行包模式

- [x] 2.1 `deploy/stratakv-server` 与 `deploy/stratakvctl` 增加自位置模式检测:脚本上级目录存在 `CMakeLists.txt` 时为源码树模式(现有行为与参数完全不变);否则为发行包模式——跳过构建、使用包内 `bin/`、runtime 数据置于 `<包根>/runtime/<project>/`、显式请求构建时报"发布包不含源码"的可执行错误、`--no-build` 接受为 no-op。验证:将 `bin/` 与脚本拷入无 `CMakeLists.txt` 的模拟包目录,完成 `up → verify → stratakv-admin put/get → down` 全流程;随后在源码树内执行 `up` 确认仍自动构建且行为不变。

## 3. 打包脚本与验收门

- [x] 3.1 新增 `deploy/stratakv-package`:幂等 Release 构建 → `cmake --install` 到 `dist/stratakv-<version>-linux-x64/` → 对包内 ELF 执行 ldd 闭包收集非基础第三方 `.so`(按设计排除表)入包内 `lib/` → 写入 `VERSION` 文件 → `tar czf` 产出归档;任一依赖收集失败时非零退出并列出缺失库与所属二进制。验证:运行脚本产出归档,解压后对每个包内二进制执行 `ldd` 确认第三方依赖全部解析到包内 `lib/`。

- [x] 3.2 打包脚本集成验收门:将归档解压到干净临时目录,以独立项目名执行 `up → verify(集群健康)→ stratakv-admin put/get 回读`,并在包外目录以 `-DCMAKE_PREFIX_PATH` 构建 examples 连集群完成读写,最后 `down`;无论成败均停止验收集群并删除临时项目,失败时保留日志目录并在 stderr 打印路径。验证:完整流程退出 0;人为破坏包内一个二进制后重跑,退出非零、日志保留且无残留进程。

## 4. 示例与用户文档

- [x] 4.1 新增 `examples/`(`sdk_example.cpp` + `CMakeLists.txt`,仅引用 `stratakv::sdk` 完成 put/get,不枚举 core/rpc 或第三方依赖)与 `docs/QUICKSTART.md`(解压 → `up` → `verify` → SDK 写入三步上手,声明 glibc 平台要求与包目录可写等已知限制)。验证:按 QUICKSTART 从零在临时目录走完全流程;检查 examples 的 CMakeLists 中不出现 stratakv core/rpc 库名或第三方链接标志。

## 5. 发布回归

- [x] 5.1 端到端发布回归与幂等验证:连续两次执行 `deploy/stratakv-package` 全流程,确认均产出有效归档且验收通过,结束后无验收进程与临时项目残留;全程不修改测试目录重组等无关脏文件。验证:两次运行退出码均为 0,`ldd` 抽查与 `git status` 复核(除本 change 相关文件外无新增改动)。
