## 1. Core Implementation

- [x] 1.1 新增 `FiberStackBlock`、`FiberStackAllocator`、`DirectStackAllocator`、配置/统计类型与工厂，并接入 Pulsar 的 CMake 安装导出；验证：独立执行 `cmake -S src/pulsar -B build/pulsar-reuse -DCMAKE_BUILD_TYPE=Release && cmake --build build/pulsar-reuse -j`，确认库和已安装公开头可编译；实施前后逐文件检查 `git -C src/pulsar diff`，只修改栈分配相关新文件/CMake 并保留现有无关脏改动。
- [x] 1.2 将现有 malloc 与 guard-page mmap 行为迁入 Direct allocator，并让 Fiber 以 move-only block + `shared_ptr` allocator 管理栈，保留旧构造函数并增加显式 allocator 重载；验证：运行 `ctest --test-dir build/pulsar-reuse -R pulsar-fiber-context-check --output-on-failure`，确认默认 128 KiB、resume/yield/异常与计数回归通过；仅编辑 Fiber/allocator 必需文件并保留这些文件中已有用户改动。
- [x] 1.3 把 Fiber 构造和 `reset()` 改为“临时 context 成功后再提交”的异常安全路径，析构只移动归还一次 block；验证：用 fault-injection 测试目标逐个触发 acquire/context 构造失败，确认可重试、无泄漏且 `TotalFiberNum()` 恢复基线；只修改该失败路径及测试接缝，不覆盖 `fiber.cpp` 已有优化。
- [x] 1.4 实现 64 MiB 默认上限、1 MiB 最大池化尺寸的 `PooledStackAllocator`：2 的幂尺寸等级、超大栈直通、跨线程 mutex、锁外系统分配/释放、noexcept 归还、`trim()` 与完整统计；验证：构建新增 `pulsar-stack-pool-check` 并运行其 size/cap/trim/stats 用例；只修改 allocator/CMake 相关文件并保留其他脏文件。
- [x] 1.5 为 Scheduler 和 IOManager 增加兼容 `SchedulerReuseOptions` 重载与统计读取接口，把既有单槽改为默认容量 1 的每 worker 有界缓存，并实现来源、origin worker、TERM/空 context、非恢复中及唯一引用资格检查；验证：运行 `pulsar-scheduler-work-stealing-check` 和新增 scheduler-cache 单测，确认默认行为、yield 后回收与停止均通过；只修改 Scheduler/IOManager 及对应测试，手工合并而不覆盖其中现有改动。
- [x] 1.6 在 Gateway fiber runtime 中加入 `--fiber-stack-cache-mib`（默认 0）与 `--fiber-cache-per-worker`（默认 1），校验模式/范围并把配置注入 IOManager；验证：构建 `stratakv-gateway`，分别检查默认参数、fiber 显式启用、非法值以及 thread 模式误配的退出结果；只编辑 Gateway 参数/构造接线及其测试，不触碰 SDK、2PC、RPC、Raft、MVCC 和 RocksDB 代码或现有无关改动。

## 2. Unit Tests

- [x] 2.1 为 move-only block、Direct allocator、最小尺寸与向上取整等级编写单测，覆盖默认 128 KiB、边界前后值、混合尺寸和超过 1 MiB 直通；验证：`ctest --test-dir build/pulsar-reuse -R pulsar-stack-pool-check --output-on-failure` 全部通过且断言实际 usable/allocation size；只新增/编辑专用测试与注册项，保留现有测试改动。
- [x] 2.2 为栈池冷 miss、热 hit、return、容量拒绝、驱逐、`trim(N)`、重复 `trim(0)`、析构排空和逐尺寸统计编写确定性单测；验证：专用测试结束时校验 `hit + miss == acquire`、cached/checked-out 为预期且底层 allocation/free 配对；只修改专用测试和必要测试接缝。
- [ ] 2.3 用可注入失败 backend 覆盖首次分配失败、取得栈后 context 构造失败、freelist 元数据分配失败及 reset 重试，验证 `std::bad_alloc`/既有异常、活跃 Fiber 数和资源计数；验证：运行专用 fault 用例并在 ASan/UBSan 构建下零 leak/invalid access；不更改无关错误语义或脏文件。
- [ ] 2.4 增加多线程 acquire/return/trim、创建线程与销毁线程不同、重复并发 resume 的压力单测；验证：固定迭代下 checksum/唯一 block 地址集合正确，并对不执行 Boost 栈切换的 allocator 并发用例运行 TSan 无 data race；只修改并发专用测试及必要同步代码。
- [ ] 2.5 在 `PULSAR_FIBER_GUARD_PAGES=ON` 构建中增加首次栈和池命中后复用栈的 guard-page death test，以及驱逐/trim 完整 unmap 检查；验证：两个越界子进程均按预期因内存保护终止，正常路径在 ASan/UBSan 下通过；只修改 guard 测试、allocator 和 CMake 相关内容。
- [ ] 2.6 为 callback Fiber 缓存覆盖正常完成、异常完成、一次/多次 yield、Timer/I/O/WaitQueue 持有、外部 `GetThis()` 别名、用户 Fiber、main/root/idle 排除、容量 0/1/N 与满池驱逐；验证：新增 `pulsar-scheduler-cache-check` 的对象 ID、执行次数、缓存指标和析构计数全部符合预期；仅编辑 Scheduler 专用测试及必要实现，保留其他测试文件已有内容。

## 3. Integration and Regression Tests

- [ ] 3.1 在 IOManager 中用 socketpair/epoll、Timer、FiberMutex/FiberSemaphore/WaitQueue 组合验证挂起 Fiber 不被回收且恢复到原 worker；验证：`pulsar-sync-check` 与新增 reuse integration case 在多 worker 下重复运行 100 轮无串栈、超时或 checksum 错误；仅修改 Pulsar 集成测试与必要复用代码。
- [ ] 3.2 覆盖未绑定 callback 被窃取、yield 后绑定恢复、外部别名跨 Scheduler stop 存活、stop 排空每 worker cache、allocator 晚于 Fiber 析构等生命周期；验证：`pulsar-scheduler-work-stealing-check` 和专用 shutdown case 在 ASan/UBSan 下通过且最终缓存为零、allocation/free 配对；只编辑相关 Scheduler/IOManager 测试和实现。
- [ ] 3.3 增加 Gateway 参数与 fiber socket 冒烟测试，对比 Direct+单槽、StackPool+单槽、StackPool+多槽的 HTTP 结果，并确认阻塞请求仍交给 bounded native request pool、过载仍返回 `503 GATEWAY_BUSY`；验证：运行新增 Gateway CTest/脚本并比较响应与现有 request-executor 指标；仅修改 Gateway 测试/接线，禁止把 SDK/2PC/同步 RPC 或存储路径迁入 Fiber。
- [ ] 3.4 分别以 guard pages OFF/ON 构建 Pulsar，运行全部 `pulsar-*`、`stratakv-test-fiber-*` 和适用 Gateway 回归；验证：两套 `ctest --output-on-failure` 记录均全绿且默认 Direct+单槽输出与变更前一致；仅修复本 change 引入的失败，不清理或格式化无关脏文件。

## 4. Performance Comparison

- [ ] 4.1 扩展生命周期 benchmark，使同一 Release 二进制支持 Direct+单槽、StackPool+单槽、StackPool+多槽，并输出预热、CPU affinity、编译器/Boost、guard、栈尺寸、任务量、逐轮原始值、allocator/cache stats 与 checksum；验证：三种模式各跑一次小规模 smoke，字段齐全且工作量/checksum 完全相同；只修改专用 benchmark/CMake，保留当前严格 context A/B 文件的已有优化。
- [ ] 4.2 在固定 CPU 上对至少 1,000,000 次默认栈短 Fiber 执行五轮同条件 A/B；验证：报告自动判定预热后系统栈分配减少至少 95%、创建+销毁中位耗时改善至少 30%，未达标时保留原始数据并标记 FAIL 而不调整口径；只生成 benchmark 结果和本 change 必需代码，不覆盖已有报告。
- [ ] 4.3 运行 wave/burst、64/128/256/512 KiB 与超大栈混合、多 worker 并发 churn，记录命中率、驱逐、锁竞争近似指标与吞吐；验证：每组至少五轮、缓存上限始终成立、任务 checksum 正确，并形成可复跑命令；只追加本 change 性能产物。
- [ ] 4.4 分别保持 10,000 和 100,000 个休眠 Fiber，测量 VmSize、RSS、checked-out stack bytes、stack-pool cached bytes 和 callback-cache bytes；验证：报告能对上对象/字节记账并明确“池不减少同时在用栈数”，不得把冷/热或不同并发量混为一表；只修改性能测试/报告相关文件。
- [ ] 4.5 对不 yield、一次 Timer/I/O yield、外部别名三类 Scheduler callback 运行单槽与多槽 A/B；验证：至少五轮报告对象 cache hit/miss、任务吞吐和 p50/p99，并证明别名 case 不发生对象复用；只修改 callback benchmark 和结果文档。
- [ ] 4.6 复跑已经消除正常路径字符串构造的严格 Pulsar/Photon context A/B，并增加池关闭/开启的 Pulsar transfer 对照；验证：相同编译参数、栈大小、guard 与 CPU 下至少五轮，Pulsar 纯 transfer 中位数回退不超过 3%，Photon 对比表标明版本和 pool 状态；只追加复用相关数据，保留 `test/fiber_context_ab_benchmark.cpp` 与既有工业对比中的用户改动。

## 5. Documentation

- [ ] 5.1 更新 `src/pulsar/README.md`，说明 Direct/Pooled allocator API、64 MiB/1 MiB 默认参数、Scheduler/IOManager 注入、多槽配置、统计、trim、guard page、对象资格和“休眠 Fiber 仍独占栈”；验证：按 README 示例编译一个外部调用程序并运行命中/trim 断言；在 Pulsar 独立 Git 仓中只合并相关段落，保留 README 已有脏改动。
- [ ] 5.2 更新 `docs/工业对比/Pulsar与Photon对比.md`，增加完全相同条件的前后/横向表，并解释 Photon thread-local/vCPU 栈池与 Pulsar 首版共享安全池、对象池的差异及差距来源；验证：逐项核对版本、命令、guard、栈尺寸、任务数、五轮原始数据、PASS/FAIL 和“机制结论/实测结论”标签；在 docs 独立仓中保留全部现有用户修改与删除，不代替用户提交。
- [ ] 5.3 更新性能报告与简历/面试参考，给出可诚实复述的结果、指标口径、容量公式和限制，不把 Fiber 池写成线程池，也不声称改变 2PC/RPC/Raft；验证：简历数字均能反查到同条件原始记录和可复跑命令，未达阈值的项目不写成收益；只新增或局部编辑相关文档并保留 docs 仓其他脏文件。
- [ ] 5.4 记录 Gateway 新参数、默认关闭栈池/默认单槽、启用示例、监控字段、容量规划与关闭回退步骤；验证：复制文档命令启动 fiber 模式并读取对应配置/统计，thread 模式误配能明确失败；只修改 Gateway/Pulsar 相关文档。

## 6. Final Verification

- [ ] 6.1 在实施前保存根仓、`src/pulsar` 独立仓和 `docs` 独立仓的 `git status --short`/目标文件 diff 基线，实施后逐仓审查差异；验证：最终 diff 只包含本 change 的有意修改加实施前已存在的用户改动，没有回滚、覆盖、删除或格式化无关文件。
- [ ] 6.2 完成 Release、guard ON/OFF、ASan/UBSan 和 allocator-only TSan 验证并汇总命令/结果；验证：适用构建与测试全绿，任何工具限制或跳过项附可复现原因，绝不把未运行写成通过；只写验证记录，不修改无关代码以掩盖失败。
- [ ] 6.3 对照 `pulsar-fiber-reuse` 的每条 Requirement/Scenario 建立测试或报告证据映射，并运行 `openspec validate add-pulsar-fiber-stack-reuse --strict`；验证：OpenSpec 严格校验通过、所有 checkbox 的证据可定位，性能阈值按实测 PASS/FAIL 记录；仅更新本 change 的任务状态/证据与相关报告，保留其他 OpenSpec change 和仓库脏改动。
