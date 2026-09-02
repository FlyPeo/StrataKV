## Context

动机见 [proposal.md](./proposal.md)，行为验收见 [specs/pulsar-fiber-reuse/spec.md](./specs/pulsar-fiber-reuse/spec.md)。当前每个非主 Fiber 直接持有 `stackAllocation_`、`stackBase_` 和映射大小：普通构建使用 `malloc/free`，guard-page 构建使用 `mmap/mprotect/munmap`。Boost.Context 的栈分配适配器在创建 one-shot context 时返回这块栈，但故意不在 context 结束时释放，因此 TERM Fiber 可以通过 `reset()` 在同一栈上重建 context，最终由 Fiber 析构单次释放。

Scheduler 每个 worker 有一个加锁的任务双端队列，未绑定任务可被其他 worker 窃取；挂起 Fiber 由 EventContext、Timer 或 WaitQueue 持有，并在原 worker 上恢复。当前 `run()` 只保留一个局部 `cbFiber`，完成的回调可复用该对象，发生 yield 的回调则把所有权交给等待源，随后作为 `fiber_` 任务恢复。这意味着系统已有“同对象同栈 reset”和“每 worker 单槽回调对象”两种局部复用，但没有跨 Fiber 生命周期的栈池，也没有有界多槽对象缓存。

该变化只处于 Gateway HTTP socket 的 Fiber 调度边界内。Gateway 后续发起的阻塞 SDK/2PC 工作仍进入有界 pthread pool，再经同步 MPRPC 到 NodeServer；NodeServer 继续按 RegionId 路由至独立 RegionPeer，Raft Apply、MVCC/Persister 与 RocksDB 路径均不新增 Fiber、池或调度队列。

## Goals / Non-Goals

**Goals:**

- 把栈的“申请、独占、归还、裁剪”从 Fiber 中分离，形成可替换且异常安全的所有权边界。
- 用一个进程内/运行时共享、跨线程安全、严格限制空闲字节的初版栈池覆盖短 Fiber churn。
- 把现有单槽回调 Fiber 扩展为每 worker 有界多槽缓存，同时让“可回收”成为可证明的终止状态，而不是依赖调用位置猜测。
- 保持默认路径、公开构造方式和 Boost.Context one-shot 语义；让 Gateway 可以逐实例启用和快速回退。
- 用统计和同条件 A/B 区分三类成本：context transfer、Fiber 对象生命周期、栈系统分配。

**Non-Goals:**

- 不改变 Fiber 栈大小默认值，不实现动态增长、分段栈、活跃栈共享或活跃 Fiber 迁移。
- 不在首版采用 Photon 的 thread-local/vCPU 栈池布局，也不复制 Photon 实现或引入新依赖。
- 不实现常驻 Fiber 从任务队列循环取活的“执行协程池”；它会改变任务局部状态、异常边界、背压和停机语义。
- 不用池容量限制同时在用/休眠的 Fiber 数；并发准入和连接背压是独立能力。
- 不改变 SDK、2PC、RPC、NodeServer、RegionPeer、Raft、MVCC 或 RocksDB 的线程和对象模型。

## Decisions

### 1. 引入 move-only 栈块和共享分配器接口

新增 `pulsar/stack_allocator.hpp`，定义以下概念；最终命名可遵循仓库风格，但所有权关系不得改变：

```cpp
struct FiberStackBlock {                 // move-only
  void* allocation;                      // malloc/mmap 返回地址
  void* stackBase;                       // guard page 之后的可用区起点
  size_t usableSize;
  size_t allocationSize;                 // 含 guard page/页对齐空间
  StackBackend backend;                  // malloc 或 mmap
};

class FiberStackAllocator {
 public:
  virtual FiberStackBlock acquire(size_t requested) = 0;
  virtual void release(FiberStackBlock&& block) noexcept = 0;
  virtual void trim(size_t keepBytes) = 0;
  virtual FiberStackStats stats() const = 0;
  virtual ~FiberStackAllocator() = default;
};
```

`DirectStackAllocator` 封装当前 `malloc/free` 与 guard-page `mmap/mprotect/munmap` 行为；`PooledStackAllocator` 实现复用并直接拥有空闲 `FiberStackBlock`。Fiber 持有一个 `shared_ptr<FiberStackAllocator>` 和一个非空 move-only block，因此分配器一定比所有引用它的 Fiber 活得久。主 Fiber 没有独立 block。

| 对象 | 实例数量 | 所有者与生命周期 | 可访问线程 |
|---|---:|---|---|
| `DirectStackAllocator` | 默认共享一个，也允许显式实例 | 静态默认句柄或调用方 `shared_ptr` | 任意线程，无空闲状态 |
| `PooledStackAllocator` | 建议每个 Gateway Scheduler/IOManager 一个 | Gateway/Scheduler 与所有相关 Fiber 共享；最后一个引用销毁时释放空闲块 | 所有 worker 与外部销毁线程 |
| `FiberStackBlock`（在用） | 每个非主 Fiber 一个 | Fiber 独占，直到 Fiber 析构 | Fiber 可跨线程转移，但同一时刻只有一个所有者 |
| `FiberStackBlock`（空闲） | 受字节上限约束 | Pooled allocator 独占 | 通过池锁访问 |
| 回调 Fiber 缓存 | 每个 Scheduler worker 一个 | Scheduler 创建，worker 退出/stop 后清空 | 仅对应 worker 热路径访问 |

选择 `shared_ptr` 分配器而非裸指针，是为了处理“池配置对象先离开作用域、用户仍持有 Fiber”的情形。选择 move-only block 而非散落的三个裸字段，是为了让 guard 元数据、底层释放方式和所有权一起移动，减少双重释放风险。

**替代方案：** 让 Fiber 析构调用全局单例池实现简单，但会引入全局可变配置、测试相互污染和运行时销毁顺序问题；让分配器弱引用 Fiber 则可能在最终归还时悬空，因此均不采用。

### 2. 首版使用共享有锁、按 2 的幂分级的有界栈池

`StackPoolOptions` 初始默认值为：空闲缓存上限 64 MiB、最大池化可用栈 1 MiB。调用方只有显式创建并注入 `PooledStackAllocator` 才启用栈池。尺寸先满足 `boost::context::stack_traits::minimum_size()`，再向上取整到 2 的幂；典型等级为 64、128、256、512 KiB 和 1 MiB。默认 128 KiB 请求因此不会膨胀。超过最大等级的请求走 Direct 路径，仍由同一个池实例统计但不缓存。

一个 mutex 同时保护所有尺寸等级 freelist、空闲块集合、`cachedBytes` 和裁剪选择。获取命中时在锁内摘除一个 block 并更新记账，底层冷分配在锁外执行；归还时在锁内判断容量并入链，超过上限的 block 在锁外直接释放；`trim()` 在锁内摘出待释放块，再在锁外执行 `free/munmap`。首版优先保证跨线程销毁与精确上限，基准若证明锁竞争明显，再在不改变接口的前提下改为分片尺寸锁或 worker 本地前端。

容量以 `allocationSize` 记账，因此 guard page 和页对齐开销都不会漏算。freelist 插入若因容器扩容抛出异常，`release()` 捕获异常并直接释放该 block，保证 Fiber 析构保持 `noexcept`。池析构等价于 `trim(0)`，但只会在最后一个 Fiber 释放分配器引用后发生。

首版缓存时保留已提交物理页，不在热归还路径执行 `madvise(DONTNEED)` 或清零；这最大化生命周期收益，RSS 通过 64 MiB 上限和显式 `trim()` 控制。文档必须说明栈残留不是跨安全域可读取的 API，任何读取未初始化栈内存的程序本身仍是未定义行为。

**替代方案：** Photon 的 thread-local/vCPU 分级池能缩短无竞争热路径，但 Pulsar 的未绑定任务会窃取，最后一个 `shared_ptr` 也可在创建线程之外释放；首版若直接照搬将需要 remote-free 队列和更复杂的关闭协议。固定尺寸池会浪费自定义栈请求，任意尺寸哈希池又容易碎片化，因此选择有限的 2 的幂等级。

### 3. Fiber 在整个对象生命周期独占栈，Boost 适配器不取得所有权

保留 Boost.Context allocator adapter，但它只把 Fiber 已经持有的 block 转换为 `stack_context{sp, size}`，其 `deallocate()` 仍为空。Fiber 构造顺序调整为：解析默认尺寸 → 从分配器取得 block → 构造临时 context → 成功后提交成员并增加全局 Fiber 数；任一步失败都由局部 guard 把 block 归还。

`reset()` 只允许 TERM 且 context 为空的非主 Fiber。它先在原 block 上构造临时 context，成功后再提交新 callback、清空旧异常并切为 READY；失败时保持 TERM、context 为空、原栈仍由该 Fiber 独占，所以可再次 reset 或安全析构。`ReleaseStack()` 只在非主 Fiber 析构中把 block 移交给分配器，随后立即清空本地元数据。

核心安全不变量如下：

1. 一个非空 block 同时至多属于一个 Fiber 或一个 allocator freelist。
2. READY、RUNNING、正在 `resume`、或由 Event/Timer/WaitQueue/Scheduler task 持有的 Fiber 永远不归还 block。
3. TERM Fiber 的 `reset()` 使用自己的 block；只有对象析构才把 block 交回池。
4. 当前 CPU 正在执行的栈不得释放、裁剪或移入缓存。
5. guard page 的起点、可用尺寸、映射尺寸和 backend 必须作为一个 block 同步移动。
6. 分配器生命周期覆盖所有借出的 block；跨线程销毁必须安全。
7. 所有失败分支最终仍满足“一个所有者或已释放”，并且 Fiber 活跃计数只统计完整构造对象。

**替代方案：** context 到 TERM 时立即把栈还池，可以更早回收内存，但外部仍能持有并 `reset()` 该 Fiber，也可能观察对象身份；这会破坏现有契约。让 Fiber cache 中的 TERM 对象先放弃栈、重用时再取栈，则同时增加复杂度并削弱 `reset()` 快路径，因此不采用。

### 4. 通过兼容重载注入配置，不使用进程级开关

保留现有 Fiber、Scheduler 和 IOManager 构造函数符号，并增加显式重载：Fiber 可接收 `shared_ptr<FiberStackAllocator>`；Scheduler 与 IOManager 可接收 `SchedulerReuseOptions`，其中包含内部 Fiber 使用的 allocator 与 `callbackFiberCachePerWorker`，IOManager 只负责原样向 Scheduler 传递。旧构造函数委托到默认 Direct allocator，并把每 worker 回调缓存容量设为 1，等价于当前局部 `cbFiber`。多槽必须把容量显式设为大于 1；设为 0 可由测试或调用方明确禁用对象缓存。

Gateway 增加成对参数 `--fiber-stack-cache-mib`（默认 0，即 Direct；正数创建对应上限的 pooled allocator）和 `--fiber-cache-per-worker`（默认 1；大于 1 启用多槽）。这两个参数只在 `--runtime fiber` 下生效，非法范围或在线程模式下非默认取值都明确拒绝启动，避免配置看似生效实际被忽略。Gateway 将配置传给 IOManager；root、idle 和 Scheduler 创建的 callback Fiber 使用该 allocator。调用方直接提交的 `Fiber::ptr` 保留它构造时选择的 allocator，Scheduler 不替换其栈所有权。首轮建议显式使用 64 MiB 栈缓存，生产默认值仍为 0。

**替代方案：** 环境变量或全局 setter 容易使同一进程的多个 Scheduler 相互影响，也无法可靠管理测试与析构顺序；修改原构造参数的默认含义会造成静默行为变化。因此使用尾部重载/配置对象。

### 5. Fiber 对象池是每 worker 的终止对象缓存，不是常驻执行协程池

每个 worker 维护一个只由自身访问的 `vector<Fiber::ptr>`，无需新增热路径锁；Scheduler stop 在 worker join 后由拥有线程清空所有缓存。Scheduler 为 callback 新建 Fiber 时写入不可由公共 API 修改的 `schedulerOwnerId` 与 `originWorkerIndex` 标记。取任务时先弹出一个缓存对象并 `reset()`；未命中才创建。

以下条件必须同时满足才可入池：

- 由当前 Scheduler 的 callback 路径创建，且 origin worker 就是当前 worker；
- 状态为 TERM，Boost context 与 caller context 均为空，`resumeInProgress` 已解除；
- 当前不是 main、root 或 idle Fiber，也不是用户提交的 `Fiber::ptr`；
- 在移入缓存前 `shared_ptr::use_count() == 1`，唯一引用就是当前 Scheduler task/局部变量；
- 缓存容量未满。

正常 callback 在 `resume()` 返回后执行资格检查。发生 yield 时不再保留在 callback 局部槽，而是由 Hook/Timer/WaitQueue 持有；事件就绪后仍按既有逻辑绑定原 OS thread 入队。当它从 `task.fiber_` 分支恢复并最终 TERM 时，同样执行资格检查，这使“yield 后完成”的内部 Fiber 也能安全回池。若 callback 曾把 `Fiber::GetThis()` 的共享引用发布到外部，`use_count()` 会阻止对象复用；Scheduler 放弃该对象后，最后一个外部引用仅按普通 Fiber 析构并把栈归还栈池。

对象缓存中的 Fiber 仍拥有栈，因此 allocator 的 `checkedOutBytes` 包含它；Scheduler 指标另外报告 `callbackCachedCount/Bytes`，避免把它误解为运行中资源。总空闲保留上界为“栈池空闲上限 + 各 worker 回调缓存上限所持栈”，文档与容量建议必须同时呈现两项。

**替代方案：** 全局 Fiber 对象池需要跨 worker 锁并可能破坏恢复亲和性；自动回收用户 Fiber 会与外部 `shared_ptr`、Fiber ID 和对象身份冲突；常驻 Fiber 循环消费 callback 会改变一次 callback 一个异常/栈帧生命周期的语义。三者均不采用。

### 6. 状态流与队列边界保持原样，只在创建和终止边缘插入缓存

回调 Fiber 的状态流为：

```text
worker task deque
  -> callback cache hit (TERM/no-context) --reset--> READY
  -> or cache miss --construct/acquire stack--> READY
  -> resume --> RUNNING
       -> callback completes/throws --> TERM --> eligible check --> worker cache or destroy
       -> I/O/Timer/WaitQueue yield --> READY + waiting owner
            -> event fires --> pinned fiber task deque --> RUNNING
                 -> TERM --> eligible check --> worker cache or destroy
```

Scheduler 现有 WorkerQueue mutex 只保护任务 deque；新的 stack-pool mutex 只保护 freelist 与容量；callback cache 由单 worker 独占，不与两把锁嵌套。底层 `malloc/free/mmap/munmap` 不在 pool mutex 内执行。该锁顺序设计避免栈系统调用阻塞任务队列，也避免 pool lock 与等待源锁形成环。

Gateway 请求流保持为：socket event → Gateway Fiber → 必要时把阻塞 SDK/2PC 调用交给既有 bounded pthread pool → synchronous MPRPC → 单 NodeServer 按 RegionId 路由 → 对应 RegionPeer/Raft Group → Raft Apply → MVCC/Persister → RocksDB。复用逻辑只出现在最左侧 Fiber 的构造/终止边缘，不进入 RPC 消息、Raft 日志、事务重试、Leader 切换或存储恢复路径。因此分布式超时、重复请求、Leader 变化和部分提交语义不受本设计修改。

### 7. 统计采用单调计数器加锁内 gauge 快照

`FiberStackStats` 包含 acquire、hit、miss、return、eviction、passthrough、system allocation/free 次数，以及 cached/checked-out/current/peak bytes；Scheduler stats 包含 callback cache hit、miss、current count/bytes 和 eviction。累计计数使用 relaxed atomics，容量相关 gauge 在 pool mutex 下更新，快照保证每个字段非负且 `hit + miss == acquire` 的已完成操作口径成立，但不承诺多个并发字段对应同一个全局瞬间。

测试可注入 Direct backend/fault injector 统计真实底层分配并触发第 N 次失败，无需拦截全进程 `malloc`。公开统计读取不持有任务队列锁，也不触发 trim。

### 8. 性能验收拆开测量栈池和对象池

新增单一 A/B benchmark 可在同一 Release 二进制中切换以下模式：Direct + 单槽（基线）、StackPool + 单槽、StackPool + 多槽。它输出机器/编译器/Boost/Photon 版本、CPU affinity、guard 模式、栈尺寸、任务数、预热数、五轮原始值、中位数、分配统计和 checksum/PASS。

- 顺序生命周期用至少 1,000,000 个短 Fiber 验证热池分配下降与创建/销毁收益。
- burst/wave 和混合尺寸验证容量、等级、驱逐及并发锁竞争。
- 10,000/100,000 休眠 Fiber 分开报告 VmSize、RSS、checked-out 与 cached bytes，明确池不能减少在用栈数量。
- callback 调度基准分别覆盖不 yield、一次 Timer/I/O yield 和外部别名，验证多槽命中及安全拒绝。
- 纯 transfer A/B 继续使用已经消除正常路径字符串构造的基准，防止资源池代码污染 13 ns 级上下文切换热路径。

与 Photon 的表格只在栈大小、guard、pool 开关、任务语义和轮次一致时并排；否则只比较机制，不给出倍率结论。

## Risks / Trade-offs

- [空闲栈会提高稳态 RSS] → 默认关闭栈池，初始上限 64 MiB，按实际映射字节记账，提供 `trim()`，停机排空，并在文档中把缓存与在用内存分开。
- [共享 mutex 在多 worker 高频创建/销毁时竞争] → 系统分配放锁外，先以正确性为优先；用并发基准决定是否演进到分片或本地前端，而不预先引入 remote-free 复杂度。
- [错误回收挂起或被外部引用的 Fiber 会造成 UAF/串栈] → 终止态、空 context、来源标记、origin worker 和唯一引用五重检查，并用 I/O/Timer/WaitQueue、跨线程和 sanitizer 测试覆盖。
- [对象缓存和栈池形成两层内存保留] → 分别设上限并分别报告 bytes；容量规划使用两者之和，不能只看 stack pool gauge。
- [guard page 映射元数据不一致会错误 unmap] → block 原子携带 backend 与全部尺寸，move-only，guard 构建加入首次与复用后的 death test。
- [异常或 OOM 破坏 reset/计数] → 临时对象成功后再提交成员；`release()` noexcept 降级为直接释放；fault injection 验证每个失败点可重试。
- [30% 生命周期目标受 allocator/CPU 影响] → 固定同机同二进制 A/B、报告原始五轮数据；未达到即如实 FAIL 并通过统计定位，不用 Photon 数字代替。
- [缓存栈保留旧字节] → 不向任务暴露栈内容，保持 C++ 未初始化读取规则；对需要更低 RSS/更强清除策略的部署使用 trim，页丢弃策略留待独立测量后再设计。

## Migration Plan

1. 先引入 Direct allocator、move-only block 与 Fiber 注入重载，在池关闭时跑完整现有 Fiber/Scheduler/IOManager 测试，证明行为和性能基线没有变化。
2. 加入 Pooled allocator、统计、fault injection、并发/guard/trim 单测；默认构造仍走 Direct。
3. 将 Scheduler 单槽改造成容量可配的 per-worker cache，默认容量保持 1；先验证完成、yield 后恢复、异常、外部别名和 stop。
4. 在 benchmark 和测试中显式启用 64 MiB 栈池与多槽缓存，完成同条件验收；只有数据达标后才在 Gateway 的可回退配置中试开。
5. 记录 Gateway canary 的命中率、cached/checked-out bytes、延迟和错误；确认无回归后再决定生产默认值，本 change 不自动把默认改为开启。

回滚无需数据迁移：Gateway 移除 pooled allocator 注入并把 callback cache 容量恢复为 1，即回到 Direct + 既有单槽。Scheduler stop 先完成/回收任务并 join worker，再清空 callback cache；最后一个 Fiber 释放 allocator 引用时排空 stack pool。若新实现本身出现问题，可保留兼容接口而让工厂始终返回 Direct allocator。
