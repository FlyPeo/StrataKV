## Purpose

为 Pulsar 定义有界栈复用与调度器内部 Fiber 复用的行为契约，在保持现有协程语义、线程安全和内存保护的前提下，降低 Gateway 短任务反复创建与销毁的分配成本，并提供可验证的资源上限、观测指标和回退路径。

## ADDED Requirements

### Requirement: Pooling is opt-in and backward compatible
Pulsar MUST 保持现有 Fiber 构造、默认 128 KiB 栈、`resume`、`yield`、`reset`、异常传播和调度行为兼容。未显式启用新复用能力时，系统 MUST 使用直接栈分配路径且不得保留空闲栈，同时 MUST 保持现有每 worker 单槽回调 Fiber 复用；多槽对象缓存必须显式配置。启用或关闭新复用能力 MUST NOT 改变任务结果、调度完成条件或 Gateway HTTP socket 的读、等待与写语义。

#### Scenario: Existing caller does not configure pooling
- **WHEN** 现有调用方按原有方式构造和运行 Fiber，且没有启用任何复用选项
- **THEN** Fiber MUST 使用默认栈大小与直接分配路径，Scheduler MUST 保持既有单槽回调 Fiber 复用，并产生与变更前相同的可观察结果

#### Scenario: Same workload runs with pooling enabled
- **WHEN** 同一组包含完成、主动让出和抛出异常的任务在启用复用后执行
- **THEN** 每个任务的执行次数、完成状态和异常结果 MUST 与关闭复用时一致

#### Scenario: Gateway opts in without changing distributed workers
- **WHEN** Gateway 为 HTTP socket Fiber 启用复用
- **THEN** SDK、2PC、同步 RPC、NodeServer、RegionPeer、Raft Apply、MVCC 和 RocksDB 工作 MUST 继续使用既有线程与进程边界

### Requirement: Stack reuse is size-safe and bounded
启用栈池后，系统 MUST 为请求提供不小于请求值的可用栈空间，并按公开记录的尺寸等级复用兼容的空闲栈。超过最大池化尺寸的请求 MUST 走直接分配与释放路径。池中空闲栈占用的记账字节 MUST NOT 超过配置上限；该上限只约束空闲缓存，不能被表述为全部活跃或休眠 Fiber 的内存上限。

#### Scenario: Warm same-size allocation hits the pool
- **WHEN** 一个已终止并销毁的 Fiber 归还了可池化栈，随后请求相同尺寸等级的新 Fiber
- **THEN** 系统 MUST 能复用该空闲栈，且提供的可用空间 MUST NOT 小于新请求尺寸

#### Scenario: Requested size is between size classes
- **WHEN** 调用方请求的栈大小不是一个尺寸等级边界
- **THEN** 系统 MUST 选择不小于请求值的最小受支持等级，不能向下取整造成可用空间不足

#### Scenario: Oversized stack bypasses the cache
- **WHEN** 请求栈大小超过配置的最大池化尺寸
- **THEN** 系统 MUST 直接分配该栈，并在其所有者销毁时直接释放而不是计入空闲缓存

#### Scenario: Return would exceed the cache limit
- **WHEN** 一个栈被归还且保留它会使空闲缓存字节超过配置上限
- **THEN** 系统 MUST 驱逐或直接释放足够资源，使操作结束后的空闲缓存字节不超过上限

#### Scenario: Many Fibers sleep concurrently
- **WHEN** 大量 Fiber 同时因 I/O、Timer 或同步等待而保持挂起
- **THEN** 每个挂起 Fiber MUST 继续独占自己的栈，系统 MUST NOT 通过池化宣称消除这些在用栈的内存成本

### Requirement: A live stack has exactly one owner
任一栈内存块在任一时刻 MUST 至多属于一个未销毁 Fiber。READY、RUNNING、正在执行切换、或仍被 I/O、Timer、WaitQueue、Scheduler task 引用的 Fiber MUST 保持其栈，且 MUST NOT 将栈放入空闲池。只有对应 Fiber 销毁后，栈才可归还；同一终止 Fiber 的 `reset` MUST 继续复用其当前独占栈而不是先将其暴露给其他 Fiber。

#### Scenario: Fiber yields to an I/O wait
- **WHEN** Fiber 在回调中让出并由 I/O 等待状态持有以便稍后恢复
- **THEN** 其 Fiber 对象与栈 MUST 保持独占，任何新任务 MUST NOT 取得该对象或栈

#### Scenario: Timer and wait-queue ownership overlap scheduler activity
- **WHEN** Timer 或 WaitQueue 持有一个 READY Fiber，其他 worker 同时创建和销毁 Fiber
- **THEN** 池操作 MUST NOT 回收、重置或覆盖被等待源持有的 Fiber 及其栈

#### Scenario: Terminated Fiber is reset through the public API
- **WHEN** 调用方对没有执行上下文的 TERM Fiber 调用 `reset`
- **THEN** Fiber MUST 在原有独占栈上创建新的执行上下文，并且该栈在 reset 期间 MUST NOT 被池中其他 Fiber 获取

#### Scenario: Two threads try to resume one Fiber
- **WHEN** 两个线程并发尝试恢复同一个 Fiber
- **THEN** 至多一个恢复操作 MUST 进入该 Fiber，另一个 MUST 按现有并发恢复错误语义失败，且对象池与栈池状态 MUST 保持一致

### Requirement: Cross-thread return is safe
Fiber 的销毁线程可以不同于其创建线程。系统 MUST 在跨线程获取、归还、驱逐和裁剪时保持所有权与计数一致，不得发生双重释放、use-after-free、数据竞争或把同一栈同时发给两个 Fiber。

#### Scenario: Unpinned task is stolen and destroyed elsewhere
- **WHEN** 未绑定 worker 的任务在一个线程创建 Fiber、由另一 worker 执行并在该线程释放最后引用
- **THEN** 栈 MUST 被安全归还或释放，随后取得该栈的 Fiber MUST 是唯一所有者

#### Scenario: Concurrent acquire, return, and trim
- **WHEN** 多个线程同时从不同尺寸等级获取和归还栈，另一个线程执行裁剪
- **THEN** 操作 MUST 完成且统计上不得出现负数、重复块或超过缓存上限的稳定状态

### Requirement: Guard-page protection survives reuse
启用栈保护构建选项时，每个新分配和复用的池化栈 MUST 保留不可访问的 guard page，栈指针、可用尺寸、底层映射尺寸和释放方式 MUST 在整个获取、归还、复用与驱逐周期内保持匹配。关闭栈保护时不得额外承诺 guard page。

#### Scenario: Reused protected stack overflows into its guard page
- **WHEN** 已从池中复用的受保护栈越界访问 guard page
- **THEN** 进程 MUST 以与首次分配的受保护栈相同的内存保护方式失败，而不是静默写入相邻内存

#### Scenario: Protected stack is evicted
- **WHEN** 一个带 guard page 的缓存栈因容量限制或裁剪被释放
- **THEN** 系统 MUST 使用与该映射元数据匹配的释放范围，并不得遗留部分映射

### Requirement: Cache trimming and shutdown release idle resources
系统 SHALL 提供将空闲栈缓存裁剪到指定保留字节数的能力。裁剪完成后，缓存字节 MUST 不大于目标值；重复裁剪 MUST 安全且幂等。Scheduler 正常停止与复用运行时销毁 MUST 排空内部 Fiber 缓存，并最终释放其拥有的空闲栈，同时不得释放仍由外部 Fiber 引用的在用栈。

#### Scenario: Explicit trim to zero
- **WHEN** 调用方在没有并发在用资源归还的条件下请求把空闲栈缓存裁剪到零
- **THEN** 调用返回时所有空闲栈 MUST 已释放且缓存字节 MUST 为零

#### Scenario: Duplicate trim request
- **WHEN** 空闲缓存已为零且调用方再次请求裁剪到零
- **THEN** 操作 MUST 成功且所有统计值 MUST 保持合法

#### Scenario: Scheduler stops with cached callback Fibers
- **WHEN** Scheduler 完成全部任务并停止时内部仍缓存终止的回调 Fiber
- **THEN** 这些内部对象 MUST 被清空，其栈 MUST 按当前栈分配策略归还或释放

#### Scenario: Pool owner shuts down before an external Fiber alias
- **WHEN** 复用运行时开始关闭但调用方仍持有一个用户 Fiber
- **THEN** 该 Fiber MUST 保持可安全销毁，其栈分配策略的生命周期 MUST 延续到最后一个使用者归还资源

### Requirement: Scheduler only reuses unobservable terminal callback Fibers
Scheduler 的内部 Fiber 缓存 MUST 有明确的对象数或字节上限。只有由 Scheduler 为回调任务创建、状态为 TERM、执行上下文为空、已经脱离全部等待源且不存在外部可达引用的 Fiber 才可进入该缓存。主 Fiber、root Fiber、idle Fiber、用户提交的 Fiber 对象和存在外部别名的内部 Fiber MUST NOT 被作为新回调任务的对象复用。

#### Scenario: Internal callback completes normally
- **WHEN** Scheduler 创建的回调 Fiber 执行到 TERM、没有外部引用且内部缓存未满
- **THEN** Scheduler MUST 能缓存该对象，并在后续回调中安全重置后使用

#### Scenario: Internal callback yields and later terminates
- **WHEN** Scheduler 创建的回调 Fiber 让出并被等待源持有，之后作为绑定原 worker 的 Fiber task 恢复并终止
- **THEN** 它在挂起期间 MUST NOT 进入对象缓存，且仅在终止并脱离等待源后才具备回收资格

#### Scenario: Callback publishes its current Fiber
- **WHEN** 一个内部回调通过当前 Fiber 接口把共享引用保存到 Scheduler 之外并随后终止
- **THEN** Scheduler MUST NOT 为新任务复用该 Fiber 对象，直到外部引用全部释放并按安全销毁路径处理

#### Scenario: Internal Fiber cache reaches its limit
- **WHEN** 一个符合条件的终止 Fiber 被归还但内部对象缓存已达到上限
- **THEN** Scheduler MUST 销毁该对象并按栈策略归还其栈，而不是无界扩大缓存

### Requirement: Allocation and reset failures preserve a recoverable state
底层栈分配或执行上下文构造失败时，系统 MUST 以 `std::bad_alloc` 或既有明确异常语义报告失败，不得返回半构造的可运行 Fiber。失败路径 MUST 释放仅由本次操作取得的资源、保持池计数一致，并且不得影响其他 Fiber；对终止 Fiber 的 reset 失败 MUST 使该 Fiber 保持不可运行但可销毁或再次 reset 的有效状态。

#### Scenario: Cold allocation fails
- **WHEN** 栈池未命中且底层内存分配失败
- **THEN** Fiber 构造 MUST 明确失败，不得增加可观察的活跃 Fiber 数或泄漏栈块

#### Scenario: Context construction fails after acquiring a stack
- **WHEN** 系统已取得栈但执行上下文构造失败
- **THEN** 取得的栈 MUST 被准确归还或释放，且后续获取 MUST NOT 观察到重复所有权

#### Scenario: Caller retries after transient failure
- **WHEN** 一次创建或 reset 因暂时性资源不足失败，资源恢复后调用方重试
- **THEN** 重试 MUST 能按正常路径成功，先前失败不得破坏 Fiber 或池状态

### Requirement: Reuse statistics are internally consistent
系统 SHALL 提供可读取的复用统计快照，至少包含栈获取请求、命中、未命中、归还、驱逐、当前缓存字节、当前在用字节、在用峰值，以及内部 Fiber 缓存的命中、未命中、当前数量和驱逐数量。计数 MUST 使用文档化口径，读取统计不得改变调度或资源所有权。

#### Scenario: One cold then one warm same-class Fiber
- **WHEN** 统计清零后先执行一次冷创建/销毁，再执行一次同尺寸等级的创建/销毁
- **THEN** 快照 MUST 分别反映底层分配未命中和后续池命中，并使当前缓存与在用字节符合实际所有权

#### Scenario: Oversized allocation is observed
- **WHEN** 一个超过最大池化尺寸的 Fiber 完成创建和销毁
- **THEN** 统计 MUST 将其记为直通或未缓存分配，且不得把其字节计入当前空闲缓存

#### Scenario: Metrics are read during concurrent load
- **WHEN** 调用方在并发获取和归还期间读取统计
- **THEN** 每个快照中的命中、未命中、缓存字节和在用字节 MUST 保持非负并符合文档化的一致性保证

### Requirement: Performance claims use reproducible same-condition comparisons
任何栈池或 Fiber 池性能结论 MUST 来自关闭与启用复用的同工作量 A/B：相同 Release 优化构建、编译器与依赖版本、机器、固定 CPU 亲和性、栈保护模式、栈尺寸、任务数量和至少五轮样本，并同时报告原始数据、统计口径与正确性结果。不得用不同语义的 Photon 用例或不同并发量替代 Pulsar 自身前后对比。

热池验收负载 SHALL 包含至少 1,000,000 次相同默认栈尺寸的顺序短 Fiber 创建/运行/销毁；预热后系统栈分配次数 MUST 比直接分配路径减少至少 95%，创建加销毁的每 Fiber 中位耗时 SHALL 改善至少 30%，已有纯上下文传递基准的中位数回退 MUST 不超过 3%。

#### Scenario: Warm lifecycle acceptance benchmark
- **WHEN** 在同一二进制中以相同负载分别运行直接分配模式和预热后的复用模式各至少五轮
- **THEN** 报告 MUST 给出逐轮原始值、中位数、分配次数与 PASS/FAIL，并按规定阈值判断生命周期收益

#### Scenario: Context-switch regression check
- **WHEN** 在启用复用前后运行同一纯上下文传递 A/B 基准
- **THEN** 启用复用后的中位传递成本 MUST NOT 比直接模式回退超过 3%

#### Scenario: Concurrent sleeping-Fiber memory benchmark
- **WHEN** 基准同时保持 10,000 和 100,000 个休眠 Fiber 并记录虚拟内存、RSS、在用栈和缓存栈
- **THEN** 报告 MUST 区分在用栈与空闲缓存，且 MUST NOT 将未减少的并发在用栈成本描述为池化收益

#### Scenario: Photon comparison is published
- **WHEN** 文档将 Pulsar 与 Photon 的结果并列用于工业对比或简历参考
- **THEN** 表格 MUST 明示两者的版本、构建参数、guard page、栈尺寸、工作量和是否启用各自栈池，并把实现差异与实测数据分开说明
