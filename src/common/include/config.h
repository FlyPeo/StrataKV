#ifndef CONFIG_H
#define CONFIG_H

const bool Debug = false;

const int debugMul = 1;  // 时间单位：time.Millisecond，不同网络环境rpc速度不同，因此需要乘以一个系数
const int HeartBeatTimeout = 25 * debugMul;  // 心跳时间一般要比选举超时小一个数量级
const int ApplyInterval = 1 * debugMul;      //

// Local persistence and snapshot publication can experience sub-second I/O
// jitter under concurrent Region load. Keep election and proposal deadlines
// comfortably above that jitter so a healthy leader is not replaced merely
// because the host is busy.
const int minRandomizedElectionTime = 2000 * debugMul;  // ms
const int maxRandomizedElectionTime = 4000 * debugMul;  // ms

const int CONSENSUS_TIMEOUT = 3000 * debugMul;  // ms

// 协程相关设置

const int FIBER_THREAD_NUM = 1;              // 协程库中线程池大小
const bool FIBER_USE_CALLER_THREAD = false;  // 是否使用caller_thread执行调度任务

#endif  // CONFIG_H
