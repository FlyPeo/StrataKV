# Tasks: make-balancer-default-on

## 1. 默认值与测试

- [x] 1.1 `DefaultAutoBalancerConfig()` 默认 `enabled=true`;翻转 `auto_balancer_state_unit_check.cpp` 默认值断言并保留显式配置/快照恢复断言。验证:Release 构建 + metadata 相关单测(auto_balancer state/heartbeat)全部通过。

## 2. 发布与文档

- [x] 2.1 版本 1.2.0;deploy/README 与 QUICKSTART 的"默认关闭"表述改为"默认开启,可 disable";完整打包验收。验证:打包 rc=0;解压新包起全新动态集群,`balancer-status` 显示 enabled=true;对包内旧项目(如有)确认配置不被覆盖;`openspec validate make-balancer-default-on` 通过。
