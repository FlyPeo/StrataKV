# StrataKV OpenSpec 工作流

本项目使用 OpenSpec 的 `spec-driven` schema：

```text
proposal -> specs -> design -> tasks -> apply -> sync -> archive
```

## 在 Codex 中使用

初始化或更新 Skills 后需要重启 Codex。新需求推荐按下面的顺序进行：

```text
$openspec-explore 分析 <问题或想法>，只调查和澄清，不修改代码
$openspec-propose <变更名称与目标>
```

检查并修改生成的 `proposal.md`、增量 `spec.md`、`design.md` 和
`tasks.md`。计划需要调整时使用：

```text
$openspec-update-change <change-name>
```

确认规格与任务后执行：

```text
$openspec-apply-change <change-name>
```

实现完成后，同步增量规格并归档：

```text
$openspec-sync-specs <change-name>
$openspec-archive-change <change-name>
```

归档工作流也会检查规格同步状态，不要在任务、测试或性能验证尚未完成时归档。

## CLI 检查命令

在项目根目录运行：

```powershell
openspec list
openspec status --change <change-name>
openspec show <change-name>
openspec validate <change-name>
openspec doctor
```

## 目录含义

```text
openspec/
|-- config.yaml                 # StrataKV 架构上下文和生成规则
|-- specs/                      # 当前生效的系统规格
`-- changes/
    |-- <change-name>/          # 活跃变更及其 proposal/specs/design/tasks
    `-- archive/                # 已完成变更
```

Spec 描述系统必须满足的行为，代码和测试负责证明实现满足 Spec。性能结论必须来自
同一环境、同一负载下可复现的前后对比。
