# agent-workflow-engine-go

本地可运行的工作流自动化执行引擎。用**受控的本地执行器**（`LocalExecutor`，基于注册处理器表）替代外部工具与模型服务，使多步骤任务在并发、失败与中断情况下仍得到一致且可解释的结果。

## 执行模型

- `Workflow` 由有序的 `Step` 列表构成，步骤默认**顺序执行**。
- 步骤设置 `Parallel []Step` 后成为**并发步骤组**：子步骤并发执行，任一子步骤失败则整组失败；取消/超时时已完成子步骤的结果保留，整组标记 `partial=true`。
- 每个步骤通过 `StepAction` 调用 `Executor.Invoke`，引擎本身不发起任何外部调用。
- 引擎每完成一个步骤即把 `RunRecord`（含 `NextStep` 恢复点、各步骤状态与输出）持久化到 `Store`。

## 重试与失败分类

- 步骤级策略 `StepPolicy{Retry, Timeout}`：单次尝试超时由 `Timeout` 控制，失败后按 `Retry.Backoff` 退避重试，最多 `Retry.MaxAttempts` 次。
- 整体执行超时由 `WithRunTimeout` 控制。
- 重试耗尽后只对外暴露**失败分类**（`FailureClass`），执行器原始错误绝不透出到 `RunRecord.Detail` 等对外字段：

| 分类 | 含义 | 对应终态 |
|---|---|---|
| `step_failed` | 步骤重试耗尽（非超时） | `failed` |
| `step_timeout` | 单步超时且重试耗尽 | `failed` |
| `run_timeout` | 整体执行超时 | `timed_out` |
| `canceled` | 主动取消 | `canceled` |
| `idempotency_conflict` | 幂等键被不同工作流占用 | （提交即拒绝） |

取消、超时与失败三者始终可区分（`StatusCanceled` / `StatusTimedOut` / `StatusFailed`）。

## 幂等规则

- 每次提交必须携带幂等键；`Store.CreateIfAbsent` 保证**并发提交相同幂等键时只有一个执行生效**。
- 重复提交（键 + 工作流指纹一致）等待既有执行到达终态后**返回既有结果**，同一步骤不会重复产生副作用（测试断言执行器调用次数为 1）。
- 同一幂等键被**不同工作流**（指纹不同）占用时，提交被明确拒绝并返回 `ErrConflict`。

## 状态恢复语义

- `FileStore` 以 JSON 文件持久化（临时文件 + rename 原子写入）；进程重启后用同一路径重新打开即可。
- `Recover` 扫描所有非终态记录，从 `NextStep` 恢复点继续，**已完成步骤绝不重放**。
- 提交时的步骤策略会快照进 `RunRecord.Policies`：恢复时即使注册了策略已变更的新版工作流，历史执行仍**按提交时规则复现**。
- 恢复前需用 `engine.Register(wf)` 重新绑定步骤 Action（函数无法序列化）；未注册工作流的执行会被显式标记失败而非悬挂。
- 取消/超时时已产生的部分结果保留在记录中并显式标记 `Partial=true`，不会静默丢弃。

## 本地验证

```bash
go build ./...
go vet ./...
go test -race -count=1 -v ./...
```

测试覆盖：并发重复提交（副作用仅一次）、幂等键冲突拒绝、重试耗尽分类（`step_failed` / `step_timeout`，且原始错误不透出）、并发步骤组、崩溃后从文件存储恢复（已完成步骤零重放）、策略快照复现、取消链路与整体超时（三者可区分、部分结果显式标记）。单测日志通过 `logRun` 打印每条执行的流水（步骤状态、尝试次数、输出、分类）与判定依据。
