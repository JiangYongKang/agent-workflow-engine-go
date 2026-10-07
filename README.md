# agent-workflow-engine-go

本地可运行的工作流自动化执行引擎。用受控的本地执行器（`LocalExecutor`）替代外部工具与模型服务，
使多步骤任务在并发、失败与中断情况下仍能得到一致且可解释的结果。

## 执行模型

- **Definition → Stage → Step**：`Stage` 之间**顺序**执行，同一 `Stage` 内的 `Step` **并发**执行。
- 每个 `Step` 通过 `Executor` 接口交给本地执行器运行；`LocalExecutor` 不访问任何外部工具或模型服务，
  支持脚本化失败（`ScriptFailures`）、不可重试标记（`SetNonRetryable`）与自定义 handler，便于确定性测试。
- 引擎在每一步状态变化时持久化执行记录（`Store`），任何时刻中断都有一致状态可恢复。

### 状态机

执行状态：`pending → running → succeeded | failed | cancelled | timed_out`（后四者为终态，互相可区分）。

步骤状态：`pending → running → succeeded`；中断收尾时：
- `running` 中被打断的步骤 → **`partial`**（已尝试、可能产生部分副作用，明确标记而非静默丢弃）；
- 未开始的步骤 → **`skipped`**；
- 已成功步骤的输出**原样保留**（部分结果不丢弃）。

## 重试与失败分类

单步骤按 `RetryPolicy{MaxAttempts, Backoff, StepTimeout}` 重试；执行级另有 `ExecTimeout`。
重试耗尽或中断后，对外接口只返回分类错误 `*StepError`（底层执行器原始错误仅作为 `Cause` 包装，
不直接透出）：

| 分类 (`FailureClass`) | 含义 | 对应执行终态 |
|---|---|---|
| `retry_exhausted` | 重试次数耗尽 | `failed` |
| `non_retryable` | 执行器返回不可重试错误（`NonRetryable(err)` 包装） | `failed` |
| `timeout` | 步骤超时或执行级超时 | `timed_out` |
| `cancelled` | 主动取消（`Engine.Cancel`） | `cancelled` |
| `duplicate` | 幂等键冲突且策略为拒绝 | —（提交即拒绝） |

## 幂等规则

- `Submit(ctx, def, idemKey, policy)` 以幂等键标识一次执行。
- 并发提交相同幂等键：只有一个执行生效，其余提交**等待并返回既有结果**；
  若策略 `RejectOnDupID=true`，重复提交立即以 `duplicate` 分类错误拒绝。
- 已完结的同键执行：重复提交返回既有结果（或按策略拒绝），**同一步骤不会重复产生副作用**
  （测试以执行器调用计数断言）。

## 状态持久化与恢复

- `Store` 接口有两个实现：`MemoryStore`（进程内）与 `FileStore`（JSON 文件，原子写，进程重启后可恢复）。
  写入带乐观锁版本号，防止并发覆盖。
- **策略快照**：提交时的 `ExecPolicy` 随执行记录固化，历史执行在策略变更后仍按当时规则复现。
- `Recover(ctx)` 扫描非终态执行并续跑：**已成功步骤跳过不重跑**，`running`/`pending` 步骤按快照策略继续；
  恢复后幂等键仍然生效。

## 断点续跑（Resume）

对已终态的 `failed` / `cancelled` / `timed_out` 执行，可显式发起续跑：
`Resume(ctx, execID, ResumeOptions{})`。**已成功执行与进行中执行会被拒绝**。

### 哪些步骤被复用、哪些被重跑

续跑生成一条**独立的新执行记录**（不改写来源记录），按来源执行中各步骤的终态判定：

| 来源步骤状态 | 续跑处理 | 新记录中 `Origin` |
|---|---|---|
| `succeeded` | **复用旧产出**，不重新执行、不重复触发副作用；`SourceExec` 追溯到真正产出生的那次执行 | `reused` |
| `partial` / `failed` / `skipped` / `pending` | 重置为 `pending` 后**真正重新执行** | `rerun` |

续跑结果的每个 `StepRecord` 都带 `Origin`/`SourceExec`，可直接看出本步骤是复用还是重跑、产出来自哪次执行。

### 追溯与只读

- 每次续跑产生独立记录：`ResumedFrom` 指向直接来源，`RootID` 指向链根（最初的执行）；
  `ResumeChain(execID)` 返回整条链（按开始时间升序），可审计哪一步在什么时候被补救过。
- 链上已有记录一经终态即**只读**：续跑只新增记录，从不动旧记录（单测以序列化快照断言）。

### 策略规则

- 默认**沿用**来源执行的策略快照（`PolicySource=inherited`），行为可复现；
- `ResumeOptions{Policy: &p}` 显式携带新策略时按新规则走（`PolicySource=override`），并固化为新快照供后续续跑沿用；
- 原始提交记为 `PolicySource=submitted`。

### 并发与幂等

- 同一续跑链上并发 `Resume`：只有一个真正生效，其余**等待并拿到同一份结果**，副作用不重复；
- 续跑途中再被取消/超时：新执行记录按既有状态机收尾（`partial`/`skipped` 标记一致），
  可对最新记录再次 `Resume` 链式推进；已成功步骤在任意多次续跑中都不会被重跑；
- 续跑执行同样受 `Cancel` 与崩溃恢复（`Recover`）覆盖。

## 本地验证

```bash
go test ./...          # 单元测试（含 -race 可通过：go test -race ./...）
go run ./cmd/demo      # 端到端演示：重试、幂等去重、崩溃恢复、断点续跑
```

测试覆盖（`workflow/engine_test.go`），单测日志打印执行流水（`[flow]` 前缀）与判定依据：

- `TestConcurrentDuplicateSubmit`：8 路并发同键提交收敛到同一执行，每动作恰好调用一次；
- `TestRetryThenSuccess` / `TestRetryExhaustedClassification` / `TestNonRetryableClassification`：重试与失败分类；
- `TestStepTimeoutClassification` / `TestExecutionTimeout`：步骤级与执行级超时，状态为 `timed_out`；
- `TestCancelMarksPartialResults`：取消后已成功步骤保留输出、中断步骤标记 `partial`、未执行步骤标记 `skipped`；
- `TestRecoverAfterInterruption` / `TestRecoverFailureClassification`：从文件存储恢复中断执行，
  已成功步骤不重跑、按快照策略复现、失败仍输出分类错误。

续跑覆盖（`workflow/resume_test.go`）：

- `TestResumeReusesSucceededOutputs`：续跑复用已成功步骤旧产出（`reused`+`SourceExec`），
  失败/未执行步骤重跑（`rerun`），副作用不重复；
- `TestConcurrentResumeConverges`：8 路并发续跑收敛到同一结果执行，每动作调用次数精确；
- `TestChainedResumesNoRepeatedSideEffects`：连续多次续跑链式推进，已成功步骤全程不重跑，
  链上记录可关联回最初执行，且最初执行记录内容逐字节未变；
- `TestResumePolicyOverride`：默认沿用原策略复现失败，显式覆盖新策略后按新规则成功，
  记录中 `PolicySource` 可分辨；
- `TestResumeRejectsInvalidTargets`：已成功/不存在的执行拒绝续跑。

## 目录结构

```
workflow/       引擎核心：types / executor / store / engine + 测试
cmd/demo/       本地端到端演示
```
