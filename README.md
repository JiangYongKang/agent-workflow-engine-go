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
  恢复后幂等键仍然生效。`Recover` 同时接管中断在一半的**续跑记录**（见下节）。

## 断点续跑（Resume）

执行以失败/取消/超时终态结束后，可用 `Engine.Resume(ctx, executionID, opt)` 显式发起一次续跑，
不必整条从头再来。仅**终态但未成功**的执行可续跑（已成功无需续跑；进行中的执行请用 `Recover`）。

### 哪些步骤复用、哪些重跑

每次续跑基于**链头最新状态**逐步骤判定（结果见 `StepResumeRecord.Outcome`）：

| 步骤在上次记录中的状态 | 本次处置 | Outcome |
|---|---|---|
| `succeeded`（首跑或任意一次续跑中成功） | **复用当时产出**，不触发执行器，已算数的副作用不重复 | `reused` |
| `failed` / `partial`（跑到一半、状态不确定）/ `skipped` / `pending` / `running` | **必须重新执行** | `reran` |

- 续跑结果中每个步骤都带有 `outcome=reused|reran`，可明确分辨是复用旧产出还是真正重跑；
  重跑步骤另含 `attempts`、`output` 与分类错误。
- 重跑仍走完整的重试 / 步骤超时 / 执行超时 / 取消分类链路。
- 全步骤均已成功时再次续跑：全部复用、**零执行器调用**。

### 追溯与审计

- 每次续跑产生一条**独立、不可变**的 `ResumeRecord`（独立 ID、独立持久化文件），字段包括：
  `RootExecutionID`（最初执行 ID，全链恒定）、`ParentID`（首次指向最初执行，其后指向上一次续跑）、
  `Attempt`（从 1 递增）、`Policy` 与 `PolicyInherited`。
- **最初执行记录保持只读、永不被改写**：续跑只新增记录，不回写 `Execution`
  （测试以序列化快照前后比对断言）。
- `ResumeHistory(executionID)` 返回 `ResumeChain{Root, Resumes(按序号升序), HeadID}`，
  可一路关联回最初执行，审计“哪一步在什么时候被补救、每一次用的哪套规则”。

### 策略规则

- 默认**沿用最初执行提交时固化的策略快照**（`PolicyInherited=true`），历史行为可复现；
- `ResumeOption{Policy: &p}` 显式携带新策略时按新规则执行，记录中 `PolicyInherited=false`，
  审计时可与继承策略的续跑区分。
- 例外：接管一条中断在一半的续跑（见下）时沿用该续跑记录自身固化的策略，忽略本次携带的策略。

### 并发收敛与再次中断

- 同一执行并发续跑：进程内同一时刻只有一个真正生效；其余调用**默认等待并拿到同一份结果**；
  `RejectIfBusy=true` 时立即以 `duplicate` 分类错误拒绝。
- 实现：每个最初执行有一把串行锁贯穿“解析→执行→收尾”，配合进行中占位（`liveResume`）让
  在执行期间到达的调用等待同一结果；执行结束后保留一个很短的**收敛宽限窗口**（50ms），
  把“已进入引擎但因调度尚未登记”的同批迟到者也合并到同一结果。严格在（宽限结束后）才发起的
  调用被视为一次新的独立续跑，产生新记录——测试 `TestResumeStrictlySequentialCreatesNewRecord` 覆盖该边界。
- 存储层另有**链头指针乐观锁 CAS**（`SaveResumeHead`），防止跨引擎/进程的并发续跑产生分叉记录；
  CAS 落败方不执行，直接返回获胜方的记录。
- 续跑途中被取消/超时：续跑记录进入 `cancelled`/`timed_out` 终态，中断步骤标记 `partial`、
  已成功步骤（含本轮已成功）保留；之后可基于链头最新状态继续续跑，已成功步骤不会被再执行一遍。
- 续跑记录本身可恢复：`Recover` 会扫描非终态 `ResumeRecord` 并由（可能是重启后的）引擎接管同一条记录，
  链头不分叉、副作用不重复。

## 本地验证

```bash
go test ./...          # 单元测试（含 -race 可通过：go test -race ./...）
go run ./cmd/demo      # 端到端演示：重试、幂等去重、崩溃恢复、断点续跑
```

测试覆盖（`workflow/engine_test.go` 与 `workflow/resume_test.go`），单测日志打印执行流水（`[flow]` 前缀）与判定依据：

- `TestConcurrentDuplicateSubmit`：8 路并发同键提交收敛到同一执行，每动作恰好调用一次；
- `TestRetryThenSuccess` / `TestRetryExhaustedClassification` / `TestNonRetryableClassification`：重试与失败分类；
- `TestStepTimeoutClassification` / `TestExecutionTimeout`：步骤级与执行级超时，状态为 `timed_out`；
- `TestCancelMarksPartialResults`：取消后已成功步骤保留输出、中断步骤标记 `partial`、未执行步骤标记 `skipped`；
- `TestRecoverAfterInterruption` / `TestRecoverFailureClassification`：从文件存储恢复中断执行，
  已成功步骤不重跑、按快照策略复现、失败仍输出分类错误。
- `TestResumeReusesPreviousOutputs`：续跑复用已成功步骤旧产出（`reused`，零执行器调用）、
  失败步骤真正重跑（`reran`），策略默认继承，原始执行记录只读不被改写；
- `TestConcurrentResumeConverges`：6 路并发续跑收敛到同一份结果、副作用不重复，
  `RejectIfBusy` 以 `duplicate` 分类拒绝；
- `TestResumeStrictlySequentialCreatesNewRecord`：严格顺序（跨宽限窗口）的两次续跑产生独立新记录，
  宽限不会错误合并后续独立续跑；
- `TestResumeRepeatedNoDuplicateSideEffects`：连续 3 次续跑成链，已成功步骤（含前一次续跑中成功的）
  全程不重跑；显式新策略与继承策略在记录中可分辨；`ParentID`/`Attempt` 成链可审计；
- `TestResumeCancelledThenResumeAgain`：续跑被取消后 `partial` 步骤必须重跑、其余复用，
  跨引擎（模拟重启）再续跑副作用计数精确；
- `TestRecoverInterruptedResume`：`Recover` 接管中断的续跑记录本身，链头不分叉、零重复副作用。

## 目录结构

```
workflow/       引擎核心：types / executor / store / engine + resume（断点续跑）及测试
cmd/demo/       本地端到端演示
```
