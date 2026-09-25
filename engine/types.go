// Package engine 提供本地可运行的工作流自动化执行引擎。
package engine

import (
	"context"
	"time"
)

// RunStatus 表示一次工作流执行的终态或中间态。
type RunStatus string

const (
	StatusPending   RunStatus = "pending"
	StatusRunning   RunStatus = "running"
	StatusSucceeded RunStatus = "succeeded"
	StatusFailed    RunStatus = "failed"
	StatusCanceled  RunStatus = "canceled"
	StatusTimedOut  RunStatus = "timed_out"
)

// Terminal 判断状态是否为终态。
func (s RunStatus) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCanceled, StatusTimedOut:
		return true
	}
	return false
}

// FailureClass 是可对外暴露的失败分类，不泄露底层执行器原始错误。
type FailureClass string

const (
	FailureNone        FailureClass = ""
	FailureStep        FailureClass = "step_failed"          // 步骤重试耗尽
	FailureStepTimeout FailureClass = "step_timeout"         // 单步超时
	FailureRunTimeout  FailureClass = "run_timeout"          // 整体执行超时
	FailureCanceled    FailureClass = "canceled"             // 主动取消
	FailureConflict    FailureClass = "idempotency_conflict" // 幂等键冲突
)

// RetryPolicy 描述单步重试策略。
type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

// StepPolicy 是步骤级策略快照，随执行记录持久化，保证历史执行按当时规则复现。
type StepPolicy struct {
	Retry   RetryPolicy
	Timeout time.Duration
}

// Step 定义一个可执行步骤。
type Step struct {
	Name     string
	Policy   StepPolicy
	Parallel []Step // 非空时作为并发步骤组执行，忽略 Action
	Action   StepAction
}

// StepAction 是受控本地执行器的执行单元。
type StepAction func(ctx context.Context, exec Executor, in map[string]any) (map[string]any, error)

// Workflow 是一次执行的静态定义。
type Workflow struct {
	Name  string
	Steps []Step
}

// StepRecord 记录单个步骤的执行历史。
type StepRecord struct {
	Name     string         `json:"name"`
	Status   RunStatus      `json:"status"`
	Attempts int            `json:"attempts"`
	Output   map[string]any `json:"output,omitempty"`
	Failure  FailureClass   `json:"failure,omitempty"`
	Detail   string         `json:"detail,omitempty"`  // 脱敏后的说明，不含原始错误
	Partial  bool           `json:"partial,omitempty"` // 取消/超时时已产生的部分结果标记
	Children []StepRecord   `json:"children,omitempty"`
}

// RunRecord 是一次执行的持久化记录。
type RunRecord struct {
	ID             string       `json:"id"`
	IdempotencyKey string       `json:"idempotency_key"`
	Workflow       Workflow     `json:"-"`
	WorkflowName   string       `json:"workflow_name"`
	Status         RunStatus    `json:"status"`
	Failure        FailureClass `json:"failure,omitempty"`
	Detail         string       `json:"detail,omitempty"`
	Steps          []StepRecord `json:"steps"`
	Policies       []StepPolicy `json:"policies"`  // 提交时的策略快照，恢复时按此复现
	NextStep       int          `json:"next_step"` // 恢复点：下一个待执行的下标
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
	Deadline       time.Time    `json:"deadline,omitempty"`
}
