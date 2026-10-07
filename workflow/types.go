// Package workflow 提供本地可运行的工作流自动化执行引擎。
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ExecutionStatus 表示一次工作流执行的生命周期状态。
type ExecutionStatus string

const (
	StatusPending   ExecutionStatus = "pending"
	StatusRunning   ExecutionStatus = "running"
	StatusSucceeded ExecutionStatus = "succeeded"
	StatusFailed    ExecutionStatus = "failed"
	StatusCancelled ExecutionStatus = "cancelled"
	StatusTimedOut  ExecutionStatus = "timed_out"
)

// Terminal 报告状态是否为终态。
func (s ExecutionStatus) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled, StatusTimedOut:
		return true
	}
	return false
}

// StepStatus 表示单个步骤执行记录的状态。
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
	// StepPartial 表示步骤已产生部分结果，但因取消/超时/失败被明确标记。
	StepPartial StepStatus = "partial"
)

// FailureClass 是重试耗尽或中断后对外暴露的可区分失败分类。
type FailureClass string

const (
	// ClassRetryExhausted 重试次数耗尽。
	ClassRetryExhausted FailureClass = "retry_exhausted"
	// ClassTimeout 步骤或执行超时。
	ClassTimeout FailureClass = "timeout"
	// ClassCancelled 执行被主动取消。
	ClassCancelled FailureClass = "cancelled"
	// ClassNonRetryable 执行器返回不可重试错误。
	ClassNonRetryable FailureClass = "non_retryable"
	// ClassDuplicate 幂等键冲突且策略为拒绝。
	ClassDuplicate FailureClass = "duplicate"
)

// StepError 是对外暴露的分类错误，底层执行器原始错误仅作为 Cause 保存，
// 不直接透出到对外接口。
type StepError struct {
	Class FailureClass
	Step  string
	Cause error
}

func (e *StepError) Error() string {
	if e.Cause != nil {
		return "step " + e.Step + ": " + string(e.Class) + ": " + e.Cause.Error()
	}
	return "step " + e.Step + ": " + string(e.Class)
}
func (e *StepError) Unwrap() error { return e.Cause }

// MarshalJSON 将 Cause 以消息字符串持久化（error 接口无法直接序列化）。
func (e *StepError) MarshalJSON() ([]byte, error) {
	cause := ""
	if e.Cause != nil {
		cause = e.Cause.Error()
	}
	return json.Marshal(struct {
		Class FailureClass `json:"class"`
		Step  string       `json:"step"`
		Cause string       `json:"cause,omitempty"`
	}{e.Class, e.Step, cause})
}

// UnmarshalJSON 恢复分类错误；Cause 还原为普通错误值。
func (e *StepError) UnmarshalJSON(b []byte) error {
	var a struct {
		Class FailureClass `json:"class"`
		Step  string       `json:"step"`
		Cause string       `json:"cause,omitempty"`
	}
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	e.Class, e.Step = a.Class, a.Step
	e.Cause = nil
	if a.Cause != "" {
		e.Cause = errors.New(a.Cause)
	}
	return nil
}

// RetryPolicy 描述单步骤的重试与超时策略。
type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
	StepTimeout time.Duration
}

// ExecPolicy 是提交时固化的执行策略快照，历史执行复现按当时规则。
type ExecPolicy struct {
	Retry         RetryPolicy
	ExecTimeout   time.Duration
	RejectOnDupID bool // 幂等键重复时拒绝而非返回既有结果
}

// Step 是工作流中的最小执行单元。
type Step struct {
	Name   string
	Action string            // 交给本地执行器的动作标识
	Input  map[string]string // 动作参数
}

// Stage 是一组并发执行的步骤；Stage 之间顺序执行。
type Stage struct {
	Name  string
	Steps []Step
}

// Definition 描述一个工作流：Stage 顺序执行，Stage 内 Step 并发执行。
type Definition struct {
	Name   string
	Stages []Stage
}

// StepRecord 持久化单个步骤的执行轨迹。
type StepRecord struct {
	Stage    string
	Step     string
	Status   StepStatus
	Attempts int
	Output   map[string]string
	Err      *StepError
}

// ResumeStatus 表示一次续跑记录的生命周期状态。
type ResumeStatus string

const (
	ResumePending   ResumeStatus = "pending"
	ResumeRunning   ResumeStatus = "running"
	ResumeSucceeded ResumeStatus = "succeeded"
	ResumeFailed    ResumeStatus = "failed"
	ResumeCancelled ResumeStatus = "cancelled"
	ResumeTimedOut  ResumeStatus = "timed_out"
)

// Terminal 报告续跑状态是否为终态。
func (s ResumeStatus) Terminal() bool {
	switch s {
	case ResumeSucceeded, ResumeFailed, ResumeCancelled, ResumeTimedOut:
		return true
	}
	return false
}

// StepOutcome 表示续跑中单个步骤的处置方式，用于区分“复用旧产出”与“真正重跑”。
type StepOutcome string

const (
	// OutcomeReused 步骤在上次执行中已成功，本次直接复用其产出，未触发执行器。
	OutcomeReused StepOutcome = "reused"
	// OutcomeReran 步骤上次未成功或状态不确定，本次真正重新执行。
	OutcomeReran StepOutcome = "reran"
)

// StepResumeRecord 是单个步骤在一次续跑中的记录：复用或重跑的判定与结果。
type StepResumeRecord struct {
	Stage    string
	Step     string
	Outcome  StepOutcome
	Status   StepStatus // 复用步骤为 succeeded；重跑步骤为本次实际终态
	Attempts int        // 仅重跑步骤有值
	Output   map[string]string
	Err      *StepError
}

// ResumeRecord 是一次续跑的独立可持久化记录，可一路关联回最初的执行。
type ResumeRecord struct {
	// ID 本次续跑记录自身的唯一 ID。
	ID string
	// RootExecutionID 最初执行的 ID，整条续跑链恒定不变。
	RootExecutionID string
	// ParentID 上一条记录：第一次续跑指向最初执行 ID，之后指向上一次续跑 ID。
	ParentID string
	// Attempt 续跑序号，从 1 开始。
	Attempt int
	// PolicyInherited 为 true 表示沿用最初执行固化的策略；false 表示本次显式携带新策略。
	PolicyInherited bool
	Policy          ExecPolicy
	Status          ResumeStatus
	Steps           []*StepResumeRecord
	Err             *StepError
	StartedAt       time.Time
	FinishedAt      time.Time
	Version         int64 // 乐观锁版本号
}

// FindStep 按 stage/step 名查找续跑步骤记录。
func (r *ResumeRecord) FindStep(stage, step string) *StepResumeRecord {
	for _, s := range r.Steps {
		if s.Stage == stage && s.Step == step {
			return s
		}
	}
	return nil
}

// ResumeChain 是最初执行与其全部续跑记录的审计视图，按续跑序号升序。
type ResumeChain struct {
	Root         *Execution
	Resumes      []*ResumeRecord
	HeadID       string // 链头记录 ID：无续跑时为最初执行 ID
	HeadIsResume bool   // 链头是否为续跑记录
}

// Execution 是一次工作流执行的完整可持久化记录。
type Execution struct {
	ID         string
	IdemKey    string
	Definition Definition
	Policy     ExecPolicy
	Status     ExecutionStatus
	Steps      []*StepRecord
	Err        *StepError
	StartedAt  time.Time
	FinishedAt time.Time
	Version    int64 // 乐观锁版本号
}

// FindStep 按 stage/step 名查找记录。
func (e *Execution) FindStep(stage, step string) *StepRecord {
	for _, r := range e.Steps {
		if r.Stage == stage && r.Step == step {
			return r
		}
	}
	return nil
}

// Executor 是受控的本地执行器接口，替代外部工具与模型服务。
type Executor interface {
	Execute(ctx context.Context, step Step) (map[string]string, error)
}

type nonRetryableError struct{ err error }

func (n nonRetryableError) Error() string { return n.err.Error() }
func (n nonRetryableError) Unwrap() error { return n.err }

// NonRetryable 包装错误，标记为不可重试。
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryableError{err: err}
}

// IsNonRetryable 判断错误是否被标记为不可重试。
func IsNonRetryable(err error) bool {
	var n nonRetryableError
	return errors.As(err, &n)
}
