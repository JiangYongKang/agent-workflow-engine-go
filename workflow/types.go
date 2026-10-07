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

// StepOrigin 标记步骤记录在一次执行中的来源，用于续跑结果审计。
type StepOrigin string

const (
	// StepOriginFresh 本次执行中新执行（原始提交）。
	StepOriginFresh StepOrigin = "fresh"
	// StepOriginReused 复用续跑链上游执行的成功产出，未重新执行。
	StepOriginReused StepOrigin = "reused"
	// StepOriginRerun 续跑中被真正重新执行（上次未完成或状态不确定）。
	StepOriginRerun StepOrigin = "rerun"
)

// PolicySource 标记一次执行实际使用的重试/超时规则来源。
type PolicySource string

const (
	// PolicySubmitted 原始提交时固化的策略。
	PolicySubmitted PolicySource = "submitted"
	// PolicyInherited 续跑沿用上游执行的策略快照。
	PolicyInherited PolicySource = "inherited"
	// PolicyOverride 续跑时显式携带的新策略。
	PolicyOverride PolicySource = "override"
)

// ResumeOptions 是续跑的可选参数。
type ResumeOptions struct {
	// Policy 非 nil 时本次续跑按新规则执行并固化为新快照；
	// 为 nil 时沿用被续跑执行的策略快照，行为可复现。
	Policy *ExecPolicy
}

// StepRecord 持久化单个步骤的执行轨迹。
type StepRecord struct {
	Stage    string
	Step     string
	Status   StepStatus
	Attempts int
	Output   map[string]string
	Err      *StepError
	// Origin 标记本记录是本次新跑、复用旧产出还是续跑重跑。
	Origin StepOrigin
	// SourceExec 仅 Origin=reused 时有效：产出最初由哪次执行生成。
	SourceExec string
}

// Execution 是一次工作流执行的完整可持久化记录。
// 原始提交与每次续跑各自产生一条独立记录，通过 RootID/ResumedFrom 串联成链；
// 链上已有记录一经终态即只读，不被后续续跑改写。
type Execution struct {
	ID         string
	IdemKey    string
	Definition Definition
	Policy     ExecPolicy
	// PolicySource 标记本次执行使用的策略来源（提交固化/续跑沿用/续跑覆盖）。
	PolicySource PolicySource
	// RootID 续跑链根（最初）执行 ID；原始执行为自身 ID。
	RootID string
	// ResumedFrom 本次续跑直接来源的执行 ID；原始执行为空。
	ResumedFrom  string
	Status       ExecutionStatus
	Steps        []*StepRecord
	Err          *StepError
	StartedAt    time.Time
	FinishedAt   time.Time
	Version      int64 // 乐观锁版本号
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
