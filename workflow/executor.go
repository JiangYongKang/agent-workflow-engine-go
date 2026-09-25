package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// LocalExecutor 是受控的本地执行器：不访问外部工具或模型服务，
// 通过脚本化的失败计划支持确定性测试。
type LocalExecutor struct {
	mu       sync.Mutex
	calls    map[string]int
	failures map[string][]error // action -> 按调用次序消费的失败脚本
	nonRetry map[string]bool
	handler  func(ctx context.Context, step Step) (map[string]string, error)
}

// NewLocalExecutor 创建本地执行器。
func NewLocalExecutor() *LocalExecutor {
	return &LocalExecutor{
		calls:    map[string]int{},
		failures: map[string][]error{},
		nonRetry: map[string]bool{},
	}
}

// ScriptFailures 为某动作编排前 n 次调用的失败序列（按调用次序消费）。
func (l *LocalExecutor) ScriptFailures(action string, errs ...error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failures[action] = append(l.failures[action], errs...)
}

// SetNonRetryable 标记某动作的失败不可重试。
func (l *LocalExecutor) SetNonRetryable(action string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nonRetry[action] = true
}

// SetHandler 自定义动作执行逻辑；未设置时使用内置确定性回显逻辑。
func (l *LocalExecutor) SetHandler(h func(ctx context.Context, step Step) (map[string]string, error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.handler = h
}

// Calls 返回某动作被实际调用的次数（用于断言副作用不重复）。
func (l *LocalExecutor) Calls(action string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls[action]
}

// Execute 实现 Executor。
func (l *LocalExecutor) Execute(ctx context.Context, step Step) (map[string]string, error) {
	l.mu.Lock()
	l.calls[step.Action]++
	var fail error
	if q := l.failures[step.Action]; len(q) > 0 {
		fail = q[0]
		l.failures[step.Action] = q[1:]
		if l.nonRetry[step.Action] {
			fail = NonRetryable(fail)
		}
	}
	h := l.handler
	l.mu.Unlock()

	if fail != nil {
		return nil, fail
	}
	if h != nil {
		return h(ctx, step)
	}
	// 内置确定性逻辑：回显输入，支持 input["sleep_ms"] 模拟耗时、
	// 支持 ctx 取消（用于取消/超时链路测试）。
	if v, ok := step.Input["sleep_ms"]; ok {
		var ms int
		if _, err := fmt.Sscanf(v, "%d", &ms); err == nil && ms > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(ms) * time.Millisecond):
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := map[string]string{"action": step.Action}
	for k, v := range step.Input {
		out["echo."+k] = v
	}
	return out, nil
}

// ErrScripted 是脚本化失败的哨兵错误，便于测试断言。
var ErrScripted = errors.New("scripted failure")
