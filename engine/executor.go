package engine

import (
	"context"
	"fmt"
	"sync"
)

// Executor 是受控的本地执行器接口，替代外部工具与模型服务。
type Executor interface {
	// Invoke 执行一个具名操作。实现必须是确定性的、本地可运行的。
	Invoke(ctx context.Context, op string, in map[string]any) (map[string]any, error)
}

// Handler 处理一次本地调用。
type Handler func(ctx context.Context, in map[string]any) (map[string]any, error)

// LocalExecutor 是本地受控执行器：通过注册的处理器表决定行为，
// 可在测试中编排失败、延迟与副作用计数。
type LocalExecutor struct {
	mu       sync.Mutex
	handlers map[string]Handler
	calls    map[string]int
}

// NewLocalExecutor 创建空的本地执行器。
func NewLocalExecutor() *LocalExecutor {
	return &LocalExecutor{
		handlers: make(map[string]Handler),
		calls:    make(map[string]int),
	}
}

// Register 注册一个操作的处理器。
func (e *LocalExecutor) Register(op string, h Handler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[op] = h
}

// Invoke 实现 Executor。未注册的操作返回错误；调用计数用于断言副作用不重复。
func (e *LocalExecutor) Invoke(ctx context.Context, op string, in map[string]any) (map[string]any, error) {
	e.mu.Lock()
	h, ok := e.handlers[op]
	if ok {
		e.calls[op]++
	}
	e.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("local executor: unregistered op %q", op)
	}
	return h(ctx, in)
}

// Calls 返回某操作被调用的次数（用于断言副作用不重复）。
func (e *LocalExecutor) Calls(op string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls[op]
}
