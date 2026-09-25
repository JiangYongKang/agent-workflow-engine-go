package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Option 引擎配置项。
type Option func(*Engine)

// WithRunTimeout 设置单次执行的整体超时。
func WithRunTimeout(d time.Duration) Option {
	return func(e *Engine) { e.runTimeout = d }
}

// Engine 是工作流执行引擎。
type Engine struct {
	exec       Executor
	store      Store
	runTimeout time.Duration

	mu        sync.Mutex
	cancels   map[string]context.CancelFunc // idempotency key -> cancel
	workflows map[string]Workflow           // 已注册工作流（用于恢复时重新绑定 Action）
}

// New 创建引擎。
func New(exec Executor, store Store, opts ...Option) *Engine {
	e := &Engine{
		exec:      exec,
		store:     store,
		cancels:   make(map[string]context.CancelFunc),
		workflows: make(map[string]Workflow),
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// fingerprint 计算工作流定义的指纹：名称 + 步骤名 + 策略快照。
// 相同幂等键只有指纹一致才允许复用既有执行。
func fingerprint(wf Workflow) string {
	type stepKey struct {
		Name   string     `json:"name"`
		Policy StepPolicy `json:"policy"`
	}
	var steps []stepKey
	var walk func(ss []Step)
	walk = func(ss []Step) {
		for _, s := range ss {
			steps = append(steps, stepKey{Name: s.Name, Policy: s.Policy})
			walk(s.Parallel)
		}
	}
	walk(wf.Steps)
	b, _ := json.Marshal(struct {
		Name  string    `json:"name"`
		Steps []stepKey `json:"steps"`
	}{wf.Name, steps})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func newRunID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Register 注册工作流定义（绑定 Action），供恢复时重新关联历史执行。
// Submit 会自动注册；进程重启后应先 Register 再 Recover。
func (e *Engine) Register(wf Workflow) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.workflows[wf.Name] = wf
}

// Submit 提交一次执行。相同幂等键的并发提交只有一个生效；
// 重复提交返回既有结果，键被不同工作流占用时返回 ErrConflict。
func (e *Engine) Submit(ctx context.Context, wf Workflow, idempotencyKey string) (*RunRecord, error) {
	e.Register(wf)

	now := time.Now()
	rec := &RunRecord{
		ID:             newRunID(),
		IdempotencyKey: idempotencyKey,
		WorkflowName:   wf.Name,
		Status:         StatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	for _, s := range wf.Steps {
		rec.Steps = append(rec.Steps, StepRecord{Name: s.Name, Status: StatusPending})
		rec.Policies = append(rec.Policies, s.Policy)
	}

	existing, created, err := e.store.CreateIfAbsent(rec, fingerprint(wf))
	if err != nil {
		return nil, err
	}
	if !created {
		// 重复提交：等待既有执行到达终态后返回其结果。
		return e.waitTerminal(ctx, existing.IdempotencyKey)
	}

	return e.run(ctx, wf, rec, 0)
}

// waitTerminal 轮询存储直到记录到达终态。
func (e *Engine) waitTerminal(ctx context.Context, key string) (*RunRecord, error) {
	for {
		rec, err := e.store.Get(key)
		if err != nil {
			return nil, err
		}
		if rec.Status.Terminal() {
			return rec, nil
		}
		select {
		case <-ctx.Done():
			return rec, nil // 返回当前已知状态，不伪造结果
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// run 执行工作流主循环，从 fromStep 开始（恢复时非 0）。
func (e *Engine) run(ctx context.Context, wf Workflow, rec *RunRecord, fromStep int) (*RunRecord, error) {
	runCtx := ctx
	cancel := func() {}
	if e.runTimeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, e.runTimeout)
	} else {
		runCtx, cancel = context.WithCancel(ctx)
	}
	e.mu.Lock()
	e.cancels[rec.IdempotencyKey] = cancel
	e.mu.Unlock()
	defer func() {
		cancel()
		e.mu.Lock()
		delete(e.cancels, rec.IdempotencyKey)
		e.mu.Unlock()
	}()

	rec.Status = StatusRunning
	rec.UpdatedAt = time.Now()
	if err := e.store.Update(rec); err != nil {
		return nil, err
	}

	for i := fromStep; i < len(wf.Steps); i++ {
		if cls, ok := runInterrupted(runCtx); ok {
			return e.finishRun(rec, cls)
		}
		e.execStep(runCtx, rec, i, wf.Steps[i])
		rec.NextStep = i + 1
		rec.UpdatedAt = time.Now()
		if err := e.store.Update(rec); err != nil {
			return nil, err
		}
		if rec.Steps[i].Status == StatusFailed {
			rec.Failure = rec.Steps[i].Failure
			rec.Detail = rec.Steps[i].Detail
			rec.Status = StatusFailed
			rec.UpdatedAt = time.Now()
			return rec, e.store.Update(rec)
		}
		if rec.Steps[i].Status == StatusCanceled || rec.Steps[i].Status == StatusTimedOut {
			// 步骤被运行级取消/超时中断：立即收尾，不再推进后续步骤。
			cls, ok := runInterrupted(runCtx)
			if !ok {
				cls = FailureCanceled
			}
			return e.finishRun(rec, cls)
		}
	}

	if cls, ok := runInterrupted(runCtx); ok {
		return e.finishRun(rec, cls)
	}
	rec.Status = StatusSucceeded
	rec.UpdatedAt = time.Now()
	return rec, e.store.Update(rec)
}

// runInterrupted 判断运行上下文是否被取消或整体超时。
func runInterrupted(ctx context.Context) (FailureClass, bool) {
	if ctx.Err() == nil {
		return FailureNone, false
	}
	if ctx.Err() == context.DeadlineExceeded {
		return FailureRunTimeout, true
	}
	return FailureCanceled, true
}

// finishRun 在取消/整体超时时收尾：未完成步骤标记为对应状态，
// 已产生的部分结果保留并显式标记 Partial。
func (e *Engine) finishRun(rec *RunRecord, cls FailureClass) (*RunRecord, error) {
	for i := rec.NextStep; i < len(rec.Steps); i++ {
		sr := &rec.Steps[i]
		if sr.Status == StatusPending || sr.Status == StatusRunning {
			sr.Partial = sr.Output != nil
			if cls == FailureRunTimeout {
				sr.Status = StatusTimedOut
			} else {
				sr.Status = StatusCanceled
			}
		}
	}
	rec.Failure = cls
	if cls == FailureRunTimeout {
		rec.Status = StatusTimedOut
		rec.Detail = "run exceeded overall timeout"
	} else {
		rec.Status = StatusCanceled
		rec.Detail = "run canceled"
	}
	rec.UpdatedAt = time.Now()
	return rec, e.store.Update(rec)
}

// execStep 执行单个步骤（含并发组），带重试与超时，结果写入 rec.Steps[idx]。
// 对外只记录失败分类与脱敏说明，不透出执行器原始错误。
func (e *Engine) execStep(ctx context.Context, rec *RunRecord, idx int, step Step) {
	sr := &rec.Steps[idx]
	sr.Status = StatusRunning

	if len(step.Parallel) > 0 {
		e.execParallel(ctx, sr, step)
		return
	}

	pol := step.Policy
	attempts := pol.Retry.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	var lastTimeout bool
	for a := 1; a <= attempts; a++ {
		sr.Attempts = a
		attemptCtx := ctx
		cancel := func() {}
		if pol.Timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, pol.Timeout)
		}
		out, err := e.invoke(attemptCtx, step, rec)
		cancel()
		if err == nil {
			sr.Status = StatusSucceeded
			sr.Output = out
			return
		}
		if cls, ok := runInterrupted(ctx); ok {
			// 运行级取消/超时：标记部分结果，不记为步骤失败。
			sr.Partial = out != nil
			if cls == FailureRunTimeout {
				sr.Status = StatusTimedOut
			} else {
				sr.Status = StatusCanceled
			}
			return
		}
		lastTimeout = attemptCtx.Err() == context.DeadlineExceeded
		if a < attempts && pol.Retry.Backoff > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(pol.Retry.Backoff):
			}
		}
	}
	// 重试耗尽：只记录分类与脱敏说明。
	sr.Status = StatusFailed
	if lastTimeout {
		sr.Failure = FailureStepTimeout
		sr.Detail = fmt.Sprintf("step %q timed out after %d attempt(s)", step.Name, sr.Attempts)
	} else {
		sr.Failure = FailureStep
		sr.Detail = fmt.Sprintf("step %q failed after %d attempt(s)", step.Name, sr.Attempts)
	}
}

// invoke 调用步骤 Action；Action 为空时视为纯编排步骤直接成功。
func (e *Engine) invoke(ctx context.Context, step Step, rec *RunRecord) (map[string]any, error) {
	if step.Action == nil {
		return map[string]any{}, nil
	}
	in := map[string]any{"run_id": rec.ID, "step": step.Name}
	return step.Action(ctx, e.exec, in)
}

// execParallel 并发执行步骤组；任一子步骤失败则整组失败。
func (e *Engine) execParallel(ctx context.Context, sr *StepRecord, step Step) {
	sr.Children = make([]StepRecord, len(step.Parallel))
	for i, c := range step.Parallel {
		sr.Children[i] = StepRecord{Name: c.Name, Status: StatusPending}
	}
	childRec := &RunRecord{ID: sr.Name, Steps: sr.Children}
	var wg sync.WaitGroup
	for i, c := range step.Parallel {
		wg.Add(1)
		go func(i int, c Step) {
			defer wg.Done()
			e.execStep(ctx, childRec, i, c)
		}(i, c)
	}
	wg.Wait()

	failed := false
	interrupted := false
	for i := range sr.Children {
		c := &sr.Children[i]
		switch c.Status {
		case StatusFailed:
			failed = true
		case StatusCanceled, StatusTimedOut:
			interrupted = true
		}
	}
	switch {
	case failed:
		sr.Status = StatusFailed
		sr.Failure = FailureStep
		sr.Detail = fmt.Sprintf("parallel step %q has failed child", step.Name)
	case interrupted:
		// 取消/超时：已完成子步骤的结果保留，整组标记为部分完成。
		sr.Partial = true
		if ctx.Err() == context.DeadlineExceeded {
			sr.Status = StatusTimedOut
		} else {
			sr.Status = StatusCanceled
		}
	default:
		sr.Status = StatusSucceeded
	}
}

// Cancel 取消一次进行中的执行；已完成执行不受影响。
func (e *Engine) Cancel(idempotencyKey string) error {
	e.mu.Lock()
	cancel, ok := e.cancels[idempotencyKey]
	e.mu.Unlock()
	if ok {
		cancel()
		return nil
	}
	rec, err := e.store.Get(idempotencyKey)
	if err != nil {
		return err
	}
	if rec.Status.Terminal() {
		return nil
	}
	// 非本进程活动执行（如重启后未恢复）：直接落终态。
	rec.Status = StatusCanceled
	rec.Failure = FailureCanceled
	rec.Detail = "run canceled"
	rec.UpdatedAt = time.Now()
	return e.store.Update(rec)
}

// Recover 扫描未完成的执行并从一致状态继续。
// 策略以执行记录中的快照为准，保证历史执行按当时规则复现。
func (e *Engine) Recover(ctx context.Context) ([]*RunRecord, error) {
	incomplete, err := e.store.ListIncomplete()
	if err != nil {
		return nil, err
	}
	var out []*RunRecord
	for _, rec := range incomplete {
		e.mu.Lock()
		wf, ok := e.workflows[rec.WorkflowName]
		e.mu.Unlock()
		if !ok {
			// 无法重新绑定 Action：显式标记失败而不是悬挂。
			rec.Status = StatusFailed
			rec.Failure = FailureStep
			rec.Detail = fmt.Sprintf("workflow %q not registered for recovery", rec.WorkflowName)
			rec.UpdatedAt = time.Now()
			if err := e.store.Update(rec); err != nil {
				return out, err
			}
			out = append(out, rec)
			continue
		}
		// 按快照策略复现：用记录中的步骤名对齐当前工作流。
		aligned := alignPolicies(wf, rec)
		final, err := e.run(ctx, aligned, rec, rec.NextStep)
		if err != nil {
			return out, err
		}
		out = append(out, final)
	}
	return out, nil
}

// alignPolicies 用执行记录中的步骤快照对齐工作流步骤（按名字），
// 保证恢复时历史执行仍按提交时的步骤集合与策略推进。
func alignPolicies(wf Workflow, rec *RunRecord) Workflow {
	byName := make(map[string]Step, len(wf.Steps))
	for _, s := range wf.Steps {
		byName[s.Name] = s
	}
	steps := make([]Step, 0, len(rec.Steps))
	for i, sr := range rec.Steps {
		if s, ok := byName[sr.Name]; ok {
			// 策略以提交时快照为准，而非当前注册的工作流定义。
			if i < len(rec.Policies) {
				s.Policy = rec.Policies[i]
			}
			steps = append(steps, s)
		}
	}
	return Workflow{Name: wf.Name, Steps: steps}
}

// Get 查询执行记录。
func (e *Engine) Get(idempotencyKey string) (*RunRecord, error) {
	return e.store.Get(idempotencyKey)
}
