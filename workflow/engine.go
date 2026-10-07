package workflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// inflight 跟踪一次进行中的执行，用于幂等键去重与取消。
type inflight struct {
	id     string
	done   chan struct{}
	cancel context.CancelFunc
}

// Engine 是工作流执行引擎。
type Engine struct {
	exec  Executor
	store Store

	mu   sync.Mutex
	live map[string]*inflight // 按幂等键去重
	byID map[string]*inflight // 按执行 ID 取消
	// liveResume 按最初执行 ID 串行化同一执行的续跑，保证并发续跑只有一个生效。
	liveResume map[string]*inflight
	// resumeLocks 为每个最初执行提供一把串行锁：续跑的“解析→执行→收尾”全程持锁。
	resumeLocks map[string]*sync.Mutex
	// resumeBatchEnd 记录每个 root 当前续跑批次的收敛宽限截止时刻。
	resumeBatchEnd map[string]time.Time
	saveMu         sync.Mutex // 串行化执行记录的变更与持久化
	logf           func(format string, args ...any)
}

// NewEngine 创建引擎。
func NewEngine(exec Executor, store Store) *Engine {
	return &Engine{
		exec:           exec,
		store:          store,
		live:           map[string]*inflight{},
		byID:           map[string]*inflight{},
		liveResume:     map[string]*inflight{},
		resumeLocks:    map[string]*sync.Mutex{},
		resumeBatchEnd: map[string]time.Time{},
		logf:           func(string, ...any) {},
	}
}

// SetLogger 注入执行流水日志函数（测试与本地验证用）。
func (e *Engine) SetLogger(logf func(format string, args ...any)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logf = logf
}

func (e *Engine) log(format string, args ...any) {
	e.mu.Lock()
	logf := e.logf
	e.mu.Unlock()
	logf(format, args...)
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b))
}

// Submit 以幂等键提交一次执行。
// 并发提交相同幂等键时只有一个执行生效；重复提交返回既有结果，
// 除非策略 RejectOnDupID 为 true，此时返回 ClassDuplicate 错误。
func (e *Engine) Submit(ctx context.Context, def Definition, idemKey string, policy ExecPolicy) (*Execution, error) {
	if idemKey == "" {
		return nil, fmt.Errorf("idempotency key must not be empty")
	}
	e.mu.Lock()
	if f, ok := e.live[idemKey]; ok {
		e.mu.Unlock()
		if policy.RejectOnDupID {
			e.log("submit key=%s rejected: duplicate in-flight", idemKey)
			return nil, &StepError{Class: ClassDuplicate, Step: idemKey}
		}
		e.log("submit key=%s dedup: waiting for in-flight exec=%s", idemKey, f.id)
		<-f.done
		return e.store.LoadByIdemKey(idemKey)
	}
	f := &inflight{id: newID(), done: make(chan struct{})}
	e.live[idemKey] = f
	e.mu.Unlock()

	// 已完结的同键执行：返回既有结果或拒绝。
	if existing, err := e.store.LoadByIdemKey(idemKey); err != nil {
		e.finish(idemKey, f)
		return nil, err
	} else if existing != nil {
		e.finish(idemKey, f)
		if policy.RejectOnDupID {
			e.log("submit key=%s rejected: duplicate completed exec=%s", idemKey, existing.ID)
			return nil, &StepError{Class: ClassDuplicate, Step: idemKey}
		}
		e.log("submit key=%s dedup: returning existing exec=%s status=%s", idemKey, existing.ID, existing.Status)
		return existing, nil
	}

	exec := &Execution{
		ID:         f.id,
		IdemKey:    idemKey,
		Definition: def,
		Policy:     policy, // 策略快照随执行固化，复现按当时规则
		Status:     StatusPending,
		StartedAt:  time.Now(),
	}
	for _, st := range def.Stages {
		for _, s := range st.Steps {
			exec.Steps = append(exec.Steps, &StepRecord{Stage: st.Name, Step: s.Name, Status: StepPending})
		}
	}
	if err := e.store.Save(exec); err != nil {
		e.finish(idemKey, f)
		return nil, err
	}
	e.log("submit exec=%s key=%s workflow=%s stages=%d", exec.ID, idemKey, def.Name, len(def.Stages))

	e.run(ctx, exec)
	e.finish(idemKey, f)
	if exec.Err != nil { // 避免把类型化 nil 塞进 error 接口
		return exec, exec.Err
	}
	return exec, nil
}

// finish 解除幂等键占位并唤醒等待中的重复提交。
func (e *Engine) finish(idemKey string, f *inflight) {
	e.mu.Lock()
	delete(e.live, idemKey)
	delete(e.byID, f.id)
	e.mu.Unlock()
	close(f.done)
}

// save 串行化持久化，保证版本单调。
func (e *Engine) save(exec *Execution) {
	e.mutate(exec, func() {})
}

// mutate 在 saveMu 保护下变更执行记录并持久化，
// 避免并发步骤写各自记录时与序列化发生数据竞争。
func (e *Engine) mutate(exec *Execution, fn func()) {
	e.saveMu.Lock()
	defer e.saveMu.Unlock()
	fn()
	if err := e.store.Save(exec); err != nil {
		e.log("persist exec=%s failed: %v", exec.ID, err)
	}
}

// run 顺序执行 Stage，Stage 内并发执行 Step。
func (e *Engine) run(ctx context.Context, exec *Execution) {
	e.mu.Lock()
	f := e.live[exec.IdemKey]
	e.mu.Unlock()

	if exec.Policy.ExecTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, exec.Policy.ExecTimeout)
		defer cancel()
	}
	if f != nil {
		ctx, f.cancel = context.WithCancel(ctx)
		defer f.cancel()
		e.mu.Lock()
		e.byID[exec.ID] = f
		e.mu.Unlock()
	}

	e.mutate(exec, func() { exec.Status = StatusRunning })

	var failed *StepError
	for _, st := range exec.Definition.Stages {
		if ctx.Err() != nil {
			failed = classifyCtx(ctx, "")
			break
		}
		e.log("exec=%s stage=%s begin steps=%d", exec.ID, st.Name, len(st.Steps))
		errs := make([]*StepError, len(st.Steps))
		var wg sync.WaitGroup
		for i, s := range st.Steps {
			wg.Add(1)
			go func(i int, s Step) {
				defer wg.Done()
				rec := exec.FindStep(st.Name, s.Name)
				errs[i] = e.runStep(ctx, exec, st.Name, s, rec)
			}(i, s)
		}
		wg.Wait()
		for _, se := range errs {
			if se != nil {
				failed = se
				break
			}
		}
		if failed != nil {
			break
		}
	}

	// 终态收尾：部分结果明确标记，不静默丢弃。
	e.mutate(exec, func() {
		for _, rec := range exec.Steps {
			switch rec.Status {
			case StepRunning:
				rec.Status = StepPartial // 已尝试、可能产生部分副作用，明确标记
			case StepPending:
				if failed != nil {
					rec.Status = StepSkipped
				}
			}
		}
		exec.FinishedAt = time.Now()
		if failed == nil {
			exec.Status = StatusSucceeded
		} else {
			exec.Err = failed
			switch failed.Class {
			case ClassCancelled:
				exec.Status = StatusCancelled
			case ClassTimeout:
				exec.Status = StatusTimedOut
			default:
				exec.Status = StatusFailed
			}
		}
	})
	if failed == nil {
		e.log("exec=%s finished status=%s", exec.ID, exec.Status)
	} else {
		e.log("exec=%s finished status=%s class=%s step=%s cause=%v",
			exec.ID, exec.Status, failed.Class, failed.Step, failed.Cause)
	}
}

// runStep 执行单步骤，含重试、超时与错误分类；已成功的步骤直接跳过（恢复语义）。
func (e *Engine) runStep(ctx context.Context, exec *Execution, stage string, s Step, rec *StepRecord) *StepError {
	if rec.Status == StepSucceeded {
		e.log("exec=%s step=%s/%s skip: already succeeded (recovered)", exec.ID, stage, s.Name)
		return nil
	}
	p := exec.Policy.Retry
	max := p.MaxAttempts
	if max < 1 {
		max = 1
	}
	var last error
	for attempt := 1; attempt <= max; attempt++ {
		if se := ctxErr(ctx, s.Name); se != nil {
			e.mutate(exec, func() { rec.Err = se })
			return se
		}
		e.mutate(exec, func() {
			rec.Status = StepRunning
			rec.Attempts = attempt
		})

		actx := ctx
		acancel := func() {}
		if p.StepTimeout > 0 {
			actx, acancel = context.WithTimeout(ctx, p.StepTimeout)
		}
		out, err := e.exec.Execute(actx, s)
		acancel()

		if err == nil {
			e.mutate(exec, func() {
				rec.Status = StepSucceeded
				rec.Output = out
				rec.Err = nil
			})
			e.log("exec=%s step=%s/%s ok attempt=%d/%d", exec.ID, stage, s.Name, attempt, max)
			return nil
		}
		last = err
		if se := ctxErr(ctx, s.Name); se != nil { // 父 ctx 取消/超时优先
			e.mutate(exec, func() { rec.Err = se })
			e.log("exec=%s step=%s/%s interrupted class=%s", exec.ID, stage, s.Name, se.Class)
			return se
		}
		if actx.Err() == context.DeadlineExceeded {
			se := &StepError{Class: ClassTimeout, Step: s.Name, Cause: err}
			e.mutate(exec, func() { rec.Err = se })
			e.log("exec=%s step=%s/%s step-timeout attempt=%d", exec.ID, stage, s.Name, attempt)
			return se
		}
		if IsNonRetryable(err) {
			se := &StepError{Class: ClassNonRetryable, Step: s.Name, Cause: err}
			e.mutate(exec, func() { rec.Err = se })
			e.log("exec=%s step=%s/%s non-retryable: %v", exec.ID, stage, s.Name, err)
			return se
		}
		e.log("exec=%s step=%s/%s retry attempt=%d/%d cause=%v", exec.ID, stage, s.Name, attempt, max, err)
		if attempt < max {
			select {
			case <-ctx.Done():
			case <-time.After(p.Backoff):
			}
		}
	}
	se := &StepError{Class: ClassRetryExhausted, Step: s.Name, Cause: last}
	e.mutate(exec, func() { rec.Err = se })
	e.log("exec=%s step=%s/%s retry-exhausted attempts=%d", exec.ID, stage, s.Name, max)
	return se
}

func classifyCtx(ctx context.Context, step string) *StepError {
	if se := ctxErr(ctx, step); se != nil {
		return se
	}
	return &StepError{Class: ClassCancelled, Step: step, Cause: ctx.Err()}
}

func ctxErr(ctx context.Context, step string) *StepError {
	switch ctx.Err() {
	case context.Canceled:
		return &StepError{Class: ClassCancelled, Step: step, Cause: ctx.Err()}
	case context.DeadlineExceeded:
		return &StepError{Class: ClassTimeout, Step: step, Cause: ctx.Err()}
	}
	return nil
}

// Cancel 取消进行中的执行；已产生的部分结果被标记为 partial。
func (e *Engine) Cancel(id string) error {
	e.mu.Lock()
	f, ok := e.byID[id]
	e.mu.Unlock()
	if !ok {
		exec, err := e.store.Load(id)
		if err != nil {
			return err
		}
		if exec == nil {
			return fmt.Errorf("execution %s not found", id)
		}
		if exec.Status.Terminal() {
			return nil // 已终态，取消为空操作
		}
		return fmt.Errorf("execution %s not running on this engine", id)
	}
	e.log("cancel exec=%s requested", id)
	f.cancel()
	return nil
}

// Recover 扫描存储中的非终态执行，从一致状态继续执行。
// 已完成的步骤不重复执行（不重复产生副作用），策略按提交时快照复现。
// 同时接管中断在一半的续跑记录：续跑途中崩溃/重启后仍可从最新状态继续。
func (e *Engine) Recover(ctx context.Context) ([]*Execution, error) {
	unfinished, err := e.store.ListUnfinished()
	if err != nil {
		return nil, err
	}
	var out []*Execution
	for _, exec := range unfinished {
		e.mu.Lock()
		if _, ok := e.live[exec.IdemKey]; ok { // 已在进行中
			e.mu.Unlock()
			continue
		}
		f := &inflight{id: exec.ID, done: make(chan struct{})}
		e.live[exec.IdemKey] = f
		e.mu.Unlock()

		e.log("recover exec=%s key=%s status=%s policy=%+v", exec.ID, exec.IdemKey, exec.Status, exec.Policy)
		e.run(ctx, exec)
		e.finish(exec.IdemKey, f)
		out = append(out, exec)
	}

	resumes, err := e.store.ListUnfinishedResumes()
	if err != nil {
		return nil, err
	}
	for _, rec := range resumes {
		root, err := e.store.Load(rec.RootExecutionID)
		if err != nil {
			return nil, err
		}
		if root == nil {
			e.log("recover resume=%s skipped: root execution %s missing", rec.ID, rec.RootExecutionID)
			continue
		}
		e.mu.Lock()
		if _, busy := e.liveResume[root.ID]; busy { // 已在续跑中
			e.mu.Unlock()
			continue
		}
		f := &inflight{id: rec.ID, done: make(chan struct{})}
		e.liveResume[root.ID] = f
		e.mu.Unlock()

		e.log("recover resume=%s root=%s attempt=%d status=%s", rec.ID, root.ID, rec.Attempt, rec.Status)
		e.runResume(ctx, root, rec)
		e.finishResume(root.ID, f)
	}
	return out, nil
}

// Get 读取执行记录。
func (e *Engine) Get(id string) (*Execution, error) { return e.store.Load(id) }
