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

// resumeInflight 跟踪一次进行中的续跑，按续跑链根 ID 去重。
type resumeInflight struct {
	resultID string // 生效续跑产生的新执行 ID
	done     chan struct{}
}

// Engine 是工作流执行引擎。
type Engine struct {
	exec  Executor
	store Store

	mu         sync.Mutex
	live       map[string]*inflight // 按幂等键去重
	byID       map[string]*inflight // 按执行 ID 取消
	resumeLive map[string]*resumeInflight
	saveMu     sync.Mutex // 串行化执行记录的变更与持久化
	logf       func(format string, args ...any)
}

// NewEngine 创建引擎。
func NewEngine(exec Executor, store Store) *Engine {
	return &Engine{
		exec:       exec,
		store:      store,
		live:       map[string]*inflight{},
		byID:       map[string]*inflight{},
		resumeLive: map[string]*resumeInflight{},
		logf:       func(string, ...any) {},
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
		ID:           f.id,
		IdemKey:      idemKey,
		Definition:   def,
		Policy:       policy, // 策略快照随执行固化，复现按当时规则
		PolicySource: PolicySubmitted,
		RootID:       f.id, // 原始执行是续跑链的根
		Status:       StatusPending,
		StartedAt:    time.Now(),
	}
	for _, st := range def.Stages {
		for _, s := range st.Steps {
			exec.Steps = append(exec.Steps, &StepRecord{
				Stage: st.Name, Step: s.Name, Status: StepPending, Origin: StepOriginFresh,
			})
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
		e.log("exec=%s step=%s/%s skip: already succeeded (origin=%s source=%s)", exec.ID, stage, s.Name, rec.Origin, rec.SourceExec)
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
	return out, nil
}

// Get 读取执行记录。
func (e *Engine) Get(id string) (*Execution, error) { return e.store.Load(id) }

// Resume 对一次已终态（failed/cancelled/timed_out）的执行发起断点续跑。
//
// 语义：
//   - 生成一条独立的新执行记录（ResumedFrom 指向来源，RootID 指向链根），
//     来源执行记录保持只读、不被改写；
//   - 已成功步骤复用旧产出（Origin=reused，SourceExec 追溯产出来源），
//     不重复触发副作用；partial/skipped/failed/pending 步骤重置后重新执行
//     （Origin=rerun）；
//   - 默认沿用来源执行的策略快照（PolicySource=inherited），行为可复现；
//     opts.Policy 非 nil 时按新规则走（PolicySource=override）并固化为新快照；
//   - 同一续跑链上并发续跑只有一个生效，其余等待并拿到同一份结果；
//   - 续跑产生的新执行若再次失败/取消/超时，可对其再次 Resume，链式推进。
func (e *Engine) Resume(ctx context.Context, execID string, opts ResumeOptions) (*Execution, error) {
	src, err := e.store.Load(execID)
	if err != nil {
		return nil, err
	}
	if src == nil {
		return nil, fmt.Errorf("execution %s not found", execID)
	}
	if !src.Status.Terminal() {
		return nil, fmt.Errorf("execution %s still %s, cannot resume", execID, src.Status)
	}
	if src.Status == StatusSucceeded {
		return nil, fmt.Errorf("execution %s already succeeded, nothing to resume", execID)
	}
	root := src.RootID
	if root == "" { // 兼容特性引入前的旧记录
		root = src.ID
	}

	// 并发续跑收敛：同一链上只有一个续跑生效，其余等待拿同一份结果。
	e.mu.Lock()
	if rf, ok := e.resumeLive[root]; ok {
		e.mu.Unlock()
		e.log("resume root=%s dedup: waiting for in-flight resume result=%s", root, rf.resultID)
		<-rf.done
		return e.store.Load(rf.resultID)
	}
	rf := &resumeInflight{done: make(chan struct{})}
	e.resumeLive[root] = rf
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.resumeLive, root)
		e.mu.Unlock()
		close(rf.done)
	}()

	// 构造续跑执行：复制定义与步骤记录，按状态判定复用或重跑。
	policy := src.Policy
	policySrc := PolicyInherited
	if opts.Policy != nil {
		policy = *opts.Policy
		policySrc = PolicyOverride
	}
	next := &Execution{
		ID:           newID(),
		IdemKey:      src.IdemKey + "#resume:" + src.ID, // 合成键，避免挤占原幂等键
		Definition:   src.Definition,
		Policy:       policy,
		PolicySource: policySrc,
		RootID:       root,
		ResumedFrom:  src.ID,
		Status:       StatusPending,
		StartedAt:    time.Now(),
	}
	rf.resultID = next.ID
	for _, rec := range src.Steps {
		nr := *rec // 拷贝记录，来源记录保持只读
		nr.Err = rec.Err
		if rec.Status == StepSucceeded {
			// 复用旧产出：追溯到真正产出生的那次执行。
			nr.Origin = StepOriginReused
			if rec.Origin == StepOriginReused && rec.SourceExec != "" {
				nr.SourceExec = rec.SourceExec
			} else {
				nr.SourceExec = src.ID
			}
		} else {
			// 状态不确定或未算数：重置后重新执行。
			nr.Status, nr.Attempts, nr.Output, nr.Err = StepPending, 0, nil, nil
			nr.Origin, nr.SourceExec = StepOriginRerun, ""
		}
		next.Steps = append(next.Steps, &nr)
	}
	if err := e.store.Save(next); err != nil {
		return nil, err
	}
	e.log("resume root=%s from=%s new-exec=%s policy-source=%s reused=%d rerun=%d",
		root, src.ID, next.ID, policySrc, countOrigin(next, StepOriginReused), countOrigin(next, StepOriginRerun))

	// 登记 inflight 使 run 可取消，随后执行。
	f := &inflight{id: next.ID, done: make(chan struct{})}
	e.mu.Lock()
	e.live[next.IdemKey] = f
	e.mu.Unlock()
	e.run(ctx, next)
	e.finish(next.IdemKey, f)

	if next.Err != nil { // 避免把类型化 nil 塞进 error 接口
		return next, next.Err
	}
	return next, nil
}

func countOrigin(exec *Execution, o StepOrigin) int {
	n := 0
	for _, r := range exec.Steps {
		if r.Origin == o {
			n++
		}
	}
	return n
}

// ResumeChain 返回 execID 所在续跑链的全部执行记录（含最初的执行），
// 按开始时间升序，便于审计哪一步在什么时候被补救过。
func (e *Engine) ResumeChain(execID string) ([]*Execution, error) {
	exec, err := e.store.Load(execID)
	if err != nil {
		return nil, err
	}
	if exec == nil {
		return nil, fmt.Errorf("execution %s not found", execID)
	}
	root := exec.RootID
	if root == "" {
		root = exec.ID
	}
	return e.store.ListResumes(root)
}
