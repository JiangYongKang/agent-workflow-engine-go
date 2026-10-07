package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ResumeOption 配置一次续跑；零值表示沿用原执行策略。
type ResumeOption struct {
	// Policy 非 nil 时本次续跑显式使用新策略，记录中标记为非继承；
	// 为 nil 时沿用最初执行提交时固化的策略快照，行为可复现。
	Policy *ExecPolicy
	// RejectIfBusy 为 true 时，同一执行已有进行中续跑则立即拒绝，
	// 而非等待其结束并返回同一份结果。
	RejectIfBusy bool
}

// Resume 对已终态但未成功（失败/取消/超时）的执行发起一次续跑。
//
// 续跑语义：
//   - 已成功步骤直接复用当时产出（OutcomeReused），不触发执行器，副作用不重复；
//   - 上次失败 / partial（中断在一半、状态不确定）/ skipped / pending 的步骤重新执行（OutcomeReran）；
//   - 每次续跑产生独立的 ResumeRecord，经 ParentID / RootExecutionID 关联回最初执行，
//     最初执行记录本身保持只读、永不被改写。
//
// 同一执行并发续跑：只有一个真正生效；其余调用默认等待并拿到同一份结果，
// RejectIfBusy=true 时以 duplicate 分类错误立即拒绝。
func (e *Engine) Resume(ctx context.Context, executionID string, opt ResumeOption) (*ResumeRecord, error) {
	root, err := e.store.Load(executionID)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("execution %s not found", executionID)
	}
	switch {
	case root.Status == StatusSucceeded:
		return nil, fmt.Errorf("execution %s already succeeded; nothing to resume", executionID)
	case !root.Status.Terminal():
		return nil, fmt.Errorf("execution %s is not terminal (status=%s); use Recover for in-flight executions", executionID, root.Status)
	}

	// t0 记录调用进入引擎的时刻，用于区分“同批并发迟到者”与“之后独立发起的新续跑”。
	t0 := time.Now()

	// 快速路径：此刻正有续跑在执行中。重叠调用在此等待同一份结果（或按策略拒绝）。
	e.mu.Lock()
	active := e.liveResume[root.ID]
	e.mu.Unlock()
	if active != nil {
		if opt.RejectIfBusy {
			e.log("resume root=%s rejected: another resume is in-flight", root.ID)
			return nil, &StepError{Class: ClassDuplicate, Step: root.ID}
		}
		e.log("resume root=%s dedup: waiting for active resume", root.ID)
		<-active.done
		return e.store.LoadResume(active.id)
	}

	// 同一 root 的“解析→执行→收尾”由 per-root 锁全程串行，
	// 保证同一进程内同一时刻只有一个续跑真正触发执行器。
	rl := e.resumeLock(root.ID)
	rl.Lock()

	// 拿锁后二次检查：在我们等待锁期间可能已有续跑注册并仍在执行。
	e.mu.Lock()
	active = e.liveResume[root.ID]
	batchEnd := e.resumeBatchEnd[root.ID]
	e.mu.Unlock()
	if active != nil {
		rl.Unlock()
		if opt.RejectIfBusy {
			return nil, &StepError{Class: ClassDuplicate, Step: root.ID}
		}
		<-active.done
		return e.store.LoadResume(active.id)
	}
	// 上一批次收尾宽限内发起的调用属于同一并发批次：直接收敛到链头结果。
	// （覆盖“进入时 runner 尚未注册 liveResume、拿锁时它已结束”的迟到者。）
	if !t0.After(batchEnd) {
		headID, err := e.store.LoadResumeHead(root.ID)
		if err != nil {
			rl.Unlock()
			return nil, err
		}
		if headID != "" {
			head, err := e.store.LoadResume(headID)
			if err != nil {
				rl.Unlock()
				return nil, err
			}
			if head != nil {
				rl.Unlock()
				e.log("resume root=%s coalesce within grace to result=%s", root.ID, head.ID)
				return e.afterRun(head)
			}
		}
	}

	var head *ResumeRecord
	headID, err := e.store.LoadResumeHead(root.ID)
	if err != nil {
		rl.Unlock()
		return nil, err
	}
	if headID != "" {
		head, err = e.store.LoadResume(headID)
		if err != nil {
			rl.Unlock()
			return nil, err
		}
		if head == nil {
			rl.Unlock()
			return nil, fmt.Errorf("resume head %s for execution %s is missing", headID, root.ID)
		}
	}

	var rec *ResumeRecord
	if head != nil && !head.Status.Terminal() {
		// 非终态续跑（崩溃遗留）：接管同一条记录，沿用其固化策略，忽略本次携带策略。
		if opt.Policy != nil {
			e.log("resume root=%s adopt=%s: explicit policy ignored", root.ID, head.ID)
		}
		e.log("resume root=%s adopt non-terminal resume=%s status=%s", root.ID, head.ID, head.Status)
		rec = head
	} else {
		// 新续跑：解析（含链头 CAS）；若 CAS 输给其他进程/引擎则返回其记录，不执行。
		r, loser, rerr := e.resolveResumeRecord(root, opt)
		if rerr != nil {
			rl.Unlock()
			return nil, rerr
		}
		if loser != nil {
			e.log("resume root=%s lost head race to %s (external)", root.ID, loser.ID)
			rl.Unlock()
			return loser, nil
		}
		rec = r
		e.log("resume root=%s id=%s attempt=%d parent=%s inherited-policy=%v reran=%d reused=%d",
			root.ID, rec.ID, rec.Attempt, rec.ParentID, rec.PolicyInherited,
			countOutcome(rec, OutcomeReran), countOutcome(rec, OutcomeReused))
	}

	// 持 root 锁执行；结束后保留一个收尾宽限窗口（仍持锁），让已在场的排队迟到者收敛，
	// 宽限结束后才放锁并允许下一次独立续跑。
	func() {
		defer rl.Unlock()
		e.runUnderInflight(ctx, root, rec)
		e.mu.Lock()
		e.resumeBatchEnd[root.ID] = time.Now().Add(resumeBatchGrace)
		e.mu.Unlock()
		e.log("resume root=%s batch grace window open for %v", root.ID, resumeBatchGrace)
		time.Sleep(resumeBatchGrace)
	}()
	return e.afterRun(rec)
}

// resumeBatchGrace 是续跑批次结束后的收敛宽限：窗口内进入的并发迟到者复用同批结果，
// 窗口之后发起的调用视为一次新的独立续跑。
const resumeBatchGrace = 50 * time.Millisecond

// resumeLock 返回（必要时创建）某最初执行的 per-root 串行锁。
func (e *Engine) resumeLock(rootID string) *sync.Mutex {
	e.mu.Lock()
	defer e.mu.Unlock()
	l, ok := e.resumeLocks[rootID]
	if !ok {
		l = &sync.Mutex{}
		e.resumeLocks[rootID] = l
	}
	return l
}

// runUnderInflight 注册续跑占位、执行并在结束时清理（调用方已持有 per-root 锁）。
func (e *Engine) runUnderInflight(ctx context.Context, root *Execution, rec *ResumeRecord) {
	f := &inflight{id: rec.ID, done: make(chan struct{})}
	e.mu.Lock()
	e.liveResume[root.ID] = f
	e.mu.Unlock()
	defer e.finishResume(root.ID, f)
	e.runResume(ctx, root, rec)
}

// afterRun 按续跑记录构造返回值（成功返回 nil 错误，失败返回分类错误）。
func (e *Engine) afterRun(rec *ResumeRecord) (*ResumeRecord, error) {
	if rec.Err != nil {
		return rec, rec.Err
	}
	return rec, nil
}

// resolveResumeRecord 基于链头确定本次要跑的记录：
// 链头是非终态续跑（崩溃遗留或本进程同伴刚抢占）则返回它；否则基于最新状态新建一条续跑记录。
// 返回值 loser 非 nil 表示链头 CAS 输给了并发者，调用方应放弃执行并收敛到 loser，
// 绝不能自己重跑，否则会重复触发副作用。
func (e *Engine) resolveResumeRecord(root *Execution, opt ResumeOption) (rec *ResumeRecord, loser *ResumeRecord, err error) {
	headID, err := e.store.LoadResumeHead(root.ID)
	if err != nil {
		return nil, nil, err
	}
	var head *ResumeRecord
	if headID != "" {
		head, err = e.store.LoadResume(headID)
		if err != nil {
			return nil, nil, err
		}
		if head == nil {
			return nil, nil, fmt.Errorf("resume head %s for execution %s is missing", headID, root.ID)
		}
	}
	// 链头是非终态续跑（崩溃遗留）：接管其既有记录与策略，已成功步骤不再重跑。
	if head != nil && !head.Status.Terminal() {
		if opt.Policy != nil {
			e.log("resume root=%s adopt=%s: explicit policy ignored, continuing with existing record", root.ID, head.ID)
		}
		e.log("resume root=%s adopt non-terminal resume=%s status=%s", root.ID, head.ID, head.Status)
		return head, nil, nil
	}

	chain, err := e.store.LoadResumeChain(root.ID)
	if err != nil {
		return nil, nil, err
	}
	attempt := 1
	for _, r := range chain {
		if r.Attempt >= attempt {
			attempt = r.Attempt + 1
		}
	}
	parent := root.ID
	if head != nil {
		parent = head.ID
	}
	policy := root.Policy // 默认沿用最初执行固化的规则
	inherited := true
	if opt.Policy != nil {
		policy = *opt.Policy
		inherited = false
	}
	rec = &ResumeRecord{
		ID:              newID(),
		RootExecutionID: root.ID,
		ParentID:        parent,
		Attempt:         attempt,
		PolicyInherited: inherited,
		Policy:          policy,
		Status:          ResumePending,
		StartedAt:       time.Now(),
	}
	for _, st := range root.Definition.Stages {
		for _, s := range st.Steps {
			status, output := resumeBaseState(root, head, st.Name, s.Name)
			sr := &StepResumeRecord{Stage: st.Name, Step: s.Name}
			if status == StepSucceeded {
				// 已经算数的成功：复用旧产出，不触发执行器。
				sr.Outcome = OutcomeReused
				sr.Status = StepSucceeded
				sr.Output = output
			} else {
				// failed/partial/skipped/pending/running：上次状态不确定，必须重跑。
				sr.Outcome = OutcomeReran
				sr.Status = StepPending
			}
			rec.Steps = append(rec.Steps, sr)
		}
	}
	if err := e.store.SaveResume(rec); err != nil {
		return nil, nil, err
	}
	// 原子占据链头：CAS 失败说明并发续跑抢先，返回获胜者记录交由调用方收敛。
	if err := e.store.SaveResumeHead(root.ID, headID, rec.ID); err != nil {
		if err == ErrVersionConflict {
			winnerID, lerr := e.store.LoadResumeHead(root.ID)
			if lerr != nil {
				return nil, nil, lerr
			}
			winner, lerr := e.store.LoadResume(winnerID)
			if lerr != nil {
				return nil, nil, lerr
			}
			e.log("resume root=%s lost head CAS to %s", root.ID, winnerID)
			return nil, winner, nil
		}
		return nil, nil, err
	}
	return rec, nil, nil
}

// resumeBaseState 取某步骤在最新链头（否则最初执行）中的状态与产出。
func resumeBaseState(root *Execution, head *ResumeRecord, stage, step string) (StepStatus, map[string]string) {
	if head != nil {
		if s := head.FindStep(stage, step); s != nil {
			return s.Status, s.Output
		}
	}
	if s := root.FindStep(stage, step); s != nil {
		return s.Status, s.Output
	}
	return StepPending, nil
}

func countOutcome(rec *ResumeRecord, o StepOutcome) int {
	n := 0
	for _, s := range rec.Steps {
		if s.Outcome == o {
			n++
		}
	}
	return n
}

// finishResume 解除续跑占位并唤醒等待中的并发续跑。
func (e *Engine) finishResume(rootID string, f *inflight) {
	e.mu.Lock()
	delete(e.liveResume, rootID)
	if f.id != "" {
		delete(e.byID, f.id)
	}
	e.mu.Unlock()
	close(f.done)
}

// mutateResume 在 saveMu 保护下变更续跑记录并持久化。
func (e *Engine) mutateResume(rec *ResumeRecord, fn func()) {
	e.saveMu.Lock()
	defer e.saveMu.Unlock()
	fn()
	if err := e.store.SaveResume(rec); err != nil {
		e.log("persist resume=%s failed: %v", rec.ID, err)
	}
}

// runResume 顺序执行 Stage，Stage 内并发；复用步骤不触发执行器。
// 工作流定义来自最初执行（记录本身只读且永不被改写）。
func (e *Engine) runResume(ctx context.Context, root *Execution, rec *ResumeRecord) {
	e.mu.Lock()
	f := e.liveResume[rec.RootExecutionID]
	e.mu.Unlock()

	if rec.Policy.ExecTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, rec.Policy.ExecTimeout)
		defer cancel()
	}
	if f != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		f.cancel = cancel
		defer cancel()
		e.mu.Lock()
		e.byID[rec.ID] = f
		e.mu.Unlock()
	}

	e.mutateResume(rec, func() { rec.Status = ResumeRunning })

	var failed *StepError
	for _, st := range root.Definition.Stages {
		if ctx.Err() != nil {
			failed = classifyCtx(ctx, "")
			break
		}
		e.log("resume=%s stage=%s begin steps=%d", rec.ID, st.Name, len(st.Steps))
		errs := make([]*StepError, len(st.Steps))
		var wg sync.WaitGroup
		for i, s := range st.Steps {
			wg.Add(1)
			go func(i int, s Step) {
				defer wg.Done()
				sr := rec.FindStep(st.Name, s.Name)
				errs[i] = e.runResumeStep(ctx, rec, st.Name, s, sr)
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

	e.mutateResume(rec, func() {
		for _, sr := range rec.Steps {
			switch sr.Status {
			case StepRunning:
				sr.Status = StepPartial // 重跑中被打断、副作用状态不确定，明确标记
			case StepPending:
				if failed != nil {
					sr.Status = StepSkipped
				}
			}
		}
		rec.FinishedAt = time.Now()
		if failed == nil {
			rec.Status = ResumeSucceeded
		} else {
			rec.Err = failed
			switch failed.Class {
			case ClassCancelled:
				rec.Status = ResumeCancelled
			case ClassTimeout:
				rec.Status = ResumeTimedOut
			default:
				rec.Status = ResumeFailed
			}
		}
	})
	if failed == nil {
		e.log("resume=%s finished status=%s", rec.ID, rec.Status)
	} else {
		e.log("resume=%s finished status=%s class=%s step=%s cause=%v",
			rec.ID, rec.Status, failed.Class, failed.Step, failed.Cause)
	}
}

// runResumeStep 执行续跑中的单个重跑步骤，含重试、超时与错误分类。
// 复用步骤与本轮已成功步骤（崩溃接管）均直接跳过，不重复触发副作用。
func (e *Engine) runResumeStep(ctx context.Context, rec *ResumeRecord, stage string, s Step, sr *StepResumeRecord) *StepError {
	if sr.Outcome == OutcomeReused {
		e.log("resume=%s step=%s/%s reuse: output from previous run kept", rec.ID, stage, s.Name)
		return nil
	}
	if sr.Status == StepSucceeded {
		e.log("resume=%s step=%s/%s skip: already succeeded in this resume", rec.ID, stage, s.Name)
		return nil
	}
	p := rec.Policy.Retry
	max := p.MaxAttempts
	if max < 1 {
		max = 1
	}
	var last error
	for attempt := 1; attempt <= max; attempt++ {
		if se := ctxErr(ctx, s.Name); se != nil {
			e.mutateResume(rec, func() { sr.Err = se })
			return se
		}
		e.mutateResume(rec, func() {
			sr.Status = StepRunning
			sr.Attempts = attempt
		})

		actx := ctx
		acancel := func() {}
		if p.StepTimeout > 0 {
			actx, acancel = context.WithTimeout(ctx, p.StepTimeout)
		}
		out, err := e.exec.Execute(actx, s)
		acancel()

		if err == nil {
			e.mutateResume(rec, func() {
				sr.Status = StepSucceeded
				sr.Output = out
				sr.Err = nil
			})
			e.log("resume=%s step=%s/%s reran ok attempt=%d/%d", rec.ID, stage, s.Name, attempt, max)
			return nil
		}
		last = err
		if se := ctxErr(ctx, s.Name); se != nil {
			e.mutateResume(rec, func() { sr.Err = se })
			e.log("resume=%s step=%s/%s interrupted class=%s", rec.ID, stage, s.Name, se.Class)
			return se
		}
		if actx.Err() == context.DeadlineExceeded {
			se := &StepError{Class: ClassTimeout, Step: s.Name, Cause: err}
			e.mutateResume(rec, func() { sr.Err = se })
			e.log("resume=%s step=%s/%s step-timeout attempt=%d", rec.ID, stage, s.Name, attempt)
			return se
		}
		if IsNonRetryable(err) {
			se := &StepError{Class: ClassNonRetryable, Step: s.Name, Cause: err}
			e.mutateResume(rec, func() { sr.Err = se })
			e.log("resume=%s step=%s/%s non-retryable: %v", rec.ID, stage, s.Name, err)
			return se
		}
		e.log("resume=%s step=%s/%s retry attempt=%d/%d cause=%v", rec.ID, stage, s.Name, attempt, max, err)
		if attempt < max {
			select {
			case <-ctx.Done():
			case <-time.After(p.Backoff):
			}
		}
	}
	se := &StepError{Class: ClassRetryExhausted, Step: s.Name, Cause: last}
	e.mutateResume(rec, func() { sr.Err = se })
	e.log("resume=%s step=%s/%s retry-exhausted attempts=%d", rec.ID, stage, s.Name, max)
	return se
}

// CancelResume 取消进行中的续跑；已产生的结果保留，中断步骤标记 partial。
func (e *Engine) CancelResume(resumeID string) error {
	e.mu.Lock()
	f, ok := e.byID[resumeID]
	e.mu.Unlock()
	if !ok {
		rec, err := e.store.LoadResume(resumeID)
		if err != nil {
			return err
		}
		if rec == nil {
			return fmt.Errorf("resume %s not found", resumeID)
		}
		if rec.Status.Terminal() {
			return nil
		}
		return fmt.Errorf("resume %s not running on this engine", resumeID)
	}
	e.log("cancel resume=%s requested", resumeID)
	f.cancel()
	return nil
}

// GetResume 读取一条续跑记录。
func (e *Engine) GetResume(resumeID string) (*ResumeRecord, error) {
	return e.store.LoadResume(resumeID)
}

// ResumeHistory 返回某执行的完整续跑链（最初执行 + 全部续跑记录），用于审计追溯。
func (e *Engine) ResumeHistory(executionID string) (*ResumeChain, error) {
	root, err := e.store.Load(executionID)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("execution %s not found", executionID)
	}
	resumes, err := e.store.LoadResumeChain(executionID)
	if err != nil {
		return nil, err
	}
	head, err := e.store.LoadResumeHead(executionID)
	if err != nil {
		return nil, err
	}
	chain := &ResumeChain{Root: root, Resumes: resumes, HeadID: executionID}
	if head != "" {
		chain.HeadID = head
		chain.HeadIsResume = true
	}
	return chain, nil
}
