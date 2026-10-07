package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// marshalExec 快照最初执行记录，用于断言续跑不改动原始记录。
func marshalExec(t *testing.T, exec *Execution) string {
	t.Helper()
	b, err := json.Marshal(exec)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func waitResumeStepRunning(t *testing.T, eng *Engine, stage, step string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		unfinished, _ := eng.store.ListUnfinishedResumes()
		for _, r := range unfinished {
			if sr := r.FindStep(stage, step); sr != nil && sr.Status == StepRunning {
				return r.ID
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("续跑步骤始终未进入 running")
	return ""
}

// TestResumeReusesPreviousOutputs 续跑复用旧产出：
// 已成功步骤直接复用（OutcomeReused、不触发执行器），失败步骤真正重跑（OutcomeReran）。
func TestResumeReusesPreviousOutputs(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	policy := defaultPolicy()
	policy.Retry.MaxAttempts = 1
	ex.ScriptFailures("act-c", ErrScripted) // 首次：c 失败，执行终态 failed

	exec, err := eng.Submit(context.Background(), twoStageDef(), "k-resume-basic", policy)
	if got := classOf(t, err); got != ClassRetryExhausted {
		t.Fatalf("class=%s", got)
	}
	if exec.Status != StatusFailed {
		t.Fatalf("status=%s", exec.Status)
	}
	before := marshalExec(t, exec)

	// 续跑：无失败脚本，c 重跑后成功；默认沿用原策略快照。
	rec, err := eng.Resume(context.Background(), exec.ID, ResumeOption{})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if rec.Status != ResumeSucceeded {
		t.Fatalf("resume status=%s err=%v", rec.Status, rec.Err)
	}

	// 每个步骤都能看出是复用还是重跑。
	if sr := rec.FindStep("s1", "a"); sr.Outcome != OutcomeReused || sr.Status != StepSucceeded || sr.Output["echo.k"] != "v1" {
		t.Fatalf("s1.a 应复用旧产出: %+v", sr)
	}
	if sr := rec.FindStep("s1", "b"); sr.Outcome != OutcomeReused {
		t.Fatalf("s1.b 应复用: %+v", sr)
	}
	if sr := rec.FindStep("s2", "c"); sr.Outcome != OutcomeReran || sr.Status != StepSucceeded || sr.Attempts != 1 {
		t.Fatalf("s2.c 应真正重跑: %+v", sr)
	}
	// 副作用不重复：a/b 整个过程只调用一次；c 首跑 1 次 + 续跑 1 次。
	if ex.Calls("act-a") != 1 || ex.Calls("act-b") != 1 {
		t.Fatalf("已成功步骤不得重复触发副作用: a=%d b=%d", ex.Calls("act-a"), ex.Calls("act-b"))
	}
	if ex.Calls("act-c") != 2 {
		t.Fatalf("c 应首跑1次+续跑1次, got %d", ex.Calls("act-c"))
	}
	// 追溯字段。
	if rec.RootExecutionID != exec.ID || rec.ParentID != exec.ID || rec.Attempt != 1 || !rec.PolicyInherited {
		t.Fatalf("追溯字段错误: root=%s parent=%s attempt=%d inherited=%v",
			rec.RootExecutionID, rec.ParentID, rec.Attempt, rec.PolicyInherited)
	}
	if rec.Policy.Retry.MaxAttempts != 1 {
		t.Fatalf("应沿用最初策略快照 MaxAttempts=1, got %d", rec.Policy.Retry.MaxAttempts)
	}
	// 最初执行记录只读、不被改写。
	after, err := eng.Get(exec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := marshalExec(t, after); got != before {
		t.Fatalf("原始执行记录被续跑改写:\nbefore=%s\nafter =%s", before, got)
	}
	if after.Status != StatusFailed {
		t.Fatalf("原始执行终态不应被翻转: %s", after.Status)
	}
	// 审计视图。
	chain, err := eng.ResumeHistory(exec.ID)
	if err != nil || len(chain.Resumes) != 1 || chain.HeadID != rec.ID || !chain.HeadIsResume {
		t.Fatalf("续跑链异常: %+v err=%v", chain, err)
	}
	t.Logf("判定依据: resume=%s 复用 a/b（不调执行器）, c 重跑成功; 原始记录 status=%s 保持只读; 策略继承=%v",
		rec.ID, after.Status, rec.PolicyInherited)
}

// TestConcurrentResumeConverges 同一执行并发续跑只有一个生效，
// 其余等待并拿到同一份结果；副作用不重复；RejectIfBusy 返回 duplicate 分类。
func TestConcurrentResumeConverges(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	policy := defaultPolicy()
	policy.Retry.MaxAttempts = 1
	ex.ScriptFailures("act-c", ErrScripted)
	// c 的重跑耗时 150ms，确保 n 路并发续跑在执行期间真正重叠（而非先后两次独立续跑）。
	slowCDef := twoStageDef()
	slowCDef.Stages[1].Steps[0].Input = map[string]string{"sleep_ms": "150"}
	exec, err := eng.Submit(context.Background(), slowCDef, "k-resume-concurrent", policy)
	if err == nil {
		t.Fatal("首次执行应失败")
	}

	const n = 6
	var wg sync.WaitGroup
	ids := make([]string, n)
	statuses := make([]ResumeStatus, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec, rerr := eng.Resume(context.Background(), exec.ID, ResumeOption{})
			if rec != nil {
				ids[i] = rec.ID
				statuses[i] = rec.Status
			}
			errs[i] = rerr
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("resume %d: %v", i, errs[i])
		}
		if ids[i] == "" || ids[i] != ids[0] || statuses[i] != ResumeSucceeded {
			t.Fatalf("并发续跑应收敛到同一份成功结果: ids=%v statuses=%v", ids, statuses)
		}
	}
	// 只有一个续跑真正执行：c 只被重跑一次。
	if ex.Calls("act-c") != 2 {
		t.Fatalf("c 应仅首跑1次+续跑1次, got %d", ex.Calls("act-c"))
	}
	if ex.Calls("act-a") != 1 || ex.Calls("act-b") != 1 {
		t.Fatalf("a=%d b=%d, 复用步骤不得触发执行器", ex.Calls("act-a"), ex.Calls("act-b"))
	}

	// 续跑已成功后再次发起：无可补救步骤，返回成功的空跑续跑记录不产生副作用；
	// 而 RejectIfBusy 在有进行中续跑时才拒绝。这里验证一条新的进行中冲突被拒绝：
	// 构造一个长耗时失败执行，并发发起一个等待者 + 一个拒绝者。
	slow := Definition{Name: "slow-resume", Stages: []Stage{
		{Name: "s1", Steps: []Step{
			{Name: "slow", Action: "act-slow", Input: map[string]string{"sleep_ms": "1000"}},
		}},
	}}
	sp := defaultPolicy()
	sp.Retry.MaxAttempts = 2
	ex.ScriptFailures("act-slow", ErrScripted, ErrScripted) // 首跑重试耗尽
	sExec, err := eng.Submit(context.Background(), slow, "k-resume-busy", sp)
	if err == nil {
		t.Fatal("首次执行应失败")
	}
	started := make(chan struct{})
	ex.SetHandler(func(ctx context.Context, step Step) (map[string]string, error) {
		close(started)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(800 * time.Millisecond):
		}
		return map[string]string{"ok": "1"}, nil
	})
	var waiterErr, rejectErr error
	var wg2 sync.WaitGroup
	wg2.Add(2)
	go func() { defer wg2.Done(); _, waiterErr = eng.Resume(context.Background(), sExec.ID, ResumeOption{}) }()
	go func() {
		defer wg2.Done()
		<-started
		time.Sleep(20 * time.Millisecond)
		_, rejectErr = eng.Resume(context.Background(), sExec.ID, ResumeOption{RejectIfBusy: true})
	}()
	wg2.Wait()
	if waiterErr != nil {
		t.Fatalf("等待者应拿到同一份结果: %v", waiterErr)
	}
	if rejectErr == nil {
		t.Fatal("RejectIfBusy 应拒绝进行中的并发续跑")
	}
	var se *StepError
	if !errors.As(rejectErr, &se) || se.Class != ClassDuplicate {
		t.Fatalf("拒绝错误分类应为 duplicate, got %v", rejectErr)
	}
	t.Logf("判定依据: %d 路并发续跑收敛到 %s, c 仅重跑1次; 进行中冲突被 duplicate 拒绝", n, ids[0])
}

// TestResumeRepeatedNoDuplicateSideEffects 连续多次续跑：
// 每次续跑基于链头最新状态；已成功步骤（无论首次还是某次续跑中成功）永不重跑，
// 每次续跑独立留痕、序号递增、父指针成链；显式新策略可被分辨。
func TestResumeRepeatedNoDuplicateSideEffects(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	def := Definition{Name: "chain", Stages: []Stage{
		{Name: "s1", Steps: []Step{{Name: "a", Action: "act-a"}}},
		{Name: "s2", Steps: []Step{{Name: "b", Action: "act-b"}}},
		{Name: "s3", Steps: []Step{{Name: "c", Action: "act-c"}}},
	}}
	policy := defaultPolicy()
	policy.Retry.MaxAttempts = 1
	// 首跑：a 成功，b 失败，c 未执行。
	ex.ScriptFailures("act-b", ErrScripted)
	exec, err := eng.Submit(context.Background(), def, "k-chain", policy)
	if err == nil {
		t.Fatal("首跑应失败")
	}
	rootSnapshot := marshalExec(t, exec)

	// 第 1 次续跑：b 仍失败（再注入一次脚本失败），c 不执行。
	ex.ScriptFailures("act-b", ErrScripted)
	r1, err := eng.Resume(context.Background(), exec.ID, ResumeOption{})
	if err == nil || r1.Status != ResumeFailed {
		t.Fatalf("r1 应失败: status=%s err=%v", r1.Status, err)
	}
	if sr := r1.FindStep("s1", "a"); sr.Outcome != OutcomeReused {
		t.Fatalf("r1: a 应复用")
	}
	if sr := r1.FindStep("s2", "b"); sr.Outcome != OutcomeReran || sr.Status != StepPartial || sr.Err == nil || sr.Err.Class != ClassRetryExhausted {
		t.Fatalf("r1: b 应重跑并失败(partial+分类错误): %+v", sr)
	}
	if sr := r1.FindStep("s3", "c"); sr.Status != StepSkipped {
		t.Fatalf("r1: 未跑到的 c 应标记 skipped: %s", sr.Status)
	}
	if ex.Calls("act-a") != 1 || ex.Calls("act-b") != 2 {
		t.Fatalf("r1 后 a=%d b=%d", ex.Calls("act-a"), ex.Calls("act-b"))
	}

	// 第 2 次续跑：显式新策略 MaxAttempts=3，b 前 2 次失败后成功，c 首次执行成功。
	newPolicy := defaultPolicy()
	newPolicy.Retry.MaxAttempts = 3
	ex.ScriptFailures("act-b", ErrScripted, ErrScripted)
	r2, err := eng.Resume(context.Background(), exec.ID, ResumeOption{Policy: &newPolicy})
	if err != nil {
		t.Fatalf("r2: %v", err)
	}
	if r2.Status != ResumeSucceeded {
		t.Fatalf("r2 status=%s err=%v", r2.Status, r2.Err)
	}
	if sr := r2.FindStep("s1", "a"); sr.Outcome != OutcomeReused {
		t.Fatalf("r2: a 应复用首跑产出")
	}
	if sr := r2.FindStep("s2", "b"); sr.Outcome != OutcomeReran || sr.Attempts != 3 {
		t.Fatalf("r2: b 应按新策略重试3次: %+v", sr)
	}
	if sr := r2.FindStep("s3", "c"); sr.Outcome != OutcomeReran || sr.Attempts != 1 {
		t.Fatalf("r2: c 应首次执行: %+v", sr)
	}
	if r2.PolicyInherited || r2.Policy.Retry.MaxAttempts != 3 {
		t.Fatalf("r2 应标记为显式新策略: inherited=%v max=%d", r2.PolicyInherited, r2.Policy.Retry.MaxAttempts)
	}
	if r1.PolicyInherited != true || r1.Policy.Retry.MaxAttempts != 1 {
		t.Fatalf("r1 应继承原始策略: inherited=%v max=%d", r1.PolicyInherited, r1.Policy.Retry.MaxAttempts)
	}

	// 第 3 次续跑：全部已成功，没有步骤需要重跑（不应触发任何执行器调用）。
	callsBefore := ex.Calls("act-a") + ex.Calls("act-b") + ex.Calls("act-c")
	r3, err := eng.Resume(context.Background(), exec.ID, ResumeOption{})
	if err != nil {
		t.Fatalf("r3: %v", err)
	}
	for _, sr := range r3.Steps {
		if sr.Outcome != OutcomeReused || sr.Status != StepSucceeded {
			t.Fatalf("r3: 全部步骤应复用: %+v", sr)
		}
	}
	callsAfter := ex.Calls("act-a") + ex.Calls("act-b") + ex.Calls("act-c")
	if callsAfter != callsBefore {
		t.Fatalf("全成功后的续跑不应触发任何执行器: before=%d after=%d", callsBefore, callsAfter)
	}

	// 全程副作用计数：a 一次（首跑），b = 首跑1 + r1一次 + r2三次 = 5，c 一次（r2）。
	if ex.Calls("act-a") != 1 || ex.Calls("act-b") != 5 || ex.Calls("act-c") != 1 {
		t.Fatalf("副作用计数错误: a=%d b=%d c=%d", ex.Calls("act-a"), ex.Calls("act-b"), ex.Calls("act-c"))
	}

	// 链追溯：3 条独立记录，序号 1/2/3，父指针 root->r1->r2->r3。
	chain, err := eng.ResumeHistory(exec.ID)
	if err != nil || len(chain.Resumes) != 3 {
		t.Fatalf("链长度错误: %d err=%v", len(chain.Resumes), err)
	}
	if chain.Resumes[0].ID != r1.ID || chain.Resumes[1].ID != r2.ID || chain.Resumes[2].ID != r3.ID {
		t.Fatalf("续跑记录应按序号升序可查")
	}
	if r1.ParentID != exec.ID || r2.ParentID != r1.ID || r3.ParentID != r2.ID {
		t.Fatalf("父指针链错误: r1.parent=%s r2.parent=%s r3.parent=%s", r1.ParentID, r2.ParentID, r3.ParentID)
	}
	if chain.HeadID != r3.ID {
		t.Fatalf("链头应为最新续跑 r3")
	}
	// 最初执行记录全程只读。
	after, _ := eng.Get(exec.ID)
	if marshalExec(t, after) != rootSnapshot {
		t.Fatal("连续续跑后原始执行记录被改动")
	}
	t.Logf("判定依据: 3 次续跑成链(%s <- %s <- %s <- root), a 全程1次 b=5次 c=1次; r1继承策略 r2显式新策略; 原始记录只读",
		r3.ID[:13], r2.ID[:13], r1.ID[:13])
}

// TestResumeCancelledThenResumeAgain 续跑途中被取消：中断步骤标记 partial、
// 续跑记录为 cancelled 终态；之后再次续跑基于该记录继续，partial 步骤必须重跑，
// 已成功步骤仍复用，不重复触发副作用。
func TestResumeCancelledThenResumeAgain(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sharedEx := NewLocalExecutor()
	eng := NewEngine(sharedEx, store)
	eng.SetLogger(func(format string, args ...any) { t.Logf("[flow] "+format, args...) })
	def := Definition{Name: "cancel-resume", Stages: []Stage{
		{Name: "s1", Steps: []Step{{Name: "a", Action: "act-a"}}},
		{Name: "s2", Steps: []Step{
			{Name: "slow", Action: "act-slow", Input: map[string]string{"sleep_ms": "2000"}},
			{Name: "peer", Action: "act-peer"},
		}},
		{Name: "s3", Steps: []Step{{Name: "c", Action: "act-c"}}},
	}}
	policy := defaultPolicy()
	policy.Retry.MaxAttempts = 1
	policy.Retry.StepTimeout = 5 * time.Second // slow 睡 2s，留余量避免高负载下步骤超时抢在取消语义前
	// 首跑：a、peer 成功，slow 慢，直接构造“首跑失败/取消现场”更稳妥：让首跑跑到 s2 后取消。
	done := make(chan struct {
		e *Execution
		r error
	}, 1)
	go func() {
		e, r := eng.Submit(context.Background(), def, "k-cr", policy)
		done <- struct {
			e *Execution
			r error
		}{e, r}
	}()
	var rootID string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		unfinished, _ := store.ListUnfinished()
		if len(unfinished) == 1 && unfinished[0].FindStep("s2", "slow").Status == StepRunning {
			rootID = unfinished[0].ID
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if rootID == "" {
		t.Fatal("首跑未进入 s2")
	}
	if err := eng.Cancel(rootID); err != nil {
		t.Fatal(err)
	}
	rootRes := <-done
	if got := classOf(t, rootRes.r); got != ClassCancelled {
		t.Fatalf("class=%s", got)
	}
	rootSnapshot, _ := store.Load(rootID)
	before := marshalExec(t, rootSnapshot)

	// 第 1 次续跑：slow 再次慢，取消这次续跑。
	done2 := make(chan struct {
		r *ResumeRecord
		e error
	}, 1)
	go func() {
		r, e := eng.Resume(context.Background(), rootID, ResumeOption{})
		done2 <- struct {
			r *ResumeRecord
			e error
		}{r, e}
	}()
	rid := waitResumeStepRunning(t, eng, "s2", "slow")
	if err := eng.CancelResume(rid); err != nil {
		t.Fatalf("cancel resume: %v", err)
	}
	res2 := <-done2
	if got := classOf(t, res2.e); got != ClassCancelled {
		t.Fatalf("续跑取消分类=%s", got)
	}
	r1 := res2.r
	if r1.Status != ResumeCancelled {
		t.Fatalf("r1 status=%s", r1.Status)
	}
	if sr := r1.FindStep("s1", "a"); sr.Outcome != OutcomeReused || sr.Status != StepSucceeded {
		t.Fatalf("r1: a 应复用首跑产出: %+v", sr)
	}
	if sr := r1.FindStep("s2", "slow"); sr.Outcome != OutcomeReran || sr.Status != StepPartial {
		t.Fatalf("r1: slow 应重跑且中断为 partial: %+v", sr)
	}
	if sr := r1.FindStep("s3", "c"); sr.Status != StepSkipped {
		t.Fatalf("r1: c 应 skipped: %s", sr.Status)
	}

	// 第 2 次续跑（模拟新进程接管同一文件存储）：partial 的 slow 必须重跑，a/peer 继续复用。
	eng2 := NewEngine(sharedEx, store)
	eng2.SetLogger(func(format string, args ...any) { t.Logf("[flow] "+format, args...) })
	ex := sharedEx
	r2, rerr := eng2.Resume(context.Background(), rootID, ResumeOption{})
	if rerr != nil {
		t.Fatalf("r2: %v", rerr)
	}
	if r2.Status != ResumeSucceeded {
		t.Fatalf("r2 status=%s err=%v", r2.Status, r2.Err)
	}
	if r2.Attempt != 2 || r2.ParentID != r1.ID {
		t.Fatalf("r2 应为 attempt=2 且父记录为 r1: attempt=%d parent=%s", r2.Attempt, r2.ParentID)
	}
	for _, want := range []struct{ stage, step string }{{"s1", "a"}, {"s2", "peer"}} {
		if sr := r2.FindStep(want.stage, want.step); sr.Outcome != OutcomeReused {
			t.Fatalf("r2: %s/%s 应复用不重跑: %+v", want.stage, want.step, sr)
		}
	}
	if sr := r2.FindStep("s2", "slow"); sr.Outcome != OutcomeReran || sr.Status != StepSucceeded {
		t.Fatalf("r2: partial 的 slow 必须重跑成功: %+v", sr)
	}
	if sr := r2.FindStep("s3", "c"); sr.Outcome != OutcomeReran || sr.Status != StepSucceeded {
		t.Fatalf("r2: c 应首次执行成功: %+v", sr)
	}
	// a/peer 整个生命周期只真正执行过一次（首跑）；slow = 首跑1 + r1一次(取消) + r2一次 = 3。
	if ex.Calls("act-a") != 1 || ex.Calls("act-peer") != 1 || ex.Calls("act-slow") != 3 {
		t.Fatalf("副作用计数: a=%d peer=%d slow=%d", ex.Calls("act-a"), ex.Calls("act-peer"), ex.Calls("act-slow"))
	}
	// 原始执行记录仍未被任何续跑/取消改动。
	rootAfter, _ := store.Load(rootID)
	if marshalExec(t, rootAfter) != before {
		t.Fatal("续跑取消/再续跑后原始执行记录被改动")
	}
	t.Logf("判定依据: r1=%s 取消(slow=partial); r2=%s 重跑 slow/c 成功, a/peer 复用; slow 全程3次; 原始记录只读",
		r1.ID[:13], r2.ID[:13])
}

// 防止 fmt 未使用的占位（后续用例可能需要）。
var _ = fmt.Sprintf

// TestRecoverInterruptedResume 续跑途中“进程崩溃”：存储中留下非终态续跑记录，
// 新引擎 Recover 接管同一条记录（而非新建），已成功步骤不重跑，链头不产生分叉。
func TestRecoverInterruptedResume(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	def := Definition{Name: "rr", Stages: []Stage{
		{Name: "s1", Steps: []Step{{Name: "a", Action: "act-a"}}},
		{Name: "s2", Steps: []Step{{Name: "b", Action: "act-b"}}},
	}}
	policy := ExecPolicy{Retry: RetryPolicy{MaxAttempts: 1, Backoff: time.Millisecond, StepTimeout: 5 * time.Second}}
	// 首跑失败现场：a 已成功，b 失败。
	root := &Execution{
		ID: "exec-rr", IdemKey: "k-rr", Definition: def, Policy: policy,
		Status: StatusFailed, StartedAt: time.Now(), FinishedAt: time.Now(),
		Steps: []*StepRecord{
			{Stage: "s1", Step: "a", Status: StepSucceeded, Attempts: 1, Output: map[string]string{"echo": "a"}},
			{Stage: "s2", Step: "b", Status: StepPartial, Attempts: 1},
		},
	}
	if err := store.Save(root); err != nil {
		t.Fatal(err)
	}
	// 续跑跑了一半“崩溃”：续跑记录 running，b 在本次续跑中已成功（崩溃前已持久化）。
	half := &ResumeRecord{
		ID: "resume-rr-1", RootExecutionID: root.ID, ParentID: root.ID,
		Attempt: 1, PolicyInherited: true, Policy: policy, Status: ResumeRunning,
		StartedAt: time.Now(),
		Steps: []*StepResumeRecord{
			{Stage: "s1", Step: "a", Outcome: OutcomeReused, Status: StepSucceeded, Output: map[string]string{"echo": "a"}},
			{Stage: "s2", Step: "b", Outcome: OutcomeReran, Status: StepSucceeded, Attempts: 1, Output: map[string]string{"ok": "1"}},
		},
	}
	if err := store.SaveResume(half); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveResumeHead(root.ID, "", half.ID); err != nil {
		t.Fatal(err)
	}

	eng, ex := newTestEngine(t, store)
	if _, err := eng.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	// 接管同一条记录：不新建、不重跑任何步骤。
	got, err := eng.GetResume(half.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != ResumeSucceeded {
		t.Fatalf("接管后续跑应收尾成功: %s", got.Status)
	}
	if ex.Calls("act-a") != 0 || ex.Calls("act-b") != 0 {
		t.Fatalf("接管不得重复触发副作用: a=%d b=%d", ex.Calls("act-a"), ex.Calls("act-b"))
	}
	chain, _ := eng.ResumeHistory(root.ID)
	if len(chain.Resumes) != 1 || chain.HeadID != half.ID {
		t.Fatalf("续跑链不应分叉: len=%d head=%s", len(chain.Resumes), chain.HeadID)
	}
	// 此时再显式 Resume：应新建 attempt=2（链头已终态），且全部复用、零副作用。
	r2, err := eng.Resume(context.Background(), root.ID, ResumeOption{})
	if err != nil {
		t.Fatalf("resume after recover: %v", err)
	}
	if r2.Attempt != 2 || r2.ParentID != half.ID || r2.Status != ResumeSucceeded {
		t.Fatalf("r2 应成链且全复用成功: attempt=%d parent=%s status=%s", r2.Attempt, r2.ParentID, r2.Status)
	}
	for _, sr := range r2.Steps {
		if sr.Outcome != OutcomeReused {
			t.Fatalf("r2 全部步骤应复用: %+v", sr)
		}
	}
	if ex.Calls("act-a") != 0 || ex.Calls("act-b") != 0 {
		t.Fatalf("全成功后续跑零副作用: a=%d b=%d", ex.Calls("act-a"), ex.Calls("act-b"))
	}
	rootAfter, _ := eng.Get(root.ID)
	if rootAfter.Status != StatusFailed {
		t.Fatalf("原始执行记录必须保持 failed 只读: %s", rootAfter.Status)
	}
	t.Logf("判定依据: Recover 接管非终态续跑 %s 并收尾, 零副作用; 后续 attempt=2 成链全复用; 原始记录保持 %s",
		half.ID, rootAfter.Status)
}

// TestResumeStrictlySequentialCreatesNewRecord 严格顺序发起（上一次续跑返回后
// 再发起）的两次续跑应是各自独立的记录：第二次不会被收敛到第一次。
func TestResumeStrictlySequentialCreatesNewRecord(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	policy := defaultPolicy()
	policy.Retry.MaxAttempts = 1
	ex.ScriptFailures("act-c", ErrScripted)
	exec, err := eng.Submit(context.Background(), twoStageDef(), "k-seq", policy)
	if err == nil {
		t.Fatal("首跑应失败")
	}
	r1, err := eng.Resume(context.Background(), exec.ID, ResumeOption{})
	if err != nil {
		t.Fatalf("r1: %v", err)
	}
	if r1.Status != ResumeSucceeded {
		t.Fatalf("r1 status=%s", r1.Status)
	}
	// 确保跨过批次收敛宽限窗口，再发起一次严格顺序的新续跑。
	time.Sleep(120 * time.Millisecond)
	r2, err := eng.Resume(context.Background(), exec.ID, ResumeOption{})
	if err != nil {
		t.Fatalf("r2: %v", err)
	}
	if r2.ID == r1.ID {
		t.Fatalf("严格顺序的续跑应产生独立新记录, got same id=%s", r2.ID)
	}
	if r2.Attempt != 2 || r2.ParentID != r1.ID {
		t.Fatalf("r2 应为 attempt=2 且 parent=r1: attempt=%d parent=%s", r2.Attempt, r2.ParentID)
	}
	for _, sr := range r2.Steps {
		if sr.Outcome != OutcomeReused {
			t.Fatalf("r2 全步骤应复用零副作用: %+v", sr)
		}
	}
	if ex.Calls("act-c") != 2 {
		t.Fatalf("c 应仅首跑1次+r1一次, got %d", ex.Calls("act-c"))
	}
	t.Logf("判定依据: 严格顺序续跑得到独立记录 r1=%s r2=%s(attempt=2), 未被宽限窗口合并, 零额外副作用",
		r1.ID[:13], r2.ID[:13])
}
