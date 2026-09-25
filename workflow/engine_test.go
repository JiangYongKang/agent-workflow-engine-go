package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestEngine(t *testing.T, store Store) (*Engine, *LocalExecutor) {
	t.Helper()
	if store == nil {
		store = NewMemoryStore()
	}
	ex := NewLocalExecutor()
	eng := NewEngine(ex, store)
	eng.SetLogger(func(format string, args ...any) { t.Logf("[flow] "+format, args...) })
	return eng, ex
}

func twoStageDef() Definition {
	return Definition{
		Name: "demo",
		Stages: []Stage{
			{Name: "s1", Steps: []Step{
				{Name: "a", Action: "act-a", Input: map[string]string{"k": "v1"}},
				{Name: "b", Action: "act-b", Input: map[string]string{"k": "v2"}},
			}},
			{Name: "s2", Steps: []Step{
				{Name: "c", Action: "act-c"},
			}},
		},
	}
}

func defaultPolicy() ExecPolicy {
	return ExecPolicy{Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond, StepTimeout: 2 * time.Second}}
}

func classOf(t *testing.T, err error) FailureClass {
	t.Helper()
	var se *StepError
	if !errors.As(err, &se) {
		t.Fatalf("判定依据: 期望对外错误为分类错误 *StepError, 实际 %T: %v", err, err)
	}
	t.Logf("判定依据: 错误分类=%s step=%s cause=%v", se.Class, se.Step, se.Cause)
	return se.Class
}

func TestSequentialAndParallelSuccess(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	exec, err := eng.Submit(context.Background(), twoStageDef(), "k-ok", defaultPolicy())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if exec.Status != StatusSucceeded {
		t.Fatalf("status=%s", exec.Status)
	}
	for _, want := range []struct{ stage, step string }{{"s1", "a"}, {"s1", "b"}, {"s2", "c"}} {
		rec := exec.FindStep(want.stage, want.step)
		if rec == nil || rec.Status != StepSucceeded {
			t.Fatalf("step %s/%s record=%+v", want.stage, want.step, rec)
		}
	}
	if rec := exec.FindStep("s1", "a"); rec.Output["echo.k"] != "v1" {
		t.Fatalf("output=%v", rec.Output)
	}
	if ex.Calls("act-a") != 1 || ex.Calls("act-b") != 1 || ex.Calls("act-c") != 1 {
		t.Fatalf("calls a=%d b=%d c=%d", ex.Calls("act-a"), ex.Calls("act-b"), ex.Calls("act-c"))
	}
	t.Logf("判定依据: 全部步骤成功且每动作恰好调用一次, status=%s", exec.Status)
}

func TestRetryThenSuccess(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	ex.ScriptFailures("act-b", ErrScripted, ErrScripted) // 前两次失败
	exec, err := eng.Submit(context.Background(), twoStageDef(), "k-retry", defaultPolicy())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if exec.Status != StatusSucceeded {
		t.Fatalf("status=%s", exec.Status)
	}
	rec := exec.FindStep("s1", "b")
	if rec.Attempts != 3 || ex.Calls("act-b") != 3 {
		t.Fatalf("attempts=%d calls=%d", rec.Attempts, ex.Calls("act-b"))
	}
	t.Logf("判定依据: 第3次尝试成功, attempts=%d", rec.Attempts)
}

func TestRetryExhaustedClassification(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	ex.ScriptFailures("act-b", ErrScripted, ErrScripted, ErrScripted, ErrScripted)
	policy := defaultPolicy()
	policy.Retry.MaxAttempts = 2
	exec, err := eng.Submit(context.Background(), twoStageDef(), "k-exhaust", policy)
	if err == nil {
		t.Fatal("expect error")
	}
	if got := classOf(t, err); got != ClassRetryExhausted {
		t.Fatalf("class=%s", got)
	}
	if exec.Status != StatusFailed {
		t.Fatalf("status=%s", exec.Status)
	}
	if ex.Calls("act-b") != 2 {
		t.Fatalf("calls=%d, 重试次数应等于 MaxAttempts", ex.Calls("act-b"))
	}
	if rec := exec.FindStep("s2", "c"); rec.Status != StepSkipped {
		t.Fatalf("后续步骤应标记 skipped, got %s", rec.Status)
	}
	t.Logf("判定依据: 重试耗尽分类=%s, 未执行步骤=%s", ClassRetryExhausted, exec.FindStep("s2", "c").Status)
}

func TestNonRetryableClassification(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	ex.SetNonRetryable("act-a")
	ex.ScriptFailures("act-a", ErrScripted)
	exec, err := eng.Submit(context.Background(), twoStageDef(), "k-nonretry", defaultPolicy())
	if got := classOf(t, err); got != ClassNonRetryable {
		t.Fatalf("class=%s", got)
	}
	if exec.Status != StatusFailed || ex.Calls("act-a") != 1 {
		t.Fatalf("status=%s calls=%d, 不可重试错误应立即失败", exec.Status, ex.Calls("act-a"))
	}
	t.Logf("判定依据: 不可重试错误不重试, calls=%d", ex.Calls("act-a"))
}

func TestStepTimeoutClassification(t *testing.T) {
	eng, _ := newTestEngine(t, nil)
	def := Definition{Name: "slow", Stages: []Stage{{Name: "s1", Steps: []Step{
		{Name: "slow", Action: "act-slow", Input: map[string]string{"sleep_ms": "500"}},
	}}}}
	policy := defaultPolicy()
	policy.Retry.StepTimeout = 50 * time.Millisecond
	policy.Retry.MaxAttempts = 1
	exec, err := eng.Submit(context.Background(), def, "k-steptimeout", policy)
	if got := classOf(t, err); got != ClassTimeout {
		t.Fatalf("class=%s", got)
	}
	if exec.Status != StatusTimedOut {
		t.Fatalf("status=%s, 超时应与失败/取消区分", exec.Status)
	}
	t.Logf("判定依据: 步骤超时分类=%s status=%s", ClassTimeout, exec.Status)
}

func TestConcurrentDuplicateSubmit(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	const n = 8
	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			exec, err := eng.Submit(context.Background(), twoStageDef(), "k-dup", defaultPolicy())
			if exec != nil {
				ids[i] = exec.ID
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("submit %d: %v", i, errs[i])
		}
		if ids[i] == "" || ids[i] != ids[0] {
			t.Fatalf("重复提交应返回同一执行: ids=%v", ids)
		}
	}
	// 同一步骤不得重复产生副作用：每个动作恰好执行一次。
	for _, a := range []string{"act-a", "act-b", "act-c"} {
		if ex.Calls(a) != 1 {
			t.Fatalf("action %s calls=%d, 幂等键下去重失败", a, ex.Calls(a))
		}
	}
	t.Logf("判定依据: %d 个并发提交收敛到执行 %s, 每动作调用一次", n, ids[0])
}

func TestDuplicateSubmitAfterCompletion(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	first, err := eng.Submit(context.Background(), twoStageDef(), "k-done", defaultPolicy())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	second, err := eng.Submit(context.Background(), twoStageDef(), "k-done", defaultPolicy())
	if err != nil || second.ID != first.ID {
		t.Fatalf("完结后重复提交应返回既有结果: id=%s err=%v", second.ID, err)
	}
	if _, err := eng.Submit(context.Background(), twoStageDef(), "k-done", ExecPolicy{RejectOnDupID: true}); err == nil {
		t.Fatal("RejectOnDupID 应拒绝")
	} else if got := classOf(t, err); got != ClassDuplicate {
		t.Fatalf("class=%s", got)
	}
	if ex.Calls("act-a") != 1 {
		t.Fatalf("calls=%d", ex.Calls("act-a"))
	}
	t.Logf("判定依据: 完结后重复提交返回既有执行 %s; 拒绝策略分类=%s", first.ID, ClassDuplicate)
}

func TestCancelMarksPartialResults(t *testing.T) {
	eng, _ := newTestEngine(t, nil)
	def := Definition{Name: "cancel-me", Stages: []Stage{
		{Name: "s1", Steps: []Step{{Name: "a", Action: "act-a"}}},
		{Name: "s2", Steps: []Step{
			{Name: "slow", Action: "act-slow", Input: map[string]string{"sleep_ms": "2000"}},
			{Name: "peer", Action: "act-peer"},
		}},
		{Name: "s3", Steps: []Step{{Name: "c", Action: "act-c"}}},
	}}
	type result struct {
		exec *Execution
		err  error
	}
	done := make(chan result, 1)
	go func() {
		exec, err := eng.Submit(context.Background(), def, "k-cancel", defaultPolicy())
		done <- result{exec, err}
	}()
	// 等待进入 s2（slow 已开始睡眠）。
	deadline := time.Now().Add(2 * time.Second)
	var execID string
	for time.Now().Before(deadline) {
		unfinished, _ := eng.store.ListUnfinished()
		if len(unfinished) == 1 && unfinished[0].FindStep("s2", "slow").Status == StepRunning {
			execID = unfinished[0].ID
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if execID == "" {
		t.Fatal("执行未进入 s2")
	}
	if err := eng.Cancel(execID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	res := <-done
	if got := classOf(t, res.err); got != ClassCancelled {
		t.Fatalf("class=%s", got)
	}
	exec := res.exec
	if exec.Status != StatusCancelled {
		t.Fatalf("status=%s, 取消应与失败/超时区分", exec.Status)
	}
	if rec := exec.FindStep("s1", "a"); rec.Status != StepSucceeded || rec.Output == nil {
		t.Fatalf("已产生的部分结果必须保留: %+v", rec)
	}
	if rec := exec.FindStep("s2", "slow"); rec.Status != StepPartial {
		t.Fatalf("中断中的步骤应标记 partial: %s", rec.Status)
	}
	if rec := exec.FindStep("s3", "c"); rec.Status != StepSkipped {
		t.Fatalf("未执行步骤应标记 skipped: %s", rec.Status)
	}
	t.Logf("判定依据: 取消后 s1.a=%s(保留输出) s2.slow=%s s3.c=%s",
		exec.FindStep("s1", "a").Status, exec.FindStep("s2", "slow").Status, exec.FindStep("s3", "c").Status)
}

func TestExecutionTimeout(t *testing.T) {
	eng, _ := newTestEngine(t, nil)
	def := Definition{Name: "exec-timeout", Stages: []Stage{{Name: "s1", Steps: []Step{
		{Name: "slow", Action: "act-slow", Input: map[string]string{"sleep_ms": "2000"}},
	}}}}
	policy := defaultPolicy()
	policy.ExecTimeout = 80 * time.Millisecond
	exec, err := eng.Submit(context.Background(), def, "k-exectimeout", policy)
	if got := classOf(t, err); got != ClassTimeout {
		t.Fatalf("class=%s", got)
	}
	if exec.Status != StatusTimedOut {
		t.Fatalf("status=%s", exec.Status)
	}
	t.Logf("判定依据: 执行级超时分类=%s status=%s", ClassTimeout, exec.Status)
}

// TestRecoverAfterInterruption 模拟进程崩溃：存储中留有非终态执行，
// 新引擎 Recover 后从一致状态继续，已成功步骤不重复执行，
// 且按记录中固化的策略快照（MaxAttempts=3）复现。
func TestRecoverAfterInterruption(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	def := Definition{Name: "recoverable", Stages: []Stage{
		{Name: "s1", Steps: []Step{{Name: "a", Action: "act-a"}}},
		{Name: "s2", Steps: []Step{{Name: "b", Action: "act-b"}, {Name: "c", Action: "act-c"}}},
	}}
	// 构造“崩溃现场”：s1.a 已成功，s2 未开始，状态 running。
	crashed := &Execution{
		ID: "exec-crashed", IdemKey: "k-crash", Definition: def,
		Policy:    ExecPolicy{Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond, StepTimeout: time.Second}},
		Status:    StatusRunning,
		StartedAt: time.Now(),
		Steps: []*StepRecord{
			{Stage: "s1", Step: "a", Status: StepSucceeded, Attempts: 1, Output: map[string]string{"echo.k": "v"}},
			{Stage: "s2", Step: "b", Status: StepPending},
			{Stage: "s2", Step: "c", Status: StepPending},
		},
	}
	if err := store.Save(crashed); err != nil {
		t.Fatal(err)
	}
	// 新引擎（模拟进程重启），b 按快照策略需失败 2 次后成功。
	eng, ex := newTestEngine(t, store)
	ex.ScriptFailures("act-b", ErrScripted, ErrScripted)
	recovered, err := eng.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 {
		t.Fatalf("recovered=%d", len(recovered))
	}
	exec := recovered[0]
	if exec.Status != StatusSucceeded {
		t.Fatalf("status=%s err=%v", exec.Status, exec.Err)
	}
	if ex.Calls("act-a") != 0 {
		t.Fatalf("已成功步骤不得重复产生副作用, calls=%d", ex.Calls("act-a"))
	}
	if ex.Calls("act-b") != 3 {
		t.Fatalf("应按快照策略 MaxAttempts=3 复现, calls=%d", ex.Calls("act-b"))
	}
	if rec := exec.FindStep("s1", "a"); rec.Output["echo.k"] != "v" {
		t.Fatalf("部分结果应保留: %+v", rec)
	}
	// 恢复后幂等键仍生效。
	again, err := eng.Submit(context.Background(), def, "k-crash", defaultPolicy())
	if err != nil || again.ID != exec.ID {
		t.Fatalf("恢复后重复提交应返回既有结果: %v", err)
	}
	t.Logf("判定依据: 恢复执行 %s 成功; act-a 未重跑; act-b 按快照重试 calls=%d", exec.ID, ex.Calls("act-b"))
}

// TestRecoverInterruptedStepReclassified 崩溃时处于 running 的步骤在恢复后
// 若仍不可完成，应得到可区分的失败分类而非原始错误。
func TestRecoverFailureClassification(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	def := Definition{Name: "d", Stages: []Stage{{Name: "s1", Steps: []Step{{Name: "a", Action: "act-a"}}}}}
	crashed := &Execution{
		ID: "exec-x", IdemKey: "k-x", Definition: def,
		Policy:    ExecPolicy{Retry: RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}},
		Status:    StatusRunning,
		StartedAt: time.Now(),
		Steps:     []*StepRecord{{Stage: "s1", Step: "a", Status: StepRunning, Attempts: 1}},
	}
	if err := store.Save(crashed); err != nil {
		t.Fatal(err)
	}
	eng, ex := newTestEngine(t, store)
	ex.ScriptFailures("act-a", fmt.Errorf("raw executor error"), fmt.Errorf("raw executor error"))
	recovered, err := eng.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	exec := recovered[0]
	if exec.Status != StatusFailed {
		t.Fatalf("status=%s", exec.Status)
	}
	if exec.Err == nil || exec.Err.Class != ClassRetryExhausted {
		t.Fatalf("应对外暴露分类错误, got %+v", exec.Err)
	}
	if exec.Err.Error() == "raw executor error" {
		t.Fatal("不得把底层执行器原始错误直接透出")
	}
	t.Logf("判定依据: 恢复后失败分类=%s, 对外错误=%q", exec.Err.Class, exec.Err.Error())
}
