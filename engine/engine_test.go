package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// logRun 打印执行流水与判定依据。
func logRun(t *testing.T, label string, rec *RunRecord) {
	t.Helper()
	t.Logf("[%s] run id=%s key=%s status=%s failure=%q detail=%q next_step=%d",
		label, rec.ID, rec.IdempotencyKey, rec.Status, rec.Failure, rec.Detail, rec.NextStep)
	for _, s := range rec.Steps {
		t.Logf("[%s]   step=%s status=%s attempts=%d failure=%q partial=%v output=%v detail=%q",
			label, s.Name, s.Status, s.Attempts, s.Failure, s.Partial, s.Output, s.Detail)
		for _, c := range s.Children {
			t.Logf("[%s]     child=%s status=%s attempts=%d failure=%q partial=%v output=%v",
				label, c.Name, c.Status, c.Attempts, c.Failure, c.Partial, c.Output)
		}
	}
}

// opAction 把步骤映射到本地执行器的一个具名操作。
func opAction(op string) StepAction {
	return func(ctx context.Context, exec Executor, in map[string]any) (map[string]any, error) {
		return exec.Invoke(ctx, op, in)
	}
}

func TestConcurrentDuplicateSubmit(t *testing.T) {
	exec := NewLocalExecutor()
	exec.Register("charge", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		time.Sleep(20 * time.Millisecond) // 放大并发窗口
		return map[string]any{"receipt": "r-1"}, nil
	})
	eng := New(exec, NewMemStore())
	wf := Workflow{
		Name:  "pay",
		Steps: []Step{{Name: "charge", Action: opAction("charge")}},
	}

	const n = 8
	recs := make([]*RunRecord, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recs[i], errs[i] = eng.Submit(context.Background(), wf, "order-1")
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("submit %d: %v", i, errs[i])
		}
		logRun(t, fmt.Sprintf("submitter-%d", i), recs[i])
		if recs[i].Status != StatusSucceeded {
			t.Fatalf("submitter %d got status %s", i, recs[i].Status)
		}
	}
	if got := exec.Calls("charge"); got != 1 {
		t.Fatalf("副作用重复：charge 被调用 %d 次，期望 1 次", got)
	}
	t.Logf("判定依据：8 个并发提交共享同一幂等键，执行器仅被调用 1 次，全部拿到 succeeded")
}

func TestIdempotencyConflict(t *testing.T) {
	exec := NewLocalExecutor()
	exec.Register("a", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{}, nil
	})
	eng := New(exec, NewMemStore())
	wf1 := Workflow{Name: "wf-a", Steps: []Step{{Name: "s1", Action: opAction("a")}}}
	wf2 := Workflow{Name: "wf-b", Steps: []Step{{Name: "s1", Action: opAction("a")}}}

	if _, err := eng.Submit(context.Background(), wf1, "k"); err != nil {
		t.Fatal(err)
	}
	_, err := eng.Submit(context.Background(), wf2, "k")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("期望 ErrConflict，得到 %v", err)
	}
	t.Logf("判定依据：同一幂等键被不同工作流占用，拒绝并返回 ErrConflict: %v", err)
}

func TestRetryExhaustedClassification(t *testing.T) {
	exec := NewLocalExecutor()
	rawErr := errors.New("boom: secret-internal-detail")
	exec.Register("flaky", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return nil, rawErr
	})
	eng := New(exec, NewMemStore())
	wf := Workflow{
		Name: "retry",
		Steps: []Step{{
			Name:   "flaky",
			Policy: StepPolicy{Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}},
			Action: opAction("flaky"),
		}},
	}
	rec, err := eng.Submit(context.Background(), wf, "k-retry")
	if err != nil {
		t.Fatal(err)
	}
	logRun(t, "retry", rec)
	if rec.Status != StatusFailed || rec.Failure != FailureStep {
		t.Fatalf("期望 failed/step_failed，得到 %s/%s", rec.Status, rec.Failure)
	}
	if rec.Steps[0].Attempts != 3 {
		t.Fatalf("期望 3 次尝试，得到 %d", rec.Steps[0].Attempts)
	}
	if got := exec.Calls("flaky"); got != 3 {
		t.Fatalf("期望执行器调用 3 次，得到 %d", got)
	}
	// 原始错误不得透出到对外记录。
	if rec.Detail == "" || contains(rec.Detail, rawErr.Error()) || contains(rec.Steps[0].Detail, rawErr.Error()) {
		t.Fatalf("原始错误被透出: run=%q step=%q", rec.Detail, rec.Steps[0].Detail)
	}
	t.Logf("判定依据：重试 3 次耗尽，分类为 step_failed，detail=%q 不含原始错误 %q", rec.Steps[0].Detail, rawErr.Error())
}

func contains(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

func TestStepTimeoutClassification(t *testing.T) {
	exec := NewLocalExecutor()
	exec.Register("slow", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
			return map[string]any{}, nil
		}
	})
	eng := New(exec, NewMemStore())
	wf := Workflow{
		Name: "timeout",
		Steps: []Step{{
			Name:   "slow",
			Policy: StepPolicy{Timeout: 10 * time.Millisecond, Retry: RetryPolicy{MaxAttempts: 2}},
			Action: opAction("slow"),
		}},
	}
	rec, err := eng.Submit(context.Background(), wf, "k-timeout")
	if err != nil {
		t.Fatal(err)
	}
	logRun(t, "timeout", rec)
	if rec.Status != StatusFailed || rec.Failure != FailureStepTimeout {
		t.Fatalf("期望 failed/step_timeout，得到 %s/%s", rec.Status, rec.Failure)
	}
	t.Logf("判定依据：单次尝试超时且重试耗尽，分类为 step_timeout（区别于 step_failed）")
}

func TestParallelGroup(t *testing.T) {
	exec := NewLocalExecutor()
	exec.Register("ok1", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{"v": 1}, nil
	})
	exec.Register("ok2", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{"v": 2}, nil
	})
	eng := New(exec, NewMemStore())
	wf := Workflow{
		Name: "par",
		Steps: []Step{{
			Name: "group",
			Parallel: []Step{
				{Name: "c1", Action: opAction("ok1")},
				{Name: "c2", Action: opAction("ok2")},
			},
		}},
	}
	rec, err := eng.Submit(context.Background(), wf, "k-par")
	if err != nil {
		t.Fatal(err)
	}
	logRun(t, "parallel", rec)
	if rec.Status != StatusSucceeded {
		t.Fatalf("期望 succeeded，得到 %s", rec.Status)
	}
	if len(rec.Steps[0].Children) != 2 {
		t.Fatalf("期望 2 个子步骤记录，得到 %d", len(rec.Steps[0].Children))
	}
	t.Logf("判定依据：并发组两个子步骤均成功，整组 succeeded")
}

func TestInterruptAndRecover(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store.json")

	crash := make(chan struct{}) // 永不关闭：模拟进程崩溃时步骤悬挂
	exec1 := NewLocalExecutor()
	exec1.Register("step1", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{"side_effect": "done"}, nil
	})
	exec1.Register("step2", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		<-crash // 模拟崩溃：永不返回
		return nil, errors.New("unreachable")
	})

	store1, err := NewFileStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	eng1 := New(exec1, store1)
	wf := Workflow{
		Name: "recoverable",
		Steps: []Step{
			{Name: "s1", Action: opAction("step1")},
			{Name: "s2", Action: opAction("step2")},
		},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = eng1.Submit(context.Background(), wf, "k-recover")
	}()
	// 等待 step1 完成并持久化（NextStep=1）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		rec, err := store1.Get("k-recover")
		if err == nil && rec.NextStep == 1 {
			logRun(t, "before-crash", rec)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("step1 未在超时前持久化")
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("判定依据：step1 已完成且落盘（next_step=1），step2 悬挂，模拟进程崩溃")

	// “进程重启”：新引擎、新执行器、同一存储文件。
	exec2 := NewLocalExecutor()
	exec2.Register("step1", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{"side_effect": "done"}, nil
	})
	exec2.Register("step2", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{"resumed": true}, nil
	})
	store2, err := NewFileStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	eng2 := New(exec2, store2)
	// 恢复需要重新注册工作流以绑定 Action（不触发任何执行）。
	eng2.Register(wf)
	recs, err := eng2.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("期望恢复 1 条执行，得到 %d", len(recs))
	}
	rec := recs[0]
	logRun(t, "after-recover", rec)
	if rec.Status != StatusSucceeded {
		t.Fatalf("期望恢复后 succeeded，得到 %s", rec.Status)
	}
	if got := exec2.Calls("step1"); got != 0 {
		t.Fatalf("恢复时重复执行了已完成步骤 step1（%d 次），副作用不幂等", got)
	}
	if got := exec2.Calls("step2"); got != 1 {
		t.Fatalf("恢复时应执行 step2 一次，得到 %d", got)
	}
	if rec.Steps[0].Output["side_effect"] != "done" {
		t.Fatalf("step1 的部分结果丢失: %v", rec.Steps[0].Output)
	}
	t.Logf("判定依据：从 next_step=1 恢复，step1 未重复执行（副作用 0 次），step2 执行 1 次，历史结果保留")
}

func TestCancelRun(t *testing.T) {
	exec := NewLocalExecutor()
	exec.Register("fast", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{"kept": true}, nil
	})
	exec.Register("block", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	eng := New(exec, NewMemStore())
	wf := Workflow{
		Name: "cancelable",
		Steps: []Step{
			{Name: "s1", Action: opAction("fast")},
			{Name: "s2", Action: opAction("block")},
		},
	}
	resCh := make(chan *RunRecord, 1)
	go func() {
		rec, _ := eng.Submit(context.Background(), wf, "k-cancel")
		resCh <- rec
	}()
	// 等执行进入 s2 后取消。
	deadline := time.Now().Add(2 * time.Second)
	for {
		rec, err := eng.Get("k-cancel")
		if err == nil && rec.NextStep == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("执行未进入 s2")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := eng.Cancel("k-cancel"); err != nil {
		t.Fatal(err)
	}
	rec := <-resCh
	logRun(t, "canceled", rec)
	if rec.Status != StatusCanceled || rec.Failure != FailureCanceled {
		t.Fatalf("期望 canceled/canceled，得到 %s/%s", rec.Status, rec.Failure)
	}
	if rec.Steps[0].Status != StatusSucceeded || rec.Steps[0].Output["kept"] != true {
		t.Fatalf("已完成步骤的结果必须保留: %+v", rec.Steps[0])
	}
	if rec.Steps[1].Status != StatusCanceled {
		t.Fatalf("被中断步骤应标记 canceled，得到 %s", rec.Steps[1].Status)
	}
	t.Logf("判定依据：取消与失败可区分（canceled ≠ failed），s1 结果保留，s2 显式标记 canceled")
}

func TestRunTimeout(t *testing.T) {
	exec := NewLocalExecutor()
	exec.Register("block", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	eng := New(exec, NewMemStore(), WithRunTimeout(20*time.Millisecond))
	wf := Workflow{
		Name:  "slow-run",
		Steps: []Step{{Name: "s1", Action: opAction("block")}},
	}
	rec, err := eng.Submit(context.Background(), wf, "k-run-timeout")
	if err != nil {
		t.Fatal(err)
	}
	logRun(t, "run-timeout", rec)
	if rec.Status != StatusTimedOut || rec.Failure != FailureRunTimeout {
		t.Fatalf("期望 timed_out/run_timeout，得到 %s/%s", rec.Status, rec.Failure)
	}
	t.Logf("判定依据：整体超时与取消、失败三者可区分：status=timed_out, failure=run_timeout")
}

func TestCancelParallelGroupPartial(t *testing.T) {
	exec := NewLocalExecutor()
	exec.Register("quick", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{"partial_result": 42}, nil
	})
	exec.Register("hang", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	eng := New(exec, NewMemStore())
	wf := Workflow{
		Name: "par-cancel",
		Steps: []Step{{
			Name: "group",
			Parallel: []Step{
				{Name: "fast-child", Action: opAction("quick")},
				{Name: "slow-child", Action: opAction("hang")},
			},
		}},
	}
	resCh := make(chan *RunRecord, 1)
	go func() {
		rec, _ := eng.Submit(context.Background(), wf, "k-par-cancel")
		resCh <- rec
	}()
	// 等 fast-child 完成、slow-child 悬挂后取消。
	deadline := time.Now().Add(2 * time.Second)
	for {
		if exec.Calls("quick") == 1 && exec.Calls("hang") == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("并发组未启动")
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond) // 确保 fast-child 结果已写入
	if err := eng.Cancel("k-par-cancel"); err != nil {
		t.Fatal(err)
	}
	rec := <-resCh
	logRun(t, "par-cancel", rec)
	if rec.Status != StatusCanceled {
		t.Fatalf("期望 canceled，得到 %s", rec.Status)
	}
	g := rec.Steps[0]
	if !g.Partial {
		t.Fatalf("取消时并发组已产生部分结果，必须显式标记 partial")
	}
	var fast *StepRecord
	for i := range g.Children {
		if g.Children[i].Name == "fast-child" {
			fast = &g.Children[i]
		}
	}
	if fast == nil || fast.Output["partial_result"] != float64(42) && fast.Output["partial_result"] != 42 {
		t.Fatalf("已完成子步骤的部分结果必须保留而不是静默丢弃: %+v", fast)
	}
	t.Logf("判定依据：并发组取消时标记 partial=true，fast-child 的结果 %v 被保留", fast.Output)
}

func TestRecoverUsesSnapshotPolicy(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store.json")

	crash := make(chan struct{})
	exec1 := NewLocalExecutor()
	exec1.Register("s1", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{}, nil
	})
	exec1.Register("s2", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		<-crash
		return nil, errors.New("unreachable")
	})
	store1, err := NewFileStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	eng1 := New(exec1, store1)
	// 提交时策略：s2 重试 3 次。
	wfV1 := Workflow{
		Name: "versioned",
		Steps: []Step{
			{Name: "s1", Action: opAction("s1")},
			{Name: "s2", Policy: StepPolicy{Retry: RetryPolicy{MaxAttempts: 3}}, Action: opAction("s2")},
		},
	}
	go eng1.Submit(context.Background(), wfV1, "k-ver")
	deadline := time.Now().Add(2 * time.Second)
	for {
		rec, err := store1.Get("k-ver")
		if err == nil && rec.NextStep == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("s1 未持久化")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// 重启后策略已变更为 MaxAttempts=1，但历史执行应按快照的 3 次复现。
	exec2 := NewLocalExecutor()
	exec2.Register("s1", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{}, nil
	})
	exec2.Register("s2", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return nil, errors.New("always fails")
	})
	store2, err := NewFileStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	eng2 := New(exec2, store2)
	wfV2 := Workflow{Name: wfV1.Name, Steps: append([]Step(nil), wfV1.Steps...)}
	wfV2.Steps[1].Policy = StepPolicy{Retry: RetryPolicy{MaxAttempts: 1}}
	eng2.Register(wfV2)

	recs, err := eng2.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rec := recs[0]
	logRun(t, "snapshot-policy", rec)
	if rec.Steps[1].Attempts != 3 {
		t.Fatalf("历史执行应按提交时策略重试 3 次，实际 %d 次", rec.Steps[1].Attempts)
	}
	if rec.Status != StatusFailed || rec.Failure != FailureStep {
		t.Fatalf("期望 failed/step_failed，得到 %s/%s", rec.Status, rec.Failure)
	}
	t.Logf("判定依据：注册的新策略 MaxAttempts=1 未生效于历史执行，实际按快照重试 %d 次", rec.Steps[1].Attempts)
}
