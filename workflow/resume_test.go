package workflow

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// failOncePolicy 一步一试，便于快速造出失败现场。
func failOncePolicy() ExecPolicy {
	return ExecPolicy{Retry: RetryPolicy{MaxAttempts: 1, Backoff: time.Millisecond, StepTimeout: 2 * time.Second}}
}

// mustJSON 序列化执行记录，用于快照比较（原始记录只读断言）。
func mustJSON(t *testing.T, exec *Execution) string {
	t.Helper()
	b, err := json.Marshal(exec)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestResumeReusesSucceededOutputs 续跑只重跑未成功步骤：
// 已成功步骤复用旧产出（Origin=reused，SourceExec 追溯来源），不重复触发副作用；
// 状态不确定/未算数步骤重新执行（Origin=rerun）。
func TestResumeReusesSucceededOutputs(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	ex.ScriptFailures("act-b", ErrScripted) // 原始执行中 act-b 失败一次
	orig, err := eng.Submit(context.Background(), twoStageDef(), "k-resume-reuse", failOncePolicy())
	if err == nil || orig.Status != StatusFailed {
		t.Fatalf("原始执行应失败: status=%s err=%v", orig.Status, err)
	}
	if ex.Calls("act-a") != 1 || ex.Calls("act-b") != 1 || ex.Calls("act-c") != 0 {
		t.Fatalf("原始执行调用计数 a=%d b=%d c=%d", ex.Calls("act-a"), ex.Calls("act-b"), ex.Calls("act-c"))
	}

	res, err := eng.Resume(context.Background(), orig.ID, ResumeOptions{})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.Status != StatusSucceeded {
		t.Fatalf("续跑应成功: status=%s err=%v", res.Status, res.Err)
	}
	if res.ID == orig.ID || res.ResumedFrom != orig.ID || res.RootID != orig.ID {
		t.Fatalf("续跑链关联错误: id=%s from=%s root=%s", res.ID, res.ResumedFrom, res.RootID)
	}
	if res.PolicySource != PolicyInherited {
		t.Fatalf("默认应沿用原策略: %s", res.PolicySource)
	}
	// 已成功步骤复用旧产出，不重复触发副作用。
	if ex.Calls("act-a") != 1 {
		t.Fatalf("act-a 不得重跑, calls=%d", ex.Calls("act-a"))
	}
	recA := res.FindStep("s1", "a")
	if recA.Origin != StepOriginReused || recA.SourceExec != orig.ID {
		t.Fatalf("s1.a 应复用旧产出: origin=%s source=%s", recA.Origin, recA.SourceExec)
	}
	if recA.Output["echo.k"] != "v1" {
		t.Fatalf("复用产出应与原始执行一致: %v", recA.Output)
	}
	// 失败与未执行步骤被真正重跑。
	if ex.Calls("act-b") != 2 || ex.Calls("act-c") != 1 {
		t.Fatalf("act-b/act-c 应各重跑一次: b=%d c=%d", ex.Calls("act-b"), ex.Calls("act-c"))
	}
	for _, want := range []struct{ stage, step string }{{"s1", "b"}, {"s2", "c"}} {
		rec := res.FindStep(want.stage, want.step)
		if rec.Origin != StepOriginRerun || rec.Status != StepSucceeded {
			t.Fatalf("%s/%s 应重跑成功: origin=%s status=%s", want.stage, want.step, rec.Origin, rec.Status)
		}
	}
	t.Logf("判定依据: s1.a reused(source=%s 未重跑), s1.b/s2.c rerun; 续跑执行 %s 成功", recA.SourceExec, res.ID)
}

// TestConcurrentResumeConverges 同一失败执行被并发续跑时，只有一个续跑真正生效，
// 其余等待并拿到同一份结果；副作用不因并发而重复。
func TestConcurrentResumeConverges(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	ex.ScriptFailures("act-b", ErrScripted)
	orig, _ := eng.Submit(context.Background(), twoStageDef(), "k-resume-conc", failOncePolicy())
	if orig.Status != StatusFailed {
		t.Fatalf("原始执行应失败: %s", orig.Status)
	}

	const n = 8
	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := eng.Resume(context.Background(), orig.ID, ResumeOptions{})
			if res != nil {
				ids[i] = res.ID
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("resume %d: %v", i, errs[i])
		}
		if ids[i] == "" || ids[i] != ids[0] {
			t.Fatalf("并发续跑应收敛到同一结果: ids=%v", ids)
		}
	}
	res, err := eng.Get(ids[0])
	if err != nil || res.Status != StatusSucceeded {
		t.Fatalf("续跑结果应成功: status=%s err=%v", res.Status, err)
	}
	// 副作用不重复：act-a 仍只调 1 次，act-b 重跑恰好 1 次，act-c 恰好 1 次。
	if ex.Calls("act-a") != 1 || ex.Calls("act-b") != 2 || ex.Calls("act-c") != 1 {
		t.Fatalf("并发续跑副作用重复: a=%d b=%d c=%d", ex.Calls("act-a"), ex.Calls("act-b"), ex.Calls("act-c"))
	}
	// 链上只有两条记录：原始 + 一次续跑。
	chain, err := eng.ResumeChain(orig.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 2 {
		t.Fatalf("chain len=%d, want 2", len(chain))
	}
	t.Logf("判定依据: %d 路并发续跑收敛到执行 %s; act-a 未重跑, act-b/act-c 各补跑一次", n, ids[0])
}

// TestChainedResumesNoRepeatedSideEffects 连续多次续跑：
// 已成功的步骤在任何一次续跑中都不被重复执行；链上每条记录独立可查、
// 可一路关联回最初的执行；最初的执行记录保持只读不被改写。
func TestChainedResumesNoRepeatedSideEffects(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	ex.ScriptFailures("act-b", ErrScripted, ErrScripted) // 原始执行与第一次续跑都失败
	orig, _ := eng.Submit(context.Background(), twoStageDef(), "k-resume-chain", failOncePolicy())
	if orig.Status != StatusFailed {
		t.Fatalf("原始执行应失败: %s", orig.Status)
	}
	origJSON := mustJSON(t, orig)

	r1, err := eng.Resume(context.Background(), orig.ID, ResumeOptions{})
	if err == nil || r1.Status != StatusFailed {
		t.Fatalf("第一次续跑应仍失败: status=%s err=%v", r1.Status, err)
	}
	if r1.RootID != orig.ID || r1.ResumedFrom != orig.ID {
		t.Fatalf("r1 链关联错误: root=%s from=%s", r1.RootID, r1.ResumedFrom)
	}

	// 基于最新状态继续续跑（以上一次续跑的记录为来源）。
	r2, err := eng.Resume(context.Background(), r1.ID, ResumeOptions{})
	if err != nil {
		t.Fatalf("第二次续跑: %v", err)
	}
	if r2.Status != StatusSucceeded {
		t.Fatalf("第二次续跑应成功: %s", r2.Status)
	}
	if r2.RootID != orig.ID || r2.ResumedFrom != r1.ID {
		t.Fatalf("r2 链关联错误: root=%s from=%s", r2.RootID, r2.ResumedFrom)
	}

	// 副作用不重复：act-a 全程只执行一次；act-b 每次执行/续跑各一次共 3 次；act-c 只在最后成功那次执行。
	if ex.Calls("act-a") != 1 || ex.Calls("act-b") != 3 || ex.Calls("act-c") != 1 {
		t.Fatalf("副作用重复: a=%d b=%d c=%d", ex.Calls("act-a"), ex.Calls("act-b"), ex.Calls("act-c"))
	}
	// r2 中 act-a 的产出追溯到最初执行。
	if rec := r2.FindStep("s1", "a"); rec.Origin != StepOriginReused || rec.SourceExec != orig.ID {
		t.Fatalf("r2 s1.a 应复用最初产出: origin=%s source=%s", rec.Origin, rec.SourceExec)
	}

	// 链上三条独立记录，可一路关联回最初执行。
	chain, err := eng.ResumeChain(orig.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 3 {
		t.Fatalf("chain len=%d, want 3", len(chain))
	}
	for i, c := range chain {
		if c.RootID != orig.ID {
			t.Fatalf("chain[%d] root=%s, want %s", i, c.RootID, orig.ID)
		}
		t.Logf("chain[%d] exec=%s status=%s from=%s policy-source=%s", i, c.ID, c.Status, c.ResumedFrom, c.PolicySource)
	}
	if chain[1].ID != r1.ID || chain[2].ID != r2.ID {
		t.Fatalf("链顺序错误: %s %s %s", chain[0].ID, chain[1].ID, chain[2].ID)
	}

	// 最初的执行记录保持只读、不被改写。
	reloaded, err := eng.Get(orig.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustJSON(t, reloaded); got != origJSON {
		t.Fatalf("原始执行记录被改写:\nbefore=%s\nafter =%s", origJSON, got)
	}
	t.Logf("判定依据: 3 次执行 act-a 共调用 %d 次(=1 不重复); 原始记录 %s 内容未变", ex.Calls("act-a"), orig.ID)
}

// TestResumePolicyOverride 续跑默认沿用原执行策略快照（可复现）；
// 显式携带新策略时按新规则走，记录中可分辨用的是哪一套。
func TestResumePolicyOverride(t *testing.T) {
	eng, ex := newTestEngine(t, nil)
	ex.ScriptFailures("act-b", ErrScripted, ErrScripted, ErrScripted) // 原始/续跑各耗 1 次，覆盖续跑第 1 次尝试仍失败
	orig, _ := eng.Submit(context.Background(), twoStageDef(), "k-resume-policy", failOncePolicy())
	if orig.Status != StatusFailed {
		t.Fatalf("原始执行应失败: %s", orig.Status)
	}

	// 沿用原策略（MaxAttempts=1）续跑会再次失败——行为可复现。
	r1, err := eng.Resume(context.Background(), orig.ID, ResumeOptions{})
	if err == nil || r1.Status != StatusFailed {
		t.Fatalf("沿用原策略应复现失败: status=%s err=%v", r1.Status, err)
	}
	if r1.PolicySource != PolicyInherited || r1.Policy.Retry.MaxAttempts != 1 {
		t.Fatalf("r1 应沿用原策略: source=%s max=%d", r1.PolicySource, r1.Policy.Retry.MaxAttempts)
	}

	// 显式携带新策略（MaxAttempts=3）：act-b 第 3 次尝试成功。
	newPolicy := ExecPolicy{Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond, StepTimeout: 2 * time.Second}}
	r2, err := eng.Resume(context.Background(), r1.ID, ResumeOptions{Policy: &newPolicy})
	if err != nil {
		t.Fatalf("override resume: %v", err)
	}
	if r2.Status != StatusSucceeded {
		t.Fatalf("覆盖策略后续跑应成功: %s", r2.Status)
	}
	if r2.PolicySource != PolicyOverride || r2.Policy.Retry.MaxAttempts != 3 {
		t.Fatalf("r2 应记录覆盖策略: source=%s max=%d", r2.PolicySource, r2.Policy.Retry.MaxAttempts)
	}
	if rec := r2.FindStep("s1", "b"); rec.Attempts != 2 {
		t.Fatalf("act-b 在续跑中应尝试 2 次(第2次成功): %d", rec.Attempts)
	}
	if ex.Calls("act-b") != 4 {
		t.Fatalf("act-b 总调用应为 原始1+续跑1+覆盖续跑2=4, 实际 %d", ex.Calls("act-b"))
	}
	t.Logf("判定依据: r1 inherited(MaxAttempts=1) 复现失败; r2 override(MaxAttempts=3) 成功, act-b attempts=%d",
		r2.FindStep("s1", "b").Attempts)
}

// TestResumeRejectsInvalidTargets 已成功的执行无可续跑；不存在的执行报错。
func TestResumeRejectsInvalidTargets(t *testing.T) {
	eng, _ := newTestEngine(t, nil)
	ok, err := eng.Submit(context.Background(), twoStageDef(), "k-resume-ok", defaultPolicy())
	if err != nil || ok.Status != StatusSucceeded {
		t.Fatalf("submit: %v", err)
	}
	if _, err := eng.Resume(context.Background(), ok.ID, ResumeOptions{}); err == nil {
		t.Fatal("已成功执行应拒绝续跑")
	} else {
		t.Logf("判定依据: 已成功执行续跑被拒: %v", err)
	}
	if _, err := eng.Resume(context.Background(), "no-such-exec", ResumeOptions{}); err == nil {
		t.Fatal("不存在的执行应报错")
	} else {
		t.Logf("判定依据: 不存在执行续跑报错: %v", err)
	}
}
