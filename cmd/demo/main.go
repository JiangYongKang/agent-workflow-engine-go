// demo 本地验证工作流引擎：顺序+并发编排、重试、幂等去重、崩溃恢复。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/highcumontoa/agent-workflow-engine-go/workflow"
)

func main() {
	dir, err := os.MkdirTemp("", "wf-demo-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	store, err := workflow.NewFileStore(dir)
	if err != nil {
		log.Fatal(err)
	}
	ex := workflow.NewLocalExecutor()
	eng := workflow.NewEngine(ex, store)
	eng.SetLogger(func(format string, args ...any) { log.Printf("[flow] "+format, args...) })

	def := workflow.Definition{
		Name: "nightly-report",
		Stages: []workflow.Stage{
			{Name: "fetch", Steps: []workflow.Step{
				{Name: "fetch-a", Action: "fetch", Input: map[string]string{"src": "a"}},
				{Name: "fetch-b", Action: "fetch", Input: map[string]string{"src": "b"}},
			}},
			{Name: "aggregate", Steps: []workflow.Step{
				{Name: "agg", Action: "aggregate"},
			}},
		},
	}
	policy := workflow.ExecPolicy{Retry: workflow.RetryPolicy{
		MaxAttempts: 3, Backoff: 50 * time.Millisecond, StepTimeout: 2 * time.Second,
	}}

	// 1) fetch 动作前两次失败，验证重试后成功。
	ex.ScriptFailures("fetch", workflow.ErrScripted, workflow.ErrScripted)
	exec, err := eng.Submit(context.Background(), def, "run-1", policy)
	fmt.Printf("== run-1: status=%s err=%v\n", exec.Status, err)

	// 2) 相同幂等键重复提交：返回既有结果，副作用不重复。
	callsBefore := ex.Calls("fetch")
	dup, err := eng.Submit(context.Background(), def, "run-1", policy)
	fmt.Printf("== duplicate submit: same-id=%v new-side-effects=%d err=%v\n",
		dup.ID == exec.ID, ex.Calls("fetch")-callsBefore, err)

	// 3) 模拟崩溃恢复：伪造一个中断的执行，由新引擎恢复。
	crashed := &workflow.Execution{
		ID: "exec-crashed", IdemKey: "run-2", Definition: def, Policy: policy,
		Status: workflow.StatusRunning, StartedAt: time.Now(),
		Steps: []*workflow.StepRecord{
			{Stage: "fetch", Step: "fetch-a", Status: workflow.StepSucceeded, Attempts: 1, Output: map[string]string{"echo.src": "a"}},
			{Stage: "fetch", Step: "fetch-b", Status: workflow.StepPending},
			{Stage: "aggregate", Step: "agg", Status: workflow.StepPending},
		},
	}
	if err := store.Save(crashed); err != nil {
		log.Fatal(err)
	}
	eng2 := workflow.NewEngine(ex, store)
	eng2.SetLogger(func(format string, args ...any) { log.Printf("[flow] "+format, args...) })
	fetchBefore := ex.Calls("fetch")
	recovered, err := eng2.Recover(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range recovered {
		// fetch 动作仅补跑 fetch-b 一次；已成功的 fetch-a 不重复产生副作用。
		fmt.Printf("== recovered %s: status=%s fetch-calls-during-recover=%d (expect 1, fetch-b only)\n",
			r.ID, r.Status, ex.Calls("fetch")-fetchBefore)
	}

	// 4) 断点续跑：aggregate 失败导致执行失败，续跑只重跑未完成步骤。
	once := workflow.ExecPolicy{Retry: workflow.RetryPolicy{
		MaxAttempts: 1, Backoff: 50 * time.Millisecond, StepTimeout: 2 * time.Second,
	}}
	ex.ScriptFailures("aggregate", workflow.ErrScripted) // 仅第一次 aggregate 失败
	failed, err := eng.Submit(context.Background(), def, "run-3", once)
	fmt.Printf("== run-3: status=%s err=%v\n", failed.Status, err)

	fetchBefore, aggBefore := ex.Calls("fetch"), ex.Calls("aggregate")
	resumed, err := eng.Resume(context.Background(), failed.ID, workflow.ResumeOptions{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("== resume of run-3: exec=%s status=%s policy-source=%s fetch-rerun=%d agg-rerun=%d\n",
		resumed.ID, resumed.Status, resumed.PolicySource,
		ex.Calls("fetch")-fetchBefore, ex.Calls("aggregate")-aggBefore)
	for _, rec := range resumed.Steps {
		fmt.Printf("   step=%s/%s origin=%s source=%s status=%s\n",
			rec.Stage, rec.Step, rec.Origin, rec.SourceExec, rec.Status)
	}
	chain, err := eng.ResumeChain(failed.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("== resume chain of run-3: %d records\n", len(chain))
	for i, c := range chain {
		fmt.Printf("   [%d] exec=%s status=%s from=%s policy-source=%s\n",
			i, c.ID, c.Status, c.ResumedFrom, c.PolicySource)
	}
}
