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

	// 4) 断点续跑：一条执行失败后，显式 Resume 只处理未成功的步骤。
	resumeDef := workflow.Definition{
		Name: "resume-demo",
		Stages: []workflow.Stage{
			{Name: "prepare", Steps: []workflow.Step{
				{Name: "p1", Action: "prep", Input: map[string]string{"id": "1"}},
				{Name: "p2", Action: "prep", Input: map[string]string{"id": "2"}},
			}},
			{Name: "finish", Steps: []workflow.Step{
				{Name: "f1", Action: "finish"},
			}},
		},
	}
	resumePolicy := workflow.ExecPolicy{Retry: workflow.RetryPolicy{
		MaxAttempts: 1, Backoff: 10 * time.Millisecond, StepTimeout: 2 * time.Second,
	}}
	ex.ScriptFailures("finish", workflow.ErrScripted) // finish 首次失败
	failed, err := eng.Submit(context.Background(), resumeDef, "run-3", resumePolicy)
	fmt.Printf("== run-3: status=%s err=%v\n", failed.Status, err)
	prepBefore := ex.Calls("prep")
	finishBefore := ex.Calls("finish")

	// 续跑：默认沿用原策略；已成功的 prepare 直接复用，finish 真正重跑。
	rec, rerr := eng.Resume(context.Background(), failed.ID, workflow.ResumeOption{})
	if rerr != nil {
		log.Fatal(rerr)
	}
	for _, s := range rec.Steps {
		fmt.Printf("== resume#%d step=%s/%s outcome=%s status=%s\n", rec.Attempt, s.Stage, s.Step, s.Outcome, s.Status)
	}
	fmt.Printf("== resume: status=%s reused-prep-side-effects=%d finish-side-effects=%d\n",
		rec.Status, ex.Calls("prep")-prepBefore, ex.Calls("finish")-finishBefore)

	// 追溯：原始执行只读，续跑链可一路关联回最初执行。
	chain, err := eng.ResumeHistory(failed.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("== audit: root=%s(root-status=%s, untouched) resumes=%d head=%s inherited-policy=%v\n",
		chain.Root.ID, chain.Root.Status, len(chain.Resumes), chain.HeadID, rec.PolicyInherited)
}
