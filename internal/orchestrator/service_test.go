package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/orchestrator"
)

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		c.Advance(d)
		return nil
	}
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func TestServiceClaimExecutionSerializesConcurrentClaims(t *testing.T) {
	ctx := context.Background()
	clock := newManualClock()
	svc := orchestrator.New(4, orchestrator.WithClock(clock))
	t.Cleanup(svc.Close)

	loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: "run-concurrent"})
	if err != nil {
		t.Fatalf("load run: %v", err)
	}

	const workers = 64
	start := make(chan struct{})
	errCh := make(chan error, workers)
	var claimed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			claim, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, fmt.Sprintf("worker-%d", i), clock.Now().Add(time.Minute))
			if err != nil {
				errCh <- err
				return
			}

			if claim.Claimed {
				claimed.Add(1)
			}
		}(i)
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("claim execution: %v", err)
	}

	if got := claimed.Load(); got != 1 {
		t.Fatalf("claimed executions: got %d, want 1", got)
	}

	duplicate, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, "late-worker", clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("duplicate claim: %v", err)
	}

	if duplicate.Claimed {
		t.Fatalf("active lease should fence duplicate claim: %+v", duplicate)
	}
}

func TestServiceClaimExecutionRejectsLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	clock := newManualClock()
	svc := orchestrator.New(2, orchestrator.WithClock(clock))
	t.Cleanup(svc.Close)

	loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: "run-takeover"})
	if err != nil {
		t.Fatalf("load run: %v", err)
	}

	first, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-a", clock.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("claim first: %v", err)
	}

	if !first.Claimed || first.ClaimToken == "" {
		t.Fatalf("expected first claim: %+v", first)
	}

	clock.Advance(2 * time.Second)
	second, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-b", clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim second: %v", err)
	}

	if second.Claimed || second.ClaimToken != "" {
		t.Fatalf("expected expired lease takeover to be rejected: first=%+v second=%+v", first, second)
	}

	if _, err := svc.CompleteExecutionByClaim(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-a", first.ClaimToken, dal.ExecutionStatusSucceeded, "", ""); !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("expired completion error: got %v, want conflict", err)
	}
}

func TestServiceListPendingExcludesExpiredRunningClaims(t *testing.T) {
	ctx := context.Background()
	clock := newManualClock()
	svc := orchestrator.New(2, orchestrator.WithClock(clock))
	t.Cleanup(svc.Close)

	loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: "run-expired-pending"})
	if err != nil {
		t.Fatalf("load run: %v", err)
	}

	claim, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-a", clock.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("claim root: %v", err)
	}

	if !claim.Claimed {
		t.Fatalf("expected claim: %+v", claim)
	}

	pending, err := svc.ListPending(ctx, loaded.RunID, 10)
	if err != nil {
		t.Fatalf("list active pending: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("active claim should be hidden from pending list: %+v", pending)
	}

	clock.Advance(2 * time.Second)
	pending, err = svc.ListPending(ctx, loaded.RunID, 10)
	if err != nil {
		t.Fatalf("list expired pending: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("expired claim should not be listed for takeover: %+v", pending)
	}
}

func TestServiceLoadRunUsesProvidedRootExecutionRecord(t *testing.T) {
	ctx := context.Background()
	clock := newManualClock()
	svc := orchestrator.New(2, orchestrator.WithClock(clock))
	t.Cleanup(svc.Close)

	loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{
		RunID: "run-custom-root",
		Root: dal.TaskExecutionRecord{
			RunID:         "run-custom-root",
			TaskID:        "custom-root-task",
			TaskKey:       dal.RootTaskKey,
			Name:          dal.RootTaskKey,
			TaskAttemptID: "custom-root-attempt",
			SegmentID:     "custom-root-segment",
			ExecutionID:   "custom-root-execution",
			CellID:        "iad-a",
			Attempt:       1,
		},
	})

	if err != nil {
		t.Fatalf("load run: %v", err)
	}

	if loaded.Root.ExecutionID != "custom-root-execution" {
		t.Fatalf("root execution id: got %q", loaded.Root.ExecutionID)
	}

	claim, err := svc.ClaimExecution(ctx, loaded.RunID, "custom-root-execution", "worker-a", clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim custom root: %v", err)
	}

	if !claim.Claimed {
		t.Fatalf("expected custom root claim: %+v", claim)
	}
}

func TestServiceLoadRunIsIdempotent(t *testing.T) {
	ctx := context.Background()
	clock := newManualClock()
	svc := orchestrator.New(2, orchestrator.WithClock(clock))
	t.Cleanup(svc.Close)

	spec := orchestrator.RunSpec{
		RunID: "run-idempotent-load",
		Tasks: []orchestrator.TaskSpec{{
			TaskKey:       "build",
			ParentTaskKey: dal.RootTaskKey,
			Name:          "build",
			CellID:        "iad-a",
		}},
	}

	loaded, err := svc.LoadRun(ctx, spec)
	if err != nil {
		t.Fatalf("load run: %v", err)
	}

	rootClaim, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-root", clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim root: %v", err)
	}

	result, err := svc.CompleteExecutionByClaim(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-root", rootClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("complete root: %v", err)
	}

	if len(result.Children) != 1 {
		t.Fatalf("expected activated child: %+v", result)
	}

	again, err := svc.LoadRun(ctx, spec)
	if err != nil {
		t.Fatalf("idempotent load run: %v", err)
	}

	if again.Root.ExecutionID != loaded.Root.ExecutionID {
		t.Fatalf("root changed across idempotent load: first=%q second=%q", loaded.Root.ExecutionID, again.Root.ExecutionID)
	}

	if len(again.Pending) != 1 || again.Pending[0].ExecutionID != result.Children[0].ExecutionID {
		t.Fatalf("idempotent load should return current pending child, got %+v want %s", again.Pending, result.Children[0].ExecutionID)
	}
}

func TestServiceLoadRunHydratesExecutionSnapshots(t *testing.T) {
	ctx := context.Background()
	clock := newManualClock()
	svc := orchestrator.New(2, orchestrator.WithClock(clock))
	t.Cleanup(svc.Close)

	runID := "run-hydrated-snapshots"
	root := orchestratorTestRecord(runID, dal.RootTaskKey, "", dal.RootTaskKey, "iad-a")
	child := orchestratorTestRecord(runID, "build", dal.RootTaskKey, "build", "iad-a")

	loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{
		RunID: runID,
		Tasks: []orchestrator.TaskSpec{{
			TaskKey:       "build",
			ParentTaskKey: dal.RootTaskKey,
			Name:          "build",
			CellID:        "iad-a",
		}},
		Executions: []orchestrator.TaskExecutionSnapshot{
			{Record: root, Status: dal.ExecutionStatusSucceeded},
			{Record: child, Status: dal.ExecutionStatusPending},
		},
	})

	if err != nil {
		t.Fatalf("load hydrated run: %v", err)
	}

	if loaded.Summary.Total != 2 || loaded.Summary.Succeeded != 1 || loaded.Summary.Incomplete != 1 {
		t.Fatalf("hydrated summary: %+v", loaded.Summary)
	}

	claim, err := svc.ClaimExecution(ctx, loaded.RunID, child.ExecutionID, "worker-child", clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim hydrated child: %v", err)
	}
	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected hydrated child claim: %+v", claim)
	}

	result, err := svc.CompleteExecutionByClaim(ctx, loaded.RunID, child.ExecutionID, "worker-child", claim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("complete hydrated child: %v", err)
	}
	if result.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		t.Fatalf("hydrated finalization outcome: %+v", result)
	}
}

func TestServiceLoadRunRejectsSnapshotIdentityDrift(t *testing.T) {
	ctx := context.Background()
	runID := "run-snapshot-drift"
	root := orchestratorTestRecord(runID, dal.RootTaskKey, "", dal.RootTaskKey, "iad-a")
	build := orchestratorTestRecord(runID, "build", dal.RootTaskKey, "build", "iad-a")

	tests := []struct {
		name   string
		record dal.TaskExecutionRecord
		want   string
	}{
		{
			name: "run id mismatch",
			record: func() dal.TaskExecutionRecord {
				rec := build
				rec.RunID = "run-other"
				return rec
			}(),
			want: "run_id",
		},
		{
			name: "task id mismatch",
			record: func() dal.TaskExecutionRecord {
				rec := build
				rec.TaskID = root.TaskID
				return rec
			}(),
			want: "task_id",
		},
		{
			name: "task attempt mismatch",
			record: func() dal.TaskExecutionRecord {
				rec := build
				rec.TaskAttemptID = root.TaskAttemptID
				return rec
			}(),
			want: "task_attempt_id",
		},
		{
			name: "execution id collision",
			record: func() dal.TaskExecutionRecord {
				rec := build
				rec.ExecutionID = root.ExecutionID
				return rec
			}(),
			want: "execution_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newManualClock()
			svc := orchestrator.New(2, orchestrator.WithClock(clock))
			t.Cleanup(svc.Close)

			_, err := svc.LoadRun(ctx, orchestrator.RunSpec{
				RunID:  runID,
				CellID: "iad-a",
				Tasks: []orchestrator.TaskSpec{{
					TaskKey:       "build",
					ParentTaskKey: dal.RootTaskKey,
					Name:          "build",
					CellID:        "iad-a",
				}},
				Executions: []orchestrator.TaskExecutionSnapshot{{
					Record: tt.record,
					Status: dal.ExecutionStatusRunning,
				}},
			})

			if !errors.Is(err, dal.ErrConflict) {
				t.Fatalf("LoadRun error = %v, want conflict", err)
			}

			if err != nil && !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadRun error = %q, want field %q", err.Error(), tt.want)
			}
		})
	}
}

func TestServiceLoadRunHydratesRootRetrySnapshot(t *testing.T) {
	ctx := context.Background()
	clock := newManualClock()
	svc := orchestrator.New(2, orchestrator.WithClock(clock))
	t.Cleanup(svc.Close)

	runID := "run-root-retry-snapshot"
	root := orchestratorTestRecord(runID, dal.RootTaskKey, "", dal.RootTaskKey, "iad-a")
	root.TaskAttemptID = runID + ":root:attempt:2"
	root.SegmentID = "segment-root-retry"
	root.ExecutionID = "execution-root-retry"
	root.Attempt = 2

	loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{
		RunID:  runID,
		CellID: "iad-a",
		Tasks: []orchestrator.TaskSpec{{
			TaskKey:       "build",
			ParentTaskKey: dal.RootTaskKey,
			Name:          "build",
			CellID:        "iad-a",
		}},
		Executions: []orchestrator.TaskExecutionSnapshot{{
			Record: root,
			Status: dal.ExecutionStatusSucceeded,
		}},
	})
	if err != nil {
		t.Fatalf("load run: %v", err)
	}

	if loaded.Root.ExecutionID != root.ExecutionID ||
		loaded.Root.SegmentID != root.SegmentID ||
		loaded.Root.TaskAttemptID != root.TaskAttemptID ||
		loaded.Root.Attempt != root.Attempt {
		t.Fatalf("root snapshot was not hydrated: got %+v want %+v", loaded.Root, root)
	}
}

func TestServiceCompleteExecutionActivatesChildrenInMemory(t *testing.T) {
	ctx := context.Background()
	clock := newManualClock()
	svc := orchestrator.New(2, orchestrator.WithClock(clock))
	t.Cleanup(svc.Close)

	loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{
		RunID: "run-children",
		Tasks: []orchestrator.TaskSpec{{
			TaskKey:       "build",
			ParentTaskKey: dal.RootTaskKey,
			Name:          "build",
			CellID:        "iad-a",
		}},
	})

	if err != nil {
		t.Fatalf("load run: %v", err)
	}

	rootClaim, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-root", clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim root: %v", err)
	}

	rootResult, err := svc.CompleteExecutionByClaim(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-root", rootClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("complete root: %v", err)
	}

	if rootResult.Outcome != dal.ExecutionFinalizationOutcomeContinued || rootResult.Activated != 1 || len(rootResult.Children) != 1 {
		t.Fatalf("root result: %+v", rootResult)
	}

	if rootResult.Summary.Total != 2 || rootResult.Summary.Succeeded != 1 || rootResult.Summary.Incomplete != 1 {
		t.Fatalf("root summary: %+v", rootResult.Summary)
	}

	child := rootResult.Children[0]
	childClaim, err := svc.ClaimExecution(ctx, loaded.RunID, child.ExecutionID, "worker-child", clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim child: %v", err)
	}

	childResult, err := svc.CompleteExecutionByClaim(ctx, loaded.RunID, child.ExecutionID, "worker-child", childClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("complete child: %v", err)
	}

	if childResult.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		t.Fatalf("child result: %+v", childResult)
	}

	if childResult.Summary.Total != 2 || childResult.Summary.Succeeded != 2 || childResult.Summary.Incomplete != 0 {
		t.Fatalf("child summary: %+v", childResult.Summary)
	}
}

func TestServiceCompleteExecutionRejectsDuplicateAfterActivatingChildren(t *testing.T) {
	ctx := context.Background()
	clock := newManualClock()
	svc := orchestrator.New(2, orchestrator.WithClock(clock))
	t.Cleanup(svc.Close)

	loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{
		RunID: "run-duplicate-completion",
		Tasks: []orchestrator.TaskSpec{{
			TaskKey:       "build",
			ParentTaskKey: dal.RootTaskKey,
			Name:          "build",
			CellID:        "iad-a",
		}},
	})

	if err != nil {
		t.Fatalf("load run: %v", err)
	}

	rootClaim, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-root", clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim root: %v", err)
	}

	rootResult, err := svc.CompleteExecutionByClaim(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-root", rootClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("complete root: %v", err)
	}

	if rootResult.Outcome != dal.ExecutionFinalizationOutcomeContinued || rootResult.Activated != 1 || len(rootResult.Children) != 1 {
		t.Fatalf("root result: %+v", rootResult)
	}

	if _, err := svc.CompleteExecutionByClaim(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker-root", rootClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", ""); !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("expected duplicate completion conflict, got %v", err)
	}

	pending, err := svc.ListPending(ctx, loaded.RunID, 10)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}

	if len(pending) != 1 || pending[0].ExecutionID != rootResult.Children[0].ExecutionID {
		t.Fatalf("pending children after duplicate completion = %+v, want only %+v", pending, rootResult.Children[0])
	}
}

func orchestratorTestRecord(runID, taskKey, parentTaskKey, name, cellID string) dal.TaskExecutionRecord {
	taskID := runID + ":" + taskKey
	parentTaskID := ""
	if parentTaskKey != "" {
		parentTaskID = runID + ":" + parentTaskKey
	}

	return dal.TaskExecutionRecord{
		RunID:         runID,
		TaskID:        taskID,
		ParentTaskID:  parentTaskID,
		TaskKey:       taskKey,
		Name:          name,
		TaskAttemptID: taskID + ":attempt:1",
		SegmentID:     taskID + ":segment",
		SegmentName:   name,
		ExecutionID:   taskID + ":attempt:1:execution",
		CellID:        cellID,
		Attempt:       1,
	}
}
