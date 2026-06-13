package orchestrator_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/orchestrator"
)

type benchmarkDispatch struct {
	runID       string
	executionID string
}

type benchmarkFanoutDispatch struct {
	runID string
	root  string
}

func BenchmarkService_ClaimCompleteLeaf(b *testing.B) {
	benchmarkServiceClaimCompleteLeaf(b, runtime.GOMAXPROCS(0), runtime.GOMAXPROCS(0))
}

func BenchmarkService_ClaimCompleteLeafSingleShard(b *testing.B) {
	benchmarkServiceClaimCompleteLeaf(b, 1, runtime.GOMAXPROCS(0))
}

func BenchmarkService_ClaimCompleteRootChild(b *testing.B) {
	benchmarkServiceClaimCompleteRootChild(b, runtime.GOMAXPROCS(0), runtime.GOMAXPROCS(0))
}

func BenchmarkService_LoadClaimCompleteFanoutWidth(b *testing.B) {
	for _, width := range []int{1, 10, 100, 1000, 5000} {
		b.Run(fmt.Sprintf("children_%05d", width), func(b *testing.B) {
			benchmarkServiceLoadClaimCompleteFanoutWidth(b, width)
		})
	}
}

func benchmarkServiceClaimCompleteLeaf(b *testing.B, shardCount, workerCount int) {
	ctx := context.Background()
	svc := orchestrator.New(shardCount)
	b.Cleanup(svc.Close)

	dispatches := make(chan benchmarkDispatch, b.N)
	for i := 0; i < b.N; i++ {
		runID := fmt.Sprintf("bench-leaf-%d", i)
		loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: runID})
		if err != nil {
			b.Fatalf("load run %s: %v", runID, err)
		}

		dispatches <- benchmarkDispatch{runID: runID, executionID: loaded.Root.ExecutionID}
	}
	close(dispatches)

	leaseUntil := time.Now().Add(time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	errCh := make(chan error, workerCount)
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		workerID := fmt.Sprintf("bench-worker-%d", worker)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dispatch := range dispatches {
				claim, err := svc.ClaimExecution(ctx, dispatch.runID, dispatch.executionID, workerID, leaseUntil)
				if err != nil {
					errCh <- err
					return
				}

				if !claim.Claimed {
					errCh <- fmt.Errorf("execution %s was not claimed", dispatch.executionID)
					return
				}

				result, err := svc.CompleteExecutionByClaim(ctx, dispatch.runID, dispatch.executionID, workerID, claim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
				if err != nil {
					errCh <- err
					return
				}

				if result.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
					errCh <- fmt.Errorf("execution %s outcome %q", dispatch.executionID, result.Outcome)
					return
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	close(errCh)
	for err := range errCh {
		b.Fatal(err)
	}

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "terminal_runs/s")
	}

	b.ReportMetric(float64(shardCount), "orchestrator_shards")
	b.ReportMetric(float64(workerCount), "worker_count")
}

func benchmarkServiceLoadClaimCompleteFanoutWidth(b *testing.B, width int) {
	if width <= 0 {
		b.Fatal("fanout width must be positive")
	}

	ctx := context.Background()
	tasks := benchmarkFanoutTasks(width)
	leaseUntil := time.Now().Add(time.Hour)
	shardCount := runtime.GOMAXPROCS(0)
	runsPerService := max(1, min(128, 20000/width))

	b.ReportAllocs()
	b.ReportMetric(float64(width), "fanout_width")
	b.ReportMetric(float64(runsPerService), "runs_per_service")
	b.ResetTimer()
	start := time.Now()

	activated := 0
	for i := 0; i < b.N; {
		svc := orchestrator.New(shardCount)
		batch := min(runsPerService, b.N-i)
		for j := 0; j < batch; j++ {
			runIndex := i + j
			runID := fmt.Sprintf("bench-fanout-width-%d-%d", width, runIndex)
			loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{
				RunID: runID,
				Tasks: tasks,
			})

			if err != nil {
				svc.Close()
				b.Fatalf("load run %s: %v", runID, err)
			}

			claim, err := svc.ClaimExecution(ctx, runID, loaded.Root.ExecutionID, "bench-worker", leaseUntil)
			if err != nil {
				svc.Close()
				b.Fatalf("claim root %s: %v", runID, err)
			}

			if !claim.Claimed {
				svc.Close()
				b.Fatalf("root execution %s was not claimed", loaded.Root.ExecutionID)
			}

			result, err := svc.CompleteExecutionByClaim(ctx, runID, loaded.Root.ExecutionID, "bench-worker", claim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
			if err != nil {
				svc.Close()
				b.Fatalf("complete root %s: %v", runID, err)
			}

			if result.Outcome != dal.ExecutionFinalizationOutcomeContinued || result.Activated != width || len(result.Children) != width {
				svc.Close()
				b.Fatalf("root result for width %d: outcome=%q activated=%d children=%d", width, result.Outcome, result.Activated, len(result.Children))
			}

			activated += result.Activated
		}

		svc.Close()
		i += batch
	}

	elapsed := time.Since(start)
	b.StopTimer()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "parent_completions/s")
		b.ReportMetric(float64(activated)/elapsed.Seconds(), "activated_tasks/s")
	}

	b.ReportMetric(float64(shardCount), "orchestrator_shards")
}

func benchmarkFanoutTasks(width int) []orchestrator.TaskSpec {
	tasks := make([]orchestrator.TaskSpec, 0, width)
	for i := 0; i < width; i++ {
		key := fmt.Sprintf("child-%05d", i)
		tasks = append(tasks, orchestrator.TaskSpec{
			TaskKey:       key,
			ParentTaskKey: dal.RootTaskKey,
			Name:          key,
		})
	}

	return tasks
}

func benchmarkServiceClaimCompleteRootChild(b *testing.B, shardCount, workerCount int) {
	ctx := context.Background()
	svc := orchestrator.New(shardCount)
	b.Cleanup(svc.Close)

	dispatches := make(chan benchmarkFanoutDispatch, b.N)
	for i := 0; i < b.N; i++ {
		runID := fmt.Sprintf("bench-fanout-%d", i)
		loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{
			RunID: runID,
			Tasks: []orchestrator.TaskSpec{{
				TaskKey:       "child",
				ParentTaskKey: dal.RootTaskKey,
			}},
		})

		if err != nil {
			b.Fatalf("load run %s: %v", runID, err)
		}

		dispatches <- benchmarkFanoutDispatch{runID: runID, root: loaded.Root.ExecutionID}
	}
	close(dispatches)

	leaseUntil := time.Now().Add(time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	errCh := make(chan error, workerCount)
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		workerID := fmt.Sprintf("bench-worker-%d", worker)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dispatch := range dispatches {
				rootClaim, err := svc.ClaimExecution(ctx, dispatch.runID, dispatch.root, workerID, leaseUntil)
				if err != nil {
					errCh <- err
					return
				}

				if !rootClaim.Claimed {
					errCh <- fmt.Errorf("root execution %s was not claimed", dispatch.root)
					return
				}

				rootResult, err := svc.CompleteExecutionByClaim(ctx, dispatch.runID, dispatch.root, workerID, rootClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
				if err != nil {
					errCh <- err
					return
				}

				if rootResult.Outcome != dal.ExecutionFinalizationOutcomeContinued || len(rootResult.Children) != 1 {
					errCh <- fmt.Errorf("root execution %s result %+v", dispatch.root, rootResult)
					return
				}

				child := rootResult.Children[0]
				childClaim, err := svc.ClaimExecution(ctx, dispatch.runID, child.ExecutionID, workerID, leaseUntil)
				if err != nil {
					errCh <- err
					return
				}

				if !childClaim.Claimed {
					errCh <- fmt.Errorf("child execution %s was not claimed", child.ExecutionID)
					return
				}

				childResult, err := svc.CompleteExecutionByClaim(ctx, dispatch.runID, child.ExecutionID, workerID, childClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
				if err != nil {
					errCh <- err
					return
				}

				if childResult.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
					errCh <- fmt.Errorf("child execution %s outcome %q", child.ExecutionID, childResult.Outcome)
					return
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	close(errCh)
	for err := range errCh {
		b.Fatal(err)
	}

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "terminal_runs/s")
		b.ReportMetric(float64(2*b.N)/elapsed.Seconds(), "terminal_executions/s")
	}
	b.ReportMetric(float64(shardCount), "orchestrator_shards")
	b.ReportMetric(float64(workerCount), "worker_count")
}
