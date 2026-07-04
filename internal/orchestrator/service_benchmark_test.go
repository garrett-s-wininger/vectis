package orchestrator_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/orchestrator"

	"google.golang.org/grpc"
)

type benchmarkDispatch struct {
	runID       string
	executionID string
}

type benchmarkFanoutDispatch struct {
	runID string
	root  string
}

type benchmarkClaimedDispatch struct {
	runID       string
	executionID string
	claimToken  string
}

func BenchmarkService_ClaimCompleteLeaf(b *testing.B) {
	benchmarkServiceClaimCompleteLeaf(b, runtime.GOMAXPROCS(0), runtime.GOMAXPROCS(0))
}

func BenchmarkService_ClaimCompleteLeafSingleShard(b *testing.B) {
	benchmarkServiceClaimCompleteLeaf(b, 1, runtime.GOMAXPROCS(0))
}

func BenchmarkGRPCService_ClaimCompleteLeafRuntime(b *testing.B) {
	benchmarkGRPCServiceClaimCompleteLeaf(b, runtime.GOMAXPROCS(0), runtime.GOMAXPROCS(0))
}

func BenchmarkGRPCStreamService_ClaimCompleteLeafRuntime(b *testing.B) {
	benchmarkGRPCStreamServiceClaimCompleteLeaf(b, runtime.GOMAXPROCS(0), runtime.GOMAXPROCS(0))
}

func BenchmarkGRPCService_ClaimSleepCompleteLeafRuntime(b *testing.B) {
	for _, workDuration := range []time.Duration{
		100 * time.Microsecond,
		time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
	} {
		b.Run(benchmarkWorkDurationName(workDuration), func(b *testing.B) {
			benchmarkGRPCServiceClaimSleepCompleteLeaf(b, runtime.GOMAXPROCS(0), runtime.GOMAXPROCS(0), workDuration)
		})
	}
}

func BenchmarkProtoService_ClaimCompleteLeafRuntime(b *testing.B) {
	benchmarkProtoServiceClaimCompleteLeaf(b, runtime.GOMAXPROCS(0), runtime.GOMAXPROCS(0))
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

func BenchmarkService_CompleteTerminalSnapshot(b *testing.B) {
	for _, totalTasks := range []int{1000, 5000, 10000, 25000} {
		b.Run(fmt.Sprintf("total_tasks_%05d", totalTasks), func(b *testing.B) {
			benchmarkServiceCompleteTerminalSnapshot(b, totalTasks)
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

func benchmarkGRPCServiceClaimCompleteLeaf(b *testing.B, shardCount, workerCount int) {
	benchmarkGRPCServiceClaimSleepCompleteLeaf(b, shardCount, workerCount, 0)
}

func benchmarkGRPCServiceClaimSleepCompleteLeaf(b *testing.B, shardCount, workerCount int, workDuration time.Duration) {
	ctx := context.Background()
	svc := orchestrator.New(shardCount)
	b.Cleanup(svc.Close)

	client, cleanup := newOrchestratorTestClient(b, svc)
	defer cleanup()

	dispatches := make(chan benchmarkDispatch, b.N)
	for i := 0; i < b.N; i++ {
		runID := fmt.Sprintf("bench-grpc-leaf-%s-%d", benchmarkWorkDurationName(workDuration), i)
		loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: runID})
		if err != nil {
			b.Fatalf("load run %s: %v", runID, err)
		}

		dispatches <- benchmarkDispatch{runID: runID, executionID: loaded.Root.ExecutionID}
	}
	close(dispatches)

	leaseUntil := time.Now().Add(time.Hour).UnixNano()
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
				claim, err := client.ClaimExecution(ctx, &api.ClaimExecutionRequest{
					RunId:              testString(dispatch.runID),
					ExecutionId:        testString(dispatch.executionID),
					Owner:              testString(workerID),
					LeaseUntilUnixNano: testInt64(leaseUntil),
				})

				if err != nil {
					errCh <- err
					return
				}

				if !claim.GetClaimed() {
					errCh <- fmt.Errorf("execution %s was not claimed", dispatch.executionID)
					return
				}

				if workDuration > 0 {
					time.Sleep(workDuration)
				}

				result, err := client.CompleteExecution(ctx, &api.CompleteExecutionRequest{
					RunId:       testString(dispatch.runID),
					ExecutionId: testString(dispatch.executionID),
					Owner:       testString(workerID),
					ClaimToken:  testString(claim.GetClaimToken()),
					Status:      testString(dal.ExecutionStatusSucceeded),
				})

				if err != nil {
					errCh <- err
					return
				}

				if result.GetOutcome() != string(dal.ExecutionFinalizationOutcomeRunSucceeded) {
					errCh <- fmt.Errorf("execution %s outcome %q", dispatch.executionID, result.GetOutcome())
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
	b.ReportMetric(float64(workDuration)/float64(time.Millisecond), "work_ms")
	b.ReportMetric(float64(workerCount), "worker_count")
	if workDuration > 0 {
		idealElapsed := time.Duration((b.N+workerCount-1)/workerCount) * workDuration
		if idealElapsed > 0 {
			b.ReportMetric(float64(idealElapsed)/float64(elapsed)*100, "ideal_wall_pct")
		}

		if excess := elapsed - idealElapsed; excess > 0 {
			b.ReportMetric(float64(excess)/float64(b.N)/float64(time.Microsecond), "excess_wall_us/op")
		}
	}
}

func benchmarkGRPCStreamServiceClaimCompleteLeaf(b *testing.B, shardCount, workerCount int) {
	ctx := context.Background()
	svc := orchestrator.New(shardCount)
	b.Cleanup(svc.Close)

	client, cleanup := newOrchestratorTestClient(b, svc)
	defer cleanup()

	streams := make([]api.OrchestratorService_ExecutionStreamClient, workerCount)
	for worker := 0; worker < workerCount; worker++ {
		stream, err := client.ExecutionStream(ctx)
		if err != nil {
			b.Fatalf("open benchmark stream %d: %v", worker, err)
		}

		streams[worker] = stream
	}

	defer func() {
		for _, stream := range streams {
			if stream != nil {
				_ = stream.CloseSend()
			}
		}
	}()

	dispatches := make(chan benchmarkDispatch, b.N)
	for i := 0; i < b.N; i++ {
		runID := fmt.Sprintf("bench-grpc-stream-leaf-%d", i)
		loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: runID})
		if err != nil {
			b.Fatalf("load run %s: %v", runID, err)
		}

		dispatches <- benchmarkDispatch{runID: runID, executionID: loaded.Root.ExecutionID}
	}
	close(dispatches)

	leaseUntil := time.Now().Add(time.Hour).UnixNano()
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	errCh := make(chan error, workerCount)
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		workerID := fmt.Sprintf("bench-worker-%d", worker)
		stream := streams[worker]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dispatch := range dispatches {
				if err := stream.Send(&api.ExecutionStreamRequest{Claim: &api.ClaimExecutionRequest{
					RunId:              testString(dispatch.runID),
					ExecutionId:        testString(dispatch.executionID),
					Owner:              testString(workerID),
					LeaseUntilUnixNano: testInt64(leaseUntil),
				}}); err != nil {
					errCh <- err
					return
				}

				claimResp, err := stream.Recv()
				if err != nil {
					errCh <- err
					return
				}

				claim := claimResp.GetClaim()
				if claim == nil {
					errCh <- fmt.Errorf("execution %s returned no claim response", dispatch.executionID)
					return
				}

				if !claim.GetClaimed() {
					errCh <- fmt.Errorf("execution %s was not claimed", dispatch.executionID)
					return
				}

				if err := stream.Send(&api.ExecutionStreamRequest{Complete: &api.CompleteExecutionRequest{
					RunId:       testString(dispatch.runID),
					ExecutionId: testString(dispatch.executionID),
					Owner:       testString(workerID),
					ClaimToken:  testString(claim.GetClaimToken()),
					Status:      testString(dal.ExecutionStatusSucceeded),
				}}); err != nil {
					errCh <- err
					return
				}

				resultResp, err := stream.Recv()
				if err != nil {
					errCh <- err
					return
				}

				result := resultResp.GetComplete()
				if result == nil {
					errCh <- fmt.Errorf("execution %s returned no complete response", dispatch.executionID)
					return
				}

				if result.GetOutcome() != string(dal.ExecutionFinalizationOutcomeRunSucceeded) {
					errCh <- fmt.Errorf("execution %s outcome %q", dispatch.executionID, result.GetOutcome())
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

func benchmarkProtoServiceClaimCompleteLeaf(b *testing.B, shardCount, workerCount int) {
	ctx := context.Background()
	svc := orchestrator.New(shardCount)
	b.Cleanup(svc.Close)

	grpcServer := grpc.NewServer()
	server := orchestrator.RegisterOrchestratorService(grpcServer, svc, mocks.NopLogger{})
	b.Cleanup(grpcServer.Stop)

	dispatches := make(chan benchmarkDispatch, b.N)
	for i := 0; i < b.N; i++ {
		runID := fmt.Sprintf("bench-proto-leaf-%d", i)
		loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: runID})
		if err != nil {
			b.Fatalf("load run %s: %v", runID, err)
		}

		dispatches <- benchmarkDispatch{runID: runID, executionID: loaded.Root.ExecutionID}
	}
	close(dispatches)

	leaseUntil := time.Now().Add(time.Hour).UnixNano()
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
				claim, err := server.ClaimExecution(ctx, &api.ClaimExecutionRequest{
					RunId:              testString(dispatch.runID),
					ExecutionId:        testString(dispatch.executionID),
					Owner:              testString(workerID),
					LeaseUntilUnixNano: testInt64(leaseUntil),
				})

				if err != nil {
					errCh <- err
					return
				}

				if !claim.GetClaimed() {
					errCh <- fmt.Errorf("execution %s was not claimed", dispatch.executionID)
					return
				}

				result, err := server.CompleteExecution(ctx, &api.CompleteExecutionRequest{
					RunId:       testString(dispatch.runID),
					ExecutionId: testString(dispatch.executionID),
					Owner:       testString(workerID),
					ClaimToken:  testString(claim.GetClaimToken()),
					Status:      testString(dal.ExecutionStatusSucceeded),
				})

				if err != nil {
					errCh <- err
					return
				}

				if result.GetOutcome() != string(dal.ExecutionFinalizationOutcomeRunSucceeded) {
					errCh <- fmt.Errorf("execution %s outcome %q", dispatch.executionID, result.GetOutcome())
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

func benchmarkServiceCompleteTerminalSnapshot(b *testing.B, totalTasks int) {
	if totalTasks < 2 {
		b.Fatal("totalTasks must include root plus at least one child")
	}

	ctx := context.Background()
	shardCount := runtime.GOMAXPROCS(0)
	svc := orchestrator.New(shardCount)
	b.Cleanup(svc.Close)

	childCount := totalTasks - 1
	tasks := benchmarkFanoutTasks(childCount)
	leaseUntil := time.Now().Add(time.Hour)
	dispatches := make([]benchmarkClaimedDispatch, 0, b.N)

	for i := 0; i < b.N; i++ {
		workerID := fmt.Sprintf("bench-worker-%d", i)
		runID := fmt.Sprintf("bench-terminal-snapshot-%d-%d", totalTasks, i)
		loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{
			RunID: runID,
			Tasks: tasks,
		})

		if err != nil {
			b.Fatalf("load run %s: %v", runID, err)
		}

		rootClaim, err := svc.ClaimExecution(ctx, runID, loaded.Root.ExecutionID, workerID, leaseUntil)
		if err != nil {
			b.Fatalf("claim root %s: %v", runID, err)
		}

		if !rootClaim.Claimed {
			b.Fatalf("root execution %s was not claimed", loaded.Root.ExecutionID)
		}

		rootResult, err := svc.CompleteExecutionByClaim(ctx, runID, loaded.Root.ExecutionID, workerID, rootClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
		if err != nil {
			b.Fatalf("complete root %s: %v", runID, err)
		}

		if rootResult.Outcome != dal.ExecutionFinalizationOutcomeContinued || len(rootResult.Children) != childCount {
			b.Fatalf("root result for total %d: outcome=%q children=%d", totalTasks, rootResult.Outcome, len(rootResult.Children))
		}

		for childIndex := 0; childIndex < childCount-1; childIndex++ {
			child := rootResult.Children[childIndex]
			childClaim, err := svc.ClaimExecution(ctx, runID, child.ExecutionID, workerID, leaseUntil)
			if err != nil {
				b.Fatalf("claim child %s: %v", child.ExecutionID, err)
			}

			if !childClaim.Claimed {
				b.Fatalf("child execution %s was not claimed", child.ExecutionID)
			}

			result, err := svc.CompleteExecutionByClaim(ctx, runID, child.ExecutionID, workerID, childClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
			if err != nil {
				b.Fatalf("complete child %s: %v", child.ExecutionID, err)
			}

			if isBenchmarkTerminalOutcome(result.Outcome) {
				b.Fatalf("pre-terminal child %s outcome=%q", child.ExecutionID, result.Outcome)
			}
		}

		last := rootResult.Children[childCount-1]
		lastClaim, err := svc.ClaimExecution(ctx, runID, last.ExecutionID, workerID, leaseUntil)
		if err != nil {
			b.Fatalf("claim terminal child %s: %v", last.ExecutionID, err)
		}

		if !lastClaim.Claimed {
			b.Fatalf("terminal child execution %s was not claimed", last.ExecutionID)
		}

		dispatches = append(dispatches, benchmarkClaimedDispatch{
			runID:       runID,
			executionID: last.ExecutionID,
			claimToken:  lastClaim.ClaimToken,
		})
	}

	b.ReportAllocs()
	b.ReportMetric(float64(totalTasks), "snapshot_items")
	b.ResetTimer()
	start := time.Now()

	for i, dispatch := range dispatches {
		workerID := fmt.Sprintf("bench-worker-%d", i)
		result, err := svc.CompleteExecutionByClaim(ctx, dispatch.runID, dispatch.executionID, workerID, dispatch.claimToken, dal.ExecutionStatusSucceeded, "", "")
		if err != nil {
			b.Fatalf("complete terminal child %s: %v", dispatch.executionID, err)
		}

		if result.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded || len(result.Executions) != totalTasks {
			b.Fatalf("terminal result for total %d: outcome=%q snapshots=%d", totalTasks, result.Outcome, len(result.Executions))
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()
	if elapsed > 0 {
		totalItems := float64(totalTasks * b.N)
		b.ReportMetric(totalItems/elapsed.Seconds(), "snapshot_items/s")
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "terminal_completions/s")
	}

	b.ReportMetric(float64(shardCount), "orchestrator_shards")
}

func isBenchmarkTerminalOutcome(outcome dal.ExecutionFinalizationOutcome) bool {
	switch outcome {
	case dal.ExecutionFinalizationOutcomeRunSucceeded,
		dal.ExecutionFinalizationOutcomeRunFailed,
		dal.ExecutionFinalizationOutcomeRunCancelled:
		return true
	default:
		return false
	}
}

func benchmarkWorkDurationName(duration time.Duration) string {
	if duration == 0 {
		return "work_0"
	}

	if duration%time.Millisecond == 0 {
		return fmt.Sprintf("work_%dms", duration/time.Millisecond)
	}

	if duration%time.Microsecond == 0 {
		return fmt.Sprintf("work_%dus", duration/time.Microsecond)
	}

	return fmt.Sprintf("work_%dns", duration/time.Nanosecond)
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
