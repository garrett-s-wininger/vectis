package orchestrator_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/orchestrator"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const orchestratorBufSize = 1024 * 1024

func newOrchestratorTestClient(t testing.TB, svc *orchestrator.Service) (api.OrchestratorServiceClient, func()) {
	t.Helper()

	listener := bufconn.Listen(orchestratorBufSize)
	server := grpc.NewServer()
	orchestrator.RegisterOrchestratorService(server, svc, interfaces.NewLogger("orchestrator-test"))
	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		t.Fatalf("dial orchestrator bufconn: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	}

	return api.NewOrchestratorServiceClient(conn), cleanup
}

func TestGRPCServiceClaimComplete(t *testing.T) {
	ctx := context.Background()
	svc := orchestrator.New(2)
	t.Cleanup(svc.Close)
	client, cleanup := newOrchestratorTestClient(t, svc)
	t.Cleanup(cleanup)

	loaded, err := client.LoadRun(ctx, &api.LoadRunRequest{
		RunId: testString("grpc-run"),
		Root: &api.OrchestratorTaskExecution{
			RunId:         testString("grpc-run"),
			TaskId:        testString("grpc-root-task"),
			TaskKey:       testString(dal.RootTaskKey),
			Name:          testString(dal.RootTaskKey),
			TaskAttemptId: testString("grpc-root-attempt"),
			SegmentId:     testString("grpc-root-segment"),
			ExecutionId:   testString("grpc-root-execution"),
			CellId:        testString("iad-a"),
			Attempt:       testInt32(1),
		},
		Tasks: []*api.OrchestratorTaskSpec{{
			TaskKey:       testString("child"),
			ParentTaskKey: testString(dal.RootTaskKey),
		}},
	})

	if err != nil {
		t.Fatalf("load run: %v", err)
	}

	if loaded.GetRoot().GetExecutionId() != "grpc-root-execution" {
		t.Fatalf("root execution id: got %q", loaded.GetRoot().GetExecutionId())
	}

	claim, err := client.ClaimExecution(ctx, &api.ClaimExecutionRequest{
		RunId:              testString(loaded.GetRunId()),
		ExecutionId:        testString(loaded.GetRoot().GetExecutionId()),
		Owner:              testString("worker-a"),
		LeaseUntilUnixNano: testInt64(time.Now().Add(time.Minute).UnixNano()),
	})

	if err != nil {
		t.Fatalf("claim root: %v", err)
	}

	if !claim.GetClaimed() || claim.GetClaimToken() == "" {
		t.Fatalf("claim root: %+v", claim)
	}

	completed, err := client.CompleteExecution(ctx, &api.CompleteExecutionRequest{
		RunId:       testString(loaded.GetRunId()),
		ExecutionId: testString(loaded.GetRoot().GetExecutionId()),
		Owner:       testString("worker-a"),
		ClaimToken:  testString(claim.GetClaimToken()),
		Status:      testString(dal.ExecutionStatusSucceeded),
	})

	if err != nil {
		t.Fatalf("complete root: %v", err)
	}

	if completed.GetOutcome() != string(dal.ExecutionFinalizationOutcomeContinued) || len(completed.GetChildren()) != 1 || completed.GetActivated() != 1 {
		t.Fatalf("complete root result: %+v", completed)
	}

	if _, err := client.CompleteExecution(ctx, &api.CompleteExecutionRequest{
		RunId:       testString(loaded.GetRunId()),
		ExecutionId: testString(loaded.GetRoot().GetExecutionId()),
		Owner:       testString("worker-a"),
		ClaimToken:  testString(claim.GetClaimToken()),
		Status:      testString(dal.ExecutionStatusSucceeded),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale completion status: got %v, want %s", err, codes.FailedPrecondition)
	}
}

func BenchmarkGRPCService_ClaimCompleteLeaf(b *testing.B) {
	ctx := context.Background()
	svc := orchestrator.New(8)
	b.Cleanup(svc.Close)
	client, cleanup := newOrchestratorTestClient(b, svc)
	b.Cleanup(cleanup)

	dispatches := make(chan benchmarkDispatch, b.N)
	for i := 0; i < b.N; i++ {
		runID := fmt.Sprintf("grpc-leaf-%d", i)
		loaded, err := client.LoadRun(ctx, &api.LoadRunRequest{RunId: testString(runID)})
		if err != nil {
			b.Fatalf("load run %s: %v", runID, err)
		}

		dispatches <- benchmarkDispatch{runID: runID, executionID: loaded.GetRoot().GetExecutionId()}
	}
	close(dispatches)

	leaseUntil := time.Now().Add(time.Hour).UnixNano()
	workerCount := 8
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	errCh := make(chan error, workerCount)
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		workerID := fmt.Sprintf("grpc-worker-%d", worker)
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

	b.ReportMetric(float64(workerCount), "worker_count")
}

func testString(v string) *string {
	return &v
}

func testInt64(v int64) *int64 {
	return &v
}

func testInt32(v int32) *int32 {
	return &v
}
