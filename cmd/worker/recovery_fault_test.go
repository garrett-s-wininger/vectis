package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/observability"
	"vectis/internal/orchestrator"
	"vectis/internal/workercore"
	workersdk "vectis/sdk/workercore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type recoveryRPCFaults struct {
	loseCompletion  atomic.Bool
	completionCalls atomic.Int64
	ackFault atomic.Int64
	ackCalls atomic.Int64
}

func (f *recoveryRPCFaults) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod == api.OrchestratorService_AcknowledgeRunPersistence_FullMethodName {
		f.ackCalls.Add(1)
		if f.ackFault.Load() == 1 {
			return nil, status.Error(codes.Unavailable, "injected acknowledgment request loss")
		}
	}

	resp, err := handler(ctx, req)
	if err != nil {
		return resp, err
	}

	if info.FullMethod == api.OrchestratorService_CompleteExecution_FullMethodName {
		f.completionCalls.Add(1)
		if f.loseCompletion.CompareAndSwap(true, false) {
			return nil, status.Error(codes.Unavailable, "injected completion response loss")
		}
	}

	if info.FullMethod == api.OrchestratorService_AcknowledgeRunPersistence_FullMethodName && f.ackFault.CompareAndSwap(2, 0) {
		return nil, status.Error(codes.Unavailable, "injected acknowledgment response loss")
	}

	return resp, nil
}

type recoveryFaultStream struct {
	grpc.ServerStream
	faults *recoveryRPCFaults
}

func (s *recoveryFaultStream) SendMsg(message any) error {
	if response, ok := message.(*api.ExecutionStreamResponse); ok && response.GetComplete() != nil {
		s.faults.completionCalls.Add(1)
		if s.faults.loseCompletion.CompareAndSwap(true, false) {
			return status.Error(codes.Unavailable, "injected stream completion response loss")
		}
	}

	return s.ServerStream.SendMsg(message)
}

func recoveryFaultClient(t *testing.T, service *orchestrator.Service, faults *recoveryRPCFaults, unary bool) (*grpcExecutionChoreographer, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(faults.unary), grpc.StreamInterceptor(
		func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, &recoveryFaultStream{ServerStream: stream, faults: faults})
		}))

	orchestrator.RegisterOrchestratorService(server, service, interfaces.NewLogger("recovery-fault-test"))
	go func() { _ = server.Serve(listener) }()

	conn, err := grpc.NewClient("passthrough:///recovery", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		server.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}

	closeServer := func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	}

	t.Cleanup(closeServer)
	return &grpcExecutionChoreographer{client: api.NewOrchestratorServiceClient(conn), streamDisabled: unary}, closeServer
}

type recoveryCountingCore struct {
	workercore.Core
	calls atomic.Int64
}

func (c *recoveryCountingCore) ExecuteTask(ctx context.Context, req workercore.ExecuteTaskRequest) error {
	c.calls.Add(1)
	return c.Core.ExecuteTask(ctx, req)
}

func TestWorkerFault_DurableCompletionAcrossRPCFailures(t *testing.T) {
	for _, terminalStatus := range []string{dal.RunStatusSucceeded, dal.RunStatusFailed, dal.RunStatusCancelled} {
		for _, transport := range []string{"stream", "unary"} {
			for _, failure := range []string{"completion_response_lost", "ack_request_lost", "ack_response_lost", "orchestrator_restart_after_commit"} {
				t.Run(terminalStatus+"/"+transport+"/"+failure, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()

					seed, _, j, env, _, db := retirementWorker(t)
					service := orchestrator.New(1)
					t.Cleanup(service.Close)

					faults := &recoveryRPCFaults{}
					switch failure {
					case "completion_response_lost":
						faults.loseCompletion.Store(true)
					case "ack_response_lost":
						faults.ackFault.Store(2)
					default:
						faults.ackFault.Store(1)
					}

					choreographer, closeServer := recoveryFaultClient(t, service, faults, transport == "unary")
					core := &recoveryCountingCore{Core: testWorkerCore(job.NewExecutor())}
					wantOutcome := observability.WorkerOutcomeSuccess
					wantExecutionStatus := terminalStatus

					if terminalStatus == dal.RunStatusFailed {
						wantOutcome = observability.WorkerOutcomeFailed
					}

					if terminalStatus == dal.RunStatusCancelled {
						wantOutcome = observability.WorkerOutcomeAborted
						wantExecutionStatus = dal.ExecutionStatusAborted
						core.Core = errorWorkerCore{err: workercore.NewTaskResultError(api.RunOutcome_RUN_OUTCOME_UNKNOWN, workersdk.ReasonCancelled, "injected core cancellation")}
					}

					makeWorker := func(id string, ch executionChoreographer) *worker {
						clock := mocks.NewMockClock()
						clock.SetNow(time.Now())
						return &worker{
							ctx: ctx, runCtx: ctx, logger: seed.logger, workerID: id, cellID: dal.DefaultCellID,
							store: seed.store, retirements: seed.retirements, choreographer: ch,
							queue: mocks.NewMockQueueClient(), logClient: mocks.NewMockLogClient(), core: core,
							clock: clock, renewInterval: time.Hour,
						}
					}

					first := makeWorker("first", choreographer)
					j.Root.With = map[string]string{"success": "true"}
					if terminalStatus == dal.RunStatusFailed {
						j.Root.With["success"] = "false"
					}

					deliveryID := "original-delivery"
					j.DeliveryId = &deliveryID
					if outcome := first.runTaskExecution(ctx, j, j.GetId(), env.RunID, deliveryID, env); outcome != wantOutcome {
						t.Fatalf("completion outcome = %q", outcome)
					}

					assertPersisted := func() {
						t.Helper()
						state, found, err := seed.store.GetRunStatus(ctx, env.RunID)
						if err != nil || !found || state != terminalStatus {
							t.Fatalf("durable outcome = %s, found=%v, err=%v", state, found, err)
						}

						var facts int
						if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_task_final_facts WHERE run_id = ?`, env.RunID).Scan(&facts); err != nil || facts != 1 {
							t.Fatalf("final facts = %d, err=%v", facts, err)
						}

						var factStatus string
						if err := db.QueryRowContext(ctx, `SELECT execution_status FROM run_task_final_facts WHERE run_id = ?`, env.RunID).Scan(&factStatus); err != nil || factStatus != wantExecutionStatus {
							t.Fatalf("final fact status = %s, err=%v", factStatus, err)
						}
					}

					assertPersisted()
					var receipts int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM orchestrator_retirements`).Scan(&receipts); err != nil {
						t.Fatal(err)
					}

					wantReceipts, wantRetained := 1, int64(1)
					if failure == "completion_response_lost" {
						wantReceipts, wantRetained = 0, 0
						if faults.completionCalls.Load() != 2 {
							t.Fatalf("completion was not replayed exactly once: %d", faults.completionCalls.Load())
						}
					}

					if failure == "ack_response_lost" {
						wantRetained = 0
					}

					if runs, _, _, _ := service.RetentionStats(); receipts != wantReceipts || runs != wantRetained || faults.ackCalls.Load() != 1 {
						t.Fatalf("fault boundary: receipts=%d runs=%d ack calls=%d", receipts, runs, faults.ackCalls.Load())
					}

					faults.ackFault.Store(0)
					if failure == "orchestrator_restart_after_commit" {
						closeServer()
						service.Close()
						service = orchestrator.New(1)
						t.Cleanup(service.Close)
						choreographer, _ = recoveryFaultClient(t, service, &recoveryRPCFaults{}, transport == "unary")
					}

					replacement := makeWorker("replacement", choreographer)
					if err := replacement.drainRetirements(ctx); err != nil {
						t.Fatal(err)
					}

					if due, err := replacement.retirements.ListDue(ctx, time.Now().Unix(), 256); err != nil || len(due) != 0 {
						t.Fatalf("receipt recovery: %+v, %v", due, err)
					}

					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM orchestrator_retirements`).Scan(&receipts); err != nil || receipts != 0 {
						t.Fatalf("receipts remaining = %d, err=%v", receipts, err)
					}

					if runs, _, _, _ := service.RetentionStats(); runs != 0 {
						t.Fatalf("graphs retained after recovery: %d", runs)
					}

					if outcome := replacement.runTaskExecution(ctx, j, j.GetId(), env.RunID, "delayed-delivery", env); outcome != observability.WorkerOutcomeSkippedUnclaimed {
						t.Fatalf("delayed delivery outcome = %q", outcome)
					}

					assertPersisted()
					if core.calls.Load() != 1 {
						t.Fatalf("task executed %d times", core.calls.Load())
					}
				})
			}
		}
	}
}
