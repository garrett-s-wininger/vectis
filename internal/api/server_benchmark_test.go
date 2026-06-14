package api_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vectis/internal/api"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/migrations"

	_ "github.com/mattn/go-sqlite3"
)

func BenchmarkAPIServer_RunRead_GetRun(b *testing.B) {
	for _, mode := range []string{"live", "final_facts"} {
		for _, childTasks := range []int{100, 1000, 5000} {
			b.Run(fmt.Sprintf("%s/tasks_%d", mode, childTasks), func(b *testing.B) {
				server, repos := newBenchmarkAPIServer(b)
				runID := seedBenchmarkAPIRunReadTasks(b, context.Background(), repos, mode, childTasks)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID, nil)
					req.SetPathValue("id", runID)
					rec := httptest.NewRecorder()

					server.GetRun(rec, req)
					if rec.Code != http.StatusOK {
						b.Fatalf("GetRun status=%d: %s", rec.Code, rec.Body.String())
					}
				}
			})
		}
	}
}

func BenchmarkAPIServer_RunRead_GetRunTasks(b *testing.B) {
	for _, mode := range []string{"live", "final_facts"} {
		for _, childTasks := range []int{100, 1000, 5000} {
			b.Run(fmt.Sprintf("%s/tasks_%d/limit_200", mode, childTasks), func(b *testing.B) {
				server, repos := newBenchmarkAPIServer(b)
				runID := seedBenchmarkAPIRunReadTasks(b, context.Background(), repos, mode, childTasks)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/tasks?limit=200", nil)
					req.SetPathValue("id", runID)
					rec := httptest.NewRecorder()

					server.GetRunTasks(rec, req)
					if rec.Code != http.StatusOK {
						b.Fatalf("GetRunTasks status=%d: %s", rec.Code, rec.Body.String())
					}
				}
			})
		}
	}
}

func BenchmarkAPIServer_Write_CreateJob(b *testing.B) {
	server, _ := newBenchmarkAPIServer(b)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		jobID := fmt.Sprintf("bench-api-create-%06d", i)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(fmt.Sprintf(benchAPIJobDefinition, jobID)))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		server.CreateJob(rec, req)
		if rec.Code != http.StatusCreated {
			b.Fatalf("CreateJob status=%d: %s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkAPIServer_Write_TriggerJobAccepted(b *testing.B) {
	server, repos, db := newBenchmarkAPIServerWithDB(b)
	ctx := context.Background()
	jobID := "bench-api-trigger-accepted"
	seedBenchmarkAPIStoredJob(b, ctx, repos, jobID)

	ingress := newBenchmarkAPIIngress()
	server.SetExecutionIngress(ingress)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/trigger", http.NoBody)
		req.SetPathValue("id", jobID)
		rec := httptest.NewRecorder()

		server.TriggerJob(rec, req)
		if rec.Code != http.StatusAccepted {
			b.Fatalf("TriggerJob status=%d: %s", rec.Code, rec.Body.String())
		}

		b.StopTimer()
		submission := ingress.wait(b)
		if submission.Envelope == nil || submission.Envelope.RunID == "" {
			b.Fatalf("missing execution envelope in trigger submission")
		}

		waitBenchmarkAPIDispatched(b, db, submission.Envelope.RunID)
		b.StartTimer()
	}
}

func BenchmarkAPIServer_Write_TriggerJobDispatch(b *testing.B) {
	server, repos, db := newBenchmarkAPIServerWithDB(b)
	ctx := context.Background()
	jobID := "bench-api-trigger"
	seedBenchmarkAPIStoredJob(b, ctx, repos, jobID)

	ingress := newBenchmarkAPIIngress()
	server.SetExecutionIngress(ingress)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/trigger", http.NoBody)
		req.SetPathValue("id", jobID)
		rec := httptest.NewRecorder()

		server.TriggerJob(rec, req)
		if rec.Code != http.StatusAccepted {
			b.Fatalf("TriggerJob status=%d: %s", rec.Code, rec.Body.String())
		}

		submission := ingress.wait(b)
		if submission.Envelope == nil || submission.Envelope.RunID == "" {
			b.Fatalf("missing execution envelope in trigger submission")
		}

		waitBenchmarkAPIDispatched(b, db, submission.Envelope.RunID)
	}
}

func BenchmarkAPIServer_Write_RunJobAccepted(b *testing.B) {
	server, _, db := newBenchmarkAPIServerWithDB(b)
	ingress := newBenchmarkAPIIngress()
	server.SetExecutionIngress(ingress)

	const body = `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		server.RunJob(rec, req)
		if rec.Code != http.StatusAccepted {
			b.Fatalf("RunJob status=%d: %s", rec.Code, rec.Body.String())
		}

		b.StopTimer()
		submission := ingress.wait(b)
		if submission.Envelope == nil || submission.Envelope.RunID == "" {
			b.Fatalf("missing execution envelope in run submission")
		}

		waitBenchmarkAPIDispatched(b, db, submission.Envelope.RunID)
		b.StartTimer()
	}
}

func BenchmarkAPIServer_Write_RunJobDispatch(b *testing.B) {
	server, _, db := newBenchmarkAPIServerWithDB(b)
	ingress := newBenchmarkAPIIngress()
	server.SetExecutionIngress(ingress)

	const body = `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		server.RunJob(rec, req)
		if rec.Code != http.StatusAccepted {
			b.Fatalf("RunJob status=%d: %s", rec.Code, rec.Body.String())
		}

		submission := ingress.wait(b)
		if submission.Envelope == nil || submission.Envelope.RunID == "" {
			b.Fatalf("missing execution envelope in run submission")
		}

		waitBenchmarkAPIDispatched(b, db, submission.Envelope.RunID)
	}
}

func BenchmarkAPIServer_Write_ForceFailRun(b *testing.B) {
	server, repos := newBenchmarkAPIServer(b)
	ctx := context.Background()
	jobID := "bench-api-force-fail"
	seedBenchmarkAPIStoredJob(b, ctx, repos, jobID)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runID := seedBenchmarkAPIRun(b, ctx, repos, jobID, i+1)
		b.StartTimer()

		req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/force-fail", strings.NewReader(`{"reason":"benchmark"}`))
		req.SetPathValue("id", runID)
		rec := httptest.NewRecorder()

		server.ForceFailRun(rec, req)
		if rec.Code != http.StatusNoContent {
			b.Fatalf("ForceFailRun status=%d: %s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkAPIServer_Write_ForceRequeueRun(b *testing.B) {
	server, repos := newBenchmarkAPIServer(b)
	ctx := context.Background()
	jobID := "bench-api-force-requeue"
	seedBenchmarkAPIStoredJob(b, ctx, repos, jobID)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runID := seedBenchmarkAPIRun(b, ctx, repos, jobID, i+1)
		if err := repos.Runs().MarkRunFailed(ctx, runID, dal.FailureCodeExecution, "benchmark"); err != nil {
			b.Fatalf("mark run failed: %v", err)
		}

		b.StartTimer()

		req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/force-requeue", http.NoBody)
		req.SetPathValue("id", runID)
		rec := httptest.NewRecorder()

		server.ForceRequeueRun(rec, req)
		if rec.Code != http.StatusNoContent {
			b.Fatalf("ForceRequeueRun status=%d: %s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkAPIServer_Write_CancelRun(b *testing.B) {
	server, repos := newBenchmarkAPIServer(b)
	ctx := context.Background()
	jobID := "bench-api-cancel"
	seedBenchmarkAPIStoredJob(b, ctx, repos, jobID)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runID := seedBenchmarkAPIRun(b, ctx, repos, jobID, i+1)
		claimBenchmarkAPIRun(b, ctx, repos.Runs(), runID)
		b.StartTimer()

		req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/cancel", http.NoBody)
		req.SetPathValue("id", runID)
		rec := httptest.NewRecorder()

		server.CancelRun(rec, req)
		if rec.Code != http.StatusAccepted {
			b.Fatalf("CancelRun status=%d: %s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkAPIServer_Write_RepairMarkAbandoned(b *testing.B) {
	server, repos := newBenchmarkAPIServer(b)
	ctx := context.Background()
	jobID := "bench-api-repair-abandoned"
	seedBenchmarkAPIStoredJob(b, ctx, repos, jobID)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runID := seedBenchmarkAPIRun(b, ctx, repos, jobID, i+1)
		claimBenchmarkAPIRun(b, ctx, repos.Runs(), runID)
		if err := repos.Runs().MarkRunOrphaned(ctx, runID, dal.OrphanReasonLeaseExpired); err != nil {
			b.Fatalf("mark run orphaned: %v", err)
		}

		b.StartTimer()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+runID+"/repair/abandoned", strings.NewReader(`{"reason":"benchmark"}`))
		req.SetPathValue("id", runID)
		rec := httptest.NewRecorder()

		server.RepairMarkRunAbandoned(rec, req)
		if rec.Code != http.StatusNoContent {
			b.Fatalf("RepairMarkRunAbandoned status=%d: %s", rec.Code, rec.Body.String())
		}
	}
}

func newBenchmarkAPIServer(b *testing.B) (*api.APIServer, *dal.SQLRepositories) {
	b.Helper()

	server, repos, _ := newBenchmarkAPIServerWithDB(b)
	return server, repos
}

func newBenchmarkAPIServerWithDB(b *testing.B) (*api.APIServer, *dal.SQLRepositories, *sql.DB) {
	b.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatalf("open benchmark db: %v", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := migrations.Run(db, "sqlite3"); err != nil {
		_ = db.Close()
		b.Fatalf("run migrations: %v", err)
	}

	b.Cleanup(func() { _ = db.Close() })
	return api.NewAPIServer(mocks.NopLogger{}, db), dal.NewSQLRepositories(db), db
}

type benchmarkAPIIngress struct {
	submitted chan cell.ExecutionSubmission
}

func newBenchmarkAPIIngress() *benchmarkAPIIngress {
	return &benchmarkAPIIngress{submitted: make(chan cell.ExecutionSubmission, 1)}
}

func (i *benchmarkAPIIngress) SubmitExecution(ctx context.Context, submission cell.ExecutionSubmission) error {
	select {
	case i.submitted <- submission:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (i *benchmarkAPIIngress) wait(b *testing.B) cell.ExecutionSubmission {
	b.Helper()

	select {
	case submission := <-i.submitted:
		return submission
	case <-time.After(2 * time.Second):
		b.Fatal("timed out waiting for async execution submit")
		return cell.ExecutionSubmission{}
	}
}

func seedBenchmarkAPIStoredJob(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, jobID string) {
	b.Helper()

	if err := repos.Jobs().Create(ctx, jobID, fmt.Sprintf(benchAPIJobDefinition, jobID), 1); err != nil {
		b.Fatalf("create benchmark job: %v", err)
	}
}

func seedBenchmarkAPIRun(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, jobID string, runIndex int) string {
	b.Helper()

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, &runIndex, 1)
	if err != nil {
		b.Fatalf("create benchmark run: %v", err)
	}

	return runID
}

func claimBenchmarkAPIRun(b *testing.B, ctx context.Context, runs dal.RunsRepository, runID string) {
	b.Helper()

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		b.Fatalf("get pending execution: %v", err)
	}

	claim, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "bench-worker", time.Now().Add(time.Minute))
	if err != nil {
		b.Fatalf("claim execution: %v", err)
	}

	if !claim.Claimed {
		b.Fatalf("execution was not claimed")
	}
}

func waitBenchmarkAPIDispatched(b *testing.B, db *sql.DB, runID string) {
	b.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		var dispatchedAt sql.NullInt64
		err := db.QueryRowContext(context.Background(), "SELECT last_dispatched_at FROM job_runs WHERE run_id = ?", runID).Scan(&dispatchedAt)
		if err != nil {
			b.Fatalf("query last_dispatched_at: %v", err)
		}

		if dispatchedAt.Valid {
			return
		}

		if time.Now().After(deadline) {
			b.Fatalf("timed out waiting for run dispatch completion")
		}

		time.Sleep(10 * time.Microsecond)
	}
}

func seedBenchmarkAPIRunReadTasks(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, mode string, childTasks int) string {
	b.Helper()

	switch mode {
	case "live":
		return seedBenchmarkAPIRunReadLiveTasks(b, ctx, repos, childTasks)
	case "final_facts":
		return seedBenchmarkAPIRunReadFinalFacts(b, ctx, repos, childTasks)
	default:
		b.Fatalf("unknown api run read benchmark mode %q", mode)
		return ""
	}
}

func seedBenchmarkAPIRunReadLiveTasks(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, childTasks int) string {
	b.Helper()

	jobID := fmt.Sprintf("bench-api-run-read-live-%d", childTasks)
	if err := repos.Jobs().Create(ctx, jobID, fmt.Sprintf(benchAPIJobDefinition, jobID), 1); err != nil {
		b.Fatalf("create benchmark job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		b.Fatalf("create benchmark run: %v", err)
	}

	for i := 0; i < childTasks; i++ {
		taskKey := benchmarkAPIRunReadTaskKey(i)
		if _, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
			RunID:        runID,
			TaskKey:      taskKey,
			Name:         taskKey,
			SpecHash:     "bench-spec",
			TargetCellID: dal.DefaultCellID,
		}); err != nil {
			b.Fatalf("seed live task %d: %v", i, err)
		}
	}

	return runID
}

func seedBenchmarkAPIRunReadFinalFacts(b *testing.B, ctx context.Context, repos *dal.SQLRepositories, childTasks int) string {
	b.Helper()

	jobID := fmt.Sprintf("bench-api-run-read-final-facts-%d", childTasks)
	if err := repos.Jobs().Create(ctx, jobID, fmt.Sprintf(benchAPIJobDefinition, jobID), 1); err != nil {
		b.Fatalf("create benchmark job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		b.Fatalf("create benchmark run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		b.Fatalf("get root execution: %v", err)
	}

	snapshots := make([]dal.TaskExecutionSnapshot, 0, childTasks+1)
	observedAt := time.Now().UnixNano()
	snapshots = append(snapshots, dal.TaskExecutionSnapshot{
		Record: dal.TaskExecutionRecord{
			RunID:         dispatch.RunID,
			TaskID:        dispatch.TaskID,
			TaskKey:       dispatch.TaskKey,
			Name:          dispatch.TaskName,
			TaskAttemptID: dispatch.TaskAttemptID,
			SegmentID:     dispatch.SegmentID,
			ExecutionID:   dispatch.ExecutionID,
			CellID:        dispatch.CellID,
			Attempt:       dispatch.Attempt,
		},
		Status:               dal.ExecutionStatusSucceeded,
		AcceptedAtUnixNano:   observedAt,
		StartedAtUnixNano:    observedAt,
		FinishedAtUnixNano:   observedAt,
		LastObservedUnixNano: observedAt,
		EventSequence:        1,
	})

	for i := 0; i < childTasks; i++ {
		taskKey := benchmarkAPIRunReadTaskKey(i)
		taskID := benchmarkAPIRunReadTaskID(runID, taskKey)
		attemptID := benchmarkAPIRunReadTaskAttemptID(taskID)
		snapshots = append(snapshots, dal.TaskExecutionSnapshot{
			Record: dal.TaskExecutionRecord{
				RunID:         runID,
				ParentTaskID:  dispatch.TaskID,
				TaskKey:       taskKey,
				Name:          taskKey,
				TaskAttemptID: attemptID,
				SegmentID:     benchmarkAPIRunReadTaskSegmentID(taskID),
				ExecutionID:   benchmarkAPIRunReadTaskExecutionID(attemptID),
				CellID:        dal.DefaultCellID,
				Attempt:       1,
			},
			Status:               dal.ExecutionStatusSucceeded,
			AcceptedAtUnixNano:   observedAt,
			StartedAtUnixNano:    observedAt,
			FinishedAtUnixNano:   observedAt,
			LastObservedUnixNano: observedAt,
			EventSequence:        int64(i + 2),
		})
	}

	if err := repos.Runs().ApplyTerminalExecutionSnapshot(ctx, dal.TerminalExecutionSnapshotUpdate{
		RunID:      runID,
		Outcome:    dal.ExecutionFinalizationOutcomeRunSucceeded,
		Executions: snapshots,
	}); err != nil {
		b.Fatalf("apply terminal execution snapshot: %v", err)
	}

	return runID
}

const benchAPIJobDefinition = `{"id":"%s","root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`

func benchmarkAPIRunReadTaskKey(i int) string {
	return fmt.Sprintf("task-%06d", i)
}

func benchmarkAPIRunReadTaskID(runID, taskKey string) string {
	return runID + ":" + taskKey
}

func benchmarkAPIRunReadTaskAttemptID(taskID string) string {
	return taskID + ":attempt:1"
}

func benchmarkAPIRunReadTaskSegmentID(taskID string) string {
	return taskID + ":segment"
}

func benchmarkAPIRunReadTaskExecutionID(attemptID string) string {
	return attemptID + ":execution"
}
