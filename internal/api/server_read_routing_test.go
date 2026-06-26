package api_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	apigen "vectis/api/gen/go"
	"vectis/internal/dal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeOrchestratorReadClient struct {
	completion      *apigen.OrchestratorRunTaskCompletion
	completionErr   error
	completionCalls int
	snapshot        *apigen.GetRunTaskSnapshotResponse
	snapshotErr     error
	snapshotCalls   int
}

func (f *fakeOrchestratorReadClient) LoadRun(context.Context, *apigen.LoadRunRequest, ...grpc.CallOption) (*apigen.LoadRunResponse, error) {
	return nil, status.Error(codes.Unimplemented, "load run not implemented")
}

func (f *fakeOrchestratorReadClient) ListPending(context.Context, *apigen.ListPendingRequest, ...grpc.CallOption) (*apigen.ListPendingResponse, error) {
	return nil, status.Error(codes.Unimplemented, "list pending not implemented")
}

func (f *fakeOrchestratorReadClient) ClaimExecution(context.Context, *apigen.ClaimExecutionRequest, ...grpc.CallOption) (*apigen.ClaimExecutionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "claim execution not implemented")
}

func (f *fakeOrchestratorReadClient) RenewExecutionLease(context.Context, *apigen.RenewExecutionLeaseRequest, ...grpc.CallOption) (*apigen.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "renew execution lease not implemented")
}

func (f *fakeOrchestratorReadClient) CompleteExecution(context.Context, *apigen.CompleteExecutionRequest, ...grpc.CallOption) (*apigen.CompleteExecutionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "complete execution not implemented")
}

func (f *fakeOrchestratorReadClient) GetRunTaskCompletion(context.Context, *apigen.GetRunTaskCompletionRequest, ...grpc.CallOption) (*apigen.OrchestratorRunTaskCompletion, error) {
	f.completionCalls++
	if f.completionErr != nil {
		return nil, f.completionErr
	}

	return f.completion, nil
}

func (f *fakeOrchestratorReadClient) GetRunTaskSnapshot(context.Context, *apigen.GetRunTaskSnapshotRequest, ...grpc.CallOption) (*apigen.GetRunTaskSnapshotResponse, error) {
	f.snapshotCalls++
	if f.snapshotErr != nil {
		return nil, f.snapshotErr
	}

	return f.snapshot, nil
}

func (f *fakeOrchestratorReadClient) ExecutionStream(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[apigen.ExecutionStreamRequest, apigen.ExecutionStreamResponse], error) {
	return nil, status.Error(codes.Unimplemented, "execution stream not implemented")
}

func createRunForReadRoutingTest(t *testing.T, db *sql.DB, jobID string) string {
	t.Helper()

	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	insertStoredJobForTest(t, db, jobID, `{"id":"`+jobID+`","root":{"uses":"builtins/shell"}}`)

	runID, _, err := repos.Runs().CreateRun(context.Background(), jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	return runID
}

func publishActiveHotStateOwnerForReadRoutingTest(t *testing.T, db *sql.DB, runID string) {
	t.Helper()

	runs := dal.NewSQLRepositoriesWithCellID(db, "local").Runs()
	if err := runs.UpsertRunHotStateOwner(context.Background(), dal.RunHotStateOwnerUpdate{
		RunID:      runID,
		CellID:     "local",
		OwnerID:    "orchestrator:registry:local",
		OwnerEpoch: "epoch-read-route",
		LeaseUntil: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("upsert active hot-state owner: %v", err)
	}
}

func TestAPIServer_GetRunTasks_ActiveHotStateOwnerUsesOrchestratorSnapshot(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	runID := createRunForReadRoutingTest(t, db, "job-read-route-tasks")
	publishActiveHotStateOwnerForReadRoutingTest(t, db, runID)

	rootTaskID := "task-root-hot"
	childTaskID := "task-child-hot"
	rootAttemptID := "attempt-root-hot"
	childAttemptID := "attempt-child-hot"
	rootExecutionID := "execution-root-hot"
	childExecutionID := "execution-child-hot"
	rootTaskKey := dal.RootTaskKey
	childTaskKey := "child"
	cellID := "local"
	rootName := "root"
	childName := "child"
	rootStatus := dal.ExecutionStatusRunning
	childStatus := dal.ExecutionStatusPending
	attempt := int32(1)
	now := time.Now().UTC()
	acceptedAt := now.Add(-2 * time.Second).UnixNano()
	startedAt := now.Add(-time.Second).UnixNano()
	nextCursor := int64(200)

	fake := &fakeOrchestratorReadClient{
		snapshot: &apigen.GetRunTaskSnapshotResponse{
			RunId: &runID,
			Executions: []*apigen.OrchestratorTaskExecution{
				{
					RunId:              &runID,
					TaskId:             &rootTaskID,
					TaskKey:            &rootTaskKey,
					Name:               &rootName,
					TaskAttemptId:      &rootAttemptID,
					ExecutionId:        &rootExecutionID,
					CellId:             &cellID,
					Attempt:            &attempt,
					Status:             &rootStatus,
					AcceptedAtUnixNano: &acceptedAt,
					StartedAtUnixNano:  &startedAt,
				},
				{
					RunId:         &runID,
					TaskId:        &childTaskID,
					ParentTaskId:  &rootTaskID,
					TaskKey:       &childTaskKey,
					Name:          &childName,
					TaskAttemptId: &childAttemptID,
					ExecutionId:   &childExecutionID,
					CellId:        &cellID,
					Attempt:       &attempt,
					Status:        &childStatus,
				},
			},
			NextCursor: &nextCursor,
		},
	}

	server.SetOrchestratorClient(fake)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/tasks?limit=2", nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()
	server.GetRunTasks(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GetRunTasks: expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var got struct {
		Data []struct {
			TaskID       string  `json:"task_id"`
			ParentTaskID *string `json:"parent_task_id,omitempty"`
			TaskKey      string  `json:"task_key"`
			Status       string  `json:"status"`
			Attempts     []struct {
				AttemptID       string  `json:"attempt_id"`
				ExecutionID     string  `json:"execution_id"`
				ExecutionStatus string  `json:"execution_status"`
				AcceptedAt      *string `json:"accepted_at,omitempty"`
				StartedAt       *string `json:"started_at,omitempty"`
			} `json:"attempts"`
		} `json:"data"`
		NextCursor *int64 `json:"next_cursor,omitempty"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode task response: %v", err)
	}

	if fake.snapshotCalls != 1 {
		t.Fatalf("orchestrator snapshot calls: got %d, want 1", fake.snapshotCalls)
	}

	if len(got.Data) != 2 {
		t.Fatalf("task rows: got %d, want 2: %+v", len(got.Data), got.Data)
	}

	if got.NextCursor == nil || *got.NextCursor != nextCursor {
		t.Fatalf("next cursor: got %+v, want %d", got.NextCursor, nextCursor)
	}

	if got.Data[0].TaskID != rootTaskID || got.Data[0].Status != rootStatus || got.Data[0].Attempts[0].ExecutionID != rootExecutionID {
		t.Fatalf("root task row came from wrong source: %+v", got.Data[0])
	}

	if got.Data[0].Attempts[0].AcceptedAt == nil || got.Data[0].Attempts[0].StartedAt == nil {
		t.Fatalf("root task timing missing from hot snapshot: %+v", got.Data[0].Attempts[0])
	}

	if got.Data[1].TaskID != childTaskID || got.Data[1].ParentTaskID == nil || *got.Data[1].ParentTaskID != rootTaskID {
		t.Fatalf("child task row missing hot hierarchy: %+v", got.Data[1])
	}

	fake.snapshot = nil
	fake.snapshotErr = status.Error(codes.NotFound, "run not loaded")
	rec = httptest.NewRecorder()
	server.GetRunTasks(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GetRunTasks fallback: expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	got = struct {
		Data []struct {
			TaskID       string  `json:"task_id"`
			ParentTaskID *string `json:"parent_task_id,omitempty"`
			TaskKey      string  `json:"task_key"`
			Status       string  `json:"status"`
			Attempts     []struct {
				AttemptID       string  `json:"attempt_id"`
				ExecutionID     string  `json:"execution_id"`
				ExecutionStatus string  `json:"execution_status"`
				AcceptedAt      *string `json:"accepted_at,omitempty"`
				StartedAt       *string `json:"started_at,omitempty"`
			} `json:"attempts"`
		} `json:"data"`
		NextCursor *int64 `json:"next_cursor,omitempty"`
	}{}

	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode fallback task response: %v", err)
	}

	if len(got.Data) != 1 {
		t.Fatalf("fallback should return sparse database task rows, got %d: %+v", len(got.Data), got.Data)
	}
}

func TestAPIServer_GetRun_ActiveHotStateOwnerUsesOrchestratorTaskCompletion(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	runID := createRunForReadRoutingTest(t, db, "job-read-route-completion")
	publishActiveHotStateOwnerForReadRoutingTest(t, db, runID)

	total := int32(3)
	succeeded := int32(1)
	terminalFailed := int32(1)
	incomplete := int32(1)
	fake := &fakeOrchestratorReadClient{
		completion: &apigen.OrchestratorRunTaskCompletion{
			RunId:          &runID,
			Total:          &total,
			Succeeded:      &succeeded,
			TerminalFailed: &terminalFailed,
			Incomplete:     &incomplete,
		},
	}

	server.SetOrchestratorClient(fake)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID, nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()
	server.GetRun(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GetRun: expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var got struct {
		Status         string `json:"status"`
		TaskCompletion *struct {
			Total          int `json:"total"`
			Succeeded      int `json:"succeeded"`
			TerminalFailed int `json:"terminal_failed"`
			Incomplete     int `json:"incomplete"`
		} `json:"task_completion"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode get run response: %v", err)
	}

	if fake.completionCalls != 1 {
		t.Fatalf("orchestrator completion calls: got %d, want 1", fake.completionCalls)
	}

	if got.Status != dal.RunStatusRunning {
		t.Fatalf("status: got %q, want %q", got.Status, dal.RunStatusRunning)
	}

	if got.TaskCompletion == nil {
		t.Fatal("missing task completion summary")
	}

	if got.TaskCompletion.Total != 3 || got.TaskCompletion.Succeeded != 1 || got.TaskCompletion.TerminalFailed != 1 || got.TaskCompletion.Incomplete != 1 {
		t.Fatalf("task completion should come from orchestrator, got %+v", got.TaskCompletion)
	}
}
