package cellingress

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/proto"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestCellIngressAdmitsRetriedRunWithoutChangingFrozenPayload(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	queue := mocks.NewMockQueueService()
	server := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	server.SetAcceptanceStore(repos.CellExecutionAcceptances())
	first := validJobRequestForCellAttempt(t, "iad-a", 1)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, newExecutionRequest(t, executionBody(t, first)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("first admission: %d %s", response.Code, response.Body.String())
	}

	env, _, err := cell.ExecutionEnvelopeFromRequest(first)
	if err != nil {
		t.Fatal(err)
	}

	if err := repos.Runs().ApplyTerminalExecutionSnapshot(ctx, dal.TerminalExecutionSnapshotUpdate{
		RunID: env.RunID, Outcome: dal.ExecutionFinalizationOutcomeRunFailed, Executions: []dal.TaskExecutionSnapshot{{
			Record: dal.TaskExecutionRecord{RunID: env.RunID, TaskID: env.TaskID, TaskKey: env.TaskKey, Name: env.TaskName, TaskAttemptID: env.TaskAttemptID, SegmentID: env.SegmentID, ExecutionID: env.ExecutionID, CellID: env.CellID, Attempt: env.TaskAttempt}, Status: dal.ExecutionStatusFailed,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	var frozenHash string
	if err := db.QueryRow("SELECT execution_payload_hash FROM job_runs WHERE run_id = ?", env.RunID).Scan(&frozenHash); err != nil {
		t.Fatal(err)
	}

	second := validJobRequestForCellAttempt(t, "iad-a", 2)
	next, _, err := cell.ExecutionEnvelopeFromRequest(second)
	if err != nil {
		t.Fatal(err)
	}

	next.ExecutionID = "execution-2"
	next.RootExecutionID = next.ExecutionID
	next.CreatedAtUnixNano = 456
	raw, err := cell.EncodeExecutionEnvelope(next)
	if err != nil {
		t.Fatal(err)
	}

	second.Metadata[cell.ExecutionEnvelopeMetadataKey] = string(raw)
	second.Metadata["traceparent"] = "new-trace"
	second.Job.DeliveryId = proto.String("new-delivery")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, newExecutionRequest(t, executionBody(t, second)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("retry admission: %d %s", response.Code, response.Body.String())
	}

	run, err := repos.Runs().GetRun(ctx, env.RunID)
	if err != nil || run.Status != dal.RunStatusQueued {
		t.Fatalf("cell kept old terminal state: %+v, %v", run, err)
	}

	var currentHash string
	if err := db.QueryRow("SELECT execution_payload_hash FROM job_runs WHERE run_id = ?", env.RunID).Scan(&currentHash); err != nil || currentHash != frozenHash {
		t.Fatalf("frozen payload changed: %s -> %s, %v", frozenHash, currentHash, err)
	}

	tasks, _, err := repos.Runs().ListRunTasks(ctx, env.RunID, 0, 10)
	if err != nil || len(tasks) != 1 || len(tasks[0].Attempts) != 2 {
		t.Fatalf("cell lost history: %+v, %v", tasks, err)
	}

	if tasks[0].Attempts[0].Status != dal.TaskStatusFailed || tasks[0].Attempts[1].Attempt != 2 || tasks[0].Attempts[1].Status != dal.TaskStatusAccepted {
		t.Fatalf("wrong cell attempts: %+v", tasks[0].Attempts)
	}

	response = httptest.NewRecorder()
	server.ServeHTTP(response, newExecutionRequest(t, executionBody(t, first)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("old duplicate: %d %s", response.Code, response.Body.String())
	}

	tasks, _, err = repos.Runs().ListRunTasks(ctx, env.RunID, 0, 10)
	if err != nil || len(tasks) != 1 || len(tasks[0].Attempts) != 2 || tasks[0].Attempts[1].Status != dal.TaskStatusAccepted {
		t.Fatalf("old receipt changed current state: %+v, %v", tasks, err)
	}

	queued := len(queue.GetJobRequests())
	bad := validJobRequestForCellAttempt(t, "iad-a", 3)
	badEnv, _, err := cell.ExecutionEnvelopeFromRequest(bad)
	if err != nil {
		t.Fatal(err)
	}

	bad.Job.Root.With["script"] = "different command"
	badEnv.Job.Root.With["script"] = "different command"
	badEnv.ExecutionID, badEnv.RootExecutionID = "execution-3", "execution-3"
	raw, err = cell.EncodeExecutionEnvelope(badEnv)
	if err != nil {
		t.Fatal(err)
	}

	bad.Metadata[cell.ExecutionEnvelopeMetadataKey] = string(raw)
	response = httptest.NewRecorder()
	server.ServeHTTP(response, newExecutionRequest(t, executionBody(t, bad)))
	if response.Code != http.StatusConflict || len(queue.GetJobRequests()) != queued {
		t.Fatalf("changed command was admitted: %d %s", response.Code, response.Body.String())
	}
}
