package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "vectis/api/gen/go"
	apiserver "vectis/internal/api"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/observability"
	"vectis/internal/testutil/dbtest"
)

func TestWorkerAttemptHistoryFromExecutionThroughCompactionToAPI(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runID, _, err := repos.Runs().CreateRun(ctx, "attempt-history", nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	logs := mocks.NewMockLogClient()
	childExecutions := map[string]bool{}
	for generation := 1; generation <= 2; generation++ {
		if generation == 2 {
			if err := repos.Runs().RequeueRunForRetry(ctx, runID); err != nil {
				t.Fatal(err)
			}
		}

		root, err := repos.Runs().GetPendingExecution(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}

		root.DefinitionHash = "attempt-history-definition"
		script := "echo attempt-history-child; exit 7"
		if runtime.GOOS == "windows" {
			script = "echo attempt-history-child & exit /b 7"
		}

		j := &pb.Job{Id: proto.String("attempt-history"), RunId: &runID,
			Root: &pb.Node{Id: proto.String("root-node"), Uses: proto.String("builtins/parallel"),
				Steps: []*pb.Node{{Id: proto.String("child"), Uses: proto.String("builtins/script"),
					With: workerFastScriptInputsForTest(script)}}}}
		ch := newLocalOrchestratorChoreographer(t)
		w := &worker{ctx: ctx, runCtx: ctx, logger: interfaces.NewLogger("attempt-history-test"),
			workerID: "history-worker", cellID: "local", clock: mocks.NewMockClock(), renewInterval: time.Hour,
			queue: mocks.NewMockQueueClient(), logClient: logs, core: testWorkerCore(job.NewExecutor()),
			store: repos.Runs(), choreographer: ch, catalog: cell.NewCatalogEventPublisher("local", repos.CatalogEvents())}
		req := &pb.JobRequest{Job: j}
		env, err := cell.AttachExecutionEnvelope(req, root, int64(generation))

		if err != nil {
			t.Fatal(err)
		}

		if outcome := w.runTaskExecution(ctx, j, j.GetId(), runID, fmt.Sprintf("root-%d", generation), env); outcome != observability.WorkerOutcomeSuccess {
			t.Fatalf("root: %s", outcome)
		}

		pending, err := ch.service.ListPending(ctx, runID, 10)
		if err != nil || len(pending) != 1 {
			t.Fatalf("pending children: %+v, %v", pending, err)
		}

		child := pending[0]
		if child.Attempt != generation || childExecutions[child.ExecutionID] {
			t.Fatalf("reused child attempt: %+v", child)
		}

		childExecutions[child.ExecutionID] = true
		childReq := &pb.JobRequest{Job: j}
		childEnv, err := cell.AttachExecutionEnvelope(childReq, executionDispatchRecordFromTaskExecution(j, env, child), int64(generation+10))
		if err != nil {
			t.Fatal(err)
		}

		if outcome := w.runTaskExecution(ctx, j, j.GetId(), runID, fmt.Sprintf("child-%d", generation), childEnv); outcome == observability.WorkerOutcomeSuccess {
			t.Fatal("demo child should fail")
		}
	}

	server := apiserver.NewAPIServer(mocks.NewMockLogger(), db)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/tasks", nil)
	req.SetPathValue("id", runID)
	rec := httptest.NewRecorder()
	server.GetRunTasks(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("tasks API: %d %s", rec.Code, rec.Body.String())
	}

	var response struct {
		Data []struct {
			TaskKey  string `json:"task_key"`
			Attempts []struct {
				Attempt     int    `json:"attempt"`
				ExecutionID string `json:"execution_id"`
			} `json:"attempts"`
		} `json:"data"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}

	if len(response.Data) != 2 {
		t.Fatalf("task response: %s", rec.Body.String())
	}

	for _, task := range response.Data {
		if len(task.Attempts) != 2 || task.Attempts[0].Attempt != 1 || task.Attempts[1].Attempt != 2 {
			t.Fatalf("task history: %s", rec.Body.String())
		}
	}

	seen := map[string]bool{}
	for _, chunk := range logs.GetChunks() {
		if strings.Contains(string(chunk.GetData()), "attempt-history-child") {
			if !childExecutions[chunk.GetExecutionId()] {
				t.Fatalf("log attributed to wrong execution: %q", chunk.GetExecutionId())
			}

			seen[chunk.GetExecutionId()] = true
		}
	}

	if len(seen) != 2 {
		t.Fatalf("logs missing from one attempt: %+v", seen)
	}

	var snapshots int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cell_catalog_events WHERE event_type = ?", cell.CatalogEventTypeTerminalSnapshot).Scan(&snapshots); err != nil || snapshots != 2 {
		t.Fatalf("terminal receipt lost on retry: %d, %v", snapshots, err)
	}
}
