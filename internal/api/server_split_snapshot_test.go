package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	apigen "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/database"

	"google.golang.org/protobuf/proto"
)

func TestSplitRunTaskReadsRequireCurrentRootGeneration(t *testing.T) {
	for _, tc := range []struct {
		name            string
		stale, terminal bool
		wantLive        bool
	}{
		{name: "current live snapshot", wantLive: true},
		{name: "stale retry generation", stale: true},
		{name: "completed catalog", terminal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _, _, db := setupTestServer(t)
			t.Setenv(database.EnvGlobalDatabaseDSN, filepath.Join(t.TempDir(), "global.db"))
			t.Setenv(database.EnvCellDatabaseDSN, filepath.Join(t.TempDir(), "cell.db"))

			runID := createRunForReadRoutingTest(t, db, "split-read")
			runs := dal.NewSQLRepositories(db).Runs()
			root, err := runs.GetPendingExecution(context.Background(), runID)
			if err != nil {
				t.Fatal(err)
			}

			rootID := root.ExecutionID
			if tc.stale {
				rootID = "older-root"
			}

			if tc.terminal {
				if err := runs.MarkRunSucceeded(context.Background(), runID); err != nil {
					t.Fatal(err)
				}
			}

			server.SetOrchestratorClient(&fakeOrchestratorReadClient{snapshot: &apigen.GetRunTaskSnapshotResponse{
				RunId: &runID, Executions: []*apigen.OrchestratorTaskExecution{
					{RunId: &runID, TaskId: &root.TaskID, TaskKey: proto.String("root"), ExecutionId: &rootID, TaskAttemptId: &root.TaskAttemptID, Attempt: proto.Int32(1), Status: proto.String("succeeded")},
					{RunId: &runID, TaskId: proto.String(runID + ":heartbeat"), TaskKey: proto.String("heartbeat"), Name: proto.String("Live heartbeat"), ExecutionId: proto.String("child-execution"), TaskAttemptId: proto.String("child-attempt"), Attempt: proto.Int32(1), Status: proto.String("running")},
				},
			}})

			req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/tasks", nil)
			req.SetPathValue("id", runID)
			recorder := httptest.NewRecorder()
			server.GetRunTasks(recorder, req)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}

			if strings.Contains(recorder.Body.String(), "Live heartbeat") != tc.wantLive {
				t.Fatalf("unexpected snapshot: %s", recorder.Body.String())
			}
		})
	}
}
