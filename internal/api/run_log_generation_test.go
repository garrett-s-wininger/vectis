package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	pb "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

type logGenerationClient struct {
	pb.LogServiceClient
	ctx context.Context
}

func (c *logGenerationClient) GetLogs(ctx context.Context, _ *pb.GetLogsRequest, _ ...grpc.CallOption) (pb.LogService_GetLogsClient, error) {
	c.ctx = ctx
	return nil, errors.New("injected stream unavailable")
}

func TestRunLogReplayCarriesCurrentRootExecutionFence(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, "log-fence", `{"root":{"uses":"builtins/script"}}`); err != nil {
		t.Fatal(err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, "log-fence", nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	root, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	server := NewAPIServer(mocks.NewMockLogger(), db)
	client := &logGenerationClient{}
	server.SetLogClient(client)
	for _, active := range []bool{true, false} {
		if !active {
			if err := repos.Runs().MarkRunFailed(ctx, runID, dal.FailureCodeExecution, "failure"); err != nil {
				t.Fatal(err)
			}
		}

		req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/logs", nil)
		req.SetPathValue("id", runID)
		rec := httptest.NewRecorder()
		server.GetRunLogs(rec, req)
		if client.ctx == nil {
			t.Fatalf("log client not called: %d %s", rec.Code, rec.Body.String())
		}

		headers, _ := metadata.FromOutgoingContext(client.ctx)
		if got := headers.Get(getLogsRootExecutionMetadata); len(got) != 1 || got[0] != root.ExecutionID {
			t.Fatalf("root fence: %+v", headers)
		}

		if got := headers.Get(getLogsActiveRunMetadata); (len(got) > 0 && got[0] == "true") != active {
			t.Fatalf("active flag: %+v", headers)
		}

		client.ctx = nil
	}
}
