package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/dal"
)

func TestArtifactAttemptQueryFiltersAndRejectsInvalidNumbers(t *testing.T) {
	server, repos, runID := setupArtifactAPITest(t)
	root, err := repos.Runs().GetPendingExecution(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := repos.Artifacts().Record(context.Background(), dal.ArtifactCreate{RunID: runID, Name: "report",
		TaskID: root.TaskID, TaskAttemptID: root.TaskAttemptID, ExecutionID: root.ExecutionID,
		BlobKey: "sha256:aaaa", BlobAlgorithm: "sha256", BlobDigest: "aaaa", SizeBytes: 1, ArtifactShardID: "local"}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		query         string
		status, count int
	}{
		{"attempt=1", 200, 1}, {"attempt=2", 200, 0}, {"attempt=0", 400, 0}, {"attempt=-1", 400, 0},
		{"attempt=", 400, 0}, {"attempt=one", 400, 0}, {"attempt=1.5", 400, 0}, {"attempt=1&attempt=2", 400, 0},
	} {
		t.Run(tc.query, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/artifacts?"+tc.query, nil)
			req.SetPathValue("id", runID)
			rec := httptest.NewRecorder()
			server.ListRunArtifacts(rec, req)

			if rec.Code != tc.status {
				t.Fatalf("status=%d expected=%d: %s", rec.Code, tc.status, rec.Body.String())
			}

			if tc.status == 200 {
				var result struct {
					Data []json.RawMessage `json:"data"`
				}

				if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil || len(result.Data) != tc.count {
					t.Fatalf("result=%s err=%v", rec.Body.String(), err)
				}
			}
		})
	}
}
