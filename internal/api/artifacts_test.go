package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	apipb "vectis/api/gen/go"
	"vectis/internal/api"
	artifactsvc "vectis/internal/artifact"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/grpctest"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func TestAPIServer_RunArtifacts_ListAndGet(t *testing.T) {
	server, repos, runID := setupArtifactAPITest(t)
	ctx := context.Background()
	metadata := `{"kind":"coverage"}`

	rec, err := repos.Artifacts().Record(ctx, dal.ArtifactCreate{
		RunID:           runID,
		Name:            "coverage",
		Path:            "coverage/out.json",
		ContentType:     "application/json",
		BlobKey:         "sha256:aaaaaaaa",
		BlobAlgorithm:   "sha256",
		BlobDigest:      "aaaaaaaa",
		SizeBytes:       12,
		ArtifactShardID: "artifact-1",
		MetadataJSON:    &metadata,
	})
	if err != nil {
		t.Fatalf("record artifact: %v", err)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/artifacts", nil)
	listReq.SetPathValue("id", runID)
	listRec := httptest.NewRecorder()
	server.ListRunArtifacts(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("ListRunArtifacts status = %d, want %d: %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var listResp struct {
		Data []struct {
			ID              int64             `json:"id"`
			Name            string            `json:"name"`
			Path            string            `json:"path"`
			ContentType     string            `json:"content_type"`
			BlobDigest      string            `json:"blob_digest"`
			SizeBytes       int64             `json:"size_bytes"`
			ArtifactShardID string            `json:"artifact_shard_id"`
			Metadata        map[string]string `json:"metadata"`
		} `json:"data"`
		NextCursor *int64 `json:"next_cursor,omitempty"`
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}

	if len(listResp.Data) != 1 || listResp.NextCursor != nil {
		t.Fatalf("unexpected list response: %+v", listResp)
	}

	row := listResp.Data[0]
	if row.ID != rec.ID || row.Name != "coverage" || row.Path != "coverage/out.json" {
		t.Fatalf("unexpected artifact row identity: %+v", row)
	}

	if row.ContentType != "application/json" || row.BlobDigest != "aaaaaaaa" || row.SizeBytes != 12 || row.ArtifactShardID != "artifact-1" {
		t.Fatalf("unexpected artifact row content fields: %+v", row)
	}

	if row.Metadata["kind"] != "coverage" {
		t.Fatalf("metadata = %+v, want kind=coverage", row.Metadata)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/artifacts/coverage", nil)
	getReq.SetPathValue("id", runID)
	getReq.SetPathValue("name", "coverage")
	getRec := httptest.NewRecorder()
	server.GetRunArtifact(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetRunArtifact status = %d, want %d: %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var getResp struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResp); err != nil {
		t.Fatalf("decode get response: %v", err)
	}

	if getResp.ID != rec.ID || getResp.Name != "coverage" {
		t.Fatalf("unexpected get response: %+v", getResp)
	}
}

func TestAPIServer_RunArtifacts_ListFiltersByAttribution(t *testing.T) {
	server, repos, runID := setupArtifactAPITest(t)
	ctx := context.Background()
	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	for _, create := range []dal.ArtifactCreate{
		{
			RunID:           runID,
			TaskID:          dispatch.TaskID,
			TaskAttemptID:   dispatch.TaskAttemptID,
			ExecutionID:     dispatch.ExecutionID,
			Name:            "coverage",
			Path:            "coverage/out.json",
			BlobKey:         "sha256:aaaaaaaa",
			BlobAlgorithm:   "sha256",
			BlobDigest:      "aaaaaaaa",
			SizeBytes:       12,
			ArtifactShardID: "artifact-1",
		},
		{
			RunID:           runID,
			Name:            "logs",
			Path:            "logs/raw.txt",
			BlobKey:         "sha256:bbbbbbbb",
			BlobAlgorithm:   "sha256",
			BlobDigest:      "bbbbbbbb",
			SizeBytes:       4,
			ArtifactShardID: "artifact-1",
		},
	} {
		if _, err := repos.Artifacts().Record(ctx, create); err != nil {
			t.Fatalf("record artifact %q: %v", create.Name, err)
		}
	}

	query := url.Values{
		"task_id":         {dispatch.TaskID},
		"task_attempt_id": {dispatch.TaskAttemptID},
		"execution_id":    {dispatch.ExecutionID},
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/artifacts?"+query.Encode(), nil)
	listReq.SetPathValue("id", runID)
	listRec := httptest.NewRecorder()
	server.ListRunArtifacts(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("ListRunArtifacts status = %d, want %d: %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var listResp struct {
		Data []struct {
			Name          string  `json:"name"`
			TaskID        *string `json:"task_id,omitempty"`
			TaskAttemptID *string `json:"task_attempt_id,omitempty"`
			ExecutionID   *string `json:"execution_id,omitempty"`
		} `json:"data"`
	}

	if err := json.NewDecoder(listRec.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list response: %v", err)
	}

	if len(listResp.Data) != 1 {
		t.Fatalf("expected one filtered artifact, got %+v", listResp.Data)
	}

	row := listResp.Data[0]
	if row.Name != "coverage" || row.TaskID == nil || *row.TaskID != dispatch.TaskID || row.TaskAttemptID == nil || *row.TaskAttemptID != dispatch.TaskAttemptID || row.ExecutionID == nil || *row.ExecutionID != dispatch.ExecutionID {
		t.Fatalf("unexpected filtered artifact row: %+v", row)
	}
}

func TestAPIServer_DownloadRunArtifact(t *testing.T) {
	server, repos, runID := setupArtifactAPITest(t)
	ctx := context.Background()
	artifactServer, store := startArtifactAPITestServer(t)
	content := []byte("coverage artifact bytes")

	desc, err := store.Put(ctx, bytes.NewReader(content), artifactsvc.PutOptions{})
	if err != nil {
		t.Fatalf("put blob: %v", err)
	}

	_, err = repos.Artifacts().Record(ctx, dal.ArtifactCreate{
		RunID:           runID,
		Name:            "coverage",
		Path:            "reports/coverage.txt",
		ContentType:     "text/plain; charset=utf-8",
		BlobKey:         desc.Key,
		BlobAlgorithm:   desc.Algorithm,
		BlobDigest:      desc.Digest,
		SizeBytes:       desc.Size,
		ArtifactShardID: "pinned",
	})
	if err != nil {
		t.Fatalf("record artifact: %v", err)
	}

	viper.Set("artifact.grpc.resolver.address", artifactServer.Addr())
	t.Cleanup(func() {
		viper.Set("artifact.grpc.resolver.address", "")
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID+"/artifacts/coverage/download", nil)
	req.SetPathValue("id", runID)
	req.SetPathValue("name", "coverage")
	rec := httptest.NewRecorder()
	server.DownloadRunArtifact(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("DownloadRunArtifact status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if !bytes.Equal(rec.Body.Bytes(), content) {
		t.Fatalf("download body = %q, want %q", rec.Body.Bytes(), content)
	}

	if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}

	if got := rec.Header().Get("Content-Length"); got != "23" {
		t.Fatalf("Content-Length = %q", got)
	}

	if got := rec.Header().Get("Content-Disposition"); !strings.Contains(got, "attachment") || !strings.Contains(got, "coverage.txt") {
		t.Fatalf("Content-Disposition = %q", got)
	}
}

func setupArtifactAPITest(t *testing.T) (*api.APIServer, *dal.SQLRepositories, string) {
	t.Helper()

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	server := api.NewAPIServer(mocks.NewMockLogger(), db)
	runID := createArtifactAPITestRun(t, context.Background(), repos, "job-api-artifacts")

	return server, repos, runID
}

func createArtifactAPITestRun(t *testing.T, ctx context.Context, repos *dal.SQLRepositories, jobID string) string {
	t.Helper()

	def := `{"id":"` + jobID + `","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	return runID
}

func startArtifactAPITestServer(t *testing.T) (*grpctest.Server, *artifactsvc.LocalStore) {
	t.Helper()

	store, err := artifactsvc.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	server := grpctest.StartServer(t, func(srv *grpc.Server) {
		apipb.RegisterArtifactServiceServer(srv, artifactsvc.NewServer(store))
	})

	return server, store
}
