package api_test

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"vectis/internal/dal"
)

func TestAPIServer_PublicConnectionConventionalJobsLifecycle(t *testing.T) {
	t.Parallel()
	server, _, _, _ := setupTestServer(t)
	server.SetSourceCheckoutRoot(filepath.Join(t.TempDir(), "checkouts"))
	remote := initAPIRemoteGitRepo(t)
	definition := `{"root":{"id":"root","uses":"builtins/script","with":{"script":"echo connected"}}}`
	for _, path := range []string{".vectis.json", ".vectis/build.json", ".vectis/jobs/legacy.json"} {
		writeAPIFileAndCommit(t, remote, path, definition, "add "+path)
	}

	handler := server.Handler()
	created := doJSONRequest(
		t,
		handler,
		http.MethodPost,
		"/api/v1/source-repositories",
		map[string]any{
			"repository_id":  "public",
			"namespace":      "/",
			"checkout_mode":  "managed",
			"canonical_url":  remote,
			"default_ref":    "HEAD",
			"credential_ref": "",
			"authoring_mode": "read_only",
		},
	)

	if created.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}

	connection := decodeSourceRepositoryResponse(t, created)
	if connection.CredentialRef != "" {
		t.Fatalf("unexpected ownership/auth: %+v", connection)
	}

	synced := httptest.NewRecorder()
	handler.ServeHTTP(synced, httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/public/sync", nil))
	if synced.Code != http.StatusOK ||
		decodeSourceRepositoryResponse(t, synced).Sync.Status != dal.SourceSyncStatusSucceeded {
		t.Fatalf("sync: %d %s", synced.Code, synced.Body.String())
	}

	listed := httptest.NewRecorder()
	handler.ServeHTTP(listed, httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/public/jobs", nil))
	if listed.Code != http.StatusOK {
		t.Fatalf("list: %d %s", listed.Code, listed.Body.String())
	}

	jobs := decodeSourceRepositoryJobsResponse(t, listed)
	if len(jobs.Jobs) != 3 || len(jobs.Invalid) != 0 {
		t.Fatalf("jobs: %s", listed.Body.String())
	}

	for jobID, expected := range map[string]string{"ci": ".vectis.json", "build": ".vectis/build.json", "legacy": ".vectis/jobs/legacy.json"} {
		read := httptest.NewRecorder()
		handler.ServeHTTP(read, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+jobID+"?repository_id=public", nil))
		if read.Code != http.StatusOK {
			t.Fatalf("read %s: %d %s", jobID, read.Code, read.Body.String())
		}

		if got := decodeSourceRepositoryJobDefinitionResponse(t, read).Source.Path; got != expected {
			t.Fatalf("read %s path=%s", jobID, got)
		}
	}

	triggered := doJSONRequest(
		t,
		handler,
		http.MethodPost,
		"/api/v1/jobs/trigger/ci?namespace=%2F",
		map[string]any{"repository_id": "public", "namespace": "/"},
	)

	if triggered.Code != http.StatusAccepted {
		t.Fatalf("trigger: %d %s", triggered.Code, triggered.Body.String())
	}

	snapshot := decodeSourceJobTriggerResponse(t, triggered)
	if snapshot.JobID != "ci" || snapshot.Source.Path != ".vectis.json" || snapshot.Source.ResolvedCommit == "" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}
