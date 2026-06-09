package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
)

func TestAPIServer_SourceBackedJobLifecycle(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()

	repoPath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, repoPath, "true", "first definition")
	firstCommit := apiGitOutput(t, repoPath, "rev-parse", "HEAD")
	firstBlob := apiGitOutput(t, repoPath, "rev-parse", "HEAD:.vectis/jobs/build.json")

	registerBody := map[string]any{
		"repository_id": "vectis-local",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": repoPath,
		"default_ref":   "HEAD",
	}

	registerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", registerBody)
	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	var repoResp struct {
		RepositoryID string `json:"repository_id"`
		Namespace    string `json:"namespace"`
		SourceKind   string `json:"source_kind"`
		CheckoutPath string `json:"checkout_path"`
		Enabled      bool   `json:"enabled"`
	}

	if err := json.NewDecoder(registerRec.Body).Decode(&repoResp); err != nil {
		t.Fatal(err)
	}

	if repoResp.RepositoryID != "vectis-local" || repoResp.Namespace != "/" || repoResp.SourceKind != dal.SourceKindLocalCheckout || repoResp.CheckoutPath != repoPath || !repoResp.Enabled {
		t.Fatalf("repository response mismatch: %+v", repoResp)
	}

	resolveBody := map[string]any{
		"path": ".vectis/jobs/build.json",
	}

	resolveRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/vectis-local/definitions/resolve", resolveBody)
	if resolveRec.Code != http.StatusOK {
		t.Fatalf("resolve source definition: status=%d body=%s", resolveRec.Code, resolveRec.Body.String())
	}

	resolveResp := decodeResolvedSourceDefinitionResponse(t, resolveRec)
	if resolveResp.RepositoryID != "vectis-local" || resolveResp.Source.ResolvedCommit != firstCommit || resolveResp.Source.BlobSHA != firstBlob {
		t.Fatalf("resolve response mismatch: %+v", resolveResp)
	}

	var resolvedJob api.Job
	if err := json.Unmarshal(resolveResp.Definition, &resolvedJob); err != nil {
		t.Fatalf("resolved definition JSON: %v", err)
	}

	if resolvedJob.GetRoot().GetWith()["command"] != "true" {
		t.Fatalf("resolved definition command: got %+v", resolvedJob.GetRoot().GetWith())
	}

	if _, _, err := repos.Jobs().GetDefinition(context.Background(), "build"); !dal.IsNotFound(err) {
		t.Fatalf("resolve should not create stored job, got err=%v", err)
	}

	createBody := map[string]any{
		"repository_id": "vectis-local",
		"path":          ".vectis/jobs/build.json",
	}

	createRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/jobs/source/build", createBody)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create source job: status=%d body=%s", createRec.Code, createRec.Body.String())
	}

	createResp := decodeSourceJobResponse(t, createRec)
	if createResp.JobID != "build" || createResp.Version != 1 || createResp.Source.ResolvedCommit != firstCommit || createResp.Source.BlobSHA != firstBlob {
		t.Fatalf("create response mismatch: %+v", createResp)
	}

	if createResp.DefinitionHash == "" {
		t.Fatal("expected definition hash")
	}

	definitionJSON, version, err := repos.Jobs().GetDefinition(context.Background(), "build")
	if err != nil {
		t.Fatalf("GetDefinition: %v", err)
	}

	if version != 1 {
		t.Fatalf("stored version: got %d, want 1", version)
	}

	var job api.Job
	if err := json.Unmarshal([]byte(definitionJSON), &job); err != nil {
		t.Fatalf("definition JSON: %v", err)
	}

	if job.GetRoot().GetWith()["command"] != "true" {
		t.Fatalf("stored definition command: got %+v", job.GetRoot().GetWith())
	}

	sourceRec, err := repos.Sources().GetDefinitionSource(context.Background(), "build", 1)
	if err != nil {
		t.Fatalf("GetDefinitionSource v1: %v", err)
	}

	if sourceRec.RepositoryID != "vectis-local" || sourceRec.RequestedRef != "HEAD" || sourceRec.ResolvedCommit != firstCommit || sourceRec.BlobSHA != firstBlob {
		t.Fatalf("stored source provenance mismatch: %+v", sourceRec)
	}

	getSourceRec := httptest.NewRecorder()
	getSourceReq := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/build/source", nil)
	handler.ServeHTTP(getSourceRec, getSourceReq)
	if getSourceRec.Code != http.StatusOK {
		t.Fatalf("get current job source: status=%d body=%s", getSourceRec.Code, getSourceRec.Body.String())
	}

	getSourceResp := decodeSourceJobResponse(t, getSourceRec)
	if getSourceResp.JobID != "build" || getSourceResp.Version != 1 || getSourceResp.Source.ResolvedCommit != firstCommit || getSourceResp.Source.BlobSHA != firstBlob {
		t.Fatalf("current job source response mismatch: %+v", getSourceResp)
	}

	writeAPIJobDefinitionAndCommit(t, repoPath, "false", "second definition")
	secondCommit := apiGitOutput(t, repoPath, "rev-parse", "HEAD")

	resolveOldRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/vectis-local/definitions/resolve", map[string]any{
		"ref":  firstCommit,
		"path": ".vectis/jobs/build.json",
	})

	if resolveOldRec.Code != http.StatusOK {
		t.Fatalf("resolve old source definition: status=%d body=%s", resolveOldRec.Code, resolveOldRec.Body.String())
	}

	resolveOldResp := decodeResolvedSourceDefinitionResponse(t, resolveOldRec)
	if resolveOldResp.Source.ResolvedCommit != firstCommit {
		t.Fatalf("old resolve commit: got %q, want %q", resolveOldResp.Source.ResolvedCommit, firstCommit)
	}

	if err := json.Unmarshal(resolveOldResp.Definition, &resolvedJob); err != nil {
		t.Fatalf("old resolved definition JSON: %v", err)
	}

	if resolvedJob.GetRoot().GetWith()["command"] != "true" {
		t.Fatalf("old resolved definition command: got %+v", resolvedJob.GetRoot().GetWith())
	}

	updateRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/jobs/source/build", createBody)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update source job: status=%d body=%s", updateRec.Code, updateRec.Body.String())
	}

	updateResp := decodeSourceJobResponse(t, updateRec)
	if updateResp.JobID != "build" || updateResp.Version != 2 || updateResp.Source.ResolvedCommit != secondCommit {
		t.Fatalf("update response mismatch: %+v", updateResp)
	}

	sourceRec, err = repos.Sources().GetDefinitionSource(context.Background(), "build", 2)
	if err != nil {
		t.Fatalf("GetDefinitionSource v2: %v", err)
	}

	if sourceRec.ResolvedCommit != secondCommit {
		t.Fatalf("v2 commit: got %q, want %q", sourceRec.ResolvedCommit, secondCommit)
	}

	getSourceRec = httptest.NewRecorder()
	getSourceReq = httptest.NewRequest(http.MethodGet, "/api/v1/jobs/build/source", nil)
	handler.ServeHTTP(getSourceRec, getSourceReq)
	if getSourceRec.Code != http.StatusOK {
		t.Fatalf("get updated job source: status=%d body=%s", getSourceRec.Code, getSourceRec.Body.String())
	}

	getSourceResp = decodeSourceJobResponse(t, getSourceRec)
	if getSourceResp.JobID != "build" || getSourceResp.Version != 2 || getSourceResp.Source.ResolvedCommit != secondCommit {
		t.Fatalf("updated job source response mismatch: %+v", getSourceResp)
	}

	getSourceRec = httptest.NewRecorder()
	getSourceReq = httptest.NewRequest(http.MethodGet, "/api/v1/jobs/build/source?version=1", nil)
	handler.ServeHTTP(getSourceRec, getSourceReq)
	if getSourceRec.Code != http.StatusOK {
		t.Fatalf("get historical job source: status=%d body=%s", getSourceRec.Code, getSourceRec.Body.String())
	}

	getSourceResp = decodeSourceJobResponse(t, getSourceRec)
	if getSourceResp.JobID != "build" || getSourceResp.Version != 1 || getSourceResp.Source.ResolvedCommit != firstCommit {
		t.Fatalf("historical job source response mismatch: %+v", getSourceResp)
	}
}

func TestAPIServer_CreateJobFromSourceRejectsDisabledRepository(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, db := setupTestServer(t)
	handler := server.Handler()
	repoPath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, repoPath, "true", "definition")

	repos := dal.NewSQLRepositories(db)
	if _, err := repos.Sources().CreateRepository(context.Background(), dal.SourceRepositoryRecord{
		RepositoryID: "disabled-repo",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: repoPath,
		DefaultRef:   "HEAD",
		Enabled:      false,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	body := map[string]any{
		"repository_id": "disabled-repo",
		"path":          ".vectis/jobs/build.json",
	}

	rec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/jobs/source/build", body)
	assertAPIError(t, rec, http.StatusConflict, "source_repository_disabled")

	resolveRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/disabled-repo/definitions/resolve", map[string]any{
		"path": ".vectis/jobs/build.json",
	})

	assertAPIError(t, resolveRec, http.StatusConflict, "source_repository_disabled")

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/disabled-repo", nil)
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get disabled source repository: status=%d body=%s", getRec.Code, getRec.Body.String())
	}
}

func TestAPIServer_GetJobSourceReturnsNotFoundForPlainJob(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, db := setupTestServer(t)
	handler := server.Handler()
	insertStoredJobForTest(t, db, "plain", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/plain/source", nil)
	handler.ServeHTTP(rec, req)
	assertAPIError(t, rec, http.StatusNotFound, "job_source_not_found")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/jobs/plain/source?version=99", nil)
	handler.ServeHTTP(rec, req)
	assertAPIError(t, rec, http.StatusNotFound, "job_version_not_found")
}

func decodeResolvedSourceDefinitionResponse(t *testing.T, rec *httptest.ResponseRecorder) struct {
	RepositoryID   string          `json:"repository_id"`
	DefinitionHash string          `json:"definition_hash"`
	Definition     json.RawMessage `json:"definition"`
	Source         struct {
		RepositoryID   string `json:"repository_id"`
		RequestedRef   string `json:"requested_ref"`
		ResolvedCommit string `json:"resolved_commit"`
		Path           string `json:"path"`
		BlobSHA        string `json:"blob_sha"`
	} `json:"source"`
} {
	t.Helper()

	var out struct {
		RepositoryID   string          `json:"repository_id"`
		DefinitionHash string          `json:"definition_hash"`
		Definition     json.RawMessage `json:"definition"`
		Source         struct {
			RepositoryID   string `json:"repository_id"`
			RequestedRef   string `json:"requested_ref"`
			ResolvedCommit string `json:"resolved_commit"`
			Path           string `json:"path"`
			BlobSHA        string `json:"blob_sha"`
		} `json:"source"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	return out
}

func decodeSourceJobResponse(t *testing.T, rec *httptest.ResponseRecorder) struct {
	JobID          string `json:"job_id"`
	Version        int    `json:"version"`
	DefinitionHash string `json:"definition_hash"`
	Source         struct {
		RepositoryID   string `json:"repository_id"`
		RequestedRef   string `json:"requested_ref"`
		ResolvedCommit string `json:"resolved_commit"`
		Path           string `json:"path"`
		BlobSHA        string `json:"blob_sha"`
	} `json:"source"`
} {
	t.Helper()

	var out struct {
		JobID          string `json:"job_id"`
		Version        int    `json:"version"`
		DefinitionHash string `json:"definition_hash"`
		Source         struct {
			RepositoryID   string `json:"repository_id"`
			RequestedRef   string `json:"requested_ref"`
			ResolvedCommit string `json:"resolved_commit"`
			Path           string `json:"path"`
			BlobSHA        string `json:"blob_sha"`
		} `json:"source"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	return out
}

func doJSONRequest(t *testing.T, handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	return rec
}

func initAPIGitRepo(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}

	repo := t.TempDir()
	apiGit(t, repo, "init")
	apiGit(t, repo, "config", "user.name", "Vectis Test")
	apiGit(t, repo, "config", "user.email", "vectis@example.invalid")
	apiGit(t, repo, "config", "commit.gpgsign", "false")

	return repo
}

func writeAPIJobDefinitionAndCommit(t *testing.T, repo, command, message string) {
	t.Helper()

	writeAPIFileAndCommit(t, repo, ".vectis/jobs/build.json", `{
		"root": {"id": "root", "uses": "builtins/shell", "with": {"command": "`+command+`"}}
	}`+"\n", message)
}

func writeAPIFileAndCommit(t *testing.T, repo, name, content, message string) {
	t.Helper()

	path := filepath.Join(repo, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	apiGit(t, repo, "add", name)
	apiGit(t, repo, "commit", "-m", message)
}

func apiGitOutput(t *testing.T, repo string, args ...string) string {
	t.Helper()

	cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}

	return strings.TrimSpace(string(out))
}

func apiGit(t *testing.T, repo string, args ...string) {
	t.Helper()
	_ = apiGitOutput(t, repo, args...)
}
