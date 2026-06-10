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

	"github.com/spf13/viper"
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
		CheckoutMode string `json:"checkout_mode"`
		Enabled      bool   `json:"enabled"`
		Sync         struct {
			Status string `json:"status"`
		} `json:"sync"`
	}

	if err := json.NewDecoder(registerRec.Body).Decode(&repoResp); err != nil {
		t.Fatal(err)
	}

	if repoResp.RepositoryID != "vectis-local" ||
		repoResp.Namespace != "/" ||
		repoResp.SourceKind != dal.SourceKindLocalCheckout ||
		repoResp.CheckoutPath != repoPath ||
		repoResp.CheckoutMode != dal.SourceCheckoutModeExternal ||
		repoResp.Sync.Status != dal.SourceSyncStatusNever ||
		!repoResp.Enabled {
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

	getSourceDefinitionRec := httptest.NewRecorder()
	getSourceDefinitionReq := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/build/source/definition?version=1", nil)
	handler.ServeHTTP(getSourceDefinitionRec, getSourceDefinitionReq)
	if getSourceDefinitionRec.Code != http.StatusOK {
		t.Fatalf("get historical source definition: status=%d body=%s", getSourceDefinitionRec.Code, getSourceDefinitionRec.Body.String())
	}

	getSourceDefinitionResp := decodeSourceJobDefinitionResponse(t, getSourceDefinitionRec)
	if getSourceDefinitionResp.JobID != "build" ||
		getSourceDefinitionResp.Version != 1 ||
		getSourceDefinitionResp.DefinitionHash != createResp.DefinitionHash ||
		getSourceDefinitionResp.Source.ResolvedCommit != firstCommit ||
		getSourceDefinitionResp.Source.BlobSHA != firstBlob {
		t.Fatalf("historical source definition response mismatch: %+v", getSourceDefinitionResp)
	}

	if err := json.Unmarshal(getSourceDefinitionResp.Definition, &resolvedJob); err != nil {
		t.Fatalf("historical source definition JSON: %v", err)
	}

	if resolvedJob.GetRoot().GetWith()["command"] != "true" {
		t.Fatalf("historical source definition command: got %+v", resolvedJob.GetRoot().GetWith())
	}

	getSourceDefinitionRec = httptest.NewRecorder()
	getSourceDefinitionReq = httptest.NewRequest(http.MethodGet, "/api/v1/jobs/build/source/definition", nil)
	handler.ServeHTTP(getSourceDefinitionRec, getSourceDefinitionReq)
	if getSourceDefinitionRec.Code != http.StatusOK {
		t.Fatalf("get current source definition: status=%d body=%s", getSourceDefinitionRec.Code, getSourceDefinitionRec.Body.String())
	}

	getSourceDefinitionResp = decodeSourceJobDefinitionResponse(t, getSourceDefinitionRec)
	if getSourceDefinitionResp.JobID != "build" ||
		getSourceDefinitionResp.Version != 2 ||
		getSourceDefinitionResp.DefinitionHash != updateResp.DefinitionHash ||
		getSourceDefinitionResp.Source.ResolvedCommit != secondCommit {
		t.Fatalf("current source definition response mismatch: %+v", getSourceDefinitionResp)
	}

	if err := json.Unmarshal(getSourceDefinitionResp.Definition, &resolvedJob); err != nil {
		t.Fatalf("current source definition JSON: %v", err)
	}

	if resolvedJob.GetRoot().GetWith()["command"] != "false" {
		t.Fatalf("current source definition command: got %+v", resolvedJob.GetRoot().GetWith())
	}

	triggerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/jobs/trigger/build", map[string]any{})
	if triggerRec.Code != http.StatusAccepted {
		t.Fatalf("trigger source-backed job: status=%d body=%s", triggerRec.Code, triggerRec.Body.String())
	}

	var triggerResp struct {
		RunID string `json:"run_id"`
	}

	if err := json.NewDecoder(triggerRec.Body).Decode(&triggerResp); err != nil {
		t.Fatal(err)
	}

	if triggerResp.RunID == "" {
		t.Fatal("expected trigger response run_id")
	}

	listRunsRec := httptest.NewRecorder()
	listRunsReq := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/build/runs", nil)
	handler.ServeHTTP(listRunsRec, listRunsReq)
	if listRunsRec.Code != http.StatusOK {
		t.Fatalf("list source-backed job runs: status=%d body=%s", listRunsRec.Code, listRunsRec.Body.String())
	}

	var runsResp struct {
		Data []struct {
			RunID             string `json:"run_id"`
			DefinitionVersion int    `json:"definition_version"`
			Source            *struct {
				RepositoryID   string `json:"repository_id"`
				RequestedRef   string `json:"requested_ref"`
				ResolvedCommit string `json:"resolved_commit"`
				Path           string `json:"path"`
				BlobSHA        string `json:"blob_sha"`
			} `json:"source,omitempty"`
		} `json:"data"`
	}

	if err := json.NewDecoder(listRunsRec.Body).Decode(&runsResp); err != nil {
		t.Fatal(err)
	}

	if len(runsResp.Data) != 1 {
		t.Fatalf("expected one run row, got %+v", runsResp.Data)
	}

	if runsResp.Data[0].RunID != triggerResp.RunID || runsResp.Data[0].DefinitionVersion != 2 || runsResp.Data[0].Source == nil || runsResp.Data[0].Source.ResolvedCommit != secondCommit {
		t.Fatalf("run list source provenance mismatch: %+v", runsResp.Data[0])
	}

	getRunRec := httptest.NewRecorder()
	getRunReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+triggerResp.RunID, nil)
	handler.ServeHTTP(getRunRec, getRunReq)
	if getRunRec.Code != http.StatusOK {
		t.Fatalf("get source-backed run: status=%d body=%s", getRunRec.Code, getRunRec.Body.String())
	}

	var runResp struct {
		RunID             string `json:"run_id"`
		DefinitionVersion int    `json:"definition_version"`
		Source            *struct {
			RepositoryID   string `json:"repository_id"`
			RequestedRef   string `json:"requested_ref"`
			ResolvedCommit string `json:"resolved_commit"`
			Path           string `json:"path"`
			BlobSHA        string `json:"blob_sha"`
		} `json:"source,omitempty"`
	}

	if err := json.NewDecoder(getRunRec.Body).Decode(&runResp); err != nil {
		t.Fatal(err)
	}

	if runResp.RunID != triggerResp.RunID || runResp.DefinitionVersion != 2 || runResp.Source == nil || runResp.Source.ResolvedCommit != secondCommit {
		t.Fatalf("run detail source provenance mismatch: %+v", runResp)
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

func TestAPIServer_GetSourceRepositoryStatus(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, _ := setupTestServer(t)
	handler := server.Handler()
	repoPath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, repoPath, "true", "definition")
	commit := apiGitOutput(t, repoPath, "rev-parse", "HEAD")

	registerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "vectis-local",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": repoPath,
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	statusRec := httptest.NewRecorder()
	statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/status", nil)
	handler.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("get source repository status: status=%d body=%s", statusRec.Code, statusRec.Body.String())
	}

	statusResp := decodeSourceRepositoryStatusResponse(t, statusRec)
	if statusResp.RepositoryID != "vectis-local" || statusResp.Namespace != "/" || !statusResp.Enabled || statusResp.Status != "ok" {
		t.Fatalf("status response mismatch: %+v", statusResp)
	}

	if statusResp.CheckoutPath != repoPath ||
		statusResp.CheckoutMode != dal.SourceCheckoutModeExternal ||
		!statusResp.PathExists ||
		!statusResp.PathIsDirectory ||
		!statusResp.GitRepository ||
		statusResp.Sync.Status != dal.SourceSyncStatusNever ||
		statusResp.DefaultRef != "HEAD" ||
		!statusResp.DefaultRefResolved ||
		statusResp.ResolvedCommit != commit ||
		statusResp.Error != nil {
		t.Fatalf("checkout status mismatch: %+v", statusResp)
	}

	disableRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/vectis-local", map[string]any{
		"enabled": false,
	})

	if disableRec.Code != http.StatusOK {
		t.Fatalf("disable source repository: status=%d body=%s", disableRec.Code, disableRec.Body.String())
	}

	statusRec = httptest.NewRecorder()
	statusReq = httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/status", nil)
	handler.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("get disabled source repository status: status=%d body=%s", statusRec.Code, statusRec.Body.String())
	}

	statusResp = decodeSourceRepositoryStatusResponse(t, statusRec)
	if statusResp.Enabled || statusResp.Status != "disabled" || statusResp.ResolvedCommit != commit || statusResp.Error != nil {
		t.Fatalf("disabled status mismatch: %+v", statusResp)
	}

	nonGitPath := t.TempDir()
	registerRec = doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "not-git",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": nonGitPath,
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register non-git source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	statusRec = httptest.NewRecorder()
	statusReq = httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/not-git/status", nil)
	handler.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("get non-git source repository status: status=%d body=%s", statusRec.Code, statusRec.Body.String())
	}

	statusResp = decodeSourceRepositoryStatusResponse(t, statusRec)
	if statusResp.Status != "unavailable" ||
		!statusResp.PathExists ||
		!statusResp.PathIsDirectory ||
		statusResp.GitRepository ||
		statusResp.Error == nil ||
		statusResp.Error.Code != "not_git_checkout" {
		t.Fatalf("non-git status mismatch: %+v", statusResp)
	}
}

func TestAPIServer_CreateManagedSourceRepositoryDerivesCheckoutPath(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	viper.Reset()
	t.Cleanup(viper.Reset)

	checkoutRoot := t.TempDir()
	viper.Set("source.checkout_root", checkoutRoot)

	server, _, _, db := setupTestServer(t)
	handler := server.Handler()

	rec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "github.com/acme/Big Repo.git",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_mode": dal.SourceCheckoutModeManaged,
		"default_ref":   "main",
	})

	if rec.Code != http.StatusCreated {
		t.Fatalf("create managed source repository: status=%d body=%s", rec.Code, rec.Body.String())
	}

	resp := decodeSourceRepositoryResponse(t, rec)
	if resp.RepositoryID != "github.com/acme/Big Repo.git" ||
		resp.CheckoutMode != dal.SourceCheckoutModeManaged ||
		resp.CheckoutPath == "" ||
		resp.DefaultRef != "main" {
		t.Fatalf("managed repository response mismatch: %+v", resp)
	}

	rel, err := filepath.Rel(checkoutRoot, resp.CheckoutPath)
	if err != nil {
		t.Fatalf("managed checkout path rel: %v", err)
	}

	if strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		t.Fatalf("managed checkout path should be under configured root: root=%q path=%q rel=%q", checkoutRoot, resp.CheckoutPath, rel)
	}

	if base := filepath.Base(resp.CheckoutPath); !strings.HasPrefix(base, "github.com-acme-big-repo.git-") {
		t.Fatalf("managed checkout path should include sanitized repository id, got %q", base)
	}

	stored, err := dal.NewSQLRepositories(db).Sources().GetRepository(context.Background(), "github.com/acme/Big Repo.git")
	if err != nil {
		t.Fatalf("GetRepository: %v", err)
	}

	if stored.CheckoutPath != resp.CheckoutPath || stored.CheckoutMode != dal.SourceCheckoutModeManaged {
		t.Fatalf("stored managed repository mismatch: %+v response=%+v", stored, resp)
	}
}

func TestAPIServer_SyncSourceRepository(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, _ := setupTestServer(t)
	handler := server.Handler()
	repoPath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, repoPath, "true", "definition")
	commit := apiGitOutput(t, repoPath, "rev-parse", "HEAD")

	registerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "vectis-local",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": repoPath,
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	syncRec := httptest.NewRecorder()
	syncReq := httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/vectis-local/sync", nil)
	handler.ServeHTTP(syncRec, syncReq)
	if syncRec.Code != http.StatusOK {
		t.Fatalf("sync source repository: status=%d body=%s", syncRec.Code, syncRec.Body.String())
	}

	syncResp := decodeSourceRepositoryResponse(t, syncRec)
	if syncResp.RepositoryID != "vectis-local" ||
		syncResp.Sync.Status != dal.SourceSyncStatusSucceeded ||
		syncResp.Sync.Ref != "HEAD" ||
		syncResp.Sync.Commit != commit ||
		syncResp.Sync.Error != "" ||
		syncResp.Sync.LastStartedAtUnix <= 0 ||
		syncResp.Sync.LastFinishedAtUnix < syncResp.Sync.LastStartedAtUnix {
		t.Fatalf("sync response mismatch: %+v", syncResp)
	}

	statusRec := httptest.NewRecorder()
	statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/status", nil)
	handler.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("get synced source repository status: status=%d body=%s", statusRec.Code, statusRec.Body.String())
	}

	statusResp := decodeSourceRepositoryStatusResponse(t, statusRec)
	if statusResp.Sync.Status != dal.SourceSyncStatusSucceeded ||
		statusResp.Sync.Ref != "HEAD" ||
		statusResp.Sync.Commit != commit {
		t.Fatalf("status sync response mismatch: %+v", statusResp)
	}

	nonGitPath := t.TempDir()
	registerRec = doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "not-git",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": nonGitPath,
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register non-git source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	syncRec = httptest.NewRecorder()
	syncReq = httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/not-git/sync", nil)
	handler.ServeHTTP(syncRec, syncReq)
	if syncRec.Code != http.StatusOK {
		t.Fatalf("sync non-git source repository: status=%d body=%s", syncRec.Code, syncRec.Body.String())
	}

	syncResp = decodeSourceRepositoryResponse(t, syncRec)
	if syncResp.RepositoryID != "not-git" ||
		syncResp.Sync.Status != dal.SourceSyncStatusFailed ||
		syncResp.Sync.Ref != "HEAD" ||
		syncResp.Sync.Commit != "" ||
		!strings.Contains(syncResp.Sync.Error, "not_git_checkout") ||
		syncResp.Sync.LastStartedAtUnix <= 0 ||
		syncResp.Sync.LastFinishedAtUnix < syncResp.Sync.LastStartedAtUnix {
		t.Fatalf("failed sync response mismatch: %+v", syncResp)
	}
}

func TestAPIServer_SyncManagedSourceRepositoryClonesAndFetches(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	viper.Reset()
	t.Cleanup(viper.Reset)

	checkoutRoot := t.TempDir()
	viper.Set("source.checkout_root", checkoutRoot)

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()
	remotePath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, remotePath, "true", "first definition")
	firstCommit := apiGitOutput(t, remotePath, "rev-parse", "HEAD")

	registerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "managed-repo",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_mode": dal.SourceCheckoutModeManaged,
		"canonical_url": remotePath,
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register managed source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	registerResp := decodeSourceRepositoryResponse(t, registerRec)
	if _, err := os.Stat(registerResp.CheckoutPath); !os.IsNotExist(err) {
		t.Fatalf("managed checkout should not exist before sync, path=%q err=%v", registerResp.CheckoutPath, err)
	}

	syncRec := httptest.NewRecorder()
	syncReq := httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/managed-repo/sync", nil)
	handler.ServeHTTP(syncRec, syncReq)
	if syncRec.Code != http.StatusOK {
		t.Fatalf("sync managed source repository: status=%d body=%s", syncRec.Code, syncRec.Body.String())
	}

	syncResp := decodeSourceRepositoryResponse(t, syncRec)
	if syncResp.Sync.Status != dal.SourceSyncStatusSucceeded ||
		syncResp.Sync.Ref != "HEAD" ||
		syncResp.Sync.Commit != firstCommit ||
		syncResp.Sync.Error != "" {
		t.Fatalf("managed sync response mismatch: %+v", syncResp)
	}

	if _, err := os.Stat(syncResp.CheckoutPath); err != nil {
		t.Fatalf("managed checkout should exist after sync: %v", err)
	}

	writeAPIJobDefinitionAndCommit(t, remotePath, "false", "second definition")
	secondCommit := apiGitOutput(t, remotePath, "rev-parse", "HEAD")

	syncRec = httptest.NewRecorder()
	syncReq = httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/managed-repo/sync", nil)
	handler.ServeHTTP(syncRec, syncReq)
	if syncRec.Code != http.StatusOK {
		t.Fatalf("resync managed source repository: status=%d body=%s", syncRec.Code, syncRec.Body.String())
	}

	syncResp = decodeSourceRepositoryResponse(t, syncRec)
	if syncResp.Sync.Status != dal.SourceSyncStatusSucceeded || syncResp.Sync.Commit != secondCommit {
		t.Fatalf("managed resync response mismatch: %+v", syncResp)
	}

	resolveRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/managed-repo/definitions/resolve", map[string]any{
		"path": ".vectis/jobs/build.json",
	})

	if resolveRec.Code != http.StatusOK {
		t.Fatalf("resolve from managed checkout: status=%d body=%s", resolveRec.Code, resolveRec.Body.String())
	}

	resolveResp := decodeResolvedSourceDefinitionResponse(t, resolveRec)
	if resolveResp.Source.ResolvedCommit != secondCommit {
		t.Fatalf("managed resolve commit mismatch: %+v", resolveResp)
	}

	var job api.Job
	if err := json.Unmarshal(resolveResp.Definition, &job); err != nil {
		t.Fatalf("managed resolved definition JSON: %v", err)
	}

	if job.GetRoot().GetWith()["command"] != "false" {
		t.Fatalf("managed resolved definition command: got %+v", job.GetRoot().GetWith())
	}

	defaultBranch := apiGitOutput(t, remotePath, "branch", "--show-current")
	apiGit(t, remotePath, "checkout", "-b", "feature/source-ref")
	writeAPIJobDefinitionAndCommit(t, remotePath, "feature", "feature definition")
	writeAPIFileAndCommit(t, remotePath, ".vectis/jobs/README.md", "not a job definition\n", "feature note")
	featureCommit := apiGitOutput(t, remotePath, "rev-parse", "HEAD")
	featureBlob := apiGitOutput(t, remotePath, "rev-parse", "HEAD:.vectis/jobs/build.json")
	apiGit(t, remotePath, "checkout", defaultBranch)

	syncRec = httptest.NewRecorder()
	syncReq = httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/managed-repo/sync", nil)
	handler.ServeHTTP(syncRec, syncReq)
	if syncRec.Code != http.StatusOK {
		t.Fatalf("sync managed feature branch: status=%d body=%s", syncRec.Code, syncRec.Body.String())
	}

	resolveRec = doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/managed-repo/definitions/resolve", map[string]any{
		"ref":  "feature/source-ref",
		"path": ".vectis/jobs/build.json",
	})

	if resolveRec.Code != http.StatusOK {
		t.Fatalf("resolve managed feature branch: status=%d body=%s", resolveRec.Code, resolveRec.Body.String())
	}

	resolveResp = decodeResolvedSourceDefinitionResponse(t, resolveRec)
	if resolveResp.Source.RequestedRef != "feature/source-ref" || resolveResp.Source.ResolvedCommit != featureCommit {
		t.Fatalf("managed feature resolve mismatch: %+v", resolveResp)
	}

	if err := json.Unmarshal(resolveResp.Definition, &job); err != nil {
		t.Fatalf("managed feature definition JSON: %v", err)
	}

	if job.GetRoot().GetWith()["command"] != "feature" {
		t.Fatalf("managed feature definition command: got %+v", job.GetRoot().GetWith())
	}

	branchesRec := httptest.NewRecorder()
	branchesReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/refs/branches?prefix=feature/&limit=5", nil)
	handler.ServeHTTP(branchesRec, branchesReq)
	if branchesRec.Code != http.StatusOK {
		t.Fatalf("list managed feature branches: status=%d body=%s", branchesRec.Code, branchesRec.Body.String())
	}

	var branchesResp struct {
		RepositoryID string `json:"repository_id"`
		Prefix       string `json:"prefix"`
		Limit        int    `json:"limit"`
		Branches     []struct {
			Name   string `json:"name"`
			Ref    string `json:"ref"`
			Commit string `json:"commit"`
			Remote string `json:"remote"`
		} `json:"branches"`
	}

	if err := json.NewDecoder(branchesRec.Body).Decode(&branchesResp); err != nil {
		t.Fatalf("decode managed branches: %v", err)
	}

	if branchesResp.RepositoryID != "managed-repo" || branchesResp.Prefix != "feature/" || branchesResp.Limit != 5 || len(branchesResp.Branches) != 1 {
		t.Fatalf("managed branches response mismatch: %+v", branchesResp)
	}

	branch := branchesResp.Branches[0]
	if branch.Name != "feature/source-ref" ||
		branch.Ref != "refs/remotes/origin/feature/source-ref" ||
		branch.Commit != featureCommit ||
		branch.Remote != "origin" {
		t.Fatalf("managed branch mismatch: %+v", branch)
	}

	treeRec := httptest.NewRecorder()
	treeReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/tree?ref=feature/source-ref&path=.vectis/jobs&limit=10", nil)
	handler.ServeHTTP(treeRec, treeReq)
	if treeRec.Code != http.StatusOK {
		t.Fatalf("list managed source tree: status=%d body=%s", treeRec.Code, treeRec.Body.String())
	}

	var treeResp struct {
		RepositoryID   string `json:"repository_id"`
		RequestedRef   string `json:"requested_ref"`
		ResolvedCommit string `json:"resolved_commit"`
		Path           string `json:"path"`
		Recursive      bool   `json:"recursive"`
		Limit          int    `json:"limit"`
		Entries        []struct {
			Path      string `json:"path"`
			Name      string `json:"name"`
			Type      string `json:"type"`
			Mode      string `json:"mode"`
			ObjectSHA string `json:"object_sha"`
			SizeBytes int64  `json:"size_bytes"`
		} `json:"entries"`
	}

	if err := json.NewDecoder(treeRec.Body).Decode(&treeResp); err != nil {
		t.Fatalf("decode managed tree: %v", err)
	}

	if treeResp.RepositoryID != "managed-repo" ||
		treeResp.RequestedRef != "feature/source-ref" ||
		treeResp.ResolvedCommit != featureCommit ||
		treeResp.Path != ".vectis/jobs" ||
		treeResp.Recursive ||
		treeResp.Limit != 10 ||
		len(treeResp.Entries) != 2 {
		t.Fatalf("managed tree response mismatch: %+v", treeResp)
	}

	treeEntries := map[string]struct {
		Path      string `json:"path"`
		Name      string `json:"name"`
		Type      string `json:"type"`
		Mode      string `json:"mode"`
		ObjectSHA string `json:"object_sha"`
		SizeBytes int64  `json:"size_bytes"`
	}{}

	for _, entry := range treeResp.Entries {
		treeEntries[entry.Path] = entry
	}

	entry := treeEntries[".vectis/jobs/build.json"]
	if entry.Path != ".vectis/jobs/build.json" ||
		entry.Name != "build.json" ||
		entry.Type != "blob" ||
		entry.Mode != "100644" ||
		entry.ObjectSHA != featureBlob ||
		entry.SizeBytes == 0 {
		t.Fatalf("managed tree entry mismatch: %+v", entry)
	}

	definitionsRec := httptest.NewRecorder()
	definitionsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/definitions?ref=feature/source-ref&path=.vectis/jobs&limit=5", nil)
	handler.ServeHTTP(definitionsRec, definitionsReq)
	if definitionsRec.Code != http.StatusOK {
		t.Fatalf("list managed source definitions: status=%d body=%s", definitionsRec.Code, definitionsRec.Body.String())
	}

	var definitionsResp struct {
		RepositoryID   string `json:"repository_id"`
		RequestedRef   string `json:"requested_ref"`
		ResolvedCommit string `json:"resolved_commit"`
		Path           string `json:"path"`
		Limit          int    `json:"limit"`
		Definitions    []struct {
			Path      string `json:"path"`
			Name      string `json:"name"`
			BlobSHA   string `json:"blob_sha"`
			SizeBytes int64  `json:"size_bytes"`
		} `json:"definitions"`
	}

	if err := json.NewDecoder(definitionsRec.Body).Decode(&definitionsResp); err != nil {
		t.Fatalf("decode managed definitions: %v", err)
	}

	if definitionsResp.RepositoryID != "managed-repo" ||
		definitionsResp.RequestedRef != "feature/source-ref" ||
		definitionsResp.ResolvedCommit != featureCommit ||
		definitionsResp.Path != ".vectis/jobs" ||
		definitionsResp.Limit != 5 ||
		len(definitionsResp.Definitions) != 1 {
		t.Fatalf("managed definitions response mismatch: %+v", definitionsResp)
	}

	definition := definitionsResp.Definitions[0]
	if definition.Path != ".vectis/jobs/build.json" ||
		definition.Name != "build.json" ||
		definition.BlobSHA != featureBlob ||
		definition.SizeBytes == 0 {
		t.Fatalf("managed definition file mismatch: %+v", definition)
	}

	dryRunImportRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/managed-repo/definitions/import", map[string]any{
		"ref":     "feature/source-ref",
		"path":    ".vectis/jobs",
		"limit":   5,
		"dry_run": true,
	})

	if dryRunImportRec.Code != http.StatusOK {
		t.Fatalf("dry-run managed source import: status=%d body=%s", dryRunImportRec.Code, dryRunImportRec.Body.String())
	}

	importResp := decodeSourceDefinitionsImportResponse(t, dryRunImportRec)
	if importResp.RepositoryID != "managed-repo" ||
		importResp.RequestedRef != "feature/source-ref" ||
		importResp.ResolvedCommit != featureCommit ||
		importResp.Path != ".vectis/jobs" ||
		!importResp.DryRun ||
		importResp.Limit != 5 ||
		importResp.Summary.Total != 1 ||
		importResp.Summary.WouldCreate != 1 ||
		len(importResp.Results) != 1 {
		t.Fatalf("dry-run import response mismatch: %+v", importResp)
	}

	if got := importResp.Results[0]; got.JobID != "build" || got.Status != "would_create" || got.Version != 1 || got.DefinitionHash == "" || got.Source.Path != ".vectis/jobs/build.json" || got.Source.BlobSHA != featureBlob {
		t.Fatalf("dry-run import result mismatch: %+v", got)
	}

	if _, _, err := repos.Jobs().GetDefinition(context.Background(), "build"); !dal.IsNotFound(err) {
		t.Fatalf("dry-run import should not create job, got err=%v", err)
	}

	applyImportRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/managed-repo/definitions/import", map[string]any{
		"ref":   "feature/source-ref",
		"path":  ".vectis/jobs",
		"limit": 5,
	})

	if applyImportRec.Code != http.StatusOK {
		t.Fatalf("apply managed source import: status=%d body=%s", applyImportRec.Code, applyImportRec.Body.String())
	}

	importResp = decodeSourceDefinitionsImportResponse(t, applyImportRec)
	if importResp.DryRun ||
		importResp.Summary.Total != 1 ||
		importResp.Summary.Created != 1 ||
		len(importResp.Results) != 1 ||
		importResp.Results[0].JobID != "build" ||
		importResp.Results[0].Status != "created" ||
		importResp.Results[0].Version != 1 {
		t.Fatalf("apply import response mismatch: %+v", importResp)
	}

	definitionJSON, version, err := repos.Jobs().GetDefinition(context.Background(), "build")
	if err != nil {
		t.Fatalf("GetDefinition imported build: %v", err)
	}

	if version != 1 {
		t.Fatalf("imported build version: got %d, want 1", version)
	}

	if err := json.Unmarshal([]byte(definitionJSON), &job); err != nil {
		t.Fatalf("imported build definition JSON: %v", err)
	}

	if job.GetRoot().GetWith()["command"] != "feature" {
		t.Fatalf("imported build command: got %+v", job.GetRoot().GetWith())
	}

	importedSource, err := repos.Sources().GetDefinitionSource(context.Background(), "build", 1)
	if err != nil {
		t.Fatalf("GetDefinitionSource imported build: %v", err)
	}

	if importedSource.RepositoryID != "managed-repo" ||
		importedSource.RequestedRef != "feature/source-ref" ||
		importedSource.ResolvedCommit != featureCommit ||
		importedSource.DefinitionPath != ".vectis/jobs/build.json" ||
		importedSource.BlobSHA != featureBlob {
		t.Fatalf("imported source provenance mismatch: %+v", importedSource)
	}

	unchangedImportRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/managed-repo/definitions/import", map[string]any{
		"ref":   "feature/source-ref",
		"path":  ".vectis/jobs",
		"limit": 5,
	})

	if unchangedImportRec.Code != http.StatusOK {
		t.Fatalf("unchanged managed source import: status=%d body=%s", unchangedImportRec.Code, unchangedImportRec.Body.String())
	}

	importResp = decodeSourceDefinitionsImportResponse(t, unchangedImportRec)
	if importResp.Summary.Unchanged != 1 || len(importResp.Results) != 1 || importResp.Results[0].Status != "unchanged" || importResp.Results[0].Version != 1 {
		t.Fatalf("unchanged import response mismatch: %+v", importResp)
	}
}

func TestAPIServer_TriggerManagedSourceRepositoryJobCreatesRunSnapshot(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	viper.Reset()
	t.Cleanup(viper.Reset)

	checkoutRoot := t.TempDir()
	viper.Set("source.checkout_root", checkoutRoot)

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()
	remotePath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, remotePath, "source-trigger", "source trigger definition")
	commit := apiGitOutput(t, remotePath, "rev-parse", "HEAD")
	blob := apiGitOutput(t, remotePath, "rev-parse", "HEAD:.vectis/jobs/build.json")

	registerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "managed-repo",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_mode": dal.SourceCheckoutModeManaged,
		"canonical_url": remotePath,
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register managed source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	syncRec := httptest.NewRecorder()
	syncReq := httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/managed-repo/sync", nil)
	handler.ServeHTTP(syncRec, syncReq)
	if syncRec.Code != http.StatusOK {
		t.Fatalf("sync managed source repository: status=%d body=%s", syncRec.Code, syncRec.Body.String())
	}

	triggerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/managed-repo/jobs/build/trigger", map[string]any{
		"ref": "HEAD",
	})

	if triggerRec.Code != http.StatusAccepted {
		t.Fatalf("trigger source repository job: status=%d body=%s", triggerRec.Code, triggerRec.Body.String())
	}

	triggerResp := decodeSourceJobTriggerResponse(t, triggerRec)
	if triggerResp.JobID != "build" ||
		triggerResp.RunID == "" ||
		triggerResp.RunIndex != 1 ||
		triggerResp.DefinitionVersion != 1 ||
		triggerResp.DefinitionHash == "" ||
		triggerResp.Source.RepositoryID != "managed-repo" ||
		triggerResp.Source.RequestedRef != "HEAD" ||
		triggerResp.Source.ResolvedCommit != commit ||
		triggerResp.Source.Path != ".vectis/jobs/build.json" ||
		triggerResp.Source.BlobSHA != blob {
		t.Fatalf("source trigger response mismatch: %+v", triggerResp)
	}

	if _, err := repos.Jobs().GetNamespaceID(context.Background(), "build"); !dal.IsNotFound(err) {
		t.Fatalf("source trigger should not create stored job row, got err=%v", err)
	}

	definitionJSON, err := repos.Jobs().GetDefinitionVersion(context.Background(), "build", 1)
	if err != nil {
		t.Fatalf("GetDefinitionVersion source snapshot: %v", err)
	}

	var job api.Job
	if err := json.Unmarshal([]byte(definitionJSON), &job); err != nil {
		t.Fatalf("source snapshot definition JSON: %v", err)
	}

	if job.GetRoot().GetWith()["command"] != "source-trigger" {
		t.Fatalf("source snapshot command: got %+v", job.GetRoot().GetWith())
	}

	sourceRec, err := repos.Sources().GetDefinitionSource(context.Background(), "build", 1)
	if err != nil {
		t.Fatalf("GetDefinitionSource source snapshot: %v", err)
	}

	if sourceRec.RepositoryID != "managed-repo" ||
		sourceRec.RequestedRef != "HEAD" ||
		sourceRec.ResolvedCommit != commit ||
		sourceRec.DefinitionPath != ".vectis/jobs/build.json" ||
		sourceRec.BlobSHA != blob {
		t.Fatalf("source snapshot provenance mismatch: %+v", sourceRec)
	}

	runRec, err := repos.Runs().GetRun(context.Background(), triggerResp.RunID)
	if err != nil {
		t.Fatalf("GetRun source snapshot: %v", err)
	}

	if runRec.JobID != "build" ||
		runRec.RunIndex != 1 ||
		runRec.Status != dal.RunStatusQueued ||
		runRec.DefinitionVersion != 1 ||
		runRec.DefinitionHash != triggerResp.DefinitionHash {
		t.Fatalf("source snapshot run mismatch: %+v", runRec)
	}

	getRunRec := httptest.NewRecorder()
	getRunReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+triggerResp.RunID, nil)
	handler.ServeHTTP(getRunRec, getRunReq)
	if getRunRec.Code != http.StatusOK {
		t.Fatalf("get source repository run: status=%d body=%s", getRunRec.Code, getRunRec.Body.String())
	}

	var runResp struct {
		RunID             string `json:"run_id"`
		DefinitionVersion int    `json:"definition_version"`
		Source            *struct {
			RepositoryID   string `json:"repository_id"`
			RequestedRef   string `json:"requested_ref"`
			ResolvedCommit string `json:"resolved_commit"`
			Path           string `json:"path"`
			BlobSHA        string `json:"blob_sha"`
		} `json:"source,omitempty"`
	}

	if err := json.NewDecoder(getRunRec.Body).Decode(&runResp); err != nil {
		t.Fatal(err)
	}

	if runResp.RunID != triggerResp.RunID ||
		runResp.DefinitionVersion != 1 ||
		runResp.Source == nil ||
		runResp.Source.RepositoryID != "managed-repo" ||
		runResp.Source.ResolvedCommit != commit ||
		runResp.Source.BlobSHA != blob {
		t.Fatalf("source repository run response mismatch: %+v", runResp)
	}

	secondTriggerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/managed-repo/jobs/build/trigger", map[string]any{
		"ref": "HEAD",
	})

	if secondTriggerRec.Code != http.StatusAccepted {
		t.Fatalf("trigger source repository job again: status=%d body=%s", secondTriggerRec.Code, secondTriggerRec.Body.String())
	}

	secondTriggerResp := decodeSourceJobTriggerResponse(t, secondTriggerRec)
	if secondTriggerResp.RunIndex != 2 || secondTriggerResp.DefinitionVersion != 2 {
		t.Fatalf("second source trigger response mismatch: %+v", secondTriggerResp)
	}
}

func TestAPIServer_GetJobSourceDefinitionReadsDisabledRepository(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, _ := setupTestServer(t)
	handler := server.Handler()
	repoPath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, repoPath, "true", "definition")

	registerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "vectis-local",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": repoPath,
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	createRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/jobs/source/build", map[string]any{
		"repository_id": "vectis-local",
		"path":          ".vectis/jobs/build.json",
	})

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create source job: status=%d body=%s", createRec.Code, createRec.Body.String())
	}

	disableRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/vectis-local", map[string]any{
		"enabled": false,
	})

	if disableRec.Code != http.StatusOK {
		t.Fatalf("disable source repository: status=%d body=%s", disableRec.Code, disableRec.Body.String())
	}

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/build/source/definition", nil)
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get source definition from disabled repository: status=%d body=%s", getRec.Code, getRec.Body.String())
	}

	resp := decodeSourceJobDefinitionResponse(t, getRec)
	if resp.JobID != "build" || resp.Version != 1 || resp.Source.RepositoryID != "vectis-local" {
		t.Fatalf("source definition response mismatch: %+v", resp)
	}
}

func TestAPIServer_UpdateSourceRepository(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, _ := setupTestServer(t)
	handler := server.Handler()
	repoPath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, repoPath, "true", "definition")

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

	commit := apiGitOutput(t, repoPath, "rev-parse", "HEAD")
	updateBody := map[string]any{
		"checkout_mode":  dal.SourceCheckoutModeManaged,
		"default_ref":    commit,
		"canonical_url":  "https://example.invalid/vectis.git",
		"credential_ref": "secret://git/vectis",
		"enabled":        false,
	}

	updateRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/vectis-local", updateBody)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update source repository: status=%d body=%s", updateRec.Code, updateRec.Body.String())
	}

	var updateResp struct {
		RepositoryID  string `json:"repository_id"`
		CheckoutPath  string `json:"checkout_path"`
		CheckoutMode  string `json:"checkout_mode"`
		CanonicalURL  string `json:"canonical_url"`
		DefaultRef    string `json:"default_ref"`
		CredentialRef string `json:"credential_ref"`
		Enabled       bool   `json:"enabled"`
		Sync          struct {
			Status string `json:"status"`
		} `json:"sync"`
	}

	if err := json.NewDecoder(updateRec.Body).Decode(&updateResp); err != nil {
		t.Fatal(err)
	}

	if updateResp.RepositoryID != "vectis-local" ||
		updateResp.CheckoutPath != repoPath ||
		updateResp.CheckoutMode != dal.SourceCheckoutModeManaged ||
		updateResp.CanonicalURL != "https://example.invalid/vectis.git" ||
		updateResp.DefaultRef != commit ||
		updateResp.CredentialRef != "secret://git/vectis" ||
		updateResp.Sync.Status != dal.SourceSyncStatusNever ||
		updateResp.Enabled {
		t.Fatalf("update response mismatch: %+v", updateResp)
	}

	resolveRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/vectis-local/definitions/resolve", map[string]any{
		"path": ".vectis/jobs/build.json",
	})

	assertAPIError(t, resolveRec, http.StatusConflict, "source_repository_disabled")
	enableRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/vectis-local", map[string]any{
		"enabled": true,
	})

	if enableRec.Code != http.StatusOK {
		t.Fatalf("enable source repository: status=%d body=%s", enableRec.Code, enableRec.Body.String())
	}

	resolveRec = doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/vectis-local/definitions/resolve", map[string]any{
		"path": ".vectis/jobs/build.json",
	})

	if resolveRec.Code != http.StatusOK {
		t.Fatalf("resolve after enable: status=%d body=%s", resolveRec.Code, resolveRec.Body.String())
	}

	resolveResp := decodeResolvedSourceDefinitionResponse(t, resolveRec)
	if resolveResp.Source.RequestedRef != commit || resolveResp.Source.ResolvedCommit != commit {
		t.Fatalf("resolve after update should use updated default ref: %+v", resolveResp)
	}

	missingRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/missing", map[string]any{
		"enabled": false,
	})

	assertAPIError(t, missingRec, http.StatusNotFound, "source_repository_not_found")

	invalidModeRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/vectis-local", map[string]any{
		"checkout_mode": "magic",
	})

	assertAPIError(t, invalidModeRec, http.StatusBadRequest, "unsupported_checkout_mode")
}

func TestAPIServer_UpdateSourceRepositoryRejectsDuplicateCheckoutPath(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, _ := setupTestServer(t)
	handler := server.Handler()
	firstPath := initAPIGitRepo(t)
	secondPath := initAPIGitRepo(t)

	for _, body := range []map[string]any{
		{
			"repository_id": "first",
			"source_kind":   dal.SourceKindLocalCheckout,
			"checkout_path": firstPath,
			"default_ref":   "HEAD",
		},
		{
			"repository_id": "second",
			"source_kind":   dal.SourceKindLocalCheckout,
			"checkout_path": secondPath,
			"default_ref":   "HEAD",
		},
	} {
		rec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", body)
		if rec.Code != http.StatusCreated {
			t.Fatalf("register source repository: status=%d body=%s", rec.Code, rec.Body.String())
		}
	}

	updateRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/second", map[string]any{
		"checkout_path": firstPath,
	})

	assertAPIError(t, updateRec, http.StatusConflict, "source_repository_conflict")
	createRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "alias",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": firstPath,
	})

	assertAPIError(t, createRec, http.StatusConflict, "source_repository_conflict")
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

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/jobs/plain/source/definition", nil)
	handler.ServeHTTP(rec, req)
	assertAPIError(t, rec, http.StatusNotFound, "job_source_not_found")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/jobs/plain/source/definition?version=99", nil)
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

func decodeSourceJobTriggerResponse(t *testing.T, rec *httptest.ResponseRecorder) struct {
	JobID             string `json:"job_id"`
	RunID             string `json:"run_id"`
	RunIndex          int    `json:"run_index"`
	DefinitionVersion int    `json:"definition_version"`
	DefinitionHash    string `json:"definition_hash"`
	Source            struct {
		RepositoryID   string `json:"repository_id"`
		RequestedRef   string `json:"requested_ref"`
		ResolvedCommit string `json:"resolved_commit"`
		Path           string `json:"path"`
		BlobSHA        string `json:"blob_sha"`
	} `json:"source"`
} {
	t.Helper()

	var out struct {
		JobID             string `json:"job_id"`
		RunID             string `json:"run_id"`
		RunIndex          int    `json:"run_index"`
		DefinitionVersion int    `json:"definition_version"`
		DefinitionHash    string `json:"definition_hash"`
		Source            struct {
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

func decodeSourceDefinitionsImportResponse(t *testing.T, rec *httptest.ResponseRecorder) struct {
	RepositoryID   string `json:"repository_id"`
	RequestedRef   string `json:"requested_ref"`
	ResolvedCommit string `json:"resolved_commit"`
	Path           string `json:"path"`
	Limit          int    `json:"limit"`
	DryRun         bool   `json:"dry_run"`
	UpdateExisting bool   `json:"update_existing"`
	Summary        struct {
		Total       int `json:"total"`
		Created     int `json:"created"`
		Updated     int `json:"updated"`
		Unchanged   int `json:"unchanged"`
		WouldCreate int `json:"would_create"`
		WouldUpdate int `json:"would_update"`
		Conflicted  int `json:"conflicted"`
		Invalid     int `json:"invalid"`
	} `json:"summary"`
	Results []struct {
		JobID          string `json:"job_id"`
		Status         string `json:"status"`
		Version        int    `json:"version"`
		DefinitionHash string `json:"definition_hash"`
		Error          string `json:"error"`
		Source         struct {
			RepositoryID   string `json:"repository_id"`
			RequestedRef   string `json:"requested_ref"`
			ResolvedCommit string `json:"resolved_commit"`
			Path           string `json:"path"`
			BlobSHA        string `json:"blob_sha"`
		} `json:"source"`
	} `json:"results"`
} {
	t.Helper()

	var out struct {
		RepositoryID   string `json:"repository_id"`
		RequestedRef   string `json:"requested_ref"`
		ResolvedCommit string `json:"resolved_commit"`
		Path           string `json:"path"`
		Limit          int    `json:"limit"`
		DryRun         bool   `json:"dry_run"`
		UpdateExisting bool   `json:"update_existing"`
		Summary        struct {
			Total       int `json:"total"`
			Created     int `json:"created"`
			Updated     int `json:"updated"`
			Unchanged   int `json:"unchanged"`
			WouldCreate int `json:"would_create"`
			WouldUpdate int `json:"would_update"`
			Conflicted  int `json:"conflicted"`
			Invalid     int `json:"invalid"`
		} `json:"summary"`
		Results []struct {
			JobID          string `json:"job_id"`
			Status         string `json:"status"`
			Version        int    `json:"version"`
			DefinitionHash string `json:"definition_hash"`
			Error          string `json:"error"`
			Source         struct {
				RepositoryID   string `json:"repository_id"`
				RequestedRef   string `json:"requested_ref"`
				ResolvedCommit string `json:"resolved_commit"`
				Path           string `json:"path"`
				BlobSHA        string `json:"blob_sha"`
			} `json:"source"`
		} `json:"results"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	return out
}

func decodeSourceRepositoryResponse(t *testing.T, rec *httptest.ResponseRecorder) struct {
	RepositoryID  string `json:"repository_id"`
	Namespace     string `json:"namespace"`
	SourceKind    string `json:"source_kind"`
	CheckoutPath  string `json:"checkout_path"`
	CheckoutMode  string `json:"checkout_mode"`
	CanonicalURL  string `json:"canonical_url"`
	DefaultRef    string `json:"default_ref"`
	CredentialRef string `json:"credential_ref"`
	Enabled       bool   `json:"enabled"`
	Sync          struct {
		Status             string `json:"status"`
		LastStartedAtUnix  int64  `json:"last_started_at_unix"`
		LastFinishedAtUnix int64  `json:"last_finished_at_unix"`
		Ref                string `json:"ref"`
		Commit             string `json:"commit"`
		Error              string `json:"error"`
	} `json:"sync"`
} {
	t.Helper()

	var out struct {
		RepositoryID  string `json:"repository_id"`
		Namespace     string `json:"namespace"`
		SourceKind    string `json:"source_kind"`
		CheckoutPath  string `json:"checkout_path"`
		CheckoutMode  string `json:"checkout_mode"`
		CanonicalURL  string `json:"canonical_url"`
		DefaultRef    string `json:"default_ref"`
		CredentialRef string `json:"credential_ref"`
		Enabled       bool   `json:"enabled"`
		Sync          struct {
			Status             string `json:"status"`
			LastStartedAtUnix  int64  `json:"last_started_at_unix"`
			LastFinishedAtUnix int64  `json:"last_finished_at_unix"`
			Ref                string `json:"ref"`
			Commit             string `json:"commit"`
			Error              string `json:"error"`
		} `json:"sync"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	return out
}

func decodeSourceRepositoryStatusResponse(t *testing.T, rec *httptest.ResponseRecorder) struct {
	RepositoryID       string `json:"repository_id"`
	Namespace          string `json:"namespace"`
	SourceKind         string `json:"source_kind"`
	Enabled            bool   `json:"enabled"`
	Status             string `json:"status"`
	CheckoutPath       string `json:"checkout_path"`
	CheckoutMode       string `json:"checkout_mode"`
	PathExists         bool   `json:"path_exists"`
	PathIsDirectory    bool   `json:"path_is_directory"`
	GitRepository      bool   `json:"git_repository"`
	WorkTreePath       string `json:"work_tree_path"`
	HeadRef            string `json:"head_ref"`
	DefaultRef         string `json:"default_ref"`
	DefaultRefResolved bool   `json:"default_ref_resolved"`
	ResolvedCommit     string `json:"resolved_commit"`
	Sync               struct {
		Status             string `json:"status"`
		LastStartedAtUnix  int64  `json:"last_started_at_unix"`
		LastFinishedAtUnix int64  `json:"last_finished_at_unix"`
		Ref                string `json:"ref"`
		Commit             string `json:"commit"`
		Error              string `json:"error"`
	} `json:"sync"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
} {
	t.Helper()

	var out struct {
		RepositoryID       string `json:"repository_id"`
		Namespace          string `json:"namespace"`
		SourceKind         string `json:"source_kind"`
		Enabled            bool   `json:"enabled"`
		Status             string `json:"status"`
		CheckoutPath       string `json:"checkout_path"`
		CheckoutMode       string `json:"checkout_mode"`
		PathExists         bool   `json:"path_exists"`
		PathIsDirectory    bool   `json:"path_is_directory"`
		GitRepository      bool   `json:"git_repository"`
		WorkTreePath       string `json:"work_tree_path"`
		HeadRef            string `json:"head_ref"`
		DefaultRef         string `json:"default_ref"`
		DefaultRefResolved bool   `json:"default_ref_resolved"`
		ResolvedCommit     string `json:"resolved_commit"`
		Sync               struct {
			Status             string `json:"status"`
			LastStartedAtUnix  int64  `json:"last_started_at_unix"`
			LastFinishedAtUnix int64  `json:"last_finished_at_unix"`
			Ref                string `json:"ref"`
			Commit             string `json:"commit"`
			Error              string `json:"error"`
		} `json:"sync"`
		Error *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	return out
}

func decodeSourceJobDefinitionResponse(t *testing.T, rec *httptest.ResponseRecorder) struct {
	JobID          string          `json:"job_id"`
	Version        int             `json:"version"`
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
		JobID          string          `json:"job_id"`
		Version        int             `json:"version"`
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
