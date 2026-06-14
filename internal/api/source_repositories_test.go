package api_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"

	"github.com/spf13/viper"
)

func TestAPIServer_SourceRepositoryJobLifecycle(t *testing.T) {
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
		RepositoryID  string `json:"repository_id"`
		Namespace     string `json:"namespace"`
		SourceKind    string `json:"source_kind"`
		CheckoutPath  string `json:"checkout_path"`
		CheckoutMode  string `json:"checkout_mode"`
		AuthoringMode string `json:"authoring_mode"`
		Authoring     struct {
			Mode             string `json:"mode"`
			WriteDefinitions bool   `json:"write_definitions"`
			LocalCommits     bool   `json:"local_commits"`
			Reason           string `json:"reason"`
		} `json:"authoring"`
		Enabled bool `json:"enabled"`
		Sync    struct {
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
		repoResp.AuthoringMode != dal.SourceAuthoringModeReadOnly ||
		repoResp.Authoring.Mode != dal.SourceAuthoringModeReadOnly ||
		repoResp.Authoring.WriteDefinitions ||
		repoResp.Authoring.LocalCommits ||
		repoResp.Authoring.Reason != "read_only" ||
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

	definitionRec := httptest.NewRecorder()
	definitionReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/jobs/build/definition?ref=HEAD", nil)
	handler.ServeHTTP(definitionRec, definitionReq)
	if definitionRec.Code != http.StatusOK {
		t.Fatalf("get current source definition: status=%d body=%s", definitionRec.Code, definitionRec.Body.String())
	}

	definitionResp := decodeSourceRepositoryJobDefinitionResponse(t, definitionRec)
	if definitionResp.JobID != "build" ||
		definitionResp.DefinitionHash == "" ||
		definitionResp.Source.ResolvedCommit != secondCommit {
		t.Fatalf("current source definition response mismatch: %+v", definitionResp)
	}

	if err := json.Unmarshal(definitionResp.Definition, &resolvedJob); err != nil {
		t.Fatalf("current source definition JSON: %v", err)
	}

	if resolvedJob.GetRoot().GetWith()["command"] != "false" {
		t.Fatalf("current source definition command: got %+v", resolvedJob.GetRoot().GetWith())
	}

	triggerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/vectis-local/jobs/build/trigger", map[string]any{
		"ref": "HEAD",
	})
	if triggerRec.Code != http.StatusAccepted {
		t.Fatalf("trigger source job: status=%d body=%s", triggerRec.Code, triggerRec.Body.String())
	}

	triggerResp := decodeSourceJobTriggerResponse(t, triggerRec)
	if triggerResp.RunID == "" || triggerResp.DefinitionVersion != 1 || triggerResp.Source.ResolvedCommit != secondCommit {
		t.Fatalf("source trigger response mismatch: %+v", triggerResp)
	}
	if triggerResp.DefinitionHash == "" {
		t.Fatal("expected trigger response run_id")
	}

	if _, err := repos.Jobs().GetNamespaceID(context.Background(), "build"); !dal.IsNotFound(err) {
		t.Fatalf("direct source trigger should not create stored job row, got err=%v", err)
	}

	listRunsRec := httptest.NewRecorder()
	listRunsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/jobs/build/runs", nil)
	handler.ServeHTTP(listRunsRec, listRunsReq)
	if listRunsRec.Code != http.StatusOK {
		t.Fatalf("list source job runs: status=%d body=%s", listRunsRec.Code, listRunsRec.Body.String())
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

	if runsResp.Data[0].RunID != triggerResp.RunID || runsResp.Data[0].DefinitionVersion != 1 || runsResp.Data[0].Source == nil || runsResp.Data[0].Source.ResolvedCommit != secondCommit {
		t.Fatalf("run list source provenance mismatch: %+v", runsResp.Data[0])
	}

	getRunRec := httptest.NewRecorder()
	getRunReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+triggerResp.RunID, nil)
	handler.ServeHTTP(getRunRec, getRunReq)
	if getRunRec.Code != http.StatusOK {
		t.Fatalf("get source run: status=%d body=%s", getRunRec.Code, getRunRec.Body.String())
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

	if runResp.RunID != triggerResp.RunID || runResp.DefinitionVersion != 1 || runResp.Source == nil || runResp.Source.ResolvedCommit != secondCommit {
		t.Fatalf("run detail source provenance mismatch: %+v", runResp)
	}

	getRunDefinitionRec := httptest.NewRecorder()
	getRunDefinitionReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+triggerResp.RunID+"/definition", nil)
	handler.ServeHTTP(getRunDefinitionRec, getRunDefinitionReq)
	if getRunDefinitionRec.Code != http.StatusOK {
		t.Fatalf("get source run definition: status=%d body=%s", getRunDefinitionRec.Code, getRunDefinitionRec.Body.String())
	}

	var runDefinitionResp struct {
		RunID             string          `json:"run_id"`
		JobID             string          `json:"job_id"`
		DefinitionVersion int             `json:"definition_version"`
		DefinitionHash    string          `json:"definition_hash"`
		Definition        json.RawMessage `json:"definition"`
		Source            *struct {
			ResolvedCommit string `json:"resolved_commit"`
		} `json:"source,omitempty"`
	}

	if err := json.NewDecoder(getRunDefinitionRec.Body).Decode(&runDefinitionResp); err != nil {
		t.Fatalf("decode run definition: %v", err)
	}

	if err := json.Unmarshal(runDefinitionResp.Definition, &resolvedJob); err != nil {
		t.Fatalf("decode run definition job: %v", err)
	}

	if runDefinitionResp.RunID != triggerResp.RunID ||
		runDefinitionResp.JobID != "build" ||
		runDefinitionResp.DefinitionVersion != 1 ||
		runDefinitionResp.DefinitionHash != triggerResp.DefinitionHash ||
		runDefinitionResp.Source == nil ||
		runDefinitionResp.Source.ResolvedCommit != secondCommit ||
		resolvedJob.GetRoot().GetWith()["command"] != "false" {
		t.Fatalf("run definition response mismatch: resp=%+v job=%+v", runDefinitionResp, resolvedJob.GetRoot().GetWith())
	}
}

func TestAPIServer_ListSourceSchedules(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	t.Setenv("VECTIS_SOURCE_SCHEDULES", `[{"schedule_id":"nightly-build","repository_id":"vectis-local","job_id":"build","cron_spec":"30 8 * * *","ref":"main","enabled":true}]`)
	t.Setenv("VECTIS_API_SERVER_SOURCE_SCHEDULES", "")

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()
	ctx := context.Background()

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		DefaultRef:   "main",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "other",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/other",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create second source repository: %v", err)
	}

	nextRun := time.Date(2026, 5, 1, 8, 30, 0, 0, time.UTC)
	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "nightly-build",
		JobID:              "build",
		CronSpec:           "30 8 * * *",
		NextRunAt:          nextRun,
		SourceRepositoryID: "vectis-local",
		SourceRef:          "main",
		Enabled:            true,
	}); err != nil {
		t.Fatalf("create source schedule: %v", err)
	}

	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "other-build",
		JobID:              "other",
		CronSpec:           "0 9 * * *",
		NextRunAt:          nextRun,
		SourceRepositoryID: "other",
		SourcePath:         ".vectis/jobs/other.json",
		Enabled:            false,
	}); err != nil {
		t.Fatalf("create second source schedule: %v", err)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-schedules", nil)
	listRec := httptest.NewRecorder()
	handler.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list source schedules: status=%d body=%s", listRec.Code, listRec.Body.String())
	}

	var listResp struct {
		Namespace string `json:"namespace"`
		Schedules []struct {
			ScheduleID     string `json:"schedule_id"`
			RepositoryID   string `json:"repository_id"`
			Namespace      string `json:"namespace"`
			JobID          string `json:"job_id"`
			CronSpec       string `json:"cron_spec"`
			NextRunAt      string `json:"next_run_at"`
			Ref            string `json:"ref"`
			Path           string `json:"path"`
			PathDerived    bool   `json:"path_derived"`
			ConfiguredRef  string `json:"configured_ref"`
			ConfiguredPath string `json:"configured_path"`
			Declared       bool   `json:"declared"`
			Enabled        bool   `json:"enabled"`
		} `json:"schedules"`
	}

	if err := json.NewDecoder(listRec.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list source schedules: %v", err)
	}

	if listResp.Namespace != "/" || len(listResp.Schedules) != 2 {
		t.Fatalf("list source schedules mismatch: %+v", listResp)
	}

	if listResp.Schedules[0].ScheduleID != "other-build" ||
		listResp.Schedules[0].RepositoryID != "other" ||
		listResp.Schedules[0].Path != ".vectis/jobs/other.json" ||
		listResp.Schedules[0].PathDerived ||
		listResp.Schedules[0].Declared ||
		listResp.Schedules[0].Enabled {
		t.Fatalf("first source schedule mismatch: %+v", listResp.Schedules[0])
	}

	if listResp.Schedules[1].ScheduleID != "nightly-build" ||
		listResp.Schedules[1].RepositoryID != "vectis-local" ||
		listResp.Schedules[1].Namespace != "/" ||
		listResp.Schedules[1].JobID != "build" ||
		listResp.Schedules[1].CronSpec != "30 8 * * *" ||
		listResp.Schedules[1].NextRunAt != nextRun.Format(time.RFC3339) ||
		listResp.Schedules[1].Ref != "main" ||
		listResp.Schedules[1].Path != ".vectis/jobs/build.json" ||
		!listResp.Schedules[1].PathDerived ||
		listResp.Schedules[1].ConfiguredRef != "main" ||
		listResp.Schedules[1].ConfiguredPath != "" ||
		!listResp.Schedules[1].Declared ||
		!listResp.Schedules[1].Enabled {
		t.Fatalf("second source schedule mismatch: %+v", listResp.Schedules[1])
	}

	repoReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/schedules", nil)
	repoRec := httptest.NewRecorder()
	handler.ServeHTTP(repoRec, repoReq)
	if repoRec.Code != http.StatusOK {
		t.Fatalf("list repository source schedules: status=%d body=%s", repoRec.Code, repoRec.Body.String())
	}

	var repoResp struct {
		Namespace    string `json:"namespace"`
		RepositoryID string `json:"repository_id"`
		Schedules    []struct {
			ScheduleID string `json:"schedule_id"`
			Declared   bool   `json:"declared"`
		} `json:"schedules"`
	}

	if err := json.NewDecoder(repoRec.Body).Decode(&repoResp); err != nil {
		t.Fatalf("decode repository source schedules: %v", err)
	}

	if repoResp.Namespace != "/" ||
		repoResp.RepositoryID != "vectis-local" ||
		len(repoResp.Schedules) != 1 ||
		repoResp.Schedules[0].ScheduleID != "nightly-build" ||
		!repoResp.Schedules[0].Declared {
		t.Fatalf("repository source schedules mismatch: %+v", repoResp)
	}

	disableRec := doJSONRequest(t, handler, http.MethodPatch, "/api/v1/source-schedules/nightly-build", map[string]any{
		"enabled": false,
	})
	if disableRec.Code != http.StatusOK {
		t.Fatalf("disable source schedule: status=%d body=%s", disableRec.Code, disableRec.Body.String())
	}
	var disableResp struct {
		ScheduleID string `json:"schedule_id"`
		Declared   bool   `json:"declared"`
		Enabled    bool   `json:"enabled"`
	}
	if err := json.NewDecoder(disableRec.Body).Decode(&disableResp); err != nil {
		t.Fatalf("decode disable response: %v", err)
	}
	if disableResp.ScheduleID != "nightly-build" || !disableResp.Declared || disableResp.Enabled {
		t.Fatalf("disable source schedule response mismatch: %+v", disableResp)
	}

	enableRec := doJSONRequest(t, handler, http.MethodPatch, "/api/v1/source-schedules/nightly-build", map[string]any{
		"enabled": true,
	})
	if enableRec.Code != http.StatusOK {
		t.Fatalf("enable source schedule: status=%d body=%s", enableRec.Code, enableRec.Body.String())
	}
	var enableResp struct {
		ScheduleID string `json:"schedule_id"`
		Declared   bool   `json:"declared"`
		Enabled    bool   `json:"enabled"`
	}
	if err := json.NewDecoder(enableRec.Body).Decode(&enableResp); err != nil {
		t.Fatalf("decode enable response: %v", err)
	}
	if enableResp.ScheduleID != "nightly-build" || !enableResp.Declared || !enableResp.Enabled {
		t.Fatalf("enable source schedule response mismatch: %+v", enableResp)
	}

	overrideRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-schedules/nightly-build/override", map[string]any{
		"ref":    "hotfix/build",
		"path":   ".vectis/jobs/build-hotfix.json",
		"reason": "production hotfix",
	})

	if overrideRec.Code != http.StatusOK {
		t.Fatalf("set source schedule override: status=%d body=%s", overrideRec.Code, overrideRec.Body.String())
	}

	var overrideResp struct {
		ScheduleID     string `json:"schedule_id"`
		Ref            string `json:"ref"`
		Path           string `json:"path"`
		PathDerived    bool   `json:"path_derived"`
		ConfiguredRef  string `json:"configured_ref"`
		ConfiguredPath string `json:"configured_path"`
		Declared       bool   `json:"declared"`
		Override       *struct {
			Ref           string `json:"ref"`
			Path          string `json:"path"`
			Reason        string `json:"reason"`
			CreatedAtUnix int64  `json:"created_at_unix"`
		} `json:"override"`
	}

	if err := json.NewDecoder(overrideRec.Body).Decode(&overrideResp); err != nil {
		t.Fatalf("decode override response: %v", err)
	}

	if overrideResp.ScheduleID != "nightly-build" ||
		overrideResp.Ref != "hotfix/build" ||
		overrideResp.Path != ".vectis/jobs/build-hotfix.json" ||
		overrideResp.PathDerived ||
		overrideResp.ConfiguredRef != "main" ||
		overrideResp.ConfiguredPath != "" ||
		!overrideResp.Declared ||
		overrideResp.Override == nil ||
		overrideResp.Override.Ref != "hotfix/build" ||
		overrideResp.Override.Path != ".vectis/jobs/build-hotfix.json" ||
		overrideResp.Override.Reason != "production hotfix" ||
		overrideResp.Override.CreatedAtUnix == 0 {
		t.Fatalf("override response mismatch: %+v", overrideResp)
	}

	clearReq := httptest.NewRequest(http.MethodDelete, "/api/v1/source-schedules/nightly-build/override", nil)
	clearRec := httptest.NewRecorder()
	handler.ServeHTTP(clearRec, clearReq)
	if clearRec.Code != http.StatusOK {
		t.Fatalf("clear source schedule override: status=%d body=%s", clearRec.Code, clearRec.Body.String())
	}

	var clearResp struct {
		ScheduleID     string `json:"schedule_id"`
		Ref            string `json:"ref"`
		Path           string `json:"path"`
		ConfiguredRef  string `json:"configured_ref"`
		ConfiguredPath string `json:"configured_path"`
		Declared       bool   `json:"declared"`
		Override       *struct {
			Ref string `json:"ref"`
		} `json:"override"`
	}

	if err := json.NewDecoder(clearRec.Body).Decode(&clearResp); err != nil {
		t.Fatalf("decode clear response: %v", err)
	}

	if clearResp.ScheduleID != "nightly-build" ||
		clearResp.Ref != "main" ||
		clearResp.Path != ".vectis/jobs/build.json" ||
		clearResp.ConfiguredRef != "main" ||
		clearResp.ConfiguredPath != "" ||
		!clearResp.Declared ||
		clearResp.Override != nil {
		t.Fatalf("clear response mismatch: %+v", clearResp)
	}
}

func TestAPIServer_SourceScheduleOverrideRejectsInvalidSourceReference(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()
	ctx := context.Background()

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		DefaultRef:   "main",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "nightly-build",
		JobID:              "build",
		CronSpec:           "30 8 * * *",
		NextRunAt:          time.Date(2026, 5, 1, 8, 30, 0, 0, time.UTC),
		SourceRepositoryID: "vectis-local",
		SourceRef:          "main",
		Enabled:            true,
	}); err != nil {
		t.Fatalf("create source schedule: %v", err)
	}

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{name: "revision expression", body: map[string]any{"ref": "HEAD~1"}},
		{name: "path escape", body: map[string]any{"path": "../build.json"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-schedules/nightly-build/override", tc.body)
			assertAPIError(t, rec, http.StatusBadRequest, "invalid_source_reference")
		})
	}
}

func TestAPIServer_DeleteSourceScheduleGuards(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	t.Setenv("VECTIS_SOURCE_SCHEDULES", `[{"schedule_id":"declared-disabled","repository_id":"vectis-local","job_id":"build","cron_spec":"30 8 * * *","ref":"main","enabled":false}]`)
	t.Setenv("VECTIS_API_SERVER_SOURCE_SCHEDULES", "")

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()
	ctx := context.Background()

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		DefaultRef:   "main",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	nextRun := time.Date(2026, 5, 1, 8, 30, 0, 0, time.UTC)
	for _, rec := range []dal.CronScheduleRecord{
		{ScheduleID: "declared-disabled", JobID: "build", CronSpec: "30 8 * * *", NextRunAt: nextRun, SourceRepositoryID: "vectis-local", SourceRef: "main", Enabled: false},
		{ScheduleID: "stale-enabled", JobID: "build", CronSpec: "0 9 * * *", NextRunAt: nextRun, SourceRepositoryID: "vectis-local", SourceRef: "main", Enabled: true},
		{ScheduleID: "stale-override", JobID: "build", CronSpec: "15 9 * * *", NextRunAt: nextRun, SourceRepositoryID: "vectis-local", SourceRef: "main", Enabled: false},
		{ScheduleID: "stale-disabled", JobID: "build", CronSpec: "45 9 * * *", NextRunAt: nextRun, SourceRepositoryID: "vectis-local", SourceRef: "main", Enabled: false},
	} {
		if _, err := repos.Schedules().CreateCronSchedule(ctx, rec); err != nil {
			t.Fatalf("create source schedule %s: %v", rec.ScheduleID, err)
		}
	}

	if _, err := repos.Schedules().SetSourceCronScheduleOverride(ctx, "stale-override", dal.SourceScheduleOverride{
		Ref:           "hotfix/build",
		Reason:        "verify hotfix",
		CreatedAtUnix: 1770000000,
	}); err != nil {
		t.Fatalf("set source schedule override: %v", err)
	}

	for _, tc := range []struct {
		scheduleID string
		status     int
		code       string
	}{
		{scheduleID: "declared-disabled", status: http.StatusConflict, code: "source_schedule_declared"},
		{scheduleID: "stale-enabled", status: http.StatusConflict, code: "source_schedule_enabled"},
		{scheduleID: "stale-override", status: http.StatusConflict, code: "source_schedule_override_active"},
	} {
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/source-schedules/"+tc.scheduleID, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assertAPIError(t, rec, tc.status, tc.code)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/source-schedules/stale-disabled", nil)
	deleteRec := httptest.NewRecorder()
	handler.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("delete stale source schedule: status=%d body=%s", deleteRec.Code, deleteRec.Body.String())
	}

	if _, err := repos.Schedules().GetCronScheduleByScheduleID(ctx, "stale-disabled"); !dal.IsNotFound(err) {
		t.Fatalf("expected stale-disabled to be deleted, got %v", err)
	}

	missingReq := httptest.NewRequest(http.MethodDelete, "/api/v1/source-schedules/stale-disabled", nil)
	missingRec := httptest.NewRecorder()
	handler.ServeHTTP(missingRec, missingReq)
	assertAPIError(t, missingRec, http.StatusNotFound, "source_schedule_not_found")
}

func TestAPIServer_SourceStoredJobsDisabled(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	t.Setenv("VECTIS_SOURCE_STORED_JOBS_ENABLED", "false")
	t.Setenv("VECTIS_API_SERVER_SOURCE_STORED_JOBS_ENABLED", "")
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_SOURCE_SCHEDULES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SCHEDULES", "")

	server, _, queueService, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()

	jobBody := map[string]any{
		"id": "stored-build",
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/shell",
			"with": map[string]any{"command": "stored"},
		},
	}

	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   any
	}{
		{name: "list stored jobs", method: http.MethodGet, path: "/api/v1/jobs"},
		{name: "create stored job", method: http.MethodPost, path: "/api/v1/jobs", body: jobBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rec *httptest.ResponseRecorder
			if tc.body == nil {
				req := httptest.NewRequest(tc.method, tc.path, nil)
				rec = httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
			} else {
				rec = doJSONRequest(t, handler, tc.method, tc.path, tc.body)
			}

			assertAPIError(t, rec, http.StatusConflict, "stored_jobs_disabled")
		})
	}

	runRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/jobs/run", map[string]any{
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/shell",
			"with": map[string]any{"command": "one-off"},
		},
	})

	if runRec.Code != http.StatusAccepted {
		t.Fatalf("one-off run with stored jobs disabled: status=%d body=%s", runRec.Code, runRec.Body.String())
	}

	repoPath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, repoPath, "source", "source definition")
	registerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "vectis-local",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": repoPath,
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register source repository with stored jobs disabled: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	statusRec := httptest.NewRecorder()
	statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/source/status", nil)
	handler.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("source status with stored jobs disabled: status=%d body=%s", statusRec.Code, statusRec.Body.String())
	}

	var statusResp struct {
		StoredJobsEnabled      bool `json:"stored_jobs_enabled"`
		RepositoriesConfigured bool `json:"repositories_configured"`
		SourceJobsConfigured   bool `json:"source_jobs_configured"`
		Repositories           struct {
			Total   int `json:"total"`
			Enabled int `json:"enabled"`
		} `json:"repositories"`
	}
	if err := json.NewDecoder(statusRec.Body).Decode(&statusResp); err != nil {
		t.Fatalf("decode source status: %v", err)
	}
	if statusResp.StoredJobsEnabled ||
		!statusResp.RepositoriesConfigured ||
		!statusResp.SourceJobsConfigured ||
		statusResp.Repositories.Total != 1 ||
		statusResp.Repositories.Enabled != 1 {
		t.Fatalf("source-only status mismatch: %+v", statusResp)
	}

	jobsRec := httptest.NewRecorder()
	jobsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/jobs", nil)
	handler.ServeHTTP(jobsRec, jobsReq)
	if jobsRec.Code != http.StatusOK {
		t.Fatalf("list source repository jobs with stored jobs disabled: status=%d body=%s", jobsRec.Code, jobsRec.Body.String())
	}

	var jobsResp struct {
		RepositoryID string `json:"repository_id"`
		Jobs         []struct {
			JobID  string `json:"job_id"`
			Path   string `json:"path"`
			Source struct {
				RepositoryID   string `json:"repository_id"`
				ResolvedCommit string `json:"resolved_commit"`
				Path           string `json:"path"`
				BlobSHA        string `json:"blob_sha"`
			} `json:"source"`
		} `json:"jobs"`
	}
	if err := json.NewDecoder(jobsRec.Body).Decode(&jobsResp); err != nil {
		t.Fatalf("decode source jobs: %v", err)
	}
	if jobsResp.RepositoryID != "vectis-local" ||
		len(jobsResp.Jobs) != 1 ||
		jobsResp.Jobs[0].JobID != "build" ||
		jobsResp.Jobs[0].Path != ".vectis/jobs/build.json" ||
		jobsResp.Jobs[0].Source.RepositoryID != "vectis-local" ||
		jobsResp.Jobs[0].Source.ResolvedCommit == "" ||
		jobsResp.Jobs[0].Source.BlobSHA == "" {
		t.Fatalf("source jobs response mismatch: %+v", jobsResp)
	}

	sourceTriggerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/vectis-local/jobs/build/trigger", map[string]any{
		"ref": "HEAD",
	})

	if sourceTriggerRec.Code != http.StatusAccepted {
		t.Fatalf("source trigger with stored jobs disabled: status=%d body=%s", sourceTriggerRec.Code, sourceTriggerRec.Body.String())
	}

	triggerResp := decodeSourceJobTriggerResponse(t, sourceTriggerRec)
	if triggerResp.RunID == "" ||
		triggerResp.JobID != "build" ||
		triggerResp.RunIndex != 1 ||
		triggerResp.DefinitionVersion != 1 ||
		triggerResp.Source.RepositoryID != "vectis-local" ||
		triggerResp.Source.ResolvedCommit == "" ||
		triggerResp.Source.Path != ".vectis/jobs/build.json" ||
		triggerResp.Source.BlobSHA == "" {
		t.Fatalf("source trigger response mismatch: %+v", triggerResp)
	}

	if _, _, err := repos.Jobs().GetDefinition(context.Background(), "build"); !dal.IsNotFound(err) {
		t.Fatalf("source trigger should not create stored job, got err=%v", err)
	}

	storedSSERec := httptest.NewRecorder()
	storedSSEReq := httptest.NewRequest(http.MethodGet, "/api/v1/sse/jobs/build/runs", nil)
	handler.ServeHTTP(storedSSERec, storedSSEReq)
	assertAPIError(t, storedSSERec, http.StatusConflict, "stored_jobs_disabled")

	runLogsRec := httptest.NewRecorder()
	runLogsReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+triggerResp.RunID+"/logs", nil)
	handler.ServeHTTP(runLogsRec, runLogsReq)
	assertAPIError(t, runLogsRec, http.StatusServiceUnavailable, "log_service_unavailable")

	sourceLogsRec := httptest.NewRecorder()
	sourceLogsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/jobs/build/runs/"+triggerResp.RunID+"/logs", nil)
	handler.ServeHTTP(sourceLogsRec, sourceLogsReq)
	assertAPIError(t, sourceLogsRec, http.StatusServiceUnavailable, "log_service_unavailable")

	runsRec := httptest.NewRecorder()
	runsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/jobs/build/runs", nil)
	handler.ServeHTTP(runsRec, runsReq)
	if runsRec.Code != http.StatusOK {
		t.Fatalf("list source repository runs with stored jobs disabled: status=%d body=%s", runsRec.Code, runsRec.Body.String())
	}

	var runsResp struct {
		Data []struct {
			RunID             string `json:"run_id"`
			RunIndex          int    `json:"run_index"`
			DefinitionVersion int    `json:"definition_version"`
			Source            *struct {
				RepositoryID   string `json:"repository_id"`
				ResolvedCommit string `json:"resolved_commit"`
				Path           string `json:"path"`
				BlobSHA        string `json:"blob_sha"`
			} `json:"source,omitempty"`
		} `json:"data"`
	}
	if err := json.NewDecoder(runsRec.Body).Decode(&runsResp); err != nil {
		t.Fatalf("decode source runs: %v", err)
	}
	if len(runsResp.Data) != 1 ||
		runsResp.Data[0].RunID != triggerResp.RunID ||
		runsResp.Data[0].RunIndex != 1 ||
		runsResp.Data[0].DefinitionVersion != 1 ||
		runsResp.Data[0].Source == nil ||
		runsResp.Data[0].Source.RepositoryID != "vectis-local" ||
		runsResp.Data[0].Source.ResolvedCommit != triggerResp.Source.ResolvedCommit ||
		runsResp.Data[0].Source.Path != ".vectis/jobs/build.json" ||
		runsResp.Data[0].Source.BlobSHA != triggerResp.Source.BlobSHA {
		t.Fatalf("source runs response mismatch: %+v", runsResp.Data)
	}

	waitForNEnqueuedJobs(t, queueService, 2)
}

func TestAPIServer_SourceStoredJobBridgeRoutesRemoved(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, _ := setupTestServer(t)
	handler := server.Handler()

	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   any
	}{
		{name: "create stored job from source", method: http.MethodPost, path: "/api/v1/jobs/source/build", body: map[string]any{
			"repository_id": "vectis-local",
			"path":          ".vectis/jobs/build.json",
		}},
		{name: "update stored job from source", method: http.MethodPut, path: "/api/v1/jobs/source/build", body: map[string]any{
			"repository_id": "vectis-local",
			"path":          ".vectis/jobs/build.json",
		}},
		{name: "get stored job source", method: http.MethodGet, path: "/api/v1/jobs/build/source"},
		{name: "get stored job source definition", method: http.MethodGet, path: "/api/v1/jobs/build/source/definition"},
		{name: "import source definitions into stored jobs", method: http.MethodPost, path: "/api/v1/source-repositories/vectis-local/definitions/import", body: map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rec *httptest.ResponseRecorder
			if tc.body == nil {
				req := httptest.NewRequest(tc.method, tc.path, nil)
				rec = httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
			} else {
				rec = doJSONRequest(t, handler, tc.method, tc.path, tc.body)
			}

			if rec.Code != http.StatusNotFound {
				t.Fatalf("removed bridge route status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestAPIServer_SourceStatus(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	t.Setenv("VECTIS_SOURCE_STORED_JOBS_ENABLED", "false")
	t.Setenv("VECTIS_API_SERVER_SOURCE_STORED_JOBS_ENABLED", "")
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", `[{"repository_id":"vectis-local","checkout_path":"/work/vectis"},{"repository_id":"infra","checkout_path":"/work/infra"}]`)
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_SOURCE_SCHEDULES", `[{"schedule_id":"nightly","repository_id":"vectis-local","job_id":"build","cron_spec":"0 2 * * *"}]`)
	t.Setenv("VECTIS_API_SERVER_SOURCE_SCHEDULES", "")

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create declared source repository: %v", err)
	}
	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "old-repo",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/old",
		Enabled:      false,
	}); err != nil {
		t.Fatalf("create stale source repository: %v", err)
	}

	nextRun := time.Date(2026, 5, 1, 8, 30, 0, 0, time.UTC)
	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "nightly",
		JobID:              "build",
		CronSpec:           "0 2 * * *",
		NextRunAt:          nextRun,
		SourceRepositoryID: "vectis-local",
		SourceRef:          "main",
		Enabled:            true,
	}); err != nil {
		t.Fatalf("create declared source schedule: %v", err)
	}

	if _, err := repos.Schedules().SetSourceCronScheduleOverride(ctx, "nightly", dal.SourceScheduleOverride{
		Ref:           "hotfix/build",
		Reason:        "verify status count",
		CreatedAtUnix: 1770000000,
	}); err != nil {
		t.Fatalf("set source schedule override: %v", err)
	}

	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "old-nightly",
		JobID:              "old",
		CronSpec:           "30 3 * * *",
		NextRunAt:          nextRun,
		SourceRepositoryID: "old-repo",
		SourceRef:          "main",
		Enabled:            false,
	}); err != nil {
		t.Fatalf("create stale source schedule: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/source/status", nil)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("source status: status=%d body=%s", rec.Code, rec.Body.String())
	}

	var resp struct {
		StoredJobsEnabled      bool `json:"stored_jobs_enabled"`
		RepositoriesConfigured bool `json:"repositories_configured"`
		SourceJobsConfigured   bool `json:"source_jobs_configured"`
		SchedulesConfigured    bool `json:"schedules_configured"`
		DeclaredRepositories   int  `json:"declared_repositories"`
		DeclaredSchedules      int  `json:"declared_schedules"`
		Repositories           struct {
			Total         int `json:"total"`
			Enabled       int `json:"enabled"`
			Declared      int `json:"declared"`
			StaleDisabled int `json:"stale_disabled"`
			SyncNever     int `json:"sync_never"`
		} `json:"repositories"`
		Schedules struct {
			Total           int `json:"total"`
			Enabled         int `json:"enabled"`
			Declared        int `json:"declared"`
			StaleDisabled   int `json:"stale_disabled"`
			ActiveOverrides int `json:"active_overrides"`
		} `json:"schedules"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode source status: %v", err)
	}

	if resp.StoredJobsEnabled ||
		!resp.RepositoriesConfigured ||
		!resp.SourceJobsConfigured ||
		!resp.SchedulesConfigured ||
		resp.DeclaredRepositories != 2 ||
		resp.DeclaredSchedules != 1 ||
		resp.Repositories.Total != 2 ||
		resp.Repositories.Enabled != 1 ||
		resp.Repositories.Declared != 1 ||
		resp.Repositories.StaleDisabled != 1 ||
		resp.Repositories.SyncNever != 2 ||
		resp.Schedules.Total != 2 ||
		resp.Schedules.Enabled != 1 ||
		resp.Schedules.Declared != 1 ||
		resp.Schedules.StaleDisabled != 1 ||
		resp.Schedules.ActiveOverrides != 1 {
		t.Fatalf("unexpected source status: %+v", resp)
	}
}

func TestAPIServer_DeleteSourceRepository(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()

	if _, err := repos.Sources().CreateRepository(context.Background(), dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/source-repositories/vectis-local", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete source repository: status=%d body=%s", rec.Code, rec.Body.String())
	}

	if _, err := repos.Sources().GetRepository(context.Background(), "vectis-local"); !dal.IsNotFound(err) {
		t.Fatalf("expected deleted source repository to be missing, got %v", err)
	}

	missingReq := httptest.NewRequest(http.MethodDelete, "/api/v1/source-repositories/vectis-local", nil)
	missingRec := httptest.NewRecorder()
	handler.ServeHTTP(missingRec, missingReq)
	assertAPIError(t, missingRec, http.StatusNotFound, "source_repository_not_found")
}

func TestAPIServer_SourceRepositoryDeclarationState(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", `[{"repository_id":"declared-repo","source_kind":"local_checkout","checkout_path":"/work/declared","default_ref":"main","enabled":true}]`)
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()
	ctx := context.Background()

	for _, rec := range []dal.SourceRepositoryRecord{
		{RepositoryID: "declared-repo", NamespaceID: 1, SourceKind: dal.SourceKindLocalCheckout, CheckoutPath: "/work/declared", DefaultRef: "main", Enabled: true},
		{RepositoryID: "stale-repo", NamespaceID: 1, SourceKind: dal.SourceKindLocalCheckout, CheckoutPath: "/work/stale", DefaultRef: "main", Enabled: true},
	} {
		if _, err := repos.Sources().CreateRepository(ctx, rec); err != nil {
			t.Fatalf("CreateRepository %s: %v", rec.RepositoryID, err)
		}
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories", nil)
	listRec := httptest.NewRecorder()
	handler.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list source repositories: status=%d body=%s", listRec.Code, listRec.Body.String())
	}

	var listResp []struct {
		RepositoryID string `json:"repository_id"`
		Declared     bool   `json:"declared"`
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list source repositories: %v", err)
	}

	declaredByID := map[string]bool{}
	for _, repo := range listResp {
		declaredByID[repo.RepositoryID] = repo.Declared
	}
	if !declaredByID["declared-repo"] || declaredByID["stale-repo"] {
		t.Fatalf("repository declared state mismatch: %+v", declaredByID)
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/declared-repo/status", nil)
	statusRec := httptest.NewRecorder()
	handler.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("get source repository status: status=%d body=%s", statusRec.Code, statusRec.Body.String())
	}
	var statusResp struct {
		RepositoryID string `json:"repository_id"`
		Declared     bool   `json:"declared"`
	}
	if err := json.NewDecoder(statusRec.Body).Decode(&statusResp); err != nil {
		t.Fatalf("decode source repository status: %v", err)
	}
	if statusResp.RepositoryID != "declared-repo" || !statusResp.Declared {
		t.Fatalf("status declared state mismatch: %+v", statusResp)
	}

	deleteDeclaredReq := httptest.NewRequest(http.MethodDelete, "/api/v1/source-repositories/declared-repo", nil)
	deleteDeclaredRec := httptest.NewRecorder()
	handler.ServeHTTP(deleteDeclaredRec, deleteDeclaredReq)
	assertAPIError(t, deleteDeclaredRec, http.StatusConflict, "source_repository_declared")

	deleteStaleReq := httptest.NewRequest(http.MethodDelete, "/api/v1/source-repositories/stale-repo", nil)
	deleteStaleRec := httptest.NewRecorder()
	handler.ServeHTTP(deleteStaleRec, deleteStaleReq)
	if deleteStaleRec.Code != http.StatusNoContent {
		t.Fatalf("delete stale source repository: status=%d body=%s", deleteStaleRec.Code, deleteStaleRec.Body.String())
	}
}

func TestAPIServer_DeleteSourceRepositoryConflictsWhenReferenced(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()
	ctx := context.Background()

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	if err := repos.Jobs().Create(ctx, "build", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	if err := repos.Sources().RecordDefinitionSource(ctx, dal.JobDefinitionSourceRecord{
		JobID:          "build",
		Version:        1,
		RepositoryID:   "vectis-local",
		RequestedRef:   "main",
		ResolvedCommit: "0123456789abcdef0123456789abcdef01234567",
		DefinitionPath: ".vectis/jobs/build.json",
		BlobSHA:        "abcdef0123456789abcdef0123456789abcdef01",
	}); err != nil {
		t.Fatalf("RecordDefinitionSource: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/source-repositories/vectis-local", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assertAPIError(t, rec, http.StatusConflict, "source_repository_in_use")

	if _, err := repos.Sources().GetRepository(ctx, "vectis-local"); err != nil {
		t.Fatalf("referenced repository should remain: %v", err)
	}
}

func TestAPIServer_DeleteSourceRepositoryConflictsWhenScheduled(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()
	ctx := context.Background()

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "nightly-build",
		JobID:              "build",
		CronSpec:           "30 8 * * *",
		NextRunAt:          time.Date(2026, 5, 1, 8, 30, 0, 0, time.UTC),
		SourceRepositoryID: "vectis-local",
		SourceRef:          "main",
		Enabled:            true,
	}); err != nil {
		t.Fatalf("create source schedule: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/source-repositories/vectis-local", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assertAPIError(t, rec, http.StatusConflict, "source_repository_in_use")

	if _, err := repos.Sources().GetRepository(ctx, "vectis-local"); err != nil {
		t.Fatalf("scheduled repository should remain: %v", err)
	}
}

func TestAPIServer_ResolveSourceDefinitionRejectsDisabledRepository(t *testing.T) {
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

func TestAPIServer_ListSourceRepositoryJobsDerivesTriggerableJobs(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, _ := setupTestServer(t)
	handler := server.Handler()
	repoPath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, repoPath, "build", "build definition")
	writeAPIFileAndCommit(t, repoPath, ".vectis/jobs/team/deploy.json", `{
		"root": {"id": "root", "uses": "builtins/shell", "with": {"command": "deploy"}}
	}`+"\n", "nested definition")

	writeAPIFileAndCommit(t, repoPath, ".vectis/jobs/bad name.json", `{
		"root": {"id": "root", "uses": "builtins/shell", "with": {"command": "bad"}}
	}`+"\n", "invalid source job name")

	writeAPIFileAndCommit(t, repoPath, ".vectis/jobs/team.deploy.json", `{
		"root": {"id": "root", "uses": "builtins/shell", "with": {"command": "duplicate"}}
	}`+"\n", "duplicate derived job id")

	commit := apiGitOutput(t, repoPath, "rev-parse", "HEAD")
	buildBlob := apiGitOutput(t, repoPath, "rev-parse", "HEAD:.vectis/jobs/build.json")

	registerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "vectis-local",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": repoPath,
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	listRec := httptest.NewRecorder()
	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/jobs?limit=10", nil)
	handler.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list source repository jobs: status=%d body=%s", listRec.Code, listRec.Body.String())
	}

	resp := decodeSourceRepositoryJobsResponse(t, listRec)
	if resp.RepositoryID != "vectis-local" ||
		resp.RequestedRef != "HEAD" ||
		resp.ResolvedCommit != commit ||
		resp.Path != ".vectis/jobs" ||
		resp.Limit != 10 ||
		resp.Truncated {
		t.Fatalf("source jobs response mismatch: %+v", resp)
	}

	jobsByID := map[string]struct {
		JobID   string `json:"job_id"`
		Path    string `json:"path"`
		Name    string `json:"name"`
		BlobSHA string `json:"blob_sha"`
		Source  struct {
			RepositoryID   string `json:"repository_id"`
			RequestedRef   string `json:"requested_ref"`
			ResolvedCommit string `json:"resolved_commit"`
			Path           string `json:"path"`
			BlobSHA        string `json:"blob_sha"`
		} `json:"source"`
	}{}
	for _, job := range resp.Jobs {
		jobsByID[job.JobID] = job
	}

	build := jobsByID["build"]
	if build.Path != ".vectis/jobs/build.json" ||
		build.Name != "build.json" ||
		build.BlobSHA != buildBlob ||
		build.Source.RepositoryID != "vectis-local" ||
		build.Source.RequestedRef != "HEAD" ||
		build.Source.ResolvedCommit != commit ||
		build.Source.Path != ".vectis/jobs/build.json" ||
		build.Source.BlobSHA != buildBlob {
		t.Fatalf("build source job mismatch: %+v", build)
	}

	if _, ok := jobsByID["team.deploy"]; !ok {
		t.Fatalf("expected derived nested source job team.deploy, got %+v", resp.Jobs)
	}

	var sawInvalidName, sawDuplicate bool
	for _, invalid := range resp.Invalid {
		if invalid.Path == ".vectis/jobs/bad name.json" && strings.Contains(invalid.Error, "unsupported job_id segment") {
			sawInvalidName = true
		}
		if strings.Contains(invalid.Error, "duplicate derived job_id team.deploy") {
			sawDuplicate = true
		}
	}

	if !sawInvalidName || !sawDuplicate {
		t.Fatalf("expected invalid filename and duplicate derived job id, got %+v", resp.Invalid)
	}

	treeRec := httptest.NewRecorder()
	treeReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/tree?path=.vectis/jobs&recursive=true&limit=1", nil)
	handler.ServeHTTP(treeRec, treeReq)
	if treeRec.Code != http.StatusOK {
		t.Fatalf("list limited source tree: status=%d body=%s", treeRec.Code, treeRec.Body.String())
	}

	var treeResp struct {
		Limit      int    `json:"limit"`
		Truncated  bool   `json:"truncated"`
		NextCursor string `json:"next_cursor"`
		Entries    []struct {
			Path string `json:"path"`
		} `json:"entries"`
	}
	if err := json.NewDecoder(treeRec.Body).Decode(&treeResp); err != nil {
		t.Fatalf("decode limited source tree: %v", err)
	}
	if treeResp.Limit != 1 || !treeResp.Truncated || treeResp.NextCursor == "" || len(treeResp.Entries) != 1 {
		t.Fatalf("limited source tree response mismatch: %+v", treeResp)
	}

	treeNextRec := httptest.NewRecorder()
	treeNextReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/tree?path=.vectis/jobs&recursive=true&limit=10&cursor="+url.QueryEscape(treeResp.NextCursor), nil)
	handler.ServeHTTP(treeNextRec, treeNextReq)
	if treeNextRec.Code != http.StatusOK {
		t.Fatalf("list source tree after cursor: status=%d body=%s", treeNextRec.Code, treeNextRec.Body.String())
	}

	var treeNextResp struct {
		Entries []struct {
			Path string `json:"path"`
		} `json:"entries"`
	}
	if err := json.NewDecoder(treeNextRec.Body).Decode(&treeNextResp); err != nil {
		t.Fatalf("decode source tree after cursor: %v", err)
	}
	if len(treeNextResp.Entries) == 0 {
		t.Fatalf("expected source tree entries after cursor")
	}
	for _, entry := range treeNextResp.Entries {
		if entry.Path <= treeResp.NextCursor {
			t.Fatalf("source tree after cursor returned %q at or before %q", entry.Path, treeResp.NextCursor)
		}
	}

	definitionsRec := httptest.NewRecorder()
	definitionsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/definitions?path=.vectis/jobs&limit=1", nil)
	handler.ServeHTTP(definitionsRec, definitionsReq)
	if definitionsRec.Code != http.StatusOK {
		t.Fatalf("list limited source definitions: status=%d body=%s", definitionsRec.Code, definitionsRec.Body.String())
	}

	var definitionsResp struct {
		Limit       int    `json:"limit"`
		Truncated   bool   `json:"truncated"`
		NextCursor  string `json:"next_cursor"`
		Definitions []struct {
			Path string `json:"path"`
		} `json:"definitions"`
	}
	if err := json.NewDecoder(definitionsRec.Body).Decode(&definitionsResp); err != nil {
		t.Fatalf("decode limited source definitions: %v", err)
	}
	if definitionsResp.Limit != 1 || !definitionsResp.Truncated || definitionsResp.NextCursor == "" || len(definitionsResp.Definitions) != 1 {
		t.Fatalf("limited source definitions response mismatch: %+v", definitionsResp)
	}

	limitedJobsRec := httptest.NewRecorder()
	limitedJobsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/jobs?limit=1", nil)
	handler.ServeHTTP(limitedJobsRec, limitedJobsReq)
	if limitedJobsRec.Code != http.StatusOK {
		t.Fatalf("list limited source jobs: status=%d body=%s", limitedJobsRec.Code, limitedJobsRec.Body.String())
	}

	limitedJobsResp := decodeSourceRepositoryJobsResponse(t, limitedJobsRec)
	if limitedJobsResp.Limit != 1 ||
		!limitedJobsResp.Truncated ||
		limitedJobsResp.NextCursor == "" ||
		len(limitedJobsResp.Jobs)+len(limitedJobsResp.Invalid) != 1 {
		t.Fatalf("limited source jobs response mismatch: %+v", limitedJobsResp)
	}

	definitionRec := httptest.NewRecorder()
	definitionReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/jobs/build/definition?ref=HEAD", nil)
	handler.ServeHTTP(definitionRec, definitionReq)
	if definitionRec.Code != http.StatusOK {
		t.Fatalf("get source repository job definition: status=%d body=%s", definitionRec.Code, definitionRec.Body.String())
	}

	definitionResp := decodeSourceRepositoryJobDefinitionResponse(t, definitionRec)
	if definitionResp.JobID != "build" ||
		definitionResp.DefinitionHash == "" ||
		definitionResp.Source.RepositoryID != "vectis-local" ||
		definitionResp.Source.RequestedRef != "HEAD" ||
		definitionResp.Source.ResolvedCommit != commit ||
		definitionResp.Source.Path != ".vectis/jobs/build.json" ||
		definitionResp.Source.BlobSHA != buildBlob {
		t.Fatalf("source job definition response mismatch: %+v", definitionResp)
	}

	var resolvedJob api.Job
	if err := json.Unmarshal(definitionResp.Definition, &resolvedJob); err != nil {
		t.Fatalf("source job definition JSON: %v", err)
	}
	if resolvedJob.GetRoot().GetWith()["command"] != "build" {
		t.Fatalf("source job definition command: got %+v", resolvedJob.GetRoot().GetWith())
	}

	nestedDefinitionRec := httptest.NewRecorder()
	nestedDefinitionReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/jobs/team.deploy/definition?ref=HEAD", nil)
	handler.ServeHTTP(nestedDefinitionRec, nestedDefinitionReq)
	if nestedDefinitionRec.Code != http.StatusOK {
		t.Fatalf("get nested source repository job definition: status=%d body=%s", nestedDefinitionRec.Code, nestedDefinitionRec.Body.String())
	}

	nestedDefinitionResp := decodeSourceRepositoryJobDefinitionResponse(t, nestedDefinitionRec)
	if nestedDefinitionResp.JobID != "team.deploy" || nestedDefinitionResp.Source.Path != ".vectis/jobs/team/deploy.json" {
		t.Fatalf("nested source job definition response mismatch: %+v", nestedDefinitionResp)
	}

	if err := json.Unmarshal(nestedDefinitionResp.Definition, &resolvedJob); err != nil {
		t.Fatalf("nested source job definition JSON: %v", err)
	}
	if resolvedJob.GetRoot().GetWith()["command"] != "deploy" {
		t.Fatalf("nested source job definition command: got %+v", resolvedJob.GetRoot().GetWith())
	}

	overrideDefinitionRec := httptest.NewRecorder()
	overrideDefinitionReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/vectis-local/jobs/team.deploy/definition?ref=HEAD&path=.vectis/jobs/team.deploy.json", nil)
	handler.ServeHTTP(overrideDefinitionRec, overrideDefinitionReq)
	if overrideDefinitionRec.Code != http.StatusOK {
		t.Fatalf("get override source repository job definition: status=%d body=%s", overrideDefinitionRec.Code, overrideDefinitionRec.Body.String())
	}

	overrideDefinitionResp := decodeSourceRepositoryJobDefinitionResponse(t, overrideDefinitionRec)
	if overrideDefinitionResp.JobID != "team.deploy" || overrideDefinitionResp.Source.Path != ".vectis/jobs/team.deploy.json" {
		t.Fatalf("override source job definition response mismatch: %+v", overrideDefinitionResp)
	}

	if err := json.Unmarshal(overrideDefinitionResp.Definition, &resolvedJob); err != nil {
		t.Fatalf("override source job definition JSON: %v", err)
	}
	if resolvedJob.GetRoot().GetWith()["command"] != "duplicate" {
		t.Fatalf("override source job definition command: got %+v", resolvedJob.GetRoot().GetWith())
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

	server, _, _, _ := setupTestServer(t)
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
		Truncated    bool   `json:"truncated"`
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

	if branchesResp.RepositoryID != "managed-repo" || branchesResp.Prefix != "feature/" || branchesResp.Limit != 5 || branchesResp.Truncated || len(branchesResp.Branches) != 1 {
		t.Fatalf("managed branches response mismatch: %+v", branchesResp)
	}

	branch := branchesResp.Branches[0]
	if branch.Name != "feature/source-ref" ||
		branch.Ref != "refs/remotes/origin/feature/source-ref" ||
		branch.Commit != featureCommit ||
		branch.Remote != "origin" {
		t.Fatalf("managed branch mismatch: %+v", branch)
	}

	limitedBranchesRec := httptest.NewRecorder()
	limitedBranchesReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/refs/branches?limit=1", nil)
	handler.ServeHTTP(limitedBranchesRec, limitedBranchesReq)
	if limitedBranchesRec.Code != http.StatusOK {
		t.Fatalf("list limited managed branches: status=%d body=%s", limitedBranchesRec.Code, limitedBranchesRec.Body.String())
	}

	var limitedBranchesResp struct {
		Limit     int  `json:"limit"`
		Truncated bool `json:"truncated"`
		Branches  []struct {
			Name string `json:"name"`
		} `json:"branches"`
	}
	if err := json.NewDecoder(limitedBranchesRec.Body).Decode(&limitedBranchesResp); err != nil {
		t.Fatalf("decode limited managed branches: %v", err)
	}
	if limitedBranchesResp.Limit != 1 || !limitedBranchesResp.Truncated || len(limitedBranchesResp.Branches) != 1 {
		t.Fatalf("limited managed branches response mismatch: %+v", limitedBranchesResp)
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
}

func TestAPIServer_PutManagedSourceRepositoryJobDefinitionCommitsDefinition(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	viper.Reset()
	t.Cleanup(viper.Reset)

	checkoutRoot := t.TempDir()
	viper.Set("source.checkout_root", checkoutRoot)

	server, _, _, db := setupTestServer(t)
	repos := dal.NewSQLRepositories(db)
	handler := server.Handler()
	remotePath := initAPIGitRepo(t)
	writeAPIFileAndCommit(t, remotePath, "README.md", "managed source\n", "readme")

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

	syncRec := httptest.NewRecorder()
	syncReq := httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/managed-repo/sync", nil)
	handler.ServeHTTP(syncRec, syncReq)
	if syncRec.Code != http.StatusOK {
		t.Fatalf("sync managed source repository: status=%d body=%s", syncRec.Code, syncRec.Body.String())
	}

	readOnlyRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/managed-repo/jobs/build/definition", map[string]any{
		"definition": map[string]any{
			"root": map[string]any{
				"id":   "root",
				"uses": "builtins/shell",
				"with": map[string]any{"command": "blocked"},
			},
		},
	})

	assertAPIError(t, readOnlyRec, http.StatusConflict, "source_authoring_unavailable")
	enableAuthoringRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/managed-repo", map[string]any{
		"authoring_mode": dal.SourceAuthoringModeLocalCommit,
	})

	if enableAuthoringRec.Code != http.StatusOK {
		t.Fatalf("enable local source authoring: status=%d body=%s", enableAuthoringRec.Code, enableAuthoringRec.Body.String())
	}

	enableAuthoringResp := decodeSourceRepositoryResponse(t, enableAuthoringRec)
	if enableAuthoringResp.AuthoringMode != dal.SourceAuthoringModeLocalCommit ||
		enableAuthoringResp.Authoring.Mode != dal.SourceAuthoringModeLocalCommit ||
		!enableAuthoringResp.Authoring.WriteDefinitions ||
		!enableAuthoringResp.Authoring.LocalCommits ||
		enableAuthoringResp.Authoring.ExternalChangeRequests ||
		enableAuthoringResp.Authoring.Reason != "" {
		t.Fatalf("enable local authoring response mismatch: %+v", enableAuthoringResp)
	}

	parent := apiGitOutput(t, registerResp.CheckoutPath, "rev-parse", "HEAD")
	writeRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/managed-repo/jobs/build/definition", map[string]any{
		"expected_head": parent,
		"message":       "add build definition",
		"definition": map[string]any{
			"root": map[string]any{
				"id":   "root",
				"uses": "builtins/shell",
				"with": map[string]any{"command": "authored"},
			},
		},
	})

	if writeRec.Code != http.StatusOK {
		t.Fatalf("put managed source job definition: status=%d body=%s", writeRec.Code, writeRec.Body.String())
	}

	writeResp := decodeSourceRepositoryJobDefinitionResponse(t, writeRec)
	if writeResp.JobID != "build" ||
		writeResp.DefinitionHash == "" ||
		writeResp.Source.RepositoryID != "managed-repo" ||
		writeResp.Source.RequestedRef != "HEAD" ||
		writeResp.Source.ResolvedCommit == "" ||
		writeResp.Source.ResolvedCommit == parent ||
		writeResp.Source.Path != ".vectis/jobs/build.json" ||
		writeResp.Source.BlobSHA == "" {
		t.Fatalf("put managed source job definition response mismatch: %+v parent=%s", writeResp, parent)
	}

	if head := apiGitOutput(t, registerResp.CheckoutPath, "rev-parse", "HEAD"); head != writeResp.Source.ResolvedCommit {
		t.Fatalf("managed checkout HEAD: got %q, want %q", head, writeResp.Source.ResolvedCommit)
	}

	if _, err := repos.Jobs().GetNamespaceID(context.Background(), "build"); !dal.IsNotFound(err) {
		t.Fatalf("source definition authoring should not create stored job row, got err=%v", err)
	}

	readRec := httptest.NewRecorder()
	readReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/jobs/build/definition?ref="+writeResp.Source.ResolvedCommit, nil)
	handler.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("read authored source definition: status=%d body=%s", readRec.Code, readRec.Body.String())
	}

	readResp := decodeSourceRepositoryJobDefinitionResponse(t, readRec)
	if readResp.DefinitionHash != writeResp.DefinitionHash || readResp.Source.ResolvedCommit != writeResp.Source.ResolvedCommit {
		t.Fatalf("read authored source definition mismatch: write=%+v read=%+v", writeResp, readResp)
	}

	var job api.Job
	if err := json.Unmarshal(readResp.Definition, &job); err != nil {
		t.Fatalf("read authored definition JSON: %v", err)
	}

	if job.GetRoot().GetWith()["command"] != "authored" {
		t.Fatalf("authored definition command: got %+v", job.GetRoot().GetWith())
	}

	triggerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories/managed-repo/jobs/build/trigger", map[string]any{
		"ref": writeResp.Source.ResolvedCommit,
	})

	if triggerRec.Code != http.StatusAccepted {
		t.Fatalf("trigger authored source definition: status=%d body=%s", triggerRec.Code, triggerRec.Body.String())
	}

	triggerResp := decodeSourceJobTriggerResponse(t, triggerRec)
	if triggerResp.Source.ResolvedCommit != writeResp.Source.ResolvedCommit || triggerResp.Source.BlobSHA != writeResp.Source.BlobSHA {
		t.Fatalf("trigger authored source definition provenance mismatch: %+v write=%+v", triggerResp, writeResp)
	}

	staleRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/managed-repo/jobs/build/definition", map[string]any{
		"expected_head": parent,
		"definition": map[string]any{
			"root": map[string]any{
				"id":   "root",
				"uses": "builtins/shell",
				"with": map[string]any{"command": "stale"},
			},
		},
	})

	assertAPIError(t, staleRec, http.StatusConflict, "source_conflict")
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

	getDefinitionRec := httptest.NewRecorder()
	getDefinitionReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+triggerResp.RunID+"/definition", nil)
	handler.ServeHTTP(getDefinitionRec, getDefinitionReq)
	if getDefinitionRec.Code != http.StatusOK {
		t.Fatalf("get source repository run definition: status=%d body=%s", getDefinitionRec.Code, getDefinitionRec.Body.String())
	}

	var runDefinitionResp struct {
		RunID             string          `json:"run_id"`
		JobID             string          `json:"job_id"`
		DefinitionVersion int             `json:"definition_version"`
		DefinitionHash    string          `json:"definition_hash"`
		Definition        json.RawMessage `json:"definition"`
		Source            *struct {
			RepositoryID   string `json:"repository_id"`
			RequestedRef   string `json:"requested_ref"`
			ResolvedCommit string `json:"resolved_commit"`
			Path           string `json:"path"`
			BlobSHA        string `json:"blob_sha"`
		} `json:"source,omitempty"`
	}

	if err := json.NewDecoder(getDefinitionRec.Body).Decode(&runDefinitionResp); err != nil {
		t.Fatalf("decode run definition response: %v", err)
	}

	var runDefinitionJob api.Job
	if err := json.Unmarshal(runDefinitionResp.Definition, &runDefinitionJob); err != nil {
		t.Fatalf("decode run definition job: %v", err)
	}

	if runDefinitionResp.RunID != triggerResp.RunID ||
		runDefinitionResp.JobID != "build" ||
		runDefinitionResp.DefinitionVersion != 1 ||
		runDefinitionResp.DefinitionHash != triggerResp.DefinitionHash ||
		runDefinitionResp.Source == nil ||
		runDefinitionResp.Source.RepositoryID != "managed-repo" ||
		runDefinitionResp.Source.ResolvedCommit != commit ||
		runDefinitionResp.Source.BlobSHA != blob ||
		runDefinitionJob.GetRoot().GetWith()["command"] != "source-trigger" {
		t.Fatalf("source repository run definition response mismatch: resp=%+v job=%+v", runDefinitionResp, runDefinitionJob.GetRoot().GetWith())
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

	if err := repos.Jobs().Create(context.Background(), "build", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"stored"}}}`, 1); err != nil {
		t.Fatalf("Create stored job with same id: %v", err)
	}

	_, storedVersion, err := repos.Jobs().GetDefinition(context.Background(), "build")
	if err != nil {
		t.Fatalf("GetDefinition stored build: %v", err)
	}

	storedRunID, _, err := repos.Runs().CreateRun(context.Background(), "build", nil, storedVersion)
	if err != nil {
		t.Fatalf("CreateRun stored build: %v", err)
	}

	storedLogsRec := httptest.NewRecorder()
	storedLogsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/jobs/build/runs/"+storedRunID+"/logs", nil)
	handler.ServeHTTP(storedLogsRec, storedLogsReq)
	assertAPIError(t, storedLogsRec, http.StatusNotFound, "run_not_found")

	wrongJobLogsRec := httptest.NewRecorder()
	wrongJobLogsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/jobs/deploy/runs/"+triggerResp.RunID+"/logs", nil)
	handler.ServeHTTP(wrongJobLogsRec, wrongJobLogsReq)
	assertAPIError(t, wrongJobLogsRec, http.StatusNotFound, "run_not_found")

	sourceLogsRec := httptest.NewRecorder()
	sourceLogsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/jobs/build/runs/"+triggerResp.RunID+"/logs", nil)
	handler.ServeHTTP(sourceLogsRec, sourceLogsReq)
	assertAPIError(t, sourceLogsRec, http.StatusServiceUnavailable, "log_service_unavailable")

	listRunsRec := httptest.NewRecorder()
	listRunsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/jobs/build/runs", nil)
	handler.ServeHTTP(listRunsRec, listRunsReq)
	if listRunsRec.Code != http.StatusOK {
		t.Fatalf("list source repository job runs: status=%d body=%s", listRunsRec.Code, listRunsRec.Body.String())
	}

	var sourceRunsResp struct {
		Data []struct {
			RunID             string `json:"run_id"`
			RunIndex          int    `json:"run_index"`
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

	if err := json.NewDecoder(listRunsRec.Body).Decode(&sourceRunsResp); err != nil {
		t.Fatal(err)
	}

	if len(sourceRunsResp.Data) != 2 {
		t.Fatalf("expected two source run rows, got %+v", sourceRunsResp.Data)
	}

	if sourceRunsResp.Data[0].RunID != triggerResp.RunID ||
		sourceRunsResp.Data[0].RunIndex != 1 ||
		sourceRunsResp.Data[0].DefinitionVersion != 1 ||
		sourceRunsResp.Data[0].Source == nil ||
		sourceRunsResp.Data[0].Source.RepositoryID != "managed-repo" ||
		sourceRunsResp.Data[0].Source.ResolvedCommit != commit ||
		sourceRunsResp.Data[0].Source.BlobSHA != blob {
		t.Fatalf("first source run history row mismatch: %+v", sourceRunsResp.Data[0])
	}

	if sourceRunsResp.Data[1].RunID != secondTriggerResp.RunID ||
		sourceRunsResp.Data[1].RunIndex != 2 ||
		sourceRunsResp.Data[1].DefinitionVersion != 2 ||
		sourceRunsResp.Data[1].Source == nil ||
		sourceRunsResp.Data[1].Source.RepositoryID != "managed-repo" {
		t.Fatalf("second source run history row mismatch: %+v", sourceRunsResp.Data[1])
	}

	listRunsAfterRec := httptest.NewRecorder()
	listRunsAfterReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/jobs/build/runs?after_index=1", nil)
	handler.ServeHTTP(listRunsAfterRec, listRunsAfterReq)
	if listRunsAfterRec.Code != http.StatusOK {
		t.Fatalf("list source repository job runs after index: status=%d body=%s", listRunsAfterRec.Code, listRunsAfterRec.Body.String())
	}

	var sourceRunsAfterResp struct {
		Data []struct {
			RunID    string `json:"run_id"`
			RunIndex int    `json:"run_index"`
		} `json:"data"`
	}

	if err := json.NewDecoder(listRunsAfterRec.Body).Decode(&sourceRunsAfterResp); err != nil {
		t.Fatal(err)
	}

	if len(sourceRunsAfterResp.Data) != 1 || sourceRunsAfterResp.Data[0].RunID != secondTriggerResp.RunID || sourceRunsAfterResp.Data[0].RunIndex != 2 {
		t.Fatalf("after_index source run history mismatch: %+v", sourceRunsAfterResp.Data)
	}

	disableRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/managed-repo", map[string]any{
		"enabled": false,
	})
	if disableRec.Code != http.StatusOK {
		t.Fatalf("disable source repository: status=%d body=%s", disableRec.Code, disableRec.Body.String())
	}

	listDisabledRunsRec := httptest.NewRecorder()
	listDisabledRunsReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/managed-repo/jobs/build/runs", nil)
	handler.ServeHTTP(listDisabledRunsRec, listDisabledRunsReq)
	if listDisabledRunsRec.Code != http.StatusOK {
		t.Fatalf("list disabled source repository job runs: status=%d body=%s", listDisabledRunsRec.Code, listDisabledRunsRec.Body.String())
	}
}

func TestAPIServer_SSESourceRepositoryJobRunsReceivesSourceTrigger(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, db := setupTestServer(t)
	handler := server.Handler()
	repos := dal.NewSQLRepositories(db)
	repoPath := initAPIGitRepo(t)
	writeAPIJobDefinitionAndCommit(t, repoPath, "source-sse", "source sse definition")

	registerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "vectis-local",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": repoPath,
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL+"/api/v1/sse/source-repositories/vectis-local/jobs/build/runs", nil)
	if err != nil {
		t.Fatalf("create source sse request: %v", err)
	}

	req.Header.Set("Accept", "text/event-stream")
	sseResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect source sse: %v", err)
	}
	defer sseResp.Body.Close()

	reader := bufio.NewReader(sseResp.Body)
	var dataBuf strings.Builder
	readEvent := func(label string) struct {
		RunID    string `json:"run_id"`
		RunIndex int    `json:"run_index"`
	} {
		t.Helper()

		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("read %s source sse line: %v", label, err)
			}

			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				if dataBuf.Len() == 0 {
					continue
				}

				message := []byte(dataBuf.String())
				dataBuf.Reset()

				var ev struct {
					RunID    string `json:"run_id"`
					RunIndex int    `json:"run_index"`
				}

				if err := json.Unmarshal(message, &ev); err != nil {
					t.Fatalf("unmarshal %s source run event: %v", label, err)
				}

				return ev
			}

			if after, ok := strings.CutPrefix(line, "data:"); ok {
				data := strings.TrimSpace(after)
				dataBuf.WriteString(data)
			}
		}
	}

	triggerBody := strings.NewReader(`{"ref":"HEAD"}`)
	triggerResp, err := http.Post(httpServer.URL+"/api/v1/source-repositories/vectis-local/jobs/build/trigger", "application/json", triggerBody)
	if err != nil {
		t.Fatalf("trigger source job: %v", err)
	}

	triggerResp.Body.Close()
	if triggerResp.StatusCode != http.StatusAccepted {
		t.Fatalf("trigger source job: expected 202, got %d", triggerResp.StatusCode)
	}

	ev := readEvent("trigger")
	if ev.RunID == "" {
		t.Error("expected non-empty run_id")
	}

	if ev.RunIndex != 1 {
		t.Errorf("expected run_index 1, got %d", ev.RunIndex)
	}

	if err := repos.Runs().MarkRunSucceeded(context.Background(), ev.RunID); err != nil {
		t.Fatalf("mark source run succeeded: %v", err)
	}

	replayResp, err := http.Post(httpServer.URL+"/api/v1/runs/"+ev.RunID+"/replay", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("replay source run: %v", err)
	}
	replayResp.Body.Close()
	if replayResp.StatusCode != http.StatusAccepted {
		t.Fatalf("replay source run: expected 202, got %d", replayResp.StatusCode)
	}

	replayEvent := readEvent("replay")
	if replayEvent.RunID == "" || replayEvent.RunID == ev.RunID {
		t.Fatalf("expected distinct replay run_id, got trigger=%q replay=%q", ev.RunID, replayEvent.RunID)
	}

	if replayEvent.RunIndex != 2 {
		t.Errorf("expected replay run_index 2, got %d", replayEvent.RunIndex)
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
		"authoring_mode": dal.SourceAuthoringModeExternalChangeRequest,
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
		AuthoringMode string `json:"authoring_mode"`
		Authoring     struct {
			Mode                   string `json:"mode"`
			WriteDefinitions       bool   `json:"write_definitions"`
			LocalCommits           bool   `json:"local_commits"`
			ExternalChangeRequests bool   `json:"external_change_requests"`
			Reason                 string `json:"reason"`
		} `json:"authoring"`
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
		updateResp.AuthoringMode != dal.SourceAuthoringModeExternalChangeRequest ||
		updateResp.Authoring.Mode != dal.SourceAuthoringModeExternalChangeRequest ||
		updateResp.Authoring.WriteDefinitions ||
		updateResp.Authoring.LocalCommits ||
		updateResp.Authoring.ExternalChangeRequests ||
		updateResp.Authoring.Reason != "source_repository_disabled" ||
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

	invalidAuthoringModeRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/vectis-local", map[string]any{
		"authoring_mode": "magic",
	})

	assertAPIError(t, invalidAuthoringModeRec, http.StatusBadRequest, "unsupported_authoring_mode")

	incompatibleAuthoringModeRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/vectis-local", map[string]any{
		"checkout_mode":  dal.SourceCheckoutModeExternal,
		"authoring_mode": dal.SourceAuthoringModeLocalCommit,
	})

	assertAPIError(t, incompatibleAuthoringModeRec, http.StatusBadRequest, "incompatible_authoring_mode")
}

func TestAPIServer_SourceRepositoryRejectsInvalidDefaultRef(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	server, _, _, _ := setupTestServer(t)
	handler := server.Handler()

	createRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "invalid-ref",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": t.TempDir(),
		"default_ref":   "HEAD~1",
	})

	assertAPIError(t, createRec, http.StatusBadRequest, "invalid_source_reference")

	registerRec := doJSONRequest(t, handler, http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "vectis-local",
		"source_kind":   dal.SourceKindLocalCheckout,
		"checkout_path": t.TempDir(),
		"default_ref":   "HEAD",
	})

	if registerRec.Code != http.StatusCreated {
		t.Fatalf("register source repository: status=%d body=%s", registerRec.Code, registerRec.Body.String())
	}

	updateRec := doJSONRequest(t, handler, http.MethodPut, "/api/v1/source-repositories/vectis-local", map[string]any{
		"default_ref": "main..other",
	})

	assertAPIError(t, updateRec, http.StatusBadRequest, "invalid_source_reference")
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
	Truncated      bool   `json:"truncated"`
	NextCursor     string `json:"next_cursor"`
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
		Truncated      bool   `json:"truncated"`
		NextCursor     string `json:"next_cursor"`
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
	AuthoringMode string `json:"authoring_mode"`
	Authoring     struct {
		Mode                   string `json:"mode"`
		WriteDefinitions       bool   `json:"write_definitions"`
		LocalCommits           bool   `json:"local_commits"`
		ExternalChangeRequests bool   `json:"external_change_requests"`
		Reason                 string `json:"reason"`
	} `json:"authoring"`
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
		AuthoringMode string `json:"authoring_mode"`
		Authoring     struct {
			Mode                   string `json:"mode"`
			WriteDefinitions       bool   `json:"write_definitions"`
			LocalCommits           bool   `json:"local_commits"`
			ExternalChangeRequests bool   `json:"external_change_requests"`
			Reason                 string `json:"reason"`
		} `json:"authoring"`
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

func decodeSourceRepositoryJobsResponse(t *testing.T, rec *httptest.ResponseRecorder) struct {
	RepositoryID   string `json:"repository_id"`
	RequestedRef   string `json:"requested_ref"`
	ResolvedCommit string `json:"resolved_commit"`
	Path           string `json:"path"`
	Limit          int    `json:"limit"`
	Truncated      bool   `json:"truncated"`
	NextCursor     string `json:"next_cursor"`
	Jobs           []struct {
		JobID   string `json:"job_id"`
		Path    string `json:"path"`
		Name    string `json:"name"`
		BlobSHA string `json:"blob_sha"`
		Source  struct {
			RepositoryID   string `json:"repository_id"`
			RequestedRef   string `json:"requested_ref"`
			ResolvedCommit string `json:"resolved_commit"`
			Path           string `json:"path"`
			BlobSHA        string `json:"blob_sha"`
		} `json:"source"`
	} `json:"jobs"`
	Invalid []struct {
		Path    string `json:"path"`
		Name    string `json:"name"`
		BlobSHA string `json:"blob_sha"`
		Error   string `json:"error"`
	} `json:"invalid"`
} {
	t.Helper()

	var out struct {
		RepositoryID   string `json:"repository_id"`
		RequestedRef   string `json:"requested_ref"`
		ResolvedCommit string `json:"resolved_commit"`
		Path           string `json:"path"`
		Limit          int    `json:"limit"`
		Truncated      bool   `json:"truncated"`
		NextCursor     string `json:"next_cursor"`
		Jobs           []struct {
			JobID   string `json:"job_id"`
			Path    string `json:"path"`
			Name    string `json:"name"`
			BlobSHA string `json:"blob_sha"`
			Source  struct {
				RepositoryID   string `json:"repository_id"`
				RequestedRef   string `json:"requested_ref"`
				ResolvedCommit string `json:"resolved_commit"`
				Path           string `json:"path"`
				BlobSHA        string `json:"blob_sha"`
			} `json:"source"`
		} `json:"jobs"`
		Invalid []struct {
			Path    string `json:"path"`
			Name    string `json:"name"`
			BlobSHA string `json:"blob_sha"`
			Error   string `json:"error"`
		} `json:"invalid"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	return out
}

func decodeSourceRepositoryJobDefinitionResponse(t *testing.T, rec *httptest.ResponseRecorder) struct {
	JobID          string          `json:"job_id"`
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

func decodeSourceRepositoryStatusResponse(t *testing.T, rec *httptest.ResponseRecorder) struct {
	RepositoryID  string `json:"repository_id"`
	Namespace     string `json:"namespace"`
	SourceKind    string `json:"source_kind"`
	Enabled       bool   `json:"enabled"`
	Status        string `json:"status"`
	CheckoutPath  string `json:"checkout_path"`
	CheckoutMode  string `json:"checkout_mode"`
	AuthoringMode string `json:"authoring_mode"`
	Authoring     struct {
		Mode                   string `json:"mode"`
		WriteDefinitions       bool   `json:"write_definitions"`
		LocalCommits           bool   `json:"local_commits"`
		ExternalChangeRequests bool   `json:"external_change_requests"`
		Reason                 string `json:"reason"`
	} `json:"authoring"`
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
		RepositoryID  string `json:"repository_id"`
		Namespace     string `json:"namespace"`
		SourceKind    string `json:"source_kind"`
		Enabled       bool   `json:"enabled"`
		Status        string `json:"status"`
		CheckoutPath  string `json:"checkout_path"`
		CheckoutMode  string `json:"checkout_mode"`
		AuthoringMode string `json:"authoring_mode"`
		Authoring     struct {
			Mode                   string `json:"mode"`
			WriteDefinitions       bool   `json:"write_definitions"`
			LocalCommits           bool   `json:"local_commits"`
			ExternalChangeRequests bool   `json:"external_change_requests"`
			Reason                 string `json:"reason"`
		} `json:"authoring"`
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
