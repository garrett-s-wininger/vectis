package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/viper"

	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	sourcepkg "vectis/internal/source"
	"vectis/internal/testutil/dbtest"
)

func TestBuildAccessLogger_json(t *testing.T) {
	log, closeLog := buildAccessLogger("json")
	if closeLog != nil {
		defer func() { _ = closeLog() }()
	}

	if log == nil {
		t.Fatal("expected non-nil logger for json format")
	}
}

func TestBuildAccessLogger_text(t *testing.T) {
	log, closeLog := buildAccessLogger("text")
	if closeLog != nil {
		defer func() { _ = closeLog() }()
	}

	if log != nil {
		t.Fatal("expected nil logger for text format")
	}
}

func TestBuildAccessLogger_caseInsensitive(t *testing.T) {
	log, closeLog := buildAccessLogger("JSON")
	if closeLog != nil {
		defer func() { _ = closeLog() }()
	}

	if log == nil {
		t.Fatal("expected non-nil logger for uppercase JSON")
	}
}

func TestBuildAccessLogger_empty(t *testing.T) {
	log, closeLog := buildAccessLogger("")
	if closeLog != nil {
		defer func() { _ = closeLog() }()
	}

	if log != nil {
		t.Fatal("expected nil logger for empty format")
	}
}

func TestWarnIfProcessLocalAPICache(t *testing.T) {
	var buf bytes.Buffer
	logger := interfaces.NewLogger("api-test").WithOutput(&buf)

	warnIfProcessLocalAPICache(logger, config.APICacheBackendDatabase, true)
	warnIfProcessLocalAPICache(logger, config.APICacheBackendMemory, false)
	if buf.Len() != 0 {
		t.Fatalf("unexpected warning: %s", buf.String())
	}

	warnIfProcessLocalAPICache(logger, config.APICacheBackendMemory, true)
	if got := buf.String(); !strings.Contains(got, "api.cache.backend=memory") || !strings.Contains(got, "process-local") {
		t.Fatalf("warning = %q, want process-local memory cache warning", got)
	}
}

func TestReconcileConfiguredSourceRepositories_CreatesAndUpdates(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id": "vectis-local",
			"source_kind":   dal.SourceKindLocalCheckout,
			"checkout_path": "/work/vectis",
			"checkout_mode": dal.SourceCheckoutModeExternal,
			"default_ref":   "main",
			"enabled":       true,
		},
	})

	if err := reconcileConfiguredSourceRepositories(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile create: %v", err)
	}

	got, err := repos.Sources().GetRepository(ctx, "vectis-local")
	if err != nil {
		t.Fatalf("GetRepository: %v", err)
	}

	if got.CheckoutPath != "/work/vectis" || got.DefaultRef != "main" || !got.Enabled {
		t.Fatalf("created repository mismatch: %+v", got)
	}

	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id":  "vectis-local",
			"source_kind":    dal.SourceKindLocalCheckout,
			"checkout_path":  "/work/vectis-next",
			"checkout_mode":  dal.SourceCheckoutModeExternal,
			"authoring_mode": dal.SourceAuthoringModeReadOnly,
			"default_ref":    "release",
			"credential_ref": "repo-token",
			"enabled":        false,
		},
	})

	if err := reconcileConfiguredSourceRepositories(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile update: %v", err)
	}

	got, err = repos.Sources().GetRepository(ctx, "vectis-local")
	if err != nil {
		t.Fatalf("GetRepository after update: %v", err)
	}

	if got.CheckoutPath != "/work/vectis-next" ||
		got.DefaultRef != "release" ||
		got.CredentialRef != "repo-token" ||
		got.Enabled {
		t.Fatalf("updated repository mismatch: %+v", got)
	}
}

func TestReconcileConfiguredSourceRepositories_DerivesManagedCheckoutPath(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")

	checkoutRoot := filepath.Join(t.TempDir(), "checkouts")
	viper.Set("source.checkout_root", checkoutRoot)
	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id": "github.com/acme/Big Repo.git",
			"checkout_mode": dal.SourceCheckoutModeManaged,
			"canonical_url": "https://example.invalid/acme/big-repo.git",
		},
	})

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	if err := reconcileConfiguredSourceRepositories(context.Background(), repos, nil); err != nil {
		t.Fatalf("reconcile managed: %v", err)
	}

	got, err := repos.Sources().GetRepository(context.Background(), "github.com/acme/Big Repo.git")
	if err != nil {
		t.Fatalf("GetRepository: %v", err)
	}

	if got.CheckoutMode != dal.SourceCheckoutModeManaged ||
		got.CanonicalURL != "https://example.invalid/acme/big-repo.git" ||
		!strings.HasPrefix(got.CheckoutPath, checkoutRoot+string(filepath.Separator)) {
		t.Fatalf("managed repository mismatch: %+v", got)
	}
}

func TestReconcileConfiguredSourceRepositories_RejectsNamespaceMove(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	if _, err := repos.Namespaces().Create(ctx, "team-a", nil); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id": "vectis-local",
			"namespace":     "/team-a",
			"source_kind":   dal.SourceKindLocalCheckout,
			"checkout_path": "/work/vectis",
		},
	})

	if err := reconcileConfiguredSourceRepositories(ctx, repos, nil); err == nil {
		t.Fatal("expected namespace move error")
	}
}

func TestReconcileConfiguredSourceSchedules_CreatesAndUpdates(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_SOURCE_SCHEDULES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SCHEDULES", "")

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id": "vectis",
			"checkout_path": "/work/vectis",
			"default_ref":   "main",
			"enabled":       true,
		},
	})

	if err := reconcileConfiguredSourceRepositories(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile repositories: %v", err)
	}

	viper.Set("source.schedules", []map[string]any{
		{
			"schedule_id":   "nightly-build",
			"repository_id": "vectis",
			"job_id":        "build",
			"cron_spec":     "0 * * * *",
			"ref":           "main",
			"path":          ".vectis/jobs/build.json",
			"enabled":       true,
		},
	})

	if err := reconcileConfiguredSourceSchedules(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile schedule create: %v", err)
	}

	got, err := repos.Schedules().GetCronScheduleByScheduleID(ctx, "nightly-build")
	if err != nil {
		t.Fatalf("GetCronScheduleByScheduleID: %v", err)
	}

	if got.JobID != "build" ||
		got.SourceRepositoryID != "vectis" ||
		got.SourceRef != "main" ||
		got.SourcePath != ".vectis/jobs/build.json" ||
		got.CronSpec != "0 * * * *" ||
		!got.Enabled ||
		got.NextRunAt.IsZero() {
		t.Fatalf("created schedule mismatch: %+v", got)
	}

	viper.Set("source.schedules", []map[string]any{
		{
			"schedule_id":   "nightly-build",
			"repository_id": "vectis",
			"job_id":        "deploy",
			"cron_spec":     "30 * * * *",
			"ref":           "release",
			"path":          ".vectis/jobs/deploy.json",
			"enabled":       false,
		},
	})

	if err := reconcileConfiguredSourceSchedules(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile schedule update: %v", err)
	}

	got, err = repos.Schedules().GetCronScheduleByScheduleID(ctx, "nightly-build")
	if err != nil {
		t.Fatalf("GetCronScheduleByScheduleID after update: %v", err)
	}

	if got.JobID != "deploy" ||
		got.SourceRepositoryID != "vectis" ||
		got.SourceRef != "release" ||
		got.SourcePath != ".vectis/jobs/deploy.json" ||
		got.CronSpec != "30 * * * *" ||
		got.Enabled {
		t.Fatalf("updated schedule mismatch: %+v", got)
	}

	oldNextRun := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx, "UPDATE cron_trigger_specs SET next_run_at = ? WHERE id = ?", oldNextRun.Format(time.RFC3339), got.ID); err != nil {
		t.Fatalf("force disabled next_run_at: %v", err)
	}

	viper.Set("source.schedules", []map[string]any{
		{
			"schedule_id":   "nightly-build",
			"repository_id": "vectis",
			"job_id":        "deploy",
			"cron_spec":     "30 * * * *",
			"ref":           "release",
			"path":          ".vectis/jobs/deploy.json",
			"enabled":       true,
		},
	})

	if err := reconcileConfiguredSourceSchedules(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile schedule enable: %v", err)
	}

	got, err = repos.Schedules().GetCronScheduleByScheduleID(ctx, "nightly-build")
	if err != nil {
		t.Fatalf("GetCronScheduleByScheduleID after enable: %v", err)
	}

	if !got.Enabled {
		t.Fatalf("expected enabled schedule after reconcile: %+v", got)
	}

	if !got.NextRunAt.After(oldNextRun) {
		t.Fatalf("expected re-enabled schedule to compute a fresh next_run_at after %v, got %v", oldNextRun, got.NextRunAt)
	}
}

func TestReconcileConfiguredSourceSchedules_RejectsInvalid(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_SOURCE_SCHEDULES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SCHEDULES", "")

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	viper.Set("source.schedules", []map[string]any{
		{
			"schedule_id":   "missing-repo",
			"repository_id": "missing",
			"job_id":        "build",
			"cron_spec":     "0 * * * *",
		},
	})

	if err := reconcileConfiguredSourceSchedules(ctx, repos, nil); err == nil {
		t.Fatal("expected missing repository error")
	}

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	viper.Set("source.schedules", []map[string]any{
		{
			"schedule_id":   "bad-cron",
			"repository_id": "vectis",
			"job_id":        "build",
			"cron_spec":     "not a cron spec",
		},
	})

	if err := reconcileConfiguredSourceSchedules(ctx, repos, nil); err == nil {
		t.Fatal("expected invalid cron spec error")
	}

	viper.Set("source.schedules", []map[string]any{
		{
			"schedule_id":   "bad-job-id",
			"repository_id": "vectis",
			"job_id":        "bad/id",
			"cron_spec":     "0 * * * *",
		},
	})

	if err := reconcileConfiguredSourceSchedules(ctx, repos, nil); err == nil {
		t.Fatal("expected invalid derived source path error")
	}
}

func TestSyncConfiguredSourceRepositories_SyncsEnabledRepositories(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP", "")

	viper.Set("source.sync_configured_repositories_on_startup", true)
	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id": "vectis-local",
			"checkout_path": "/work/vectis",
			"default_ref":   "main",
			"enabled":       true,
		},
	})

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	if err := reconcileConfiguredSourceRepositories(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	calls := 0
	err := syncConfiguredSourceRepositoriesWithStatus(ctx, repos, nil, func(_ context.Context, rec dal.SourceRepositoryRecord, syncRef string) sourcepkg.GitCheckoutStatus {
		calls++
		if rec.RepositoryID != "vectis-local" || syncRef != "main" {
			t.Fatalf("sync input mismatch: rec=%+v syncRef=%q", rec, syncRef)
		}

		return sourcepkg.GitCheckoutStatus{ResolvedCommit: "abc123"}
	})

	if err != nil {
		t.Fatalf("sync: %v", err)
	}

	if calls != 1 {
		t.Fatalf("sync calls=%d, want 1", calls)
	}

	got, err := repos.Sources().GetRepository(ctx, "vectis-local")
	if err != nil {
		t.Fatalf("GetRepository: %v", err)
	}

	if got.SyncStatus != dal.SourceSyncStatusSucceeded ||
		got.LastSyncRef != "main" ||
		got.LastSyncCommit != "abc123" ||
		got.LastSyncStartedAtUnix == 0 ||
		got.LastSyncFinishedAtUnix == 0 ||
		got.LastSyncError != "" {
		t.Fatalf("sync result mismatch: %+v", got)
	}
}

func TestSyncConfiguredSourceRepositories_PersistsFailure(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP", "")

	viper.Set("source.sync_configured_repositories_on_startup", true)
	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id": "vectis-local",
			"checkout_path": "/work/vectis",
		},
	})

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	if err := reconcileConfiguredSourceRepositories(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	err := syncConfiguredSourceRepositoriesWithStatus(ctx, repos, nil, func(_ context.Context, _ dal.SourceRepositoryRecord, syncRef string) sourcepkg.GitCheckoutStatus {
		if syncRef != "HEAD" {
			t.Fatalf("syncRef=%q, want HEAD", syncRef)
		}

		return sourcepkg.GitCheckoutStatus{ErrorCode: "git_fetch_failed", ErrorMessage: "remote unavailable"}
	})

	if err == nil {
		t.Fatal("expected startup sync failure")
	}

	got, err := repos.Sources().GetRepository(ctx, "vectis-local")
	if err != nil {
		t.Fatalf("GetRepository: %v", err)
	}

	if got.SyncStatus != dal.SourceSyncStatusFailed ||
		got.LastSyncRef != "HEAD" ||
		got.LastSyncError != "git_fetch_failed: remote unavailable" ||
		got.LastSyncFinishedAtUnix == 0 {
		t.Fatalf("failed sync result mismatch: %+v", got)
	}
}

func TestSyncConfiguredSourceRepositories_PersistsFailureAfterCanceledSyncContext(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP", "")

	viper.Set("source.sync_configured_repositories_on_startup", true)
	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id": "vectis-local",
			"checkout_path": "/work/vectis",
		},
	})

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	baseCtx := context.Background()
	if err := reconcileConfiguredSourceRepositories(baseCtx, repos, nil); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	syncCtx, cancel := context.WithCancel(baseCtx)
	err := syncConfiguredSourceRepositoriesWithStatus(syncCtx, repos, nil, func(context.Context, dal.SourceRepositoryRecord, string) sourcepkg.GitCheckoutStatus {
		cancel()
		return sourcepkg.GitCheckoutStatus{ErrorCode: "git_fetch_failed", ErrorMessage: "context deadline exceeded"}
	})

	if err == nil {
		t.Fatal("expected startup sync failure")
	}

	got, err := repos.Sources().GetRepository(baseCtx, "vectis-local")
	if err != nil {
		t.Fatalf("GetRepository: %v", err)
	}

	if got.SyncStatus != dal.SourceSyncStatusFailed ||
		got.LastSyncError != "git_fetch_failed: context deadline exceeded" {
		t.Fatalf("failed sync result mismatch after canceled context: %+v", got)
	}
}

func TestSyncConfiguredSourceRepositories_SkipsDisabledRepositories(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP", "")

	viper.Set("source.sync_configured_repositories_on_startup", true)
	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id": "vectis-local",
			"checkout_path": "/work/vectis",
			"enabled":       false,
		},
	})

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	if err := reconcileConfiguredSourceRepositories(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	calls := 0
	err := syncConfiguredSourceRepositoriesWithStatus(ctx, repos, nil, func(context.Context, dal.SourceRepositoryRecord, string) sourcepkg.GitCheckoutStatus {
		calls++
		return sourcepkg.GitCheckoutStatus{ResolvedCommit: "abc123"}
	})

	if err != nil {
		t.Fatalf("sync disabled: %v", err)
	}

	if calls != 0 {
		t.Fatalf("sync calls=%d, want 0", calls)
	}

	got, err := repos.Sources().GetRepository(ctx, "vectis-local")
	if err != nil {
		t.Fatalf("GetRepository: %v", err)
	}

	if got.SyncStatus != dal.SourceSyncStatusNever ||
		got.LastSyncStartedAtUnix != 0 ||
		got.LastSyncFinishedAtUnix != 0 {
		t.Fatalf("disabled repository sync result mismatch: %+v", got)
	}
}

func TestSyncConfiguredSourceRepositoriesPeriodicCycle_ContinuesAfterFailure(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_MAX_CONCURRENCY", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_MAX_CONCURRENCY", "")
	t.Setenv("VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_FAILURE_BACKOFF", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_FAILURE_BACKOFF", "")

	viper.Set("source.sync_configured_repositories_max_concurrency", 1)
	viper.Set("source.sync_configured_repositories_failure_backoff", 0)
	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id": "bad-repo",
			"checkout_path": "/work/bad",
		},
		{
			"repository_id": "good-repo",
			"checkout_path": "/work/good",
		},
	})

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	if err := reconcileConfiguredSourceRepositories(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	calls := make([]string, 0, 2)
	err := syncConfiguredSourceRepositoriesPeriodicCycle(ctx, repos, nil, func(_ context.Context, rec dal.SourceRepositoryRecord, _ string) sourcepkg.GitCheckoutStatus {
		calls = append(calls, rec.RepositoryID)
		if rec.RepositoryID == "bad-repo" {
			return sourcepkg.GitCheckoutStatus{ErrorCode: "git_fetch_failed", ErrorMessage: "remote unavailable"}
		}

		return sourcepkg.GitCheckoutStatus{ResolvedCommit: "good123"}
	})

	if err == nil {
		t.Fatal("expected periodic sync cycle to report failed repository")
	}

	if strings.Join(calls, ",") != "bad-repo,good-repo" {
		t.Fatalf("sync calls=%v, want bad-repo,good-repo", calls)
	}

	bad, err := repos.Sources().GetRepository(ctx, "bad-repo")
	if err != nil {
		t.Fatalf("GetRepository bad: %v", err)
	}

	good, err := repos.Sources().GetRepository(ctx, "good-repo")
	if err != nil {
		t.Fatalf("GetRepository good: %v", err)
	}

	if bad.SyncStatus != dal.SourceSyncStatusFailed || bad.LastSyncError != "git_fetch_failed: remote unavailable" {
		t.Fatalf("bad repository sync mismatch: %+v", bad)
	}

	if good.SyncStatus != dal.SourceSyncStatusSucceeded || good.LastSyncCommit != "good123" {
		t.Fatalf("good repository sync mismatch: %+v", good)
	}
}

func TestSyncConfiguredSourceRepositoriesPeriodicCycle_SkipsRecentFailures(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_FAILURE_BACKOFF", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_FAILURE_BACKOFF", "")

	viper.Set("source.sync_configured_repositories_failure_backoff", time.Hour)
	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id": "recent-failure",
			"checkout_path": "/work/recent",
		},
		{
			"repository_id": "eligible",
			"checkout_path": "/work/eligible",
		},
	})

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	if err := reconcileConfiguredSourceRepositories(ctx, repos, nil); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	nowUnix := time.Now().Unix()
	if _, err := repos.Sources().UpdateRepositorySync(ctx, dal.SourceRepositorySyncRecord{
		RepositoryID:   "recent-failure",
		Status:         dal.SourceSyncStatusFailed,
		StartedAtUnix:  nowUnix - 10,
		FinishedAtUnix: nowUnix,
		Ref:            "HEAD",
		Error:          "git_fetch_failed: remote unavailable",
	}); err != nil {
		t.Fatalf("mark recent failure: %v", err)
	}

	calls := make([]string, 0, 1)
	if err := syncConfiguredSourceRepositoriesPeriodicCycle(ctx, repos, nil, func(_ context.Context, rec dal.SourceRepositoryRecord, _ string) sourcepkg.GitCheckoutStatus {
		calls = append(calls, rec.RepositoryID)
		return sourcepkg.GitCheckoutStatus{ResolvedCommit: "ok123"}
	}); err != nil {
		t.Fatalf("periodic sync cycle: %v", err)
	}

	if strings.Join(calls, ",") != "eligible" {
		t.Fatalf("sync calls=%v, want only eligible", calls)
	}

	recent, err := repos.Sources().GetRepository(ctx, "recent-failure")
	if err != nil {
		t.Fatalf("GetRepository recent-failure: %v", err)
	}

	if recent.SyncStatus != dal.SourceSyncStatusFailed || recent.LastSyncFinishedAtUnix != nowUnix {
		t.Fatalf("recent failure should remain unchanged during backoff: %+v", recent)
	}
}

func TestSyncConfiguredSourceRepositoryRecords_RespectsMaxConcurrency(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	records := make([]dal.SourceRepositoryRecord, 0, 4)
	for _, id := range []string{"repo-a", "repo-b", "repo-c", "repo-d"} {
		rec, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
			RepositoryID: id,
			SourceKind:   dal.SourceKindLocalCheckout,
			CheckoutPath: "/work/" + id,
			Enabled:      true,
		})

		if err != nil {
			t.Fatalf("CreateRepository %s: %v", id, err)
		}

		records = append(records, rec)
	}

	var mu sync.Mutex
	active := 0
	maxActive := 0
	if err := syncConfiguredSourceRepositoryRecords(ctx, repos, nil, records, 2, func(context.Context, dal.SourceRepositoryRecord, string) sourcepkg.GitCheckoutStatus {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()

		time.Sleep(20 * time.Millisecond)

		mu.Lock()
		active--
		mu.Unlock()

		return sourcepkg.GitCheckoutStatus{ResolvedCommit: "ok123"}
	}); err != nil {
		t.Fatalf("periodic sync records: %v", err)
	}

	if maxActive > 2 {
		t.Fatalf("max active syncs=%d, want <=2", maxActive)
	}

	if maxActive < 2 {
		t.Fatalf("expected syncs to run concurrently up to the limit, max active=%d", maxActive)
	}
}
