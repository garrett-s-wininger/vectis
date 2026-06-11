package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"

	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
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
