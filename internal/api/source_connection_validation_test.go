package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vectis/internal/dal"
	sourcepkg "vectis/internal/source"
)

func TestSourceConnectionValidationBeforePersistence(t *testing.T) {
	t.Parallel()
	server, _, _, db := setupTestServer(t)
	root := filepath.Join(t.TempDir(), "checkouts")
	server.SetSourceCheckoutRoot(root)
	remote := initAPIRemoteGitRepo(t)
	writeAPIFileAndCommit(t, remote, ".vectis.json", `{"root":{"id":"root","uses":"builtins/script","with":{"script":"true"}}}`, "source")

	var validationPaths []string
	server.SetSourceSyncCheckoutStatus(func(ctx context.Context, rec dal.SourceRepositoryRecord, ref string) sourcepkg.GitCheckoutStatus {
		validationPaths = append(validationPaths, rec.CheckoutPath)
		return sourcepkg.SyncManagedGitCheckout(ctx, sourcepkg.ManagedGitCheckoutRequest{CheckoutPath: rec.CheckoutPath, RemoteURL: rec.CanonicalURL, DefaultRef: ref})
	})

	h := server.Handler()
	sources := dal.NewSQLRepositories(db).Sources()
	input := map[string]any{"repository_id": "remote", "canonical_url": remote, "checkout_mode": "managed", "default_ref": "missing", "validate_connection": true}
	bad := doJSONRequest(t, h, "POST", "/api/v1/source-repositories", input)
	if bad.Code != http.StatusUnprocessableEntity {
		t.Fatalf("bad ref: %d %s", bad.Code, bad.Body.String())
	}

	if _, err := sources.GetRepository(context.Background(), "remote"); !dal.IsNotFound(err) {
		t.Fatalf("failed validation persisted a connection: %v", err)
	}

	input["default_ref"] = "main"
	input["validate_only"] = true
	preview := doJSONRequest(t, h, http.MethodPost, "/api/v1/source-repositories", input)
	if preview.Code != http.StatusOK || !strings.Contains(preview.Body.String(), apiGitOutput(t, remote, "rev-parse", "HEAD")) {
		t.Fatalf("preview: %d %s", preview.Code, preview.Body.String())
	}

	if _, err := sources.GetRepository(context.Background(), "remote"); !dal.IsNotFound(err) {
		t.Fatalf("preview persisted a connection: %v", err)
	}

	delete(input, "validate_only")
	input["validate_connection"] = false
	validationCount := len(validationPaths)
	created := doJSONRequest(t, h, "POST", "/api/v1/source-repositories", input)
	if created.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}

	if len(validationPaths) != validationCount {
		t.Fatal("saving after a preview repeated validation")
	}

	sync := httptest.NewRecorder()
	h.ServeHTTP(sync, httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/remote/sync", nil))
	if sync.Code != http.StatusOK || decodeSourceRepositoryResponse(t, sync).Sync.Status != dal.SourceSyncStatusSucceeded {
		t.Fatalf("sync: %d %s", sync.Code, sync.Body.String())
	}

	before, err := sources.GetRepository(context.Background(), "remote")
	if err != nil {
		t.Fatal(err)
	}

	preview = doJSONRequest(t, h, http.MethodPut, "/api/v1/source-repositories/remote", map[string]any{"default_ref": "HEAD", "validate_only": true})
	if preview.Code != http.StatusOK {
		t.Fatalf("edit preview: %d %s", preview.Code, preview.Body.String())
	}

	unchanged, err := sources.GetRepository(context.Background(), "remote")
	if err != nil || unchanged.DefaultRef != before.DefaultRef {
		t.Fatalf("edit preview changed saved settings: %+v %v", unchanged, err)
	}

	bad = doJSONRequest(t, h, "PUT", "/api/v1/source-repositories/remote", map[string]any{"default_ref": "missing", "validate_connection": true})
	if bad.Code != http.StatusUnprocessableEntity {
		t.Fatalf("update: %d %s", bad.Code, bad.Body.String())
	}

	after, err := sources.GetRepository(context.Background(), "remote")
	if err != nil {
		t.Fatal(err)
	}

	if after.DefaultRef != before.DefaultRef || after.LastSyncCommit != before.LastSyncCommit {
		t.Fatal("failed edit changed the saved settings or checkout status")
	}

	status := sourcepkg.NewGitCheckout(before.CheckoutPath).Status(context.Background(), before.DefaultRef)
	if status.ErrorCode != "" || status.ResolvedCommit != before.LastSyncCommit {
		t.Fatalf("failed edit changed the live checkout: %+v", status)
	}

	for _, path := range validationPaths {
		if path == before.CheckoutPath {
			continue
		}

		if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
			t.Fatalf("validation checkout was not cleaned up: %s: %v", path, err)
		}
	}
}

func TestSourceConnectionValidationSupportsLocalGitWithoutUpstream(t *testing.T) {
	t.Parallel()
	server, _, _, _ := setupTestServer(t)
	local := initAPIRemoteGitRepo(t)
	writeAPIFileAndCommit(t, local, ".vectis.json", `{"root":{"id":"root","uses":"builtins/script","with":{"script":"true"}}}`, "local")

	server.SetSourceCheckoutRoot(filepath.Dir(local))
	created := doJSONRequest(t, server.Handler(), http.MethodPost, "/api/v1/source-repositories", map[string]any{
		"repository_id": "local", "checkout_path": local, "checkout_mode": "managed", "default_ref": "main", "validate_connection": true,
	})

	if created.Code != http.StatusCreated {
		t.Fatalf("local validation: %d %s", created.Code, created.Body.String())
	}
}
