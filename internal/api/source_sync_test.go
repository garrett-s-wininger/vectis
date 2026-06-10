package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	sourcepkg "vectis/internal/source"
	"vectis/internal/testutil/dbtest"
)

func TestAPIServer_SyncSourceRepositoryReturnsRunningForDuplicate(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	db := dbtest.NewTestDB(t)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	handler := server.Handler()
	ctx := context.Background()

	if _, err := dal.NewSQLRepositories(db).Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "locked-repo",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: filepath.Join(t.TempDir(), "repo"),
		DefaultRef:   "HEAD",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var calls atomic.Int32
	server.sourceSyncCheckoutStatus = func(_ context.Context, rec dal.SourceRepositoryRecord, syncRef string) sourcepkg.GitCheckoutStatus {
		calls.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		return sourcepkg.GitCheckoutStatus{
			CheckoutPath:       rec.CheckoutPath,
			PathExists:         true,
			PathIsDirectory:    true,
			GitRepository:      true,
			DefaultRef:         syncRef,
			DefaultRefResolved: true,
			ResolvedCommit:     "0123456789abcdef0123456789abcdef01234567",
		}
	}

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/locked-repo/sync", nil)
		handler.ServeHTTP(rec, req)
		firstDone <- rec
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first sync to start")
	}

	duplicate := httptest.NewRecorder()
	duplicateReq := httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/locked-repo/sync", nil)
	handler.ServeHTTP(duplicate, duplicateReq)
	if duplicate.Code != http.StatusAccepted {
		close(release)
		t.Fatalf("duplicate sync status: got %d body=%s", duplicate.Code, duplicate.Body.String())
	}

	if got := duplicate.Header().Get("Retry-After"); got != "1" {
		close(release)
		t.Fatalf("duplicate Retry-After: got %q", got)
	}

	var duplicateResp sourceRepositoryResponse
	if err := json.NewDecoder(duplicate.Body).Decode(&duplicateResp); err != nil {
		close(release)
		t.Fatalf("decode duplicate response: %v", err)
	}

	if duplicateResp.RepositoryID != "locked-repo" ||
		duplicateResp.Sync.Status != dal.SourceSyncStatusRunning ||
		duplicateResp.Sync.Ref != "HEAD" {
		close(release)
		t.Fatalf("duplicate sync response mismatch: %+v", duplicateResp)
	}

	close(release)

	first := <-firstDone
	if first.Code != http.StatusOK {
		t.Fatalf("first sync status: got %d body=%s", first.Code, first.Body.String())
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected duplicate request not to run sync, got %d sync calls", got)
	}
}
