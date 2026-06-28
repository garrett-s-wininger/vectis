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
	"vectis/internal/observability"
	sourcepkg "vectis/internal/source"
	"vectis/internal/testutil/dbtest"

	"github.com/spf13/viper"
)

type sourceSyncMetricRecord struct {
	trigger      string
	sourceKind   string
	checkoutMode string
	outcome      string
	reason       string
}

type sourceRefHydrationMetricRecord struct {
	sourceKind   string
	checkoutMode string
	outcome      string
	reason       string
	tier         string
	cacheState   string
}

type recordingSourceSyncMetrics struct {
	mu               sync.Mutex
	records          []sourceSyncMetricRecord
	hydrationRecords []sourceRefHydrationMetricRecord
}

func (m *recordingSourceSyncMetrics) RecordSourceRepositorySync(_ context.Context, trigger, sourceKind, checkoutMode, outcome, reason string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.records = append(m.records, sourceSyncMetricRecord{
		trigger:      trigger,
		sourceKind:   sourceKind,
		checkoutMode: checkoutMode,
		outcome:      outcome,
		reason:       reason,
	})
}

func (m *recordingSourceSyncMetrics) RecordSourceRefHydration(_ context.Context, sourceKind, checkoutMode, outcome, reason, tier, cacheState string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.hydrationRecords = append(m.hydrationRecords, sourceRefHydrationMetricRecord{
		sourceKind:   sourceKind,
		checkoutMode: checkoutMode,
		outcome:      outcome,
		reason:       reason,
		tier:         tier,
		cacheState:   cacheState,
	})
}

func (m *recordingSourceSyncMetrics) has(trigger, sourceKind, checkoutMode, outcome, reason string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, rec := range m.records {
		if rec.trigger == trigger &&
			rec.sourceKind == sourceKind &&
			rec.checkoutMode == checkoutMode &&
			rec.outcome == outcome &&
			rec.reason == reason {
			return true
		}
	}

	return false
}

func (m *recordingSourceSyncMetrics) hasHydration(sourceKind, checkoutMode, outcome, reason, tier, cacheState string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, rec := range m.hydrationRecords {
		if rec.sourceKind == sourceKind &&
			rec.checkoutMode == checkoutMode &&
			rec.outcome == outcome &&
			rec.reason == reason &&
			rec.tier == tier &&
			rec.cacheState == cacheState {
			return true
		}
	}

	return false
}

func TestAPIServer_SyncSourceRepositoryReturnsRunningForDuplicate(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	db := dbtest.NewTestDB(t)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	metrics := &recordingSourceSyncMetrics{}
	server.SetSourceSyncMetrics(metrics)
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

	if !metrics.has(observability.SourceSyncTriggerManual, dal.SourceKindLocalCheckout, dal.SourceCheckoutModeExternal, observability.SourceSyncOutcomeAlreadyRunning, observability.SourceSyncReasonInMemoryLock) {
		t.Fatalf("missing in-memory lock source sync metric: %+v", metrics.records)
	}

	if !metrics.has(observability.SourceSyncTriggerManual, dal.SourceKindLocalCheckout, dal.SourceCheckoutModeExternal, observability.SourceSyncOutcomeSucceeded, observability.SourceSyncReasonNone) {
		t.Fatalf("missing manual success source sync metric: %+v", metrics.records)
	}
}

func TestAPIServer_SourceRefHydrationSingleflightsDuplicate(t *testing.T) {
	server := &APIServer{}
	rec := dal.SourceRepositoryRecord{
		RepositoryID: "managed-repo",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutMode: dal.SourceCheckoutModeManaged,
		CheckoutPath: filepath.Join(t.TempDir(), "managed"),
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var calls atomic.Int32

	server.sourceRefHydrator = func(_ context.Context, got dal.SourceRepositoryRecord, ref, preferredRemote string) sourcepkg.GitCheckoutStatus {
		if got.RepositoryID != rec.RepositoryID || ref != "feature/on-demand" {
			t.Errorf("hydrator input mismatch: rec=%+v ref=%q", got, ref)
		}

		if preferredRemote != "" {
			t.Errorf("duplicate hydration should not have cached remote yet, got %q", preferredRemote)
		}

		calls.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release

		return sourcepkg.GitCheckoutStatus{
			CheckoutPath:       got.CheckoutPath,
			DefaultRef:         ref,
			GitRepository:      true,
			DefaultRefResolved: true,
			ResolvedCommit:     "0123456789abcdef0123456789abcdef01234567",
		}
	}

	firstDone := make(chan bool, 1)
	go func() {
		firstDone <- server.hydrateSourceRepositoryRefAfterNotFound(context.Background(), rec, "feature/on-demand", sourcepkg.ErrNotFound)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first hydration to start")
	}

	secondDone := make(chan bool, 1)
	go func() {
		secondDone <- server.hydrateSourceRepositoryRefAfterNotFound(context.Background(), rec, "feature/on-demand", sourcepkg.ErrNotFound)
	}()

	select {
	case <-secondDone:
		close(release)
		t.Fatal("duplicate hydration completed before leader released")
	case <-time.After(25 * time.Millisecond):
	}

	if got := calls.Load(); got != 1 {
		close(release)
		t.Fatalf("hydrator calls while leader running: got %d, want 1", got)
	}

	close(release)

	if ok := <-firstDone; !ok {
		t.Fatal("first hydration did not report success")
	}

	if ok := <-secondDone; !ok {
		t.Fatal("duplicate hydration did not report shared success")
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("hydrator calls after duplicate completed: got %d, want 1", got)
	}
}

func TestAPIServer_SourceRefHydrationCachesSuccessfulRemote(t *testing.T) {
	server := &APIServer{}
	metrics := &recordingSourceSyncMetrics{}
	server.SetSourceSyncMetrics(metrics)
	rec := dal.SourceRepositoryRecord{
		RepositoryID: "managed-repo",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutMode: dal.SourceCheckoutModeManaged,
		CheckoutPath: filepath.Join(t.TempDir(), "managed"),
	}

	var calls atomic.Int32
	var preferredRemotesMu sync.Mutex
	var preferredRemotes []string
	server.sourceRefHydrator = func(_ context.Context, got dal.SourceRepositoryRecord, ref, preferredRemote string) sourcepkg.GitCheckoutStatus {
		if got.RepositoryID != rec.RepositoryID || ref != "feature/on-demand" {
			t.Errorf("hydrator input mismatch: rec=%+v ref=%q", got, ref)
		}

		preferredRemotesMu.Lock()
		preferredRemotes = append(preferredRemotes, preferredRemote)
		preferredRemotesMu.Unlock()
		calls.Add(1)

		return sourcepkg.GitCheckoutStatus{
			CheckoutPath:       got.CheckoutPath,
			DefaultRef:         ref,
			GitRepository:      true,
			DefaultRefResolved: true,
			ResolvedCommit:     "0123456789abcdef0123456789abcdef01234567",
			HydrationRemote:    "vectis-fallback-2",
			HydrationTier:      "fallback-2",
		}
	}

	first := server.hydrateSourceRepositoryRef(context.Background(), rec, "feature/on-demand")
	if first.ErrorCode != "" || first.HydrationCacheHit {
		t.Fatalf("first hydration status mismatch: %+v", first)
	}

	second := server.hydrateSourceRepositoryRef(context.Background(), rec, "feature/on-demand")
	if second.ErrorCode != "" || !second.HydrationCacheHit {
		t.Fatalf("second hydration status mismatch: %+v", second)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("hydrator calls=%d, want 2", got)
	}

	preferredRemotesMu.Lock()
	gotPreferred := append([]string(nil), preferredRemotes...)
	preferredRemotesMu.Unlock()
	if len(gotPreferred) != 2 || gotPreferred[0] != "" || gotPreferred[1] != "vectis-fallback-2" {
		t.Fatalf("preferred remotes = %+v", gotPreferred)
	}

	if !metrics.hasHydration(dal.SourceKindLocalCheckout, dal.SourceCheckoutModeManaged, observability.SourceSyncOutcomeSucceeded, observability.SourceSyncReasonNone, "fallback-2", observability.SourceRefHydrationCacheMiss) {
		t.Fatalf("missing hydration cache-miss metric: %+v", metrics.hydrationRecords)
	}

	if !metrics.hasHydration(dal.SourceKindLocalCheckout, dal.SourceCheckoutModeManaged, observability.SourceSyncOutcomeSucceeded, observability.SourceSyncReasonNone, "fallback-2", observability.SourceRefHydrationCacheHit) {
		t.Fatalf("missing hydration cache-hit metric: %+v", metrics.hydrationRecords)
	}
}

func TestAPIServer_SourceRefHydrationDefersToDatabaseLease(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	metrics := &recordingSourceSyncMetrics{}
	server.SetSourceSyncMetrics(metrics)
	server.sourceRefHydrationLeaseWait = 25 * time.Millisecond
	server.sourceRefHydrationLeasePollInterval = 5 * time.Millisecond

	rec := dal.SourceRepositoryRecord{
		RepositoryID: "managed-repo",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutMode: dal.SourceCheckoutModeManaged,
		CheckoutPath: filepath.Join(t.TempDir(), "managed"),
	}

	ref := "feature/on-demand"
	key := sourceRefHydrationKey(rec.RepositoryID, ref)
	leaseName := sourceRefHydrationLeaseName(key)
	now := time.Now().UTC()
	acquired, err := repos.ServiceLeases().TryAcquire(context.Background(), leaseName, "other-api", now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("acquire competing lease: %v", err)
	}

	if !acquired {
		t.Fatal("expected competing lease to be acquired")
	}

	var calls atomic.Int32
	server.sourceRefHydrator = func(_ context.Context, got dal.SourceRepositoryRecord, gotRef, preferredRemote string) sourcepkg.GitCheckoutStatus {
		calls.Add(1)
		return sourcepkg.GitCheckoutStatus{
			CheckoutPath:       got.CheckoutPath,
			DefaultRef:         gotRef,
			GitRepository:      true,
			DefaultRefResolved: true,
			ResolvedCommit:     "0123456789abcdef0123456789abcdef01234567",
			HydrationRemote:    preferredRemote,
		}
	}

	status := server.hydrateSourceRepositoryRef(context.Background(), rec, ref)
	if status.ErrorCode != sourceRefHydrationInFlightErrorCode {
		t.Fatalf("hydration status error=%q, want %q; status=%+v", status.ErrorCode, sourceRefHydrationInFlightErrorCode, status)
	}

	if got := calls.Load(); got != 0 {
		t.Fatalf("hydrator calls while DB lease held: got %d, want 0", got)
	}

	if !metrics.hasHydration(dal.SourceKindLocalCheckout, dal.SourceCheckoutModeManaged, observability.SourceSyncOutcomeFailed, sourceRefHydrationInFlightErrorCode, "unknown", observability.SourceRefHydrationCacheMiss) {
		t.Fatalf("missing in-flight hydration metric: %+v", metrics.hydrationRecords)
	}
}

func TestAPIServer_SyncSourceRepositoryReturnsRunningForDatabaseReservation(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	db := dbtest.NewTestDB(t)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	metrics := &recordingSourceSyncMetrics{}
	server.SetSourceSyncMetrics(metrics)
	handler := server.Handler()
	ctx := context.Background()
	sources := dal.NewSQLRepositories(db).Sources()

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "db-locked-repo",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: filepath.Join(t.TempDir(), "repo"),
		DefaultRef:   "HEAD",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	startedAt := time.Now().Unix()
	if _, began, err := sources.BeginRepositorySync(ctx, dal.SourceRepositorySyncRecord{
		RepositoryID:  "db-locked-repo",
		StartedAtUnix: startedAt,
		Ref:           "HEAD",
	}); err != nil {
		t.Fatalf("BeginRepositorySync: %v", err)
	} else if !began {
		t.Fatal("initial sync reservation was unexpectedly blocked")
	}

	var calls atomic.Int32
	server.sourceSyncCheckoutStatus = func(_ context.Context, rec dal.SourceRepositoryRecord, syncRef string) sourcepkg.GitCheckoutStatus {
		calls.Add(1)
		return sourcepkg.GitCheckoutStatus{}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/db-locked-repo/sync", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("sync status: got %d body=%s", rec.Code, rec.Body.String())
	}

	var resp sourceRepositoryResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.RepositoryID != "db-locked-repo" ||
		resp.Sync.Status != dal.SourceSyncStatusRunning ||
		resp.Sync.Ref != "HEAD" ||
		resp.Sync.LastStartedAtUnix != startedAt {
		t.Fatalf("running response mismatch: %+v", resp)
	}

	if got := calls.Load(); got != 0 {
		t.Fatalf("database-reserved sync should not run checkout work, got %d calls", got)
	}

	if !metrics.has(observability.SourceSyncTriggerManual, dal.SourceKindLocalCheckout, dal.SourceCheckoutModeExternal, observability.SourceSyncOutcomeAlreadyRunning, observability.SourceSyncReasonDatabaseLock) {
		t.Fatalf("missing database lock source sync metric: %+v", metrics.records)
	}
}

func TestAPIServer_SyncSourceRepositoryReclaimsStaleDatabaseReservation(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	t.Setenv("VECTIS_SOURCE_SYNC_RUNNING_TIMEOUT", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_SYNC_RUNNING_TIMEOUT", "")
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("source.sync_running_timeout", time.Second)

	db := dbtest.NewTestDB(t)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	metrics := &recordingSourceSyncMetrics{}
	server.SetSourceSyncMetrics(metrics)
	handler := server.Handler()
	ctx := context.Background()
	sources := dal.NewSQLRepositories(db).Sources()

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "stale-db-locked-repo",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: filepath.Join(t.TempDir(), "repo"),
		DefaultRef:   "HEAD",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	if _, began, err := sources.BeginRepositorySync(ctx, dal.SourceRepositorySyncRecord{
		RepositoryID:  "stale-db-locked-repo",
		StartedAtUnix: time.Now().Add(-2 * time.Second).Unix(),
		Ref:           "HEAD",
	}); err != nil {
		t.Fatalf("BeginRepositorySync: %v", err)
	} else if !began {
		t.Fatal("initial sync reservation was unexpectedly blocked")
	}

	var calls atomic.Int32
	server.sourceSyncCheckoutStatus = func(_ context.Context, rec dal.SourceRepositoryRecord, syncRef string) sourcepkg.GitCheckoutStatus {
		calls.Add(1)
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

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/stale-db-locked-repo/sync", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("sync status: got %d body=%s", rec.Code, rec.Body.String())
	}

	var resp sourceRepositoryResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.RepositoryID != "stale-db-locked-repo" ||
		resp.Sync.Status != dal.SourceSyncStatusSucceeded ||
		resp.Sync.Commit != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("stale recovery response mismatch: %+v", resp)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected stale reservation to run sync once, got %d calls", got)
	}

	if !metrics.has(observability.SourceSyncTriggerManual, dal.SourceKindLocalCheckout, dal.SourceCheckoutModeExternal, observability.SourceSyncOutcomeSucceeded, observability.SourceSyncReasonNone) {
		t.Fatalf("missing stale recovery source sync metric: %+v", metrics.records)
	}
}

func TestAPIServer_SyncSourceRepositoryUsesConfiguredCheckoutStatus(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	db := dbtest.NewTestDB(t)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	metrics := &recordingSourceSyncMetrics{}
	server.SetSourceSyncMetrics(metrics)
	handler := server.Handler()
	ctx := context.Background()

	if _, err := dal.NewSQLRepositories(db).Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID:  "private-managed-repo",
		NamespaceID:   1,
		SourceKind:    dal.SourceKindLocalCheckout,
		CheckoutMode:  dal.SourceCheckoutModeManaged,
		CheckoutPath:  filepath.Join(t.TempDir(), "managed"),
		CanonicalURL:  "ssh://git.example/acme/private-managed-repo.git",
		DefaultRef:    "main",
		CredentialRef: "encryptedfs://git/private-managed-repo",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	var calls atomic.Int32
	server.SetSourceSyncCheckoutStatus(func(_ context.Context, rec dal.SourceRepositoryRecord, syncRef string) sourcepkg.GitCheckoutStatus {
		calls.Add(1)
		if rec.RepositoryID != "private-managed-repo" ||
			rec.CheckoutMode != dal.SourceCheckoutModeManaged ||
			rec.CredentialRef != "encryptedfs://git/private-managed-repo" ||
			syncRef != "main" {
			t.Fatalf("sync record mismatch: rec=%+v syncRef=%q", rec, syncRef)
		}

		return sourcepkg.GitCheckoutStatus{
			CheckoutPath:       rec.CheckoutPath,
			PathExists:         true,
			PathIsDirectory:    true,
			GitRepository:      true,
			DefaultRef:         syncRef,
			DefaultRefResolved: true,
			ResolvedCommit:     "0123456789abcdef0123456789abcdef01234567",
		}
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/source-repositories/private-managed-repo/sync", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("sync status: got %d body=%s", rec.Code, rec.Body.String())
	}

	var resp sourceRepositoryResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected configured checkout status to run once, got %d calls", got)
	}

	if resp.RepositoryID != "private-managed-repo" ||
		resp.Sync.Status != dal.SourceSyncStatusSucceeded ||
		resp.Sync.Commit != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("sync response mismatch: %+v", resp)
	}

	if !metrics.has(observability.SourceSyncTriggerManual, dal.SourceKindLocalCheckout, dal.SourceCheckoutModeManaged, observability.SourceSyncOutcomeSucceeded, observability.SourceSyncReasonNone) {
		t.Fatalf("missing manual managed sync success metric: %+v", metrics.records)
	}
}
