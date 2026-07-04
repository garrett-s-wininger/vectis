package api_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	apigen "vectis/api/gen/go"
	"vectis/internal/api"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"

	"google.golang.org/grpc/connectivity"
)

func TestAPIServer_HealthLive_OK(t *testing.T) {
	t.Parallel()
	db := dbtest.NewTestDB(t)
	srv := api.NewAPIServer(mocks.NewMockLogger(), db)
	srv.SetQueueClient(mocks.NewMockQueueService())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	srv.HealthLive(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}

	if got := rec.Header().Get("X-Vectis-Build-Version"); got == "" {
		t.Fatal("expected X-Vectis-Build-Version header")
	}

	if got := rec.Header().Get("X-Vectis-Cell-ID"); got != "local" {
		t.Fatalf("expected X-Vectis-Cell-ID local, got %q", got)
	}
}

func TestAPIServer_HealthReady_OK(t *testing.T) {
	t.Parallel()
	db := dbtest.NewTestDB(t)
	srv := api.NewAPIServer(mocks.NewMockLogger(), db)
	srv.SetQueueClient(mocks.NewMockQueueService())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	srv.HealthReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}

	if got := rec.Header().Get("X-Vectis-Build-Commit"); got == "" {
		t.Fatal("expected X-Vectis-Build-Commit header")
	}

	if got := rec.Header().Get("X-Vectis-Cell-ID"); got != "local" {
		t.Fatalf("expected X-Vectis-Cell-ID local, got %q", got)
	}
}

func TestAPIServer_GetVersion_IncludesCellID(t *testing.T) {
	t.Parallel()
	db := dbtest.NewTestDB(t)
	srv := api.NewAPIServer(mocks.NewMockLogger(), db)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
	srv.GetVersion(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}

	var resp struct {
		CellID string `json:"cell_id"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode version response: %v", err)
	}

	if resp.CellID != "local" {
		t.Fatalf("expected cell_id local, got %q", resp.CellID)
	}
}

func TestAPIServer_HealthReady_DatabaseDown(t *testing.T) {
	t.Parallel()
	db := dbtest.NewTestDB(t)
	srv := api.NewAPIServer(mocks.NewMockLogger(), db)
	srv.SetQueueClient(mocks.NewMockQueueService())
	_ = db.Close()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	srv.HealthReady(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}
}

func TestAPIServer_HealthReady_NoQueueClient(t *testing.T) {
	t.Parallel()
	db := dbtest.NewTestDB(t)
	srv := api.NewAPIServer(mocks.NewMockLogger(), db)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	srv.HealthReady(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}
}

type queueConnStub struct {
	state connectivity.State
}

func (q queueConnStub) Enqueue(ctx context.Context, req *apigen.JobRequest) (*apigen.Empty, error) {
	return &apigen.Empty{}, nil
}

func (q queueConnStub) GRPCConnectivityState() connectivity.State {
	return q.state
}

func TestAPIServer_HealthReady_QueueNotConnected(t *testing.T) {
	t.Parallel()
	db := dbtest.NewTestDB(t)
	srv := api.NewAPIServer(mocks.NewMockLogger(), db)
	srv.SetQueueClient(queueConnStub{state: connectivity.TransientFailure})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	srv.HealthReady(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}
}

func TestAPIServer_HealthReady_QueueIdleIsReady(t *testing.T) {
	t.Parallel()
	db := dbtest.NewTestDB(t)
	srv := api.NewAPIServer(mocks.NewMockLogger(), db)
	srv.SetQueueClient(queueConnStub{state: connectivity.Idle})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	srv.HealthReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}
}

func TestAPIServer_HealthReady_WithRepositories_SkipsDBPing(t *testing.T) {
	t.Parallel()
	jobs := mocks.NewMockJobsRepository()
	runs := mocks.NewMockRunsRepository()
	srv := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), jobs, runs, mocks.StubEphemeralRunStarter{})
	srv.SetQueueClient(mocks.NewMockQueueService())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	srv.HealthReady(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}
}

func TestAPIServer_GetSchemaStatus_EmptyMigrationsTable(t *testing.T) {
	t.Parallel()
	db := dbtest.NewTestDB(t)
	if _, err := db.Exec("DELETE FROM schema_migrations"); err != nil {
		t.Fatalf("clear schema migrations: %v", err)
	}

	srv := api.NewAPIServer(mocks.NewMockLogger(), db)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/schema/status", nil)
	srv.GetSchemaStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var resp struct {
		CurrentVersion int  `json:"current_version"`
		HasSchema      bool `json:"has_schema"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode schema status: %v", err)
	}
	if resp.HasSchema {
		t.Fatalf("expected has_schema=false, got %+v", resp)
	}
}

func TestAPIServer_Handler_HealthRoutes(t *testing.T) {
	t.Parallel()
	db := dbtest.NewTestDB(t)
	srv := api.NewAPIServer(mocks.NewMockLogger(), db)
	srv.SetQueueClient(mocks.NewMockQueueService())
	h := srv.Handler()

	for _, path := range []string{"/health/live", "/health/ready"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: expected %d, got %d", path, http.StatusOK, rec.Code)
		}
	}
}

func TestAPIServer_Serve_ShutdownOnContextCancel(t *testing.T) {
	db := dbtest.NewTestDB(t)
	srv := api.NewAPIServer(mocks.NewMockLogger(), db)
	srv.SetQueueClient(mocks.NewMockQueueService())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ctx, ln)
	}()

	base := "http://" + ln.Addr().String()
	var client http.Client
	var reached bool
	for start := time.Now(); time.Since(start) < 5*time.Second; time.Sleep(10 * time.Millisecond) {
		resp, err := client.Get(base + "/health/live")
		if err == nil {
			code := resp.StatusCode
			_ = resp.Body.Close()
			if code == http.StatusOK {
				reached = true
				break
			}
		}
	}

	if !reached {
		t.Fatal("server never became reachable")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for Serve to return after cancel")
	}

	resp, err := client.Get(base + "/health/live")
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected connection error after shutdown")
	}
}

func TestAPIServer_HealthReady_DrainingAfterShutdown(t *testing.T) {
	db := dbtest.NewTestDB(t)
	srv := api.NewAPIServer(mocks.NewMockLogger(), db)
	srv.SetQueueClient(mocks.NewMockQueueService())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ctx, ln)
	}()

	base := "http://" + ln.Addr().String()
	var client http.Client
	var reached bool
	for start := time.Now(); time.Since(start) < 5*time.Second; time.Sleep(10 * time.Millisecond) {
		resp, err := client.Get(base + "/health/ready")
		if err == nil {
			code := resp.StatusCode
			_ = resp.Body.Close()

			if code == http.StatusOK {
				reached = true
				break
			}
		}
	}

	if !reached {
		t.Fatal("server never became ready")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for Serve to return after cancel")
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	srv.HealthReady(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}
}
