package api_test

import (
	"context"
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

	_, err = client.Get(base + "/health/live")
	if err == nil {
		t.Fatal("expected connection error after shutdown")
	}
}
