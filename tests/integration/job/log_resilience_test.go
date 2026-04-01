//go:build integration

package job_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/logserver"
)

type runningLogServer struct {
	server   *grpc.Server
	listener net.Listener
	addr     string
}

func startLogServer(t *testing.T, addr string, logger interfaces.Logger, store logserver.RunLogStore) *runningLogServer {
	t.Helper()

	bindAddr := addr
	if bindAddr == "" {
		bindAddr = "127.0.0.1:0"
	}

	var (
		lis net.Listener
		err error
	)

	for i := 0; i < 20; i++ {
		lis, err = net.Listen("tcp", bindAddr)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("listen %s: %v", bindAddr, err)
	}

	s := grpc.NewServer()
	api.RegisterLogServiceServer(s, logserver.NewServerWithStore(logger, store))

	go func() {
		if serveErr := s.Serve(lis); serveErr != nil {
			t.Logf("log server stopped: %v", serveErr)
		}
	}()

	return &runningLogServer{
		server:   s,
		listener: lis,
		addr:     lis.Addr().String(),
	}
}

func (s *runningLogServer) stop() {
	if s == nil {
		return
	}

	if s.server != nil {
		s.server.Stop()
	}

	if s.listener != nil {
		_ = s.listener.Close()
	}
}

func waitForStoreContains(t *testing.T, store *logserver.LocalRunLogStore, runID string, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, err := store.List(runID)
		if err == nil {
			for _, e := range entries {
				if strings.Contains(e.Data, want) {
					return
				}
			}
		}

		time.Sleep(40 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for persisted log containing %q", want)
}

func allEntriesJoined(t *testing.T, store *logserver.LocalRunLogStore, runID string) string {
	t.Helper()
	entries, err := store.List(runID)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}

	parts := make([]string, 0, len(entries))
	for _, e := range entries {
		parts = append(parts, e.Data)
	}

	return strings.Join(parts, "\n")
}

func TestIntegrationJob_LogAggregatorDiesMidRunThenRecovers(t *testing.T) {
	baseDir := t.TempDir()
	store, err := logserver.NewLocalRunLogStore(baseDir)
	if err != nil {
		t.Fatalf("new local log store: %v", err)
	}

	logger := mocks.NewMockLogger()
	srv := startLogServer(t, "", logger, store)
	t.Cleanup(func() { srv.stop() })

	conn, err := grpc.NewClient(srv.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("new grpc client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	logClient := interfaces.NewGRPCLogClient(conn)
	executor := job.NewExecutor()

	jobID := "integration-midrun-log-recovery"
	runID := "integration-run-midrun-log-recovery"
	rootID := "root-shell"
	uses := "builtins/shell"
	command := "for i in 1 2 3 4 5 6 7 8; do echo tick-$i; sleep 0.25; done"
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &uses,
			With: map[string]string{"command": command},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- executor.ExecuteJob(context.Background(), testJob, logClient, logger)
	}()

	waitForStoreContains(t, store, runID, "tick-1", 4*time.Second)

	srv.stop()
	time.Sleep(700 * time.Millisecond)
	srv = startLogServer(t, srv.addr, logger, store)

	select {
	case execErr := <-errCh:
		if execErr != nil {
			t.Fatalf("execute job failed: %v", execErr)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for job to complete")
	}

	joined := allEntriesJoined(t, store, runID)
	for i := 1; i <= 8; i++ {
		needle := fmt.Sprintf("tick-%d", i)
		if !strings.Contains(joined, needle) {
			t.Fatalf("expected persisted logs to contain %q; logs were:\n%s", needle, joined)
		}
	}

	tickLineCount := 0
	for _, line := range strings.Split(joined, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "tick-") && len(line) == len("tick-0") && line[5] >= '0' && line[5] <= '9' {
			tickLineCount++
		}
	}

	if got, want := tickLineCount, 8; got != want {
		t.Fatalf("expected exactly %d tick output lines (no drops/dupes), got %d; logs were:\n%s", want, got, joined)
	}

	if !strings.Contains(joined, `"event":"completed","status":"success"`) {
		t.Fatalf("expected success completion control event in persisted logs; logs were:\n%s", joined)
	}

	warnCalls := logger.GetWarnCalls()
	warnJoined := strings.Join(warnCalls, "\n")
	if !strings.Contains(warnJoined, "Log aggregator unavailable; spooling logs locally and retrying") {
		t.Fatalf("expected outage warning in worker logs, got: %v", warnCalls)
	}

	infoCalls := logger.GetInfoCalls()
	infoJoined := strings.Join(infoCalls, "\n")
	if !strings.Contains(infoJoined, "Log aggregator reconnected; resumed flushing spooled logs") {
		t.Fatalf("expected recovery info in worker logs, got: %v", infoCalls)
	}
}
