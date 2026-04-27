package main

import (
	"context"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
)

func TestWorkerControlServer_CancelRun_success(t *testing.T) {
	cancelCh := make(chan string, 1)
	logger := mocks.NewMockLogger()
	srv := newWorkerControlServer("worker-1", cancelCh, func() (string, string) {
		return "run-123", "token-abc"
	}, logger)

	resp, err := srv.CancelRun(context.Background(), &api.CancelRunRequest{
		RunId:       strPtr("run-123"),
		CancelToken: strPtr("token-abc"),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	select {
	case runID := <-cancelCh:
		if runID != "run-123" {
			t.Fatalf("expected run-123, got %s", runID)
		}
	default:
		t.Fatal("expected cancel on channel")
	}
}

func TestWorkerControlServer_CancelRun_wrongRunID(t *testing.T) {
	cancelCh := make(chan string, 1)
	logger := mocks.NewMockLogger()
	srv := newWorkerControlServer("worker-1", cancelCh, func() (string, string) {
		return "run-123", "token-abc"
	}, logger)

	_, err := srv.CancelRun(context.Background(), &api.CancelRunRequest{
		RunId:       strPtr("run-999"),
		CancelToken: strPtr("token-abc"),
	})

	if err == nil {
		t.Fatal("expected error for mismatched run id")
	}

	select {
	case <-cancelCh:
		t.Fatal("should not send cancel for wrong run")
	default:
	}
}

func TestWorkerControlServer_CancelRun_invalidToken(t *testing.T) {
	cancelCh := make(chan string, 1)
	logger := mocks.NewMockLogger()
	srv := newWorkerControlServer("worker-1", cancelCh, func() (string, string) {
		return "run-123", "token-abc"
	}, logger)

	_, err := srv.CancelRun(context.Background(), &api.CancelRunRequest{
		RunId:       strPtr("run-123"),
		CancelToken: strPtr("wrong-token"),
	})

	if err == nil {
		t.Fatal("expected error for invalid cancel token")
	}

	select {
	case <-cancelCh:
		t.Fatal("should not send cancel for invalid token")
	default:
	}
}

func TestWorkerControlServer_CancelRun_noRun(t *testing.T) {
	cancelCh := make(chan string, 1)
	logger := mocks.NewMockLogger()
	srv := newWorkerControlServer("worker-1", cancelCh, func() (string, string) {
		return "", ""
	}, logger)

	_, err := srv.CancelRun(context.Background(), &api.CancelRunRequest{
		RunId:       strPtr("run-123"),
		CancelToken: strPtr("token-abc"),
	})

	if err == nil {
		t.Fatal("expected error when no run is active")
	}
}

func TestWorkerControlServer_CancelRun_channelFull(t *testing.T) {
	cancelCh := make(chan string, 0)
	logger := mocks.NewMockLogger()
	srv := newWorkerControlServer("worker-1", cancelCh, func() (string, string) {
		return "run-123", "token-abc"
	}, logger)

	resp, err := srv.CancelRun(context.Background(), &api.CancelRunRequest{
		RunId:       strPtr("run-123"),
		CancelToken: strPtr("token-abc"),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp == nil {
		t.Fatal("expected non-nil response even when channel full")
	}
}

func strPtr(s string) *string {
	return &s
}
