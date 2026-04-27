package api

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStatusResponseWriter_recordsStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusResponseWriter{ResponseWriter: rec, status: http.StatusOK}

	sw.WriteHeader(http.StatusCreated)
	if sw.status != http.StatusCreated {
		t.Fatalf("status=%d", sw.status)
	}

	if !sw.wroteHeader {
		t.Fatal("expected wroteHeader true")
	}
}

func TestStatusResponseWriter_writeImplies200(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusResponseWriter{ResponseWriter: rec, status: http.StatusOK}

	_, _ = sw.Write([]byte("hello"))
	if sw.status != http.StatusOK {
		t.Fatalf("status=%d", sw.status)
	}

	if !sw.wroteHeader {
		t.Fatal("expected wroteHeader true")
	}
}

func TestStatusResponseWriter_noDoubleStatusOverride(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusResponseWriter{ResponseWriter: rec, status: http.StatusOK}

	sw.WriteHeader(http.StatusCreated)
	sw.WriteHeader(http.StatusNoContent) // should be ignored
	if sw.status != http.StatusCreated {
		t.Fatalf("status=%d", sw.status)
	}
}

func TestAccessLogMiddleware_nilLogger(t *testing.T) {
	var hit bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
	})

	h := accessLogMiddleware(nil, func(*http.Request) bool { return false }, next)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if !hit {
		t.Fatal("expected handler to run")
	}
}

func TestAccessLogMiddleware_skipped(t *testing.T) {
	var hit bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
	})

	h := accessLogMiddleware(slog.Default(), func(*http.Request) bool { return true }, next)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if !hit {
		t.Fatal("expected handler to run")
	}
}

func TestAccessLogMiddleware_logsRequest(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	h := accessLogMiddleware(logger, func(*http.Request) bool { return false }, next)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	h.ServeHTTP(rec, req)

	out := buf.String()
	if !strings.Contains(out, "http_request") {
		t.Fatalf("expected http_request log, got: %s", out)
	}

	if !strings.Contains(out, "418") {
		t.Fatalf("expected status 418 in log, got: %s", out)
	}

	if !strings.Contains(out, "GET") {
		t.Fatalf("expected method GET in log, got: %s", out)
	}
}

func TestAccessLogMiddleware_durationPresent(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Millisecond)
	})

	h := accessLogMiddleware(logger, func(*http.Request) bool { return false }, next)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	out := buf.String()
	if !strings.Contains(out, "duration=") {
		t.Fatalf("expected duration in log, got: %s", out)
	}
}
