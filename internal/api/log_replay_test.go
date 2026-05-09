package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseLogReplayRequestSinceAndBounds(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-1/logs?since_sequence=12&tail=5&replay_limit=20", nil)
	rec := httptest.NewRecorder()

	replay, ok := parseLogReplayRequest(rec, req)
	if !ok {
		t.Fatalf("expected replay request to parse: %s", rec.Body.String())
	}

	if replay.SinceSequence != 12 {
		t.Fatalf("SinceSequence = %d, want 12", replay.SinceSequence)
	}

	if replay.Tail != 5 {
		t.Fatalf("Tail = %d, want 5", replay.Tail)
	}

	if replay.ReplayLimit != 5 {
		t.Fatalf("ReplayLimit = %d, want 5 after tail cap", replay.ReplayLimit)
	}
}

func TestParseLogReplayRequestLastEventID(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-1/logs", nil)
	req.Header.Set("Last-Event-ID", "44")
	rec := httptest.NewRecorder()

	replay, ok := parseLogReplayRequest(rec, req)
	if !ok {
		t.Fatalf("expected replay request to parse: %s", rec.Body.String())
	}

	if replay.SinceSequence != 44 {
		t.Fatalf("SinceSequence = %d, want 44", replay.SinceSequence)
	}

	if replay.ReplayLimit != defaultLogReplayLimit {
		t.Fatalf("ReplayLimit = %d, want default %d", replay.ReplayLimit, defaultLogReplayLimit)
	}
}

func TestParseLogReplayRequestRejectsInvalidSince(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-1/logs?since_sequence=-1", nil)
	rec := httptest.NewRecorder()

	if _, ok := parseLogReplayRequest(rec, req); ok {
		t.Fatal("expected invalid since_sequence to fail")
	}

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	if !strings.Contains(rec.Body.String(), "invalid_since_sequence") {
		t.Fatalf("expected invalid_since_sequence error, got %s", rec.Body.String())
	}
}

func TestFormatSSEDataEventIncludesSequenceID(t *testing.T) {
	got := string(formatSSEDataEvent(7, []byte(`{"data":"x"}`)))
	want := "id: 7\ndata: {\"data\":\"x\"}\n\n"
	if got != want {
		t.Fatalf("event = %q, want %q", got, want)
	}
}
