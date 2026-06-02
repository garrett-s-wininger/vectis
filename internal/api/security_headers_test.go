package api

import (
	"net/http/httptest"
	"testing"
)

func assertNoStore(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q, want no-store", got)
	}

	if got := rec.Header().Get("Pragma"); got != "no-cache" {
		t.Fatalf("Pragma=%q, want no-cache", got)
	}

	if got := rec.Header().Get("Expires"); got != "0" {
		t.Fatalf("Expires=%q, want 0", got)
	}
}
