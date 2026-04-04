package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCorrelationMiddleware_echoesAndGenerates(t *testing.T) {
	t.Parallel()

	var seen string
	h := CorrelationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = CorrelationID(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	t.Run("generates", func(t *testing.T) {
		seen = ""
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", http.NoBody)
		h.ServeHTTP(rec, req)

		id := rec.Header().Get("X-Request-ID")
		if id == "" || seen != id {
			t.Fatalf("header=%q ctx=%q", id, seen)
		}
	})

	t.Run("reuses valid incoming", func(t *testing.T) {
		seen = ""
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/x", http.NoBody)
		req.Header.Set("X-Request-ID", "abc-123")
		h.ServeHTTP(rec, req)

		if got := rec.Header().Get("X-Request-ID"); got != "abc-123" {
			t.Fatalf("got %q", got)
		}

		if seen != "abc-123" {
			t.Fatalf("context id %q", seen)
		}
	})

	t.Run("rejects invalid", func(t *testing.T) {
		seen = ""
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/x", http.NoBody)
		req.Header.Set("X-Request-ID", "bad\nid")
		h.ServeHTTP(rec, req)

		id := rec.Header().Get("X-Request-ID")
		if id == "" || strings.Contains(id, "\n") {
			t.Fatalf("got %q", id)
		}
	})
}

func TestValidCorrelationID(t *testing.T) {
	t.Parallel()

	if !validCorrelationID("aZ09-_.") {
		t.Fatal("expected valid")
	}

	if validCorrelationID("") || validCorrelationID(strings.Repeat("x", maxCorrelationIDLen+1)) {
		t.Fatal("expected invalid length")
	}

	if validCorrelationID("café") {
		t.Fatal("non-ascii invalid")
	}
}
