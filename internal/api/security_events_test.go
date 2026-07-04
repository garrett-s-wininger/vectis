package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"vectis/internal/api/ratelimit"
	"vectis/internal/interfaces/mocks"
)

type securityRejectionRecord struct {
	Reason string
	Route  string
	Status int
}

type fakeSecurityRejectionMetrics struct {
	mu      sync.Mutex
	records []securityRejectionRecord
}

func (m *fakeSecurityRejectionMetrics) RecordSecurityRejection(_ context.Context, reason, route string, status int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, securityRejectionRecord{
		Reason: reason,
		Route:  route,
		Status: status,
	})
}

func (m *fakeSecurityRejectionMetrics) Records() []securityRejectionRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]securityRejectionRecord, len(m.records))
	copy(out, m.records)
	return out
}

func requireSecurityRejection(t *testing.T, m *fakeSecurityRejectionMetrics, reason, route string, status int) {
	t.Helper()

	for _, got := range m.Records() {
		if got.Reason == reason && got.Route == route && got.Status == status {
			return
		}
	}

	t.Fatalf("missing security rejection reason=%q route=%q status=%d in %+v", reason, route, status, m.Records())
}

func countSecurityRejections(m *fakeSecurityRejectionMetrics, reason, route string, status int) int {
	count := 0
	for _, got := range m.Records() {
		if got.Reason == reason && got.Route == route && got.Status == status {
			count++
		}
	}

	return count
}

func TestSecurityLogValueSanitizesControlCharactersAndCapsLength(t *testing.T) {
	raw := "bad\r\n\tInjected: true" + strings.Repeat("a", maxSecurityLogValueBytes+20)
	got := securityLogValue(raw)

	if len(got) > maxSecurityLogValueBytes {
		t.Fatalf("sanitized value length=%d, want <= %d", len(got), maxSecurityLogValueBytes)
	}

	if strings.ContainsAny(got, "\r\n\t") {
		t.Fatalf("sanitized value contains control characters: %q", got)
	}
}

func TestRecordSecurityRejectionDoesNotLogCredentialHeaders(t *testing.T) {
	logger := mocks.NewMockLogger()
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(logger, nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil)
	req.Pattern = "POST /api/v1/logout"
	req.RemoteAddr = "203.0.113.10:1234"
	req.Host = "api.example\r\nInjected: true"
	req.Header.Set("Origin", "https://ui.example\r\nInjected: true")
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Cookie", sessionCookieName+"=secret-cookie")
	req.Header.Set(csrfHeaderName, "secret-csrf")

	s.recordSecurityRejection(req, securityReasonCSRFTokenRequired, http.StatusForbidden)

	requireSecurityRejection(t, metrics, securityReasonCSRFTokenRequired, "POST /api/v1/logout", http.StatusForbidden)

	warns := logger.GetWarnCalls()
	if len(warns) != 1 {
		t.Fatalf("warn count=%d, want 1: %+v", len(warns), warns)
	}

	logLine := warns[0]
	if strings.ContainsAny(logLine, "\r\n") {
		t.Fatalf("security rejection log contains a newline: %q", logLine)
	}

	for _, secret := range []string{"secret-token", "secret-cookie", "secret-csrf"} {
		if strings.Contains(logLine, secret) {
			t.Fatalf("security rejection log leaked %q: %q", secret, logLine)
		}
	}
}

type alwaysDenyRateLimiter struct{}

func (alwaysDenyRateLimiter) Allow(context.Context, string, ratelimit.Rule) (bool, time.Duration, error) {
	return false, time.Second, nil
}

func TestRateLimitRejectionRecordsSecurityEvent(t *testing.T) {
	logger := mocks.NewMockLogger()
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(logger, nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	spec := routeSpec{
		Pattern:   "GET /api/v1/tokens",
		Auth:      routeAuthPolicy{},
		RateLimit: ratelimit.Rule{RefillRate: time.Second, BurstSize: 1},
	}

	handler := s.rateLimitMiddleware(alwaysDenyRateLimiter{}, spec, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
	req.Pattern = spec.Pattern
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusTooManyRequests)
	}

	requireSecurityRejection(t, metrics, securityReasonRateLimitExceeded, spec.Pattern, http.StatusTooManyRequests)
}
