package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRouteHeaderMiddlewareRejectsUnsupportedIdempotencyKey(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	handler := routeHeaderMiddleware(routeHeaderPolicy{}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), routeHeaderTestRecorder(metrics))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
	req.Pattern = "GET /api/v1/version"
	req.Header.Set(idempotencyKeyHeaderName, "retry-1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assertRouteHeaderAPIError(t, rec)
	requireSecurityRejection(t, metrics, securityReasonInvalidRequestHeader, req.Pattern, http.StatusBadRequest)
}

func TestRouteHeaderMiddlewareAllowsValidIdempotencyKeyOnOptInRoute(t *testing.T) {
	handler := routeHeaderMiddleware(routeHeadersIdempotencyPolicy(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", nil)
	req.Header.Set(idempotencyKeyHeaderName, "run-key_123.456")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
}

func TestRouteHeaderMiddlewareRejectsInvalidIdempotencyKeys(t *testing.T) {
	tests := map[string]string{
		"empty":          "",
		"leading_space":  " retry-1",
		"trailing_space": "retry-1 ",
		"comma":          "retry-1,retry-2",
		"control":        "retry-\n1",
		"non_ascii":      "retry-cafe\u0301",
		"too_long":       strings.Repeat("a", maxIdempotencyKeyBytes+1),
	}

	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			handler := routeHeaderMiddleware(routeHeadersIdempotencyPolicy(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))

			req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", nil)
			req.Header.Set(idempotencyKeyHeaderName, value)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assertRouteHeaderAPIError(t, rec)
		})
	}
}

func TestRouteHeaderMiddlewareRejectsDuplicateSingletonHeaders(t *testing.T) {
	tests := []struct {
		name   string
		header string
		first  string
		second string
	}{
		{name: "authorization", header: "Authorization", first: "Bearer one", second: "Bearer two"},
		{name: "content_type", header: "Content-Type", first: "application/json", second: "text/plain"},
		{name: "csrf", header: csrfHeaderName, first: "csrf-one", second: "csrf-two"},
		{name: "origin", header: "Origin", first: "https://one.example", second: "https://two.example"},
		{name: "referer", header: "Referer", first: "https://one.example/a", second: "https://two.example/b"},
		{name: "idempotency", header: idempotencyKeyHeaderName, first: "retry-1", second: "retry-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := routeHeaderMiddleware(routeHeadersIdempotencyPolicy(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))

			req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/run", nil)
			req.Header.Add(tt.header, tt.first)
			req.Header.Add(tt.header, tt.second)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assertRouteHeaderAPIError(t, rec)
		})
	}
}

func TestAPIServerRejectsIdempotencyKeyOnRoutesWithoutOptIn(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(nil, nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
	req.Header.Set(idempotencyKeyHeaderName, "retry-1")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	assertRouteHeaderAPIError(t, rec)
	requireSecurityRejection(t, metrics, securityReasonInvalidRequestHeader, "GET /api/v1/version", http.StatusBadRequest)
}

func assertRouteHeaderAPIError(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	assertRouteGuardAPIError(t, rec, http.StatusBadRequest, apiErrInvalidRequestHeader)
	assertNoStore(t, rec)
}

func routeHeaderTestRecorder(metrics *fakeSecurityRejectionMetrics) securityRejectionRecorder {
	return func(r *http.Request, reason string, status int) {
		metrics.RecordSecurityRejection(r.Context(), reason, securityRoute(r), status)
	}
}
