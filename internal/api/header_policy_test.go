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

func TestRequestHeaderGuardMiddlewareAllowsWellFormedBrowserSecurityHeaders(t *testing.T) {
	handler := requestHeaderGuardMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodOptions, "/api/v1/jobs", nil)
	req.Header.Set("Origin", "https://ui.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "Authorization, X-CSRF-Token, Content-Type")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-User", "?1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
}

func TestRequestHeaderGuardMiddlewareRejectsDuplicateEarlyHeaders(t *testing.T) {
	for _, header := range earlyRequestSingletonHeaders {
		t.Run(header, func(t *testing.T) {
			handler := requestHeaderGuardMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))

			req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
			req.Header.Add(header, validEarlyHeaderTestValue(header))
			req.Header.Add(header, validEarlyHeaderTestValue(header))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assertRouteHeaderAPIError(t, rec)
		})
	}
}

func TestRequestHeaderGuardMiddlewareRejectsMalformedEarlyHeaders(t *testing.T) {
	tests := []struct {
		name   string
		header string
		value  string
	}{
		{name: "origin with path", header: "Origin", value: "https://ui.example/path"},
		{name: "origin with query", header: "Origin", value: "https://ui.example?x=1"},
		{name: "origin unsupported scheme", header: "Origin", value: "file://ui.example"},
		{name: "origin control", header: "Origin", value: "https://ui.example\nInjected: true"},
		{name: "preflight method list", header: "Access-Control-Request-Method", value: "POST, GET"},
		{name: "preflight method spaced", header: "Access-Control-Request-Method", value: " POST"},
		{name: "preflight headers empty part", header: "Access-Control-Request-Headers", value: "Authorization,,Content-Type"},
		{name: "preflight headers duplicate", header: "Access-Control-Request-Headers", value: "Authorization, authorization"},
		{name: "preflight headers invalid token", header: "Access-Control-Request-Headers", value: "X Bad"},
		{name: "fetch site list", header: "Sec-Fetch-Site", value: "cross-site, same-site"},
		{name: "fetch mode control", header: "Sec-Fetch-Mode", value: "cors\n"},
		{name: "fetch dest empty", header: "Sec-Fetch-Dest", value: ""},
		{name: "fetch user unsupported", header: "Sec-Fetch-User", value: "?0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := requestHeaderGuardMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))

			req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
			req.Header.Set(tt.header, tt.value)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assertRouteHeaderAPIError(t, rec)
		})
	}
}

func TestAPIServerRejectsDuplicateOriginBeforeCORS(t *testing.T) {
	t.Setenv("VECTIS_API_CORS_ALLOWED_ORIGINS", "https://ui.example")

	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(nil, nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "localhost"
	req.Header.Add("Origin", "https://ui.example")
	req.Header.Add("Origin", "https://evil.example")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	assertRouteHeaderAPIError(t, rec)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin=%q, want empty", got)
	}

	requireSecurityRejection(t, metrics, securityReasonInvalidRequestHeader, securityRejectionUnknownRoute, http.StatusBadRequest)
}

func TestAPIServerRejectsDuplicateFetchMetadataBeforeFetchMetadataCheck(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(nil, nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "localhost"
	req.Header.Add("Sec-Fetch-Site", "cross-site")
	req.Header.Add("Sec-Fetch-Site", "same-origin")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	assertRouteHeaderAPIError(t, rec)
	requireSecurityRejection(t, metrics, securityReasonInvalidRequestHeader, securityRejectionUnknownRoute, http.StatusBadRequest)
	if countSecurityRejections(metrics, securityReasonFetchMetadataForbidden, securityRejectionUnknownRoute, http.StatusForbidden) != 0 {
		t.Fatalf("fetch metadata rejection should not run after early header rejection: %+v", metrics.Records())
	}
}

func assertRouteHeaderAPIError(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	assertRouteGuardAPIError(t, rec, http.StatusBadRequest, apiErrInvalidRequestHeader)
	assertNoStore(t, rec)
}

func validEarlyHeaderTestValue(header string) string {
	switch header {
	case "Origin":
		return "https://ui.example"
	case "Access-Control-Request-Method":
		return http.MethodPost
	case "Access-Control-Request-Headers":
		return "Authorization"
	case "Sec-Fetch-Site":
		return "cross-site"
	case "Sec-Fetch-Mode":
		return "cors"
	case "Sec-Fetch-Dest":
		return "empty"
	case "Sec-Fetch-User":
		return "?1"
	default:
		return "value"
	}
}

func routeHeaderTestRecorder(metrics *fakeSecurityRejectionMetrics) securityRejectionRecorder {
	return func(r *http.Request, reason string, status int) {
		metrics.RecordSecurityRejection(r.Context(), reason, securityRoute(r), status)
	}
}
