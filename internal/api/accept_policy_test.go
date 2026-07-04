package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/httpsecurity"
	"vectis/internal/interfaces/mocks"
)

func TestRouteAcceptMiddlewareRejectsUnacceptableMediaType(t *testing.T) {
	var gotReason string
	var gotStatus int
	handler := routeAcceptMiddleware(routeAcceptPolicy{}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not be called")
	}), func(_ *http.Request, reason string, status int) {
		gotReason = reason
		gotStatus = status
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
	req.Header.Set("Accept", "text/html")
	handler.ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusNotAcceptable, apiErrNotAcceptable)
	assertNoStore(t, rec)
	if gotReason != securityReasonNotAcceptable || gotStatus != http.StatusNotAcceptable {
		t.Fatalf("recorded rejection = (%q, %d), want (%q, %d)", gotReason, gotStatus, securityReasonNotAcceptable, http.StatusNotAcceptable)
	}
}

func TestAPIServerRejectsUnacceptableJSONAccept(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
	req.Header.Set("Accept", "text/html")
	s.Handler().ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusNotAcceptable, apiErrNotAcceptable)
	assertNoStore(t, rec)
	requireSecurityRejection(t, metrics, securityReasonNotAcceptable, "GET /api/v1/version", http.StatusNotAcceptable)
}

func TestRouteAcceptMiddlewareAllowsJSONNegotiation(t *testing.T) {
	for _, accept := range []string{"", "application/json", "application/*", "*/*", "text/html;q=0.1, application/json;q=0.9"} {
		t.Run(accept, func(t *testing.T) {
			called := false
			handler := routeAcceptMiddleware(routeAcceptPolicy{}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			}))

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
			req.Header.Set("Accept", accept)
			handler.ServeHTTP(rec, req)

			if !called {
				t.Fatal("handler was not called")
			}

			if rec.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
			}
		})
	}
}

func TestRouteAcceptPolicyMediaTypes(t *testing.T) {
	tests := []struct {
		name   string
		policy routeAcceptPolicy
		accept string
		want   bool
	}{
		{name: "health accepts arbitrary", policy: routeAcceptAnyPolicy(), accept: "text/html", want: true},
		{name: "sse accepts event stream", policy: routeAcceptSSEPolicy(), accept: httpsecurity.MediaTypeEventStream, want: true},
		{name: "sse rejects json", policy: routeAcceptSSEPolicy(), accept: httpsecurity.MediaTypeJSON, want: false},
		{name: "metrics accepts text", policy: routeAcceptMetricsPolicy(), accept: httpsecurity.MediaTypePlainText, want: true},
		{name: "metrics accepts openmetrics", policy: routeAcceptMetricsPolicy(), accept: "application/openmetrics-text; version=1.0.0", want: true},
		{name: "metrics rejects html", policy: routeAcceptMetricsPolicy(), accept: "text/html", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.policy.allows(tt.accept); got != tt.want {
				t.Fatalf("allows(%q) = %v, want %v", tt.accept, got, tt.want)
			}
		})
	}
}

func TestAPIServerAllowsHealthAcceptHeader(t *testing.T) {
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Header.Set("Accept", "text/html")
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestRouteAcceptPolicyValidateRejectsUnknownMode(t *testing.T) {
	if err := (routeAcceptPolicy{mode: routeAcceptMode(99)}).validate(); err == nil {
		t.Fatal("expected unknown accept policy mode to fail validation")
	}
}
