package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vectis/internal/interfaces/mocks"
)

func TestRouteGuardUnknownRouteReturnsJSON(t *testing.T) {
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/not-a-route", nil)
	s.Handler().ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusNotFound, apiErrRouteNotFound)
	assertNoStore(t, rec)
}

func TestRouteGuardRejectsAbsoluteFormRequestTarget(t *testing.T) {
	t.Setenv("VECTIS_API_ALLOWED_HOSTS", "api.example")

	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://api.example/api/v1/version", nil)
	s.Handler().ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusBadRequest, apiErrInvalidRequestTarget)
	assertNoStore(t, rec)
	requireSecurityRejection(t, metrics, securityReasonRequestTargetInvalid, "/api/v1/version", http.StatusBadRequest)
}

func TestRouteGuardRejectsAsteriskRequestTarget(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/version", nil)
	req.URL.Path = "*"
	req.RequestURI = "*"
	s.Handler().ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusBadRequest, apiErrInvalidRequestTarget)
	assertNoStore(t, rec)
	requireSecurityRejection(t, metrics, securityReasonRequestTargetInvalid, securityRejectionUnknownRoute, http.StatusBadRequest)
}

func TestRouteGuardRejectsNonCanonicalPathTargets(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantRoute string
	}{
		{name: "trailing slash", path: "/api/v1/version/", wantRoute: "/api/v1/version"},
		{name: "duplicate slash", path: "/api/v1//version", wantRoute: securityRejectionUnknownRoute},
		{name: "dot segment", path: "/api/v1/../api/v1/version", wantRoute: securityRejectionUnknownRoute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := &fakeSecurityRejectionMetrics{}
			s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
			s.SetAPISecurityMetrics(metrics)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			s.Handler().ServeHTTP(rec, req)

			assertRouteGuardAPIError(t, rec, http.StatusBadRequest, apiErrInvalidRequestTarget)
			assertNoStore(t, rec)
			requireSecurityRejection(t, metrics, securityReasonRequestTargetInvalid, tt.wantRoute, http.StatusBadRequest)
		})
	}
}

func TestRouteGuardMethodNotAllowedReturnsJSONAllowAndMetric(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/jobs", nil)
	s.Handler().ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
	assertNoStore(t, rec)
	assertAllowHeader(t, rec, http.MethodGet, http.MethodHead, http.MethodPost)
	requireSecurityRejection(t, metrics, securityReasonMethodNotAllowed, "/api/v1/jobs", http.StatusMethodNotAllowed)
}

func TestRouteGuardAllowsHEADForGETRoutes(t *testing.T) {
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodHead, "/api/v1/version", nil)
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD status=%d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestRouteGuardRejectsDangerousHTTPMethods(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodTrace, "/api/v1/jobs", nil)
	s.Handler().ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
	assertNoStore(t, rec)
	assertAllowHeader(t, rec, http.MethodGet, http.MethodHead, http.MethodPost)
	requireSecurityRejection(t, metrics, securityReasonUnsupportedHTTPMethod, "/api/v1/jobs", http.StatusMethodNotAllowed)
}

func TestRouteGuardLowercaseMethodReturnsJSON(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("get", "/api/v1/version", nil)
	s.Handler().ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
	assertAllowHeader(t, rec, http.MethodGet, http.MethodHead)
	requireSecurityRejection(t, metrics, securityReasonMethodNotAllowed, "/api/v1/version", http.StatusMethodNotAllowed)
}

func TestRouteGuardVariableRouteMethodNotAllowed(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/job-1/runs", nil)
	s.Handler().ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
	assertAllowHeader(t, rec, http.MethodGet, http.MethodHead)
	requireSecurityRejection(t, metrics, securityReasonMethodNotAllowed, "/api/v1/jobs/{id}/runs", http.StatusMethodNotAllowed)
}

func assertRouteGuardAPIError(t *testing.T, rec *httptest.ResponseRecorder, status int, code apiErrorCode) {
	t.Helper()

	if rec.Code != status {
		t.Fatalf("status=%d, want %d; body=%s", rec.Code, status, rec.Body.String())
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type=%q, want application/json; charset=utf-8", ct)
	}

	var body apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v; body=%s", err, rec.Body.String())
	}

	if body.Code != string(code) {
		t.Fatalf("code=%q, want %q; body=%s", body.Code, code, rec.Body.String())
	}
}

func assertAllowHeader(t *testing.T, rec *httptest.ResponseRecorder, methods ...string) {
	t.Helper()

	got := map[string]bool{}
	for part := range strings.SplitSeq(rec.Header().Get("Allow"), ",") {
		method := strings.TrimSpace(part)
		if method != "" {
			got[method] = true
		}
	}

	if len(got) != len(methods) {
		t.Fatalf("Allow=%q, want exactly %v", rec.Header().Get("Allow"), methods)
	}

	for _, method := range methods {
		if !got[method] {
			t.Fatalf("Allow=%q, missing %q", rec.Header().Get("Allow"), method)
		}
	}
}
