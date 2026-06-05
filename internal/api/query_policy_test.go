package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/interfaces/mocks"
)

func TestRouteQueryMiddlewareRejectsUnexpectedQueryParameter(t *testing.T) {
	var gotReason string
	var gotStatus int
	handler := routeQueryMiddleware(routeQueryPolicy{}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not be called")
	}), func(_ *http.Request, reason string, status int) {
		gotReason = reason
		gotStatus = status
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/version?debug=1", nil)
	handler.ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusBadRequest, apiErrInvalidQueryParameter)
	assertNoStore(t, rec)
	if gotReason != securityReasonInvalidQueryParameter || gotStatus != http.StatusBadRequest {
		t.Fatalf("recorded rejection = (%q, %d), want (%q, %d)", gotReason, gotStatus, securityReasonInvalidQueryParameter, http.StatusBadRequest)
	}
}

func TestRouteQueryMiddlewareRejectsMalformedQuery(t *testing.T) {
	handler := routeQueryMiddleware(routeQueryParams("limit"), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not be called")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.URL.RawQuery = "limit=%zz"
	handler.ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusBadRequest, apiErrInvalidQueryParameter)
	assertNoStore(t, rec)
}

func TestRouteQueryMiddlewareRejectsDuplicateQueryParameter(t *testing.T) {
	handler := routeQueryMiddleware(routeQueryParams("limit"), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not be called")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs?limit=10&limit=20", nil)
	handler.ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusBadRequest, apiErrInvalidQueryParameter)
	assertNoStore(t, rec)
}

func TestRouteQueryMiddlewareAllowsDeclaredQueryParameters(t *testing.T) {
	for _, target := range []string{
		"/api/v1/jobs",
		"/api/v1/jobs?cursor=10",
		"/api/v1/jobs?limit=25",
		"/api/v1/jobs?cursor=10&limit=25",
		"/api/v1/jobs?limit=",
	} {
		t.Run(target, func(t *testing.T) {
			called := false
			handler := routeQueryMiddleware(routeQueryParams("cursor", "limit"), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			}))

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, target, nil)
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

func TestRouteQueryPolicyValidateRejectsInvalidPolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy routeQueryPolicy
	}{
		{name: "blank", policy: routeQueryParams("limit", " ")},
		{name: "duplicate", policy: routeQueryParams("limit", "limit")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.policy.validate(); err == nil {
				t.Fatal("expected invalid query policy to fail validation")
			}
		})
	}
}

func TestAPIServerRejectsUnexpectedQueryParameter(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/version?debug=1", nil)
	s.Handler().ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusBadRequest, apiErrInvalidQueryParameter)
	assertNoStore(t, rec)
	requireSecurityRejection(t, metrics, securityReasonInvalidQueryParameter, "GET /api/v1/version", http.StatusBadRequest)
}

func TestAPIServerRejectsQueryAllowedOnlyOnDifferentMethod(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs?cursor=10", nil)
	s.Handler().ServeHTTP(rec, req)

	assertRouteGuardAPIError(t, rec, http.StatusBadRequest, apiErrInvalidQueryParameter)
	assertNoStore(t, rec)
	requireSecurityRejection(t, metrics, securityReasonInvalidQueryParameter, "POST /api/v1/jobs", http.StatusBadRequest)
}
