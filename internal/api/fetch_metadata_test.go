package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestFetchMetadataRejectsCrossSiteWithoutOrigin(t *testing.T) {
	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	metrics := &fakeSecurityRejectionMetrics{}
	s.SetAPISecurityMetrics(metrics)

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "localhost"
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	var body apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v; body=%s", err, rec.Body.String())
	}

	if body.Code != string(apiErrFetchMetadataForbidden) {
		t.Fatalf("code=%q, want %q", body.Code, apiErrFetchMetadataForbidden)
	}

	requireSecurityRejection(t, metrics, securityReasonFetchMetadataForbidden, securityRejectionUnknownRoute, http.StatusForbidden)
}

func TestFetchMetadataAllowsCrossSiteWithOriginForCORS(t *testing.T) {
	t.Setenv("VECTIS_API_CORS_ALLOWED_ORIGINS", "https://ui.example")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "localhost"
	req.Header.Set("Origin", "https://ui.example")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	assertCORSAllowed(t, rec, "https://ui.example")
}

func TestFetchMetadataAllowsProgrammaticBrowserAPIRequests(t *testing.T) {
	tests := []struct {
		name string
		mode string
		dest string
	}{
		{name: "cors empty", mode: "cors", dest: "empty"},
		{name: "same origin empty", mode: "same-origin", dest: "empty"},
		{name: "missing dest", mode: "cors"},
		{name: "missing mode", dest: "empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := dbtest.NewTestDB(t)
			s := NewAPIServer(mocks.NewMockLogger(), db)
			s.SetQueueClient(mocks.NewMockQueueService())

			req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
			req.Host = "localhost"
			req.Header.Set("Sec-Fetch-Site", "same-origin")

			if tt.mode != "" {
				req.Header.Set("Sec-Fetch-Mode", tt.mode)
			}

			if tt.dest != "" {
				req.Header.Set("Sec-Fetch-Dest", tt.dest)
			}

			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
			}
		})
	}
}

func TestFetchMetadataRejectsBrowserNavigationsAndSubresources(t *testing.T) {
	tests := []struct {
		name string
		site string
		mode string
		dest string
		user string
	}{
		{name: "same origin document navigation", site: "same-origin", mode: "navigate", dest: "document", user: "?1"},
		{name: "direct document navigation", site: "none", mode: "navigate", dest: "document", user: "?1"},
		{name: "iframe navigation", site: "same-origin", mode: "navigate", dest: "iframe"},
		{name: "image subresource", site: "same-origin", mode: "no-cors", dest: "image"},
		{name: "script subresource", site: "same-origin", mode: "no-cors", dest: "script"},
		{name: "cors subresource destination", site: "same-origin", mode: "cors", dest: "script"},
		{name: "unexpected fetch user", site: "same-origin", mode: "cors", dest: "empty", user: "?1"},
		{name: "fetch user without mode dest", site: "same-origin", user: "?1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := dbtest.NewTestDB(t)
			s := NewAPIServer(mocks.NewMockLogger(), db)
			s.SetQueueClient(mocks.NewMockQueueService())
			metrics := &fakeSecurityRejectionMetrics{}
			s.SetAPISecurityMetrics(metrics)

			req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
			req.Host = "localhost"
			req.Header.Set("Sec-Fetch-Site", tt.site)

			if tt.mode != "" {
				req.Header.Set("Sec-Fetch-Mode", tt.mode)
			}

			if tt.dest != "" {
				req.Header.Set("Sec-Fetch-Dest", tt.dest)
			}

			if tt.user != "" {
				req.Header.Set("Sec-Fetch-User", tt.user)
			}

			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)

			assertFetchMetadataForbidden(t, rec)
			requireSecurityRejection(t, metrics, securityReasonFetchMetadataForbidden, securityRejectionUnknownRoute, http.StatusForbidden)
		})
	}
}

func TestFetchMetadataRejectsNavigationEvenWithCORSOrigin(t *testing.T) {
	t.Setenv("VECTIS_API_CORS_ALLOWED_ORIGINS", "https://ui.example")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	metrics := &fakeSecurityRejectionMetrics{}
	s.SetAPISecurityMetrics(metrics)

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Host = "localhost"
	req.Header.Set("Origin", "https://ui.example")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-User", "?1")
	rec := httptest.NewRecorder()

	s.Handler().ServeHTTP(rec, req)

	assertFetchMetadataForbidden(t, rec)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin=%q, want empty", got)
	}

	requireSecurityRejection(t, metrics, securityReasonFetchMetadataForbidden, securityRejectionUnknownRoute, http.StatusForbidden)
}

func TestValidFetchMetadataRejectsUnsafeModeDestForCookieAuth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Dest", "document")

	if validFetchMetadata(req) {
		t.Fatal("validFetchMetadata accepted browser navigation metadata")
	}
}

func assertFetchMetadataForbidden(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	var body apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v; body=%s", err, rec.Body.String())
	}

	if body.Code != string(apiErrFetchMetadataForbidden) {
		t.Fatalf("code=%q, want %q", body.Code, apiErrFetchMetadataForbidden)
	}
}
