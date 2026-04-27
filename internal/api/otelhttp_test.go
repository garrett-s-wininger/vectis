package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIHTTPSpanName_withPattern(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.SetPathValue("id", "123") // simulate pattern match

	// httptest doesn't set Pattern on old Go versions; test via direct call
	got := apiHTTPSpanName("", req)
	if got != "GET /api/v1/jobs" {
		t.Fatalf("expected 'GET /api/v1/jobs', got %q", got)
	}
}

func TestAPIHTTPInstrumentationFilter_excludesHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	if apiHTTPInstrumentationFilter(req) {
		t.Fatal("expected /health/live to be filtered out")
	}
}

func TestAPIHTTPInstrumentationFilter_excludesMetrics(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	if apiHTTPInstrumentationFilter(req) {
		t.Fatal("expected /metrics to be filtered out")
	}
}

func TestAPIHTTPInstrumentationFilter_includesAPI(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	if !apiHTTPInstrumentationFilter(req) {
		t.Fatal("expected /api/v1/jobs to be included")
	}
}
