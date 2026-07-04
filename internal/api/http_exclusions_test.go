package api

import (
	"net/http"
	"testing"
)

func TestAPIHTTPExcludedFromAuxLogging_healthLive(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "/health/live", nil)
	if !apiHTTPExcludedFromAuxLogging(req) {
		t.Fatal("expected /health/live to be excluded")
	}
}

func TestAPIHTTPExcludedFromAuxLogging_healthReady(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "/health/ready", nil)
	if !apiHTTPExcludedFromAuxLogging(req) {
		t.Fatal("expected /health/ready to be excluded")
	}
}

func TestAPIHTTPExcludedFromAuxLogging_otherPaths(t *testing.T) {
	paths := []string{"/metrics", "/api/v1/catalog/status", "/api/v1/cells/status", "/api/v1/jobs", "/api/v1/tokens", "/", "/health"}
	for _, p := range paths {
		req, _ := http.NewRequest(http.MethodGet, p, nil)
		if apiHTTPExcludedFromAuxLogging(req) {
			t.Fatalf("expected %s NOT to be excluded", p)
		}
	}
}
