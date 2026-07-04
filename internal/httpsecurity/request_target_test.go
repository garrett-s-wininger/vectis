package httpsecurity

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSafeRequestTarget(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
		want bool
	}{
		{name: "origin form", req: httptest.NewRequest(http.MethodGet, "/metrics", nil), want: true},
		{name: "query string", req: httptest.NewRequest(http.MethodGet, "/metrics?format=openmetrics", nil), want: true},
		{name: "directory index", req: httptest.NewRequest(http.MethodGet, "/guide/", nil), want: true},
		{name: "absolute form", req: httptest.NewRequest(http.MethodGet, "http://example.test/metrics", nil), want: false},
		{name: "encoded static segment", req: httptest.NewRequest(http.MethodGet, "/%6detics", nil), want: false},
		{name: "encoded slash", req: httptest.NewRequest(http.MethodGet, "/assets%2Fmain.js", nil), want: false},
		{name: "dot segment", req: httptest.NewRequest(http.MethodGet, "/docs/../metrics", nil), want: false},
		{name: "duplicate slash", req: httptest.NewRequest(http.MethodGet, "/docs//metrics", nil), want: false},
		{name: "nil request", req: nil, want: false},
		{name: "nil url", req: &http.Request{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SafeRequestTarget(tt.req); got != tt.want {
				t.Fatalf("SafeRequestTarget() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSafeRequestTargetRejectsAsteriskForm(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.URL.Path = "*"
	req.RequestURI = "*"

	if SafeRequestTarget(req) {
		t.Fatal("SafeRequestTarget() accepted asterisk-form request target")
	}
}

func TestCanonicalRequestTargetRejectsDirectoryIndexAlias(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/guide/", nil)

	if !SafeRequestTarget(req) {
		t.Fatal("SafeRequestTarget() should allow directory-index paths")
	}

	if CanonicalRequestTarget(req) {
		t.Fatal("CanonicalRequestTarget() should reject trailing slash aliases")
	}
}
