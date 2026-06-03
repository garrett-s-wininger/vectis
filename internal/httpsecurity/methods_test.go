package httpsecurity

import (
	"net/http"
	"testing"
)

func TestMethodAllowedAllowsHEADForGET(t *testing.T) {
	if !MethodAllowed(http.MethodHead, http.MethodGet) {
		t.Fatal("HEAD should be allowed when GET is allowed")
	}

	if MethodAllowed("get", http.MethodGet) {
		t.Fatal("HTTP method matching should be case-sensitive")
	}
}

func TestAllowHeaderAddsHEADForGET(t *testing.T) {
	got := AllowHeader(http.MethodPost, http.MethodGet)
	want := "GET, HEAD, POST"
	if got != want {
		t.Fatalf("AllowHeader = %q, want %q", got, want)
	}
}

func TestDangerousHTTPMethod(t *testing.T) {
	for _, method := range []string{http.MethodTrace, "TRACK", http.MethodConnect, " trace "} {
		if !DangerousHTTPMethod(method) {
			t.Fatalf("DangerousHTTPMethod(%q) = false, want true", method)
		}
	}

	if DangerousHTTPMethod(http.MethodGet) {
		t.Fatal("GET should not be classified as dangerous")
	}
}
