package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestArtifactDownloadsAdmitOnlySameOriginAttachmentNavigations(t *testing.T) {
	const path = "/api/v1/runs/run-1/artifacts/report/download"
	for _, tc := range []struct {
		name, method, path, site, mode, dest, origin string
		allowed                                      bool
	}{
		{name: "native download", method: "GET", path: path, site: "same-origin", mode: "navigate", dest: "document", allowed: true},
		{name: "download empty destination", method: "GET", path: path, site: "same-origin", mode: "navigate", dest: "empty", allowed: true},
		{name: "encoded names", method: "GET", path: "/api/v1/runs/run-1/artifacts/report%20%2F%20summary/download", site: "same-origin", mode: "navigate", dest: "document", allowed: true},
		{name: "cross site", method: "GET", path: path, site: "cross-site", mode: "navigate", dest: "document"},
		{name: "same site is not same origin", method: "GET", path: path, site: "same-site", mode: "navigate", dest: "document"},
		{name: "direct navigation", method: "GET", path: path, site: "none", mode: "navigate", dest: "document"},
		{name: "wrong origin", method: "GET", path: path, site: "same-origin", mode: "navigate", dest: "document", origin: "https://other.invalid"},
		{name: "list navigation", method: "GET", path: "/api/v1/runs/run-1/artifacts", site: "same-origin", mode: "navigate", dest: "document"},
		{name: "other API navigation", method: "GET", path: "/api/v1/users", site: "same-origin", mode: "navigate", dest: "document"},
		{name: "post", method: "POST", path: path, site: "same-origin", mode: "navigate", dest: "document"},
		{name: "iframe", method: "GET", path: path, site: "same-origin", mode: "navigate", dest: "iframe"},
		{name: "image", method: "GET", path: path, site: "same-origin", mode: "no-cors", dest: "image"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "https://ui.test"+tc.path, nil)
			req.Header.Set("Sec-Fetch-Site", tc.site)
			req.Header.Set("Sec-Fetch-Mode", tc.mode)
			req.Header.Set("Sec-Fetch-Dest", tc.dest)
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}

			rec := httptest.NewRecorder()
			if got := (&uiBackend{}).browserRequestAllowed(rec, req, false); got != tc.allowed {
				t.Fatalf("allowed=%v want=%v: %s", got, tc.allowed, rec.Body.String())
			}
		})
	}
}

func TestNativeArtifactDownloadProxyKeepsSessionAuthenticationAndAttachmentHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-artifact-token" {
			t.Error("missing session authentication")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Header.Get("Cookie") != "" || r.Header.Get("Sec-Fetch-Mode") != "" {
			t.Error("browser credentials or metadata leaked upstream")
		}

		w.Header().Set("Content-Disposition", `attachment; filename="report.txt"`)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "artifact bytes\n")
	}))
	defer upstream.Close()

	b, err := newUIBackend(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	sessionID, err := b.sessions.create(uiSession{APIToken: "synthetic-artifact-token"})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "https://ui.test/api/v1/runs/run-1/artifacts/report/download", nil)
	req.AddCookie(&http.Cookie{Name: uiSessionCookieName, Value: sessionID})
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Dest", "document")
	rec := httptest.NewRecorder()
	b.apiProxyHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "artifact bytes\n" || !strings.Contains(rec.Header().Get("Content-Disposition"), "report.txt") {
		t.Fatalf("download: %d %v %s", rec.Code, rec.Header(), rec.Body.String())
	}
}
