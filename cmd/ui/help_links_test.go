package main

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestBrowserDocsURLUsesBrowserHostOrConfiguredPublicURL(t *testing.T) {
	for _, tc := range []struct {
		name, configured, public, request, want string
		port                                    int
		tls                                     bool
	}{
		{name: "hosted default", request: "http://ui.test/ui/api/context", want: defaultDocsURL},
		{name: "local HTTP", request: "http://dev-vm:8089/ui/api/context", port: 8088, want: "http://dev-vm:8088/"},
		{name: "local HTTPS IPv6", request: "https://[::1]:8089/ui/api/context", port: 8088, tls: true, want: "https://[::1]:8088/"},
		{name: "public origin", request: "http://internal:8089/ui/api/context", public: "https://ci.example.test", port: 8088, want: "https://ci.example.test:8088/"},
		{name: "explicit site overrides local", configured: "https://docs.example.test/vectis", request: "http://dev-vm:8089/ui/api/context", port: 8088, want: "https://docs.example.test/vectis/"},
		{name: "same origin path", configured: "/help/", request: "https://ui.test/ui/api/context", want: "/help/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &uiBackend{}
			if err := b.configureDocs(tc.configured, tc.port); err != nil {
				t.Fatal(err)
			}

			if tc.public != "" {
				b.publicURL, _ = url.Parse(tc.public)
			}

			req := httptest.NewRequest(http.MethodGet, tc.request, nil)
			if tc.tls {
				req.TLS = &tls.ConnectionState{}
			}

			req.Header.Set("X-Forwarded-Host", "untrusted.invalid")
			req.Header.Set("X-Forwarded-Proto", "https")
			if got := b.browserDocsURL(req); got != tc.want {
				t.Fatalf("docs URL=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestConfigureDocsRejectsUnsafeOrMalformedURLs(t *testing.T) {
	for _, raw := range []string{"javascript:alert(1)", "data:text/html,test", "//other.test", "docs.example.test", "https://user:password@example.test", "https://example.test/?token=secret", "https://example.test/#fragment", "/\\other.test"} {
		t.Run(raw, func(t *testing.T) {
			if err := (&uiBackend{}).configureDocs(raw, 0); err == nil {
				t.Fatalf("accepted %q", raw)
			}
		})
	}

	for _, port := range []int{-1, 65536} {
		if err := (&uiBackend{}).configureDocs("", port); err == nil {
			t.Fatalf("accepted port %d", port)
		}
	}
}
