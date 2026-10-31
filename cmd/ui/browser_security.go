package main

import (
	"mime"
	"net/http"
	"net/url"
	"strings"
)

func (b *uiBackend) browserRequestAllowed(w http.ResponseWriter, r *http.Request, jsonBody bool) bool {
	if b.allowLocalHTTPCookies && r.TLS == nil && !b.localHTTPRequest(r) {
		writeBFFError(w, http.StatusForbidden, "local_http_required", "HTTP development request does not match the configured listener policy")
		return false
	}

	if len(r.Header.Values("Origin")) > 1 || len(r.Header.Values("Referer")) > 1 {
		writeBFFError(w, http.StatusForbidden, "invalid_origin", "ambiguous browser origin")
		return false
	}

	for _, name := range []string{"Sec-Fetch-Site", "Sec-Fetch-Mode", "Sec-Fetch-Dest"} {
		if len(r.Header.Values(name)) > 1 {
			writeBFFError(w, http.StatusForbidden, "invalid_fetch_metadata", "ambiguous browser metadata")
			return false
		}
	}

	site := r.Header.Get("Sec-Fetch-Site")
	if site != "" && site != "same-origin" && site != "none" {
		writeBFFError(w, http.StatusForbidden, "cross_origin_forbidden", "same-origin requests are required")
		return false
	}

	for name, allowed := range map[string]map[string]bool{
		"Sec-Fetch-Mode": {"": true, "cors": true, "same-origin": true},
		"Sec-Fetch-Dest": {"": true, "empty": true},
	} {
		if !allowed[r.Header.Get(name)] {
			writeBFFError(w, http.StatusForbidden, "invalid_fetch_metadata", "API browser request required")
			return false
		}
	}

	unsafe := r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions
	raw := r.Header.Get("Origin")
	referer := false
	if raw == "" && unsafe {
		raw = r.Header.Get("Referer")
		referer = true
	}

	if raw != "" {
		origin, err := url.Parse(raw)
		expected := b.publicURL
		if expected == nil {
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}

			expected = &url.URL{Scheme: scheme, Host: r.Host}
		}

		if err != nil || origin.User != nil || origin.Opaque != "" || origin.Fragment != "" ||
			(!referer && (origin.Path != "" || origin.RawQuery != "")) ||
			!sameBrowserOrigin(origin, expected) {
			writeBFFError(w, http.StatusForbidden, "cross_origin_forbidden", "same-origin requests are required")
			return false
		}
	} else if unsafe {
		writeBFFError(w, http.StatusForbidden, "origin_required", "browser origin is required")
		return false
	}

	if jsonBody {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeBFFError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "application/json is required")
			return false
		}
	}

	return true
}

func sameBrowserOrigin(a, b *url.URL) bool {
	if a.Scheme != "http" && a.Scheme != "https" {
		return false
	}

	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Hostname(), b.Hostname()) && browserOriginPort(a) == browserOriginPort(b)
}
func browserOriginPort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}

	if u.Scheme == "https" {
		return "443"
	}

	return "80"
}
