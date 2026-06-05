package httpsecurity

import (
	"net/http"
	pathpkg "path"
	"strings"
)

// SafeRequestTarget reports whether a request uses an origin-form, unescaped,
// non-ambiguous path target. Directory-index paths such as /guide/ remain valid
// for static file servers.
func SafeRequestTarget(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}

	if r.URL.IsAbs() || r.URL.Host != "" || r.URL.Opaque != "" {
		return false
	}

	requestPath := r.URL.Path
	if requestPath == "" || requestPath[0] != '/' || requestPath == "*" {
		return false
	}

	if strings.Contains(r.URL.EscapedPath(), "%") {
		return false
	}

	clean := pathpkg.Clean(requestPath)
	return clean == requestPath || clean+"/" == requestPath
}

// CanonicalRequestTarget is SafeRequestTarget plus a strict path canonicality
// check for APIs that do not serve directory-index paths.
func CanonicalRequestTarget(r *http.Request) bool {
	if !SafeRequestTarget(r) {
		return false
	}

	return pathpkg.Clean(r.URL.Path) == r.URL.Path
}
