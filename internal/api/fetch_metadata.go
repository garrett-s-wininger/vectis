package api

import (
	"net/http"
	"strings"
)

func fetchMetadataMiddleware(next http.Handler) http.Handler {
	return (*APIServer)(nil).fetchMetadataMiddleware(next)
}

func (s *APIServer) fetchMetadataMiddleware(next http.Handler) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fetchMetadataAllowedBeforeRoute(r) {
			next.ServeHTTP(w, r)
			return
		}

		if s != nil {
			s.recordSecurityRejection(r, securityReasonFetchMetadataForbidden, http.StatusForbidden)
		}

		writeAPIErrorCode(w, http.StatusForbidden, apiErrFetchMetadataForbidden)
	})
}

func fetchMetadataAllowedBeforeRoute(r *http.Request) bool {
	if !fetchMetadataModeDestAllowed(r) {
		return false
	}

	if fetchMetadataSiteAllowed(r) {
		return true
	}

	// Cross-origin browser API calls carry Origin and must pass the CORS allowlist.
	// Cross-site browser navigations and subresource loads commonly omit Origin,
	// so reject those before route handling.
	return strings.TrimSpace(r.Header.Get("Origin")) != ""
}

func fetchMetadataSiteAllowed(r *http.Request) bool {
	site := strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")))
	switch site {
	case "", "same-origin", "same-site", "none":
		return true
	case "cross-site":
		return false
	default:
		return true
	}
}

func fetchMetadataModeDestAllowed(r *http.Request) bool {
	mode := strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Mode")))
	dest := strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Dest")))

	if strings.TrimSpace(r.Header.Get("Sec-Fetch-User")) != "" {
		return false
	}

	if mode == "" && dest == "" {
		return true
	}

	switch mode {
	case "", "cors", "same-origin":
	default:
		return false
	}

	switch dest {
	case "", "empty":
		return true
	default:
		return false
	}
}
