package api

import (
	"net/http"
	"strings"

	"vectis/internal/config"
)

const (
	corsAllowMethods = "GET, POST, PUT, DELETE, OPTIONS"
	corsMaxAge       = "600"
)

var corsAllowedRequestHeaders = map[string]string{
	"authorization":    "Authorization",
	"content-type":     "Content-Type",
	"x-correlation-id": "X-Correlation-ID",
	"x-csrf-token":     csrfHeaderName,
	"x-request-id":     "X-Request-ID",
}

func corsMiddleware(next http.Handler) http.Handler {
	return (*APIServer)(nil).corsMiddleware(next)
}

func (s *APIServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}

		allowed := corsOriginAllowed(origin)
		if isCORSPreflight(r) {
			if !allowed || !corsMethodAllowed(r.Header.Get("Access-Control-Request-Method")) {
				if s != nil {
					s.recordSecurityRejection(r, securityReasonCORSPreflightForbidden, http.StatusForbidden)
				}

				w.WriteHeader(http.StatusForbidden)
				return
			}

			headers, ok := corsAllowedHeaders(r.Header.Get("Access-Control-Request-Headers"))
			if !ok {
				if s != nil {
					s.recordSecurityRejection(r, securityReasonCORSPreflightForbidden, http.StatusForbidden)
				}

				w.WriteHeader(http.StatusForbidden)
				return
			}

			setCORSResponseHeaders(w, origin)
			w.Header().Set("Access-Control-Allow-Methods", corsAllowMethods)
			if headers != "" {
				w.Header().Set("Access-Control-Allow-Headers", headers)
			}
			w.Header().Set("Access-Control-Max-Age", corsMaxAge)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if allowed {
			setCORSResponseHeaders(w, origin)
		}

		next.ServeHTTP(w, r)
	})
}

func isCORSPreflight(r *http.Request) bool {
	return r.Method == http.MethodOptions && strings.TrimSpace(r.Header.Get("Access-Control-Request-Method")) != ""
}

func corsOriginAllowed(origin string) bool {
	for _, allowed := range config.APICORSAllowedOrigins() {
		if origin == allowed {
			return true
		}
	}

	return false
}

func corsMethodAllowed(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

func corsAllowedHeaders(raw string) (string, bool) {
	if strings.TrimSpace(raw) == "" {
		return "", true
	}

	var out []string
	seen := make(map[string]bool)
	for part := range strings.SplitSeq(raw, ",") {
		key := strings.ToLower(strings.TrimSpace(part))
		if key == "" {
			continue
		}

		canonical, ok := corsAllowedRequestHeaders[key]
		if !ok {
			return "", false
		}
		if !seen[key] {
			out = append(out, canonical)
			seen[key] = true
		}
	}

	return strings.Join(out, ", "), true
}

func setCORSResponseHeaders(w http.ResponseWriter, origin string) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Allow-Credentials", "true")
	addVaryHeader(h, "Origin")
	addVaryHeader(h, "Access-Control-Request-Method")
	addVaryHeader(h, "Access-Control-Request-Headers")
}

func addVaryHeader(h http.Header, value string) {
	for part := range strings.SplitSeq(h.Get("Vary"), ",") {
		if strings.EqualFold(strings.TrimSpace(part), value) {
			return
		}
	}

	if h.Get("Vary") == "" {
		h.Set("Vary", value)
		return
	}

	h.Set("Vary", h.Get("Vary")+", "+value)
}
