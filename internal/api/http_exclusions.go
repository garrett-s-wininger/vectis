package api

import "net/http"

func apiHTTPExcludedFromAuxLogging(r *http.Request) bool {
	switch r.URL.Path {
	case "/metrics", "/health/live", "/health/ready":
		return true
	default:
		return false
	}
}
