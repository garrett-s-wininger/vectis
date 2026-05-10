package api

import "net/http"

func apiHTTPExcludedFromAuxLogging(r *http.Request) bool {
	switch r.URL.Path {
	case "/metrics", "/health/live", "/health/ready":
		return true
	case "/api/v1/version", "/api/v1/schema/status", "/api/v1/reconciler/heartbeat", "/api/v1/audit/drops":
		return true
	case "/api/v1/db/pool-stats", "/api/v1/queue/backlog", "/api/v1/reconciler/stuck-runs":
		return true
	case "/api/v1/log/reachable", "/api/v1/audit/flush-failures", "/api/v1/cron/status":
		return true
	default:
		return false
	}
}
