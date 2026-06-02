package api

import (
	"fmt"
	"net/http"

	"vectis/internal/api/authz"
	"vectis/internal/api/ratelimit"
	"vectis/internal/config"
	"vectis/internal/httpsecurity"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
)

func (s *APIServer) Handler() http.Handler {
	mux := http.NewServeMux()

	for _, spec := range s.routeSpecs(true) {
		s.registerRoute(mux, spec)
	}

	h := http.Handler(mux)
	h = corsMiddleware(h)
	h = accessLogMiddleware(s.AccessLogger, apiHTTPExcludedFromAuxLogging, h)
	h = observability.CorrelationMiddleware(h)
	h = httpsecurity.HeaderMiddleware(httpsecurity.APIHeaderPolicy(), h)
	h = panicRecoveryMiddleware(s.logger, h)
	return instrumentHTTPServer(h)
}

func (s *APIServer) routeSpecs(includeMetrics bool) []routeSpec {
	defaultLimits := ratelimit.DefaultCategory()
	specs := make([]routeSpec, 0, 48)

	if includeMetrics && s.MetricsHandler != nil {
		specs = append(specs, routeSpec{
			Pattern: "GET /metrics",
			Handler: s.MetricsHandler,
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		})
	}

	specs = append(specs,
		routeSpec{Pattern: "GET /health/live", Handler: http.HandlerFunc(s.HealthLive), Auth: routeAuthPolicy{mode: routeAuthPublic}},   // public route: orchestrator liveness probe
		routeSpec{Pattern: "GET /health/ready", Handler: http.HandlerFunc(s.HealthReady), Auth: routeAuthPolicy{mode: routeAuthPublic}}, // public route: orchestrator readiness probe
		routeSpec{Pattern: "GET /api/v1/version", Handler: http.HandlerFunc(s.GetVersion), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/schema/status", Handler: http.HandlerFunc(s.GetSchemaStatus), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/reconciler/heartbeat", Handler: http.HandlerFunc(s.GetReconcilerHeartbeat), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/audit/drops", Handler: http.HandlerFunc(s.GetAuditDrops), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/db/pool-stats", Handler: http.HandlerFunc(s.GetDBPoolStats), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/queue/backlog", Handler: http.HandlerFunc(s.GetQueueBacklog), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/reconciler/stuck-runs", Handler: http.HandlerFunc(s.GetStuckRuns), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/log/reachable", Handler: http.HandlerFunc(s.GetLogReachable), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/audit/flush-failures", Handler: http.HandlerFunc(s.GetAuditFlushFailures), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/cron/status", Handler: http.HandlerFunc(s.GetCronStatus), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/catalog/status", Handler: http.HandlerFunc(s.GetCatalogStatus), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "GET /api/v1/cells/status", Handler: http.HandlerFunc(s.GetCellsStatus), Auth: routeAuthPolicy{Action: authz.ActionAdmin}},
		routeSpec{Pattern: "POST /api/v1/cells/{cell_id}/catalog-events", Handler: http.HandlerFunc(s.PostCellCatalogEvent), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/jobs", Handler: http.HandlerFunc(s.GetJobs), Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/jobs/{id}", Handler: http.HandlerFunc(s.GetJob), Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/jobs", Handler: http.HandlerFunc(s.CreateJob), Auth: routeAuthPolicy{Action: authz.ActionJobWrite}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/jobs/run", Handler: http.HandlerFunc(s.RunJob), Auth: routeAuthPolicy{Action: authz.ActionRunTrigger}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "DELETE /api/v1/jobs/{id}", Handler: http.HandlerFunc(s.DeleteJob), Auth: routeAuthPolicy{Action: authz.ActionJobWrite}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "PUT /api/v1/jobs/{id}", Handler: http.HandlerFunc(s.UpdateJobDefinition), Auth: routeAuthPolicy{Action: authz.ActionJobWrite}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/jobs/trigger/{id}", Handler: http.HandlerFunc(s.TriggerJob), Auth: routeAuthPolicy{Action: authz.ActionRunTrigger}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/jobs/{id}/runs", Handler: http.HandlerFunc(s.GetJobRuns), Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/sse/jobs/{id}/runs", Handler: http.HandlerFunc(s.HandleSSEJobRuns), Auth: routeAuthPolicy{Action: authz.ActionRunRead}, Cache: routeCachePolicy{mode: routeCacheHandlerManaged}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/runs/{id}", Handler: http.HandlerFunc(s.GetRun), Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/runs/{id}/tasks", Handler: http.HandlerFunc(s.GetRunTasks), Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/runs/{id}/execution-payload", Handler: http.HandlerFunc(s.GetRunExecutionPayload), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/replay", Handler: http.HandlerFunc(s.ReplayRun), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/cancel", Handler: http.HandlerFunc(s.CancelRun), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/repair/mark-succeeded", Handler: http.HandlerFunc(s.RepairMarkRunSucceeded), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/repair/mark-failed", Handler: http.HandlerFunc(s.RepairMarkRunFailed), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/repair/mark-cancelled", Handler: http.HandlerFunc(s.RepairMarkRunCancelled), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/repair/mark-abandoned", Handler: http.HandlerFunc(s.RepairMarkRunAbandoned), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/repair/mark-queued", Handler: http.HandlerFunc(s.RepairMarkRunQueued), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/force-fail", Handler: http.HandlerFunc(s.ForceFailRun), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/force-requeue", Handler: http.HandlerFunc(s.ForceRequeueRun), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/runs/{id}/logs", Handler: http.HandlerFunc(s.GetRunLogs), Auth: routeAuthPolicy{Action: authz.ActionRunRead}, Cache: routeCachePolicy{mode: routeCacheHandlerManaged}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/setup/status", Handler: http.HandlerFunc(s.GetSetupStatus), Auth: routeAuthPolicy{Action: authz.ActionSetupStatus}, RateLimit: defaultLimits.Auth},
		routeSpec{Pattern: "POST /api/v1/setup/complete", Handler: http.HandlerFunc(s.PostSetupComplete), Auth: routeAuthPolicy{Action: authz.ActionSetupComplete}, RateLimit: defaultLimits.Auth},
		routeSpec{Pattern: "POST /api/v1/login", Handler: http.HandlerFunc(s.Login), Auth: routeAuthPolicy{mode: routeAuthPublic}, RateLimit: defaultLimits.Auth}, // public route: password login creates an authenticated session
		routeSpec{Pattern: "POST /api/v1/logout", Handler: http.HandlerFunc(s.Logout), Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Auth},
		routeSpec{Pattern: "GET /api/v1/tokens", Handler: http.HandlerFunc(s.ListTokens), Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Token},
		routeSpec{Pattern: "POST /api/v1/tokens", Handler: http.HandlerFunc(s.CreateToken), Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Token},
		routeSpec{Pattern: "DELETE /api/v1/tokens/{id}", Handler: http.HandlerFunc(s.DeleteToken), Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Token},
		routeSpec{Pattern: "POST /api/v1/users/change-password", Handler: http.HandlerFunc(s.ChangePassword), Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Auth},
		routeSpec{Pattern: "POST /api/v1/users", Handler: http.HandlerFunc(s.CreateUser), Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/users", Handler: http.HandlerFunc(s.ListUsers), Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/users/{id}", Handler: http.HandlerFunc(s.GetUser), Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "PUT /api/v1/users/{id}", Handler: http.HandlerFunc(s.UpdateUser), Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "DELETE /api/v1/users/{id}", Handler: http.HandlerFunc(s.DeleteUser), Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/namespaces", Handler: http.HandlerFunc(s.ListNamespaces), Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/namespaces", Handler: http.HandlerFunc(s.CreateNamespace), Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/namespaces/{id}", Handler: http.HandlerFunc(s.GetNamespace), Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "DELETE /api/v1/namespaces/{id}", Handler: http.HandlerFunc(s.DeleteNamespace), Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/namespaces/{id}/bindings", Handler: http.HandlerFunc(s.ListBindings), Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/namespaces/{id}/bindings", Handler: http.HandlerFunc(s.CreateBinding), Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "DELETE /api/v1/namespaces/{id}/bindings/{user_id}", Handler: http.HandlerFunc(s.DeleteBinding), Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General},
	)

	return specs
}

func panicRecoveryMiddleware(log interfaces.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic in HTTP handler: %v", rec)

				// Only write an error response if headers have not yet been sent.
				// If they have, the client will see a truncated response; we log it here.
				if sw, ok := w.(*statusResponseWriter); !ok || !sw.wroteHeader {
					writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
				}
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func (s *APIServer) registerRoute(mux *http.ServeMux, spec routeSpec) {
	if spec.Pattern == "" {
		panic("api route pattern must not be empty")
	}
	if spec.Handler == nil {
		panic("api route handler must not be nil")
	}

	if err := spec.Auth.validate(); err != nil {
		panic(fmt.Sprintf("api route %q has invalid auth policy: %v", spec.Pattern, err))
	}
	if err := spec.Cache.validate(); err != nil {
		panic(fmt.Sprintf("api route %q has invalid cache policy: %v", spec.Pattern, err))
	}

	handler := s.accessControlledHandler(spec.Auth, spec.Handler)
	s.mu.RLock()
	rl := s.rateLimiter
	s.mu.RUnlock()
	if rl != nil && spec.RateLimit.RefillRate > 0 {
		handler = s.rateLimitMiddleware(rl, spec, handler)
	}
	if spec.Cache.shouldSetNoStore(spec.Auth) {
		handler = noStoreMiddleware(handler)
	}

	mux.Handle(spec.Pattern, handler)
}

func (s *APIServer) rateLimitMiddleware(rl ratelimit.RateLimiter, spec routeSpec, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := s.rateLimitKey(r, spec)
		allowed, retryAfter, err := rl.Allow(r.Context(), key, spec.RateLimit)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Rate limiter error: %v", err)
			writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
			return
		}

		if !allowed {
			writeRateLimitExceeded(w, retryAfter)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *APIServer) rateLimitKey(r *http.Request, spec routeSpec) string {
	rule := spec.RateLimit
	var baseKey string

	if rateLimitCanUseBearer(spec.Auth) {
		token, ok := bearerToken(r.Header.Get("Authorization"))

		if ok {
			baseKey = hashAPIToken(token)
		}
	}

	if baseKey == "" {
		baseKey = config.HTTPClientIP(r)
	}

	// Include route pattern and rule parameters so different endpoints have isolated buckets.
	return fmt.Sprintf("%s:%s:%d:%d", baseKey, r.Pattern, rule.RefillRate, rule.BurstSize)
}

func rateLimitCanUseBearer(policy routeAuthPolicy) bool {
	policy = policy.normalized()
	if policy.isPublic() {
		return false
	}

	switch policy.Action {
	case authz.ActionSetupStatus, authz.ActionSetupComplete:
		return false
	default:
		return true
	}
}

func (s *APIServer) registerRouteFunc(mux *http.ServeMux, spec routeSpec, handler http.HandlerFunc) {
	s.registerRoute(mux, routeSpec{
		Pattern:   spec.Pattern,
		Handler:   handler,
		Auth:      spec.Auth,
		Cache:     spec.Cache,
		RateLimit: spec.RateLimit,
	})
}
