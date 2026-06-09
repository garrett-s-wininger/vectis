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
	specs := s.routeSpecs(true)

	for _, spec := range specs {
		s.registerRoute(mux, spec)
	}

	h := http.Handler(mux)
	h = s.routeGuardMiddleware(newAPIRouteIndex(specs), h)
	h = s.corsMiddleware(h)
	h = s.fetchMetadataMiddleware(h)
	h = s.requestHeaderGuardMiddleware(h)
	h = s.hostHeaderMiddleware(h)
	h = accessLogMiddleware(s.AccessLogger, apiHTTPExcludedFromAuxLogging, h)
	h = observability.CorrelationMiddleware(h)
	headerPolicy := httpsecurity.APIHeaderPolicy()
	headerPolicy.StrictTransportSecurity = config.APIHSTSHeaderValue()
	headerPolicy.RequestSecure = config.HTTPOriginalRequestSecure
	h = httpsecurity.HeaderMiddleware(headerPolicy, h)
	h = panicRecoveryMiddleware(s.logger, h)
	return instrumentHTTPServer(h)
}

func (s *APIServer) routeSpecs(includeMetrics bool) []routeSpec {
	defaultLimits := ratelimit.DefaultCategory()
	jsonDocumentBody := routeBodyJSONPolicy(maxJSONDocumentBodyBytes)
	optionalJSONDocumentBody := routeBodyOptionalJSONPolicy(maxJSONDocumentBodyBytes)
	jobDefinitionBody := routeBodyJSONPolicy(maxJobDefinitionBodyBytes)
	optionalJobDefinitionBody := routeBodyOptionalJSONPolicy(maxJobDefinitionBodyBytes)
	specs := make([]routeSpec, 0, 48)

	if includeMetrics && s.MetricsHandler != nil {
		specs = append(specs, routeSpec{
			Pattern: "GET /metrics",
			Handler: s.MetricsHandler,
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
			Accept:  routeAcceptMetricsPolicy(),
		})
	}

	specs = append(specs, []routeSpec{
		{
			Pattern: "GET /health/live",
			Handler: http.HandlerFunc(s.HealthLive),
			// public route: orchestrator liveness probe; OK response has no representation.
			Auth:   routeAuthPolicy{mode: routeAuthPublic},
			Accept: routeAcceptAnyPolicy(),
		},
		{
			Pattern: "GET /health/ready",
			Handler: http.HandlerFunc(s.HealthReady),
			// public route: orchestrator readiness probe; OK response has no representation.
			Auth:   routeAuthPolicy{mode: routeAuthPublic},
			Accept: routeAcceptAnyPolicy(),
		},
		{
			Pattern: "GET /api/v1/version",
			Handler: http.HandlerFunc(s.GetVersion),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/schema/status",
			Handler: http.HandlerFunc(s.GetSchemaStatus),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/reconciler/heartbeat",
			Handler: http.HandlerFunc(s.GetReconcilerHeartbeat),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/audit/drops",
			Handler: http.HandlerFunc(s.GetAuditDrops),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/db/pool-stats",
			Handler: http.HandlerFunc(s.GetDBPoolStats),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/queue/backlog",
			Handler: http.HandlerFunc(s.GetQueueBacklog),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/reconciler/stuck-runs",
			Handler: http.HandlerFunc(s.GetStuckRuns),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/log/reachable",
			Handler: http.HandlerFunc(s.GetLogReachable),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/audit/flush-failures",
			Handler: http.HandlerFunc(s.GetAuditFlushFailures),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/cron/status",
			Handler: http.HandlerFunc(s.GetCronStatus),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/catalog/status",
			Handler: http.HandlerFunc(s.GetCatalogStatus),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern: "GET /api/v1/cells/status",
			Handler: http.HandlerFunc(s.GetCellsStatus),
			Auth:    routeAuthPolicy{Action: authz.ActionAdmin},
		},
		{
			Pattern:   "POST /api/v1/cells/{cell_id}/catalog-events",
			Handler:   http.HandlerFunc(s.PostCellCatalogEvent),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			Body:      jsonDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/source-repositories",
			Handler:   http.HandlerFunc(s.ListSourceRepositories),
			Auth:      routeAuthPolicy{Action: authz.ActionJobRead},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/source-repositories",
			Handler:   http.HandlerFunc(s.CreateSourceRepository),
			Auth:      routeAuthPolicy{Action: authz.ActionJobWrite},
			Body:      jsonDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/source-repositories/{id}",
			Handler:   http.HandlerFunc(s.GetSourceRepository),
			Auth:      routeAuthPolicy{Action: authz.ActionJobRead},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/source-repositories/{id}/definitions/resolve",
			Handler:   http.HandlerFunc(s.ResolveSourceDefinition),
			Auth:      routeAuthPolicy{Action: authz.ActionJobRead},
			Body:      jsonDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/jobs",
			Handler:   http.HandlerFunc(s.GetJobs),
			Auth:      routeAuthPolicy{Action: authz.ActionJobRead},
			Query:     routeQueryParams("cursor", "limit"),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/jobs/{id}",
			Handler:   http.HandlerFunc(s.GetJob),
			Auth:      routeAuthPolicy{Action: authz.ActionJobRead},
			Query:     routeQueryParams("version"),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/jobs",
			Handler:   http.HandlerFunc(s.CreateJob),
			Auth:      routeAuthPolicy{Action: authz.ActionJobWrite},
			Body:      jobDefinitionBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/jobs/run",
			Handler:   http.HandlerFunc(s.RunJob),
			Auth:      routeAuthPolicy{Action: authz.ActionRunTrigger},
			Body:      jobDefinitionBody,
			Headers:   routeHeadersIdempotencyPolicy(),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "DELETE /api/v1/jobs/{id}",
			Handler:   http.HandlerFunc(s.DeleteJob),
			Auth:      routeAuthPolicy{Action: authz.ActionJobWrite},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "PUT /api/v1/jobs/{id}",
			Handler:   http.HandlerFunc(s.UpdateJobDefinition),
			Auth:      routeAuthPolicy{Action: authz.ActionJobWrite},
			Body:      jobDefinitionBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/jobs/source/{id}",
			Handler:   http.HandlerFunc(s.CreateJobFromSource),
			Auth:      routeAuthPolicy{Action: authz.ActionJobWrite},
			Body:      jsonDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "PUT /api/v1/jobs/source/{id}",
			Handler:   http.HandlerFunc(s.UpdateJobFromSource),
			Auth:      routeAuthPolicy{Action: authz.ActionJobWrite},
			Body:      jsonDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/jobs/trigger/{id}",
			Handler:   http.HandlerFunc(s.TriggerJob),
			Auth:      routeAuthPolicy{Action: authz.ActionRunTrigger},
			Body:      optionalJobDefinitionBody,
			Headers:   routeHeadersIdempotencyPolicy(),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern: "GET /api/v1/jobs/{id}/runs",
			Handler: http.HandlerFunc(s.GetJobRuns),
			Auth:    routeAuthPolicy{Action: authz.ActionRunRead},
			Query: routeQueryParams(
				"after_index",
				"cell_id",
				"cursor",
				"limit",
				"owning_cell",
				"since",
			),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/sse/jobs/{id}/runs",
			Handler:   http.HandlerFunc(s.HandleSSEJobRuns),
			Auth:      routeAuthPolicy{Action: authz.ActionRunRead},
			Cache:     routeCachePolicy{mode: routeCacheHandlerManaged},
			Accept:    routeAcceptSSEPolicy(),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/runs/{id}",
			Handler:   http.HandlerFunc(s.GetRun),
			Auth:      routeAuthPolicy{Action: authz.ActionRunRead},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/runs/{id}/tasks",
			Handler:   http.HandlerFunc(s.GetRunTasks),
			Auth:      routeAuthPolicy{Action: authz.ActionRunRead},
			Query:     routeQueryParams("cursor", "limit"),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/runs/{id}/artifacts",
			Handler:   http.HandlerFunc(s.ListRunArtifacts),
			Auth:      routeAuthPolicy{Action: authz.ActionRunRead},
			Query:     routeQueryParams("cursor", "execution_id", "limit", "task_attempt_id", "task_id"),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/runs/{id}/artifacts/{name}",
			Handler:   http.HandlerFunc(s.GetRunArtifact),
			Auth:      routeAuthPolicy{Action: authz.ActionRunRead},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/runs/{id}/artifacts/{name}/download",
			Handler:   http.HandlerFunc(s.DownloadRunArtifact),
			Auth:      routeAuthPolicy{Action: authz.ActionRunRead},
			Accept:    routeAcceptAnyPolicy(),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/runs/{id}/execution-payload",
			Handler:   http.HandlerFunc(s.GetRunExecutionPayload),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/runs/{id}/replay",
			Handler:   http.HandlerFunc(s.ReplayRun),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			Body:      optionalJobDefinitionBody,
			Headers:   routeHeadersIdempotencyPolicy(),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/runs/{id}/cancel",
			Handler:   http.HandlerFunc(s.CancelRun),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/runs/{id}/repair/mark-succeeded",
			Handler:   http.HandlerFunc(s.RepairMarkRunSucceeded),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			Body:      optionalJSONDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/runs/{id}/repair/mark-failed",
			Handler:   http.HandlerFunc(s.RepairMarkRunFailed),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			Body:      optionalJSONDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/runs/{id}/repair/mark-cancelled",
			Handler:   http.HandlerFunc(s.RepairMarkRunCancelled),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			Body:      optionalJSONDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/runs/{id}/repair/mark-abandoned",
			Handler:   http.HandlerFunc(s.RepairMarkRunAbandoned),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			Body:      optionalJSONDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/runs/{id}/repair/mark-queued",
			Handler:   http.HandlerFunc(s.RepairMarkRunQueued),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			Body:      optionalJSONDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/runs/{id}/force-fail",
			Handler:   http.HandlerFunc(s.ForceFailRun),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			Body:      optionalJSONDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/runs/{id}/force-requeue",
			Handler:   http.HandlerFunc(s.ForceRequeueRun),
			Auth:      routeAuthPolicy{Action: authz.ActionRunOperator},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern: "GET /api/v1/runs/{id}/logs",
			Handler: http.HandlerFunc(s.GetRunLogs),
			Auth:    routeAuthPolicy{Action: authz.ActionRunRead},
			Cache:   routeCachePolicy{mode: routeCacheHandlerManaged},
			Accept:  routeAcceptSSEPolicy(),
			Query: routeQueryParams(
				"replay_limit",
				"since_sequence",
				"tail",
			),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/setup/status",
			Handler:   http.HandlerFunc(s.GetSetupStatus),
			Auth:      routeAuthPolicy{Action: authz.ActionSetupStatus},
			Cache:     routeCachePolicy{mode: routeCacheNoStore},
			RateLimit: defaultLimits.Auth,
		},
		{
			Pattern:   "POST /api/v1/setup/complete",
			Handler:   http.HandlerFunc(s.PostSetupComplete),
			Auth:      routeAuthPolicy{Action: authz.ActionSetupComplete},
			Cache:     routeCachePolicy{mode: routeCacheNoStore},
			Body:      routeBodyJSONPolicy(maxSetupCompleteBodyBytes),
			RateLimit: defaultLimits.Auth,
		},
		{
			Pattern: "POST /api/v1/login",
			Handler: http.HandlerFunc(s.Login),
			// public route: password login creates an authenticated session.
			Auth:      routeAuthPolicy{mode: routeAuthPublic},
			Cache:     routeCachePolicy{mode: routeCacheNoStore},
			Body:      routeBodyJSONPolicy(maxLoginBodyBytes),
			RateLimit: defaultLimits.Auth,
		},
		{
			Pattern:   "POST /api/v1/logout",
			Handler:   http.HandlerFunc(s.Logout),
			Auth:      routeAuthPolicy{Action: authz.ActionAPI},
			RateLimit: defaultLimits.Auth,
		},
		{
			Pattern:   "GET /api/v1/tokens",
			Handler:   http.HandlerFunc(s.ListTokens),
			Auth:      routeAuthPolicy{Action: authz.ActionAPI},
			Query:     routeQueryParams("user_id"),
			RateLimit: defaultLimits.Token,
		},
		{
			Pattern:   "POST /api/v1/tokens",
			Handler:   http.HandlerFunc(s.CreateToken),
			Auth:      routeAuthPolicy{Action: authz.ActionAPI},
			Body:      routeBodyJSONPolicy(maxTokenBodyBytes),
			RateLimit: defaultLimits.Token,
		},
		{
			Pattern:   "DELETE /api/v1/tokens/{id}",
			Handler:   http.HandlerFunc(s.DeleteToken),
			Auth:      routeAuthPolicy{Action: authz.ActionAPI},
			RateLimit: defaultLimits.Token,
		},
		{
			Pattern:   "POST /api/v1/users/change-password",
			Handler:   http.HandlerFunc(s.ChangePassword),
			Auth:      routeAuthPolicy{Action: authz.ActionAPI},
			Body:      routeBodyJSONPolicy(maxChangePasswordBodyBytes),
			RateLimit: defaultLimits.Auth,
		},
		{
			Pattern:   "POST /api/v1/users",
			Handler:   http.HandlerFunc(s.CreateUser),
			Auth:      routeAuthPolicy{Action: authz.ActionUserAdmin},
			Body:      routeBodyJSONPolicy(maxUserBodyBytes),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/users",
			Handler:   http.HandlerFunc(s.ListUsers),
			Auth:      routeAuthPolicy{Action: authz.ActionUserAdmin},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/users/{id}",
			Handler:   http.HandlerFunc(s.GetUser),
			Auth:      routeAuthPolicy{Action: authz.ActionUserAdmin},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "PUT /api/v1/users/{id}",
			Handler:   http.HandlerFunc(s.UpdateUser),
			Auth:      routeAuthPolicy{Action: authz.ActionUserAdmin},
			Body:      routeBodyJSONPolicy(maxUserBodyBytes),
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "DELETE /api/v1/users/{id}",
			Handler:   http.HandlerFunc(s.DeleteUser),
			Auth:      routeAuthPolicy{Action: authz.ActionUserAdmin},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/namespaces",
			Handler:   http.HandlerFunc(s.ListNamespaces),
			Auth:      routeAuthPolicy{Action: authz.ActionJobRead},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/namespaces",
			Handler:   http.HandlerFunc(s.CreateNamespace),
			Auth:      routeAuthPolicy{Action: authz.ActionAdmin},
			Body:      jsonDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/namespaces/{id}",
			Handler:   http.HandlerFunc(s.GetNamespace),
			Auth:      routeAuthPolicy{Action: authz.ActionJobRead},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "DELETE /api/v1/namespaces/{id}",
			Handler:   http.HandlerFunc(s.DeleteNamespace),
			Auth:      routeAuthPolicy{Action: authz.ActionAdmin},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "GET /api/v1/namespaces/{id}/bindings",
			Handler:   http.HandlerFunc(s.ListBindings),
			Auth:      routeAuthPolicy{Action: authz.ActionAdmin},
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "POST /api/v1/namespaces/{id}/bindings",
			Handler:   http.HandlerFunc(s.CreateBinding),
			Auth:      routeAuthPolicy{Action: authz.ActionAdmin},
			Body:      jsonDocumentBody,
			RateLimit: defaultLimits.General,
		},
		{
			Pattern:   "DELETE /api/v1/namespaces/{id}/bindings/{user_id}",
			Handler:   http.HandlerFunc(s.DeleteBinding),
			Auth:      routeAuthPolicy{Action: authz.ActionAdmin},
			Query:     routeQueryParams("role"),
			RateLimit: defaultLimits.General,
		},
	}...)

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

	if err := spec.Body.validate(); err != nil {
		panic(fmt.Sprintf("api route %q has invalid body policy: %v", spec.Pattern, err))
	}

	if err := spec.Accept.validate(); err != nil {
		panic(fmt.Sprintf("api route %q has invalid accept policy: %v", spec.Pattern, err))
	}

	if err := spec.Query.validate(); err != nil {
		panic(fmt.Sprintf("api route %q has invalid query policy: %v", spec.Pattern, err))
	}

	if err := spec.Headers.validate(); err != nil {
		panic(fmt.Sprintf("api route %q has invalid header policy: %v", spec.Pattern, err))
	}

	handler := s.accessControlledHandler(spec.Auth, spec.Handler)
	handler = routeBodyMiddleware(spec.Body, handler, s.recordSecurityRejection)
	handler = routeAcceptMiddleware(spec.Accept, handler, s.recordSecurityRejection)

	s.mu.RLock()
	rl := s.rateLimiter
	s.mu.RUnlock()

	if rl != nil && spec.RateLimit.RefillRate > 0 {
		handler = s.rateLimitMiddleware(rl, spec, handler)
	}

	handler = routeQueryMiddleware(spec.Query, handler, s.recordSecurityRejection)
	handler = routeHeaderMiddleware(spec.Headers, handler, s.recordSecurityRejection)

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
			s.recordSecurityRejection(r, securityReasonRateLimitExceeded, http.StatusTooManyRequests)
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
		Body:      spec.Body,
		Accept:    spec.Accept,
		Query:     spec.Query,
		Headers:   spec.Headers,
		RateLimit: spec.RateLimit,
	})
}
