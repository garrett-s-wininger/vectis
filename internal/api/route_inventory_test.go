package api

import (
	"net/http"
	"os"
	"strings"
	"testing"

	"vectis/internal/api/authz"
)

func TestAPIRouteInventory(t *testing.T) {
	s := &APIServer{}

	got := s.routeSpecs(false)
	want := []struct {
		pattern string
		auth    string
	}{
		{"GET /health/live", "public"},
		{"GET /health/ready", "public"},
		{"GET /api/v1/version", string(authz.ActionAdmin)},
		{"GET /api/v1/schema/status", string(authz.ActionAdmin)},
		{"GET /api/v1/reconciler/heartbeat", string(authz.ActionAdmin)},
		{"GET /api/v1/audit/drops", string(authz.ActionAdmin)},
		{"GET /api/v1/db/pool-stats", string(authz.ActionAdmin)},
		{"GET /api/v1/queue/backlog", string(authz.ActionAdmin)},
		{"GET /api/v1/reconciler/stuck-runs", string(authz.ActionAdmin)},
		{"GET /api/v1/log/reachable", string(authz.ActionAdmin)},
		{"GET /api/v1/audit/flush-failures", string(authz.ActionAdmin)},
		{"GET /api/v1/cron/status", string(authz.ActionAdmin)},
		{"GET /api/v1/catalog/status", string(authz.ActionAdmin)},
		{"GET /api/v1/cells/status", string(authz.ActionAdmin)},
		{"POST /api/v1/cells/{cell_id}/catalog-events", string(authz.ActionRunOperator)},
		{"GET /api/v1/jobs", string(authz.ActionJobRead)},
		{"GET /api/v1/jobs/{id}", string(authz.ActionJobRead)},
		{"POST /api/v1/jobs", string(authz.ActionJobWrite)},
		{"POST /api/v1/jobs/run", string(authz.ActionRunTrigger)},
		{"DELETE /api/v1/jobs/{id}", string(authz.ActionJobWrite)},
		{"PUT /api/v1/jobs/{id}", string(authz.ActionJobWrite)},
		{"POST /api/v1/jobs/trigger/{id}", string(authz.ActionRunTrigger)},
		{"GET /api/v1/jobs/{id}/runs", string(authz.ActionRunRead)},
		{"GET /api/v1/sse/jobs/{id}/runs", string(authz.ActionRunRead)},
		{"GET /api/v1/runs/{id}", string(authz.ActionRunRead)},
		{"GET /api/v1/runs/{id}/tasks", string(authz.ActionRunRead)},
		{"GET /api/v1/runs/{id}/execution-payload", string(authz.ActionRunOperator)},
		{"POST /api/v1/runs/{id}/replay", string(authz.ActionRunOperator)},
		{"POST /api/v1/runs/{id}/cancel", string(authz.ActionRunOperator)},
		{"POST /api/v1/runs/{id}/repair/mark-succeeded", string(authz.ActionRunOperator)},
		{"POST /api/v1/runs/{id}/repair/mark-failed", string(authz.ActionRunOperator)},
		{"POST /api/v1/runs/{id}/repair/mark-cancelled", string(authz.ActionRunOperator)},
		{"POST /api/v1/runs/{id}/repair/mark-abandoned", string(authz.ActionRunOperator)},
		{"POST /api/v1/runs/{id}/repair/mark-queued", string(authz.ActionRunOperator)},
		{"POST /api/v1/runs/{id}/force-fail", string(authz.ActionRunOperator)},
		{"POST /api/v1/runs/{id}/force-requeue", string(authz.ActionRunOperator)},
		{"GET /api/v1/runs/{id}/logs", string(authz.ActionRunRead)},
		{"GET /api/v1/setup/status", string(authz.ActionSetupStatus)},
		{"POST /api/v1/setup/complete", string(authz.ActionSetupComplete)},
		{"POST /api/v1/login", "public"},
		{"POST /api/v1/logout", string(authz.ActionAPI)},
		{"GET /api/v1/tokens", string(authz.ActionAPI)},
		{"POST /api/v1/tokens", string(authz.ActionAPI)},
		{"DELETE /api/v1/tokens/{id}", string(authz.ActionAPI)},
		{"POST /api/v1/users/change-password", string(authz.ActionAPI)},
		{"POST /api/v1/users", string(authz.ActionUserAdmin)},
		{"GET /api/v1/users", string(authz.ActionUserAdmin)},
		{"GET /api/v1/users/{id}", string(authz.ActionUserAdmin)},
		{"PUT /api/v1/users/{id}", string(authz.ActionUserAdmin)},
		{"DELETE /api/v1/users/{id}", string(authz.ActionUserAdmin)},
		{"GET /api/v1/namespaces", string(authz.ActionJobRead)},
		{"POST /api/v1/namespaces", string(authz.ActionAdmin)},
		{"GET /api/v1/namespaces/{id}", string(authz.ActionJobRead)},
		{"DELETE /api/v1/namespaces/{id}", string(authz.ActionAdmin)},
		{"GET /api/v1/namespaces/{id}/bindings", string(authz.ActionAdmin)},
		{"POST /api/v1/namespaces/{id}/bindings", string(authz.ActionAdmin)},
		{"DELETE /api/v1/namespaces/{id}/bindings/{user_id}", string(authz.ActionAdmin)},
	}

	if len(got) != len(want) {
		t.Fatalf("route count = %d, want %d", len(got), len(want))
	}

	seen := make(map[string]bool, len(got))
	for i, spec := range got {
		if spec.Pattern != want[i].pattern {
			t.Fatalf("route[%d] pattern = %q, want %q", i, spec.Pattern, want[i].pattern)
		}

		if spec.Handler == nil {
			t.Fatalf("route[%d] %q has nil handler", i, spec.Pattern)
		}

		if seen[spec.Pattern] {
			t.Fatalf("duplicate route pattern %q", spec.Pattern)
		}

		seen[spec.Pattern] = true

		if auth := authLabel(spec.Auth); auth != want[i].auth {
			t.Fatalf("route[%d] %q auth = %q, want %q", i, spec.Pattern, auth, want[i].auth)
		}

		if err := spec.Auth.validate(); err != nil {
			t.Fatalf("route[%d] %q has invalid auth policy: %v", i, spec.Pattern, err)
		}
	}
}

func TestAPIRouteInventory_metricsRequiresAdmin(t *testing.T) {
	s := &APIServer{MetricsHandler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}
	for _, spec := range s.routeSpecs(true) {
		if spec.Pattern != "GET /metrics" {
			continue
		}

		if auth := authLabel(spec.Auth); auth != string(authz.ActionAdmin) {
			t.Fatalf("/metrics auth = %q, want %q", auth, authz.ActionAdmin)
		}

		return
	}

	t.Fatal("route inventory did not include /metrics")
}

func TestAPIReferenceListsRegisteredRoutes(t *testing.T) {
	s := &APIServer{MetricsHandler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}
	doc, err := os.ReadFile("../../website/docs/using/api-reference.md")
	if err != nil {
		t.Fatal(err)
	}

	text := string(doc)

	for _, spec := range s.routeSpecs(true) {
		_, path, ok := strings.Cut(spec.Pattern, " ")
		if !ok {
			t.Fatalf("route pattern %q does not contain method and path", spec.Pattern)
		}

		if !strings.Contains(text, "`"+path+"`") {
			t.Fatalf("API reference does not list route path %q", path)
		}
	}
}

func authLabel(policy routeAuthPolicy) string {
	policy = policy.normalized()
	if policy.isPublic() {
		return "public"
	}

	return string(policy.Action)
}
