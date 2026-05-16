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
		{"GET /api/v1/version", "public"},
		{"GET /api/v1/schema/status", "public"},
		{"GET /api/v1/reconciler/heartbeat", "public"},
		{"GET /api/v1/audit/drops", "public"},
		{"GET /api/v1/db/pool-stats", "public"},
		{"GET /api/v1/queue/backlog", "public"},
		{"GET /api/v1/reconciler/stuck-runs", "public"},
		{"GET /api/v1/log/reachable", "public"},
		{"GET /api/v1/audit/flush-failures", "public"},
		{"GET /api/v1/cron/status", "public"},
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
	}
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
	if policy.Public {
		return "public"
	}

	return string(policy.Action)
}
