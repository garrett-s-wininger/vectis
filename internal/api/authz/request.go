package authz

import "net/http"

// ActionForRequest classifies r into a single Action for middleware and policy checks.
// Unknown API paths still map to ActionAPI so unauthenticated requests can be rejected uniformly.
func ActionForRequest(r *http.Request) Action {
	switch {
	case r.URL.Path == "/api/v1/setup/status" && r.Method == http.MethodGet:
		return ActionSetupStatus
	case r.URL.Path == "/api/v1/setup/complete" && r.Method == http.MethodPost:
		return ActionSetupComplete
	default:
		return ActionAPI
	}
}
