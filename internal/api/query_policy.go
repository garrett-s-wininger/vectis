package api

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type routeQueryPolicy struct {
	allowed []string
}

func routeQueryParams(allowed ...string) routeQueryPolicy {
	return routeQueryPolicy{allowed: allowed}
}

func (p routeQueryPolicy) validate() error {
	seen := make(map[string]bool, len(p.allowed))
	for _, name := range p.allowed {
		name = strings.TrimSpace(name)
		if name == "" {
			return fmt.Errorf("query parameter name must not be empty")
		}

		if seen[name] {
			return fmt.Errorf("duplicate query parameter %q", name)
		}

		seen[name] = true
	}

	return nil
}

func (p routeQueryPolicy) allows(r *http.Request) bool {
	if r == nil || r.URL == nil || r.URL.RawQuery == "" {
		return true
	}

	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return false
	}

	allowed := p.allowedSet()
	for name, got := range values {
		if !allowed[name] {
			return false
		}

		if len(got) > 1 {
			return false
		}
	}

	return true
}

func (p routeQueryPolicy) allowedSet() map[string]bool {
	allowed := make(map[string]bool, len(p.allowed))
	for _, name := range p.allowed {
		allowed[strings.TrimSpace(name)] = true
	}

	return allowed
}

func (p routeQueryPolicy) sameAllowed(want routeQueryPolicy) bool {
	if len(p.allowed) != len(want.allowed) {
		return false
	}

	gotSet := p.allowedSet()
	for _, name := range want.allowed {
		if !gotSet[strings.TrimSpace(name)] {
			return false
		}
	}

	return true
}

func routeQueryMiddleware(policy routeQueryPolicy, next http.Handler, recorders ...securityRejectionRecorder) http.Handler {
	var record securityRejectionRecorder
	if len(recorders) > 0 {
		record = recorders[0]
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !policy.allows(r) {
			if record != nil {
				record(r, securityReasonInvalidQueryParameter, http.StatusBadRequest)
			}

			setNoStore(w)
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidQueryParameter)
			return
		}

		next.ServeHTTP(w, r)
	})
}
