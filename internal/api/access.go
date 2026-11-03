package api

import (
	"net/http"

	"vectis/internal/api/authz"
)

func (s *APIServer) GetAccess(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	principal, ok := s.requirePrincipal(w, r)
	if !ok || !s.requireNamespaces(w) {
		return
	}

	namespaces, err := s.namespaces.List(ctx)
	if err != nil {
		if !s.handleDBUnavailableError(w, err) {
			s.logger.Error("Database error loading access: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		}

		return
	}

	authorizer := s.effectiveAuthorizer(true)
	actions := []authz.Action{authz.ActionJobRead, authz.ActionJobWrite, authz.ActionRunRead, authz.ActionRunTrigger, authz.ActionRunOperator, authz.ActionAdmin, authz.ActionUserAdmin, authz.ActionAPI}
	response := struct {
		Actions    []authz.Action            `json:"actions"`
		Namespaces map[string][]authz.Action `json:"namespaces"`
	}{Actions: []authz.Action{}, Namespaces: map[string][]authz.Action{}}

	for _, action := range actions {
		resource := authz.Resource{}
		if action == authz.ActionAdmin {
			resource.NamespacePath = "/"
		}

		if authorizer.Allow(ctx, principal, action, resource) {
			response.Actions = append(response.Actions, action)
		}
	}

	for _, namespace := range namespaces {
		allowed := []authz.Action{}
		for _, action := range actions {
			if authz.ActionSupportsNamespace(action) && authorizer.Allow(ctx, principal, action, authz.Resource{NamespacePath: namespace.Path}) {
				allowed = append(allowed, action)
			}
		}

		if len(allowed) > 0 {
			response.Namespaces[namespace.Path] = allowed
		}
	}

	writeJSON(w, http.StatusOK, response)
}
