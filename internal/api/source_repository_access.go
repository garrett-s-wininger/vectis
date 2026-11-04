package api

import (
	"net/http"
	"strconv"
	"vectis/internal/api/audit"
	"vectis/internal/api/authz"
	"vectis/internal/dal"
)

func (s *APIServer) SourceRepositoryNamespaceAccess(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireSources(w) || !s.requireNamespaces(w) {
		return
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, "/") {
		return
	}

	repositoryID := r.PathValue("id")
	if _, err := s.sources.GetRepository(ctx, repositoryID); err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, 404, "source_repository_not_found", "source repository not found", nil)
		} else {
			writeAPIErrorCode(w, 500, apiErrInternal)
		}

		return
	}

	if r.Method != "GET" {
		namespaceID, err := strconv.ParseInt(r.PathValue("namespace_id"), 10, 64)
		if err != nil || namespaceID <= 0 {
			writeAPIErrorCode(w, 400, apiErrInvalidNamespaceID)
			return
		}

		ns, err := s.namespaces.GetByID(ctx, namespaceID)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAPIError(w, 404, "namespace_not_found", "namespace not found", nil)
			} else {
				writeAPIErrorCode(w, 500, apiErrInternal)
			}

			return
		}

		operation := "namespace_granted"
		if r.Method == "PUT" {
			err = s.sources.GrantRepositoryNamespace(ctx, repositoryID, namespaceID)
		} else {
			operation = "namespace_revoked"
			err = s.sources.RevokeRepositoryNamespace(ctx, repositoryID, namespaceID)
		}

		if err != nil {
			writeAPIErrorCode(w, 500, apiErrInternal)
			return
		}

		_ = s.auditLog(ctx, audit.EventSourceRepositoryUpdated, actorIDFromPrincipal(p), 0, map[string]any{"repository_id": repositoryID, "namespace": ns.Path, "operation": operation})
	}

	grants, err := s.sources.RepositoryNamespaces(ctx, repositoryID)
	if err != nil {
		writeAPIErrorCode(w, 500, apiErrInternal)
		return
	}

	response := make([]map[string]any, 0, len(grants))
	for _, grant := range grants {
		response = append(response, map[string]any{"namespace_id": grant.ID, "namespace": grant.Path})
	}

	writeJSON(w, http.StatusOK, response)
}
