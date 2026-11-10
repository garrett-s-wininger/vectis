package api

import (
	"context"
	"net/http"
	"sort"

	"vectis/internal/api/authz"
)

func (s *APIServer) SetSourceCredentialReferences(fn func(context.Context) ([]string, error)) {
	s.sourceCredentialReferences = fn
}

func (s *APIServer) ListSourceCredentialReferences(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok || !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, "/") {
		return
	}

	refs := []string{}
	if s.sourceCredentialReferences != nil {
		var err error
		refs, err = s.sourceCredentialReferences(ctx)
		if err != nil {
			writeAPIError(w, http.StatusServiceUnavailable, "source_credentials_unavailable", "Unable to list repository credentials.", nil)
			return
		}
	}

	if refs == nil {
		refs = []string{}
	}

	sort.Strings(refs)
	writeJSON(w, http.StatusOK, refs)
}
