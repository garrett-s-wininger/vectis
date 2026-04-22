package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"vectis/internal/api/authz"
	"vectis/internal/dal"
)

type createNamespaceRequest struct {
	Name     string `json:"name"`
	ParentID *int64 `json:"parent_id,omitempty"`
}

type namespaceResponse struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	ParentID         *int64 `json:"parent_id,omitempty"`
	Path             string `json:"path"`
	BreakInheritance bool   `json:"break_inheritance"`
}

func namespaceRecordToResponse(rec *dal.NamespaceRecord) namespaceResponse {
	return namespaceResponse{
		ID:               rec.ID,
		Name:             rec.Name,
		ParentID:         rec.ParentID,
		Path:             rec.Path,
		BreakInheritance: rec.BreakInheritance,
	}
}

func (s *APIServer) CreateNamespace(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	var req createNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) {
		return
	}

	// Check admin permission on parent namespace
	parentPath := "/"
	if req.ParentID != nil && *req.ParentID != 0 {
		parent, err := s.namespaces.GetByID(ctx, *req.ParentID)
		if err != nil {
			if dal.IsNotFound(err) {
				http.Error(w, "parent namespace not found", http.StatusNotFound)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		parentPath = parent.Path
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, parentPath) {
		return
	}

	rec, err := s.namespaces.Create(ctx, req.Name, req.ParentID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsConflict(err) {
			http.Error(w, "namespace already exists", http.StatusConflict)
			return
		}

		if dal.IsInvalidNamespaceName(err) {
			http.Error(w, "invalid namespace name", http.StatusBadRequest)
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	resp := namespaceRecordToResponse(rec)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode namespace: %v", err)
	}
}

func (s *APIServer) ListNamespaces(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) {
		return
	}

	recs, err := s.namespaces.List(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	z := s.effectiveAuthorizer(true)
	var resp []namespaceResponse
	for _, rec := range recs {
		if z.Allow(ctx, p, authz.ActionJobRead, authz.Resource{NamespacePath: rec.Path}) {
			resp = append(resp, namespaceRecordToResponse(&rec))
		}
	}

	if resp == nil {
		resp = []namespaceResponse{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode namespaces: %v", err)
	}
}

func (s *APIServer) GetNamespace(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) {
		return
	}

	rec, err := s.namespaces.GetByID(ctx, id)
	if err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "namespace not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	if !s.authorizeNamespace(ctx, w, p, authz.ActionJobRead, rec.Path) {
		return
	}

	resp := namespaceRecordToResponse(rec)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode namespace: %v", err)
	}
}

func (s *APIServer) DeleteNamespace(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	if id == 1 {
		http.Error(w, "cannot delete root namespace", http.StatusForbidden)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) {
		return
	}

	rec, err := s.namespaces.GetByID(ctx, id)
	if err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "namespace not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, rec.Path) {
		return
	}

	hasChildren, err := s.namespaces.HasChildren(ctx, id)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if hasChildren {
		http.Error(w, "namespace has children", http.StatusConflict)
		return
	}

	hasJobs, err := s.namespaces.HasJobs(ctx, id)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if hasJobs {
		http.Error(w, "namespace has jobs", http.StatusConflict)
		return
	}

	if err := s.namespaces.Delete(ctx, id); err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "namespace not found", http.StatusNotFound)
			return
		}

		if dal.IsConflict(err) {
			http.Error(w, "namespace has children or jobs", http.StatusConflict)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	w.WriteHeader(http.StatusNoContent)
}
