package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"vectis/internal/api/audit"
	"vectis/internal/api/authz"
	"vectis/internal/dal"
)

type createNamespaceRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	ParentID    *int64 `json:"parent_id,omitempty"`
}

type namespaceResponse struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	Description      string `json:"description"`
	ParentID         *int64 `json:"parent_id,omitempty"`
	Path             string `json:"path"`
	BreakInheritance bool   `json:"break_inheritance"`
}

func namespaceRecordToResponse(rec *dal.NamespaceRecord) namespaceResponse {
	return namespaceResponse{
		ID:               rec.ID,
		Name:             rec.Name,
		Description:      rec.Description,
		ParentID:         rec.ParentID,
		Path:             rec.Path,
		BreakInheritance: rec.BreakInheritance,
	}
}

func (s *APIServer) CreateNamespace(w http.ResponseWriter, r *http.Request) {
	body, ok := readRequestBody(w, r, maxJSONDocumentBodyBytes)
	if !ok {
		return
	}

	var req createNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	if req.Name == "" {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrMissingName)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
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
				writeAPIErrorCode(w, http.StatusNotFound, apiErrParentNamespaceNotFound)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}
		parentPath = parent.Path
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, parentPath) {
		return
	}

	rec, err := s.namespaces.CreateWithDescription(ctx, req.Name, req.Description, req.ParentID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsConflict(err) {
			writeAPIErrorCode(w, http.StatusConflict, apiErrNamespaceAlreadyExists)
			return
		}

		if dal.IsInvalidNamespaceName(err) {
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidNamespaceName)
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventNamespaceCreated, actorID, rec.ID, map[string]any{
		"name":        rec.Name,
		"description": rec.Description,
		"parent_id":   req.ParentID,
		"path":        rec.Path,
	})

	resp := namespaceRecordToResponse(rec)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode namespace: %v", err)
	}
}

func (s *APIServer) ListNamespaces(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
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
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidID)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
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
			writeAPIErrorCode(w, http.StatusNotFound, apiErrNamespaceNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
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
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidID)
		return
	}

	if id == dal.RootNamespaceID {
		writeAPIErrorCode(w, http.StatusForbidden, apiErrRootNamespaceDeleteForbidden)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
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
			writeAPIErrorCode(w, http.StatusNotFound, apiErrNamespaceNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if rec.Path == dal.RootNamespacePath {
		writeAPIErrorCode(w, http.StatusForbidden, apiErrRootNamespaceDeleteForbidden)
		return
	}

	if rec.Path == dal.EphemeralNamespacePath {
		writeAPIErrorCode(w, http.StatusForbidden, apiErrSystemNamespaceDeleteForbidden)
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
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if hasChildren {
		writeAPIErrorCode(w, http.StatusConflict, apiErrNamespaceHasChildren)
		return
	}

	hasJobs, err := s.namespaces.HasJobs(ctx, id)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if hasJobs {
		writeAPIErrorCode(w, http.StatusConflict, apiErrNamespaceHasJobs)
		return
	}

	if err := s.namespaces.Delete(ctx, id); err != nil {
		if dal.IsNotFound(err) {
			writeAPIErrorCode(w, http.StatusNotFound, apiErrNamespaceNotFound)
			return
		}

		if dal.IsConflict(err) {
			writeAPIErrorCode(w, http.StatusConflict, apiErrNamespaceNotEmpty)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventNamespaceDeleted, actorID, id, map[string]any{
		"name": rec.Name,
		"path": rec.Path,
	})

	w.WriteHeader(http.StatusNoContent)
}
