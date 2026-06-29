package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"vectis/internal/api/audit"
	"vectis/internal/api/authz"
	"vectis/internal/dal"
)

type createBindingRequest struct {
	LocalUserID int64  `json:"local_user_id"`
	Role        string `json:"role"`
}

type bindingResponse struct {
	LocalUserID int64  `json:"local_user_id"`
	Username    string `json:"username,omitempty"`
	Role        string `json:"role"`
}

func (s *APIServer) CreateBinding(w http.ResponseWriter, r *http.Request) {
	nsIDStr := r.PathValue("id")
	nsID, err := strconv.ParseInt(nsIDStr, 10, 64)
	if err != nil || nsID <= 0 {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidNamespaceID)
		return
	}

	body, ok := readRequestBody(w, r, maxJSONDocumentBodyBytes)
	if !ok {
		return
	}

	var req createBindingRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	if req.LocalUserID <= 0 {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrMissingLocalUserID)
		return
	}

	if req.Role == "" {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrMissingRole)
		return
	}

	switch req.Role {
	case authz.RoleViewer, authz.RoleTrigger, authz.RoleOperator, authz.RoleAdmin:
		// ok
	default:
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRole)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireRoleBindings(w) || !s.requireAuthRepo(w) {
		return
	}

	ns, err := s.namespaces.GetByID(ctx, nsID)
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

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, ns.Path) {
		return
	}

	userExists, err := s.authRepo.UserExists(ctx, req.LocalUserID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	if !userExists {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrUserNotFound)
		return
	}

	_, err = s.roleBindings.Create(ctx, req.LocalUserID, nsID, req.Role)
	if err != nil {
		if dal.IsConflict(err) {
			writeAPIErrorCode(w, http.StatusConflict, apiErrBindingAlreadyExists)
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

	s.auditLog(ctx, audit.EventBindingCreated, actorID, req.LocalUserID, map[string]any{
		"namespace_id": nsID,
		"role":         req.Role,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(bindingResponse{
		LocalUserID: req.LocalUserID,
		Role:        req.Role,
	}); err != nil {
		s.logger.Error("Failed to encode binding: %v", err)
	}
}

func (s *APIServer) ListBindings(w http.ResponseWriter, r *http.Request) {
	nsIDStr := r.PathValue("id")
	nsID, err := strconv.ParseInt(nsIDStr, 10, 64)
	if err != nil || nsID <= 0 {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidNamespaceID)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireRoleBindings(w) {
		return
	}

	ns, err := s.namespaces.GetByID(ctx, nsID)
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

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, ns.Path) {
		return
	}

	bindings, err := s.roleBindings.ListByNamespace(ctx, nsID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	var resp []bindingResponse
	for _, b := range bindings {
		resp = append(resp, bindingResponse{
			LocalUserID: b.LocalUserID,
			Role:        b.Role,
		})
	}

	if resp == nil {
		resp = []bindingResponse{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode bindings: %v", err)
	}
}

func (s *APIServer) DeleteBinding(w http.ResponseWriter, r *http.Request) {
	nsIDStr := r.PathValue("id")
	nsID, err := strconv.ParseInt(nsIDStr, 10, 64)
	if err != nil || nsID <= 0 {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidNamespaceID)
		return
	}

	userIDStr := r.PathValue("user_id")
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil || userID <= 0 {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidUserID)
		return
	}

	role := r.URL.Query().Get("role")
	if role == "" {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrMissingRole)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireRoleBindings(w) {
		return
	}

	ns, err := s.namespaces.GetByID(ctx, nsID)
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

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, ns.Path) {
		return
	}

	if err := s.roleBindings.Delete(ctx, userID, nsID, role); err != nil {
		if dal.IsNotFound(err) {
			writeAPIErrorCode(w, http.StatusNotFound, apiErrBindingNotFound)
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

	s.auditLog(ctx, audit.EventBindingDeleted, actorID, userID, map[string]any{
		"namespace_id": nsID,
		"role":         role,
	})

	w.WriteHeader(http.StatusNoContent)
}
