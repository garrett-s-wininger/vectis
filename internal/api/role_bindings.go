package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

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
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	nsIDStr := r.PathValue("id")
	nsID, err := strconv.ParseInt(nsIDStr, 10, 64)
	if err != nil || nsID <= 0 {
		http.Error(w, "invalid namespace id", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	var req createBindingRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.LocalUserID <= 0 {
		http.Error(w, "local_user_id is required", http.StatusBadRequest)
		return
	}

	if req.Role == "" {
		http.Error(w, "role is required", http.StatusBadRequest)
		return
	}

	switch req.Role {
	case authz.RoleViewer, authz.RoleTrigger, authz.RoleOperator, authz.RoleAdmin:
		// ok
	default:
		http.Error(w, "invalid role", http.StatusBadRequest)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
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

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, ns.Path) {
		return
	}

	userExists, err := s.authRepo.UserExists(ctx, req.LocalUserID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if !userExists {
		http.Error(w, "user not found", http.StatusBadRequest)
		return
	}

	_, err = s.roleBindings.Create(ctx, req.LocalUserID, nsID, req.Role)
	if err != nil {
		if dal.IsConflict(err) {
			http.Error(w, "binding already exists", http.StatusConflict)
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
		http.Error(w, "invalid namespace id", http.StatusBadRequest)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
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

	if !s.authorizeNamespace(ctx, w, p, authz.ActionJobRead, ns.Path) {
		return
	}

	bindings, err := s.roleBindings.ListByNamespace(ctx, nsID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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
		http.Error(w, "invalid namespace id", http.StatusBadRequest)
		return
	}

	userIDStr := r.PathValue("user_id")
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil || userID <= 0 {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}

	role := r.URL.Query().Get("role")
	if role == "" {
		http.Error(w, "role query parameter is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
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

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, ns.Path) {
		return
	}

	if err := s.roleBindings.Delete(ctx, userID, nsID, role); err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "binding not found", http.StatusNotFound)
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
