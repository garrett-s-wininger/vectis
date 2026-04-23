package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/dal"
)

const maxTokenBodyBytes = 4096

var tokenExpiryPresets = map[string]time.Duration{
	"1w":    7 * 24 * time.Hour,
	"1m":    30 * 24 * time.Hour,
	"3m":    90 * 24 * time.Hour,
	"6m":    180 * 24 * time.Hour,
	"1y":    365 * 24 * time.Hour,
	"never": 0,
}

type tokenScopeRequest struct {
	Action        string `json:"action"`
	NamespacePath string `json:"namespace_path,omitempty"`
	Propagate     bool   `json:"propagate,omitempty"`
}

type createTokenRequest struct {
	Label     string               `json:"label"`
	ExpiresIn string               `json:"expires_in"`
	UserID    *int64               `json:"user_id,omitempty"`
	Scopes    []*tokenScopeRequest `json:"scopes,omitempty"`
}

type tokenResponse struct {
	ID         int64      `json:"id"`
	Label      string     `json:"label"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

type createTokenResponse struct {
	ID        int64      `json:"id"`
	Token     string     `json:"token"`
	Label     string     `json:"label"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

func apiTokenRecordToResponse(rec *dal.APITokenRecord) tokenResponse {
	resp := tokenResponse{
		ID:    rec.ID,
		Label: rec.Label,
	}

	if rec.CreatedAt.Valid {
		resp.CreatedAt = rec.CreatedAt.Time
	}

	if rec.ExpiresAt.Valid {
		resp.ExpiresAt = &rec.ExpiresAt.Time
	}

	if rec.LastUsedAt.Valid {
		resp.LastUsedAt = &rec.LastUsedAt.Time
	}

	return resp
}

func (s *APIServer) isAdminAnywhere(ctx context.Context, principal *authn.Principal) bool {
	if s.roleBindings == nil {
		return false
	}

	bindings, err := s.roleBindings.ListByUser(ctx, principal.LocalUserID)
	if err != nil {
		return false
	}

	for _, b := range bindings {
		if b.Role == authz.RoleAdmin {
			return true
		}
	}

	return false
}

func (s *APIServer) ListTokens(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if p == nil {
		writeAuthJSON(w, http.StatusUnauthorized, authAPIError{Error: AuthJSONAuthenticationRequired})
		return
	}

	if !s.requireAuthRepo(w) {
		return
	}

	targetUserID := p.LocalUserID
	if userIDStr := r.URL.Query().Get("user_id"); userIDStr != "" {
		uid, err := strconv.ParseInt(userIDStr, 10, 64)
		if err != nil || uid <= 0 {
			http.Error(w, "invalid user_id", http.StatusBadRequest)
			return
		}

		if uid != p.LocalUserID {
			if !s.isAdminAnywhere(ctx, p) {
				writeAuthJSON(w, http.StatusForbidden, authAPIError{Error: AuthJSONAuthorizationDenied})
				return
			}

			enabled, err := s.authRepo.UserEnabled(ctx, uid)
			if err != nil {
				if s.handleDBUnavailableError(w, err) {
					return
				}

				s.logger.Error("Database error checking user enabled status: %v", err)
				writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
				return
			}

			if !enabled {
				http.Error(w, "user not found or disabled", http.StatusBadRequest)
				return
			}
		}

		targetUserID = uid
	}

	tokens, err := s.authRepo.ListAPITokens(ctx, targetUserID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error listing tokens: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})

		return
	}

	s.markDBRecovered()

	resp := make([]tokenResponse, len(tokens))
	for i, t := range tokens {
		resp[i] = apiTokenRecordToResponse(t)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode tokens: %v", err)
	}
}

func (s *APIServer) CreateToken(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxTokenBodyBytes+1))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	if len(body) > maxTokenBodyBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	var req createTokenRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Label == "" {
		http.Error(w, "label is required", http.StatusBadRequest)
		return
	}

	duration, ok := tokenExpiryPresets[req.ExpiresIn]
	if !ok {
		http.Error(w, "invalid expires_in", http.StatusBadRequest)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if p == nil {
		writeAuthJSON(w, http.StatusUnauthorized, authAPIError{Error: AuthJSONAuthenticationRequired})
		return
	}

	if !s.requireAuthRepo(w) {
		return
	}

	targetUserID := p.LocalUserID
	if req.UserID != nil {
		if *req.UserID != p.LocalUserID {
			if !s.isAdminAnywhere(ctx, p) {
				writeAuthJSON(w, http.StatusForbidden, authAPIError{Error: AuthJSONAuthorizationDenied})
				return
			}

			enabled, err := s.authRepo.UserEnabled(ctx, *req.UserID)
			if err != nil {
				if s.handleDBUnavailableError(w, err) {
					return
				}

				s.logger.Error("Database error checking user enabled status: %v", err)
				writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})

				return
			}

			if !enabled {
				http.Error(w, "user not found or disabled", http.StatusBadRequest)
				return
			}
		}

		targetUserID = *req.UserID
	}

	plainToken, err := randomHexToken(apiTokenRandomBytes)
	if err != nil {
		s.logger.Error("Failed to generate token: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	tokenHash := hashAPIToken(plainToken)

	var expiresAt *time.Time
	if duration > 0 {
		t := time.Now().UTC().Add(duration)
		expiresAt = &t
	}

	var id int64
	if len(req.Scopes) > 0 {
		if s.namespaces == nil {
			http.Error(w, "namespace repository unavailable", http.StatusServiceUnavailable)
			return
		}

		dalScopes := make([]*dal.TokenScopeRecord, len(req.Scopes))
		for i, scopeReq := range req.Scopes {
			scope := &dal.TokenScopeRecord{
				Action:    scopeReq.Action,
				Propagate: scopeReq.Propagate,
			}

			if scopeReq.NamespacePath != "" {
				ns, err := s.namespaces.GetByPath(ctx, scopeReq.NamespacePath)
				if err != nil {
					if dal.IsNotFound(err) {
						http.Error(w, "namespace not found: "+scopeReq.NamespacePath, http.StatusBadRequest)
						return
					}

					if s.handleDBUnavailableError(w, err) {
						return
					}

					s.logger.Error("Database error resolving namespace: %v", err)
					writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
					return
				}

				scope.NamespaceID = sql.NullInt64{Int64: ns.ID, Valid: true}
			}

			dalScopes[i] = scope
		}

		id, err = s.authRepo.CreateAPITokenWithScopes(ctx, targetUserID, tokenHash, req.Label, expiresAt, dalScopes)
	} else {
		id, err = s.authRepo.CreateAPIToken(ctx, targetUserID, tokenHash, req.Label, expiresAt)
	}

	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error creating token: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	s.markDBRecovered()

	resp := createTokenResponse{
		ID:        id,
		Token:     plainToken,
		Label:     req.Label,
		CreatedAt: time.Now().UTC(),
	}

	if expiresAt != nil {
		resp.ExpiresAt = expiresAt
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode token response: %v", err)
	}
}

func (s *APIServer) DeleteToken(w http.ResponseWriter, r *http.Request) {
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

	if p == nil {
		writeAuthJSON(w, http.StatusUnauthorized, authAPIError{Error: AuthJSONAuthenticationRequired})
		return
	}

	if !s.requireAuthRepo(w) {
		return
	}

	ownerID, err := s.authRepo.GetAPITokenOwner(ctx, id)
	if err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "token not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error getting token owner: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	if ownerID != p.LocalUserID {
		if !s.isAdminAnywhere(ctx, p) {
			writeAuthJSON(w, http.StatusForbidden, authAPIError{Error: AuthJSONAuthorizationDenied})
			return
		}
	}

	if err := s.authRepo.DeleteAPIToken(ctx, id); err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "token not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error deleting token: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	s.markDBRecovered()
	w.WriteHeader(http.StatusNoContent)
}
