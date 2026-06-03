package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"vectis/internal/api/audit"
	"vectis/internal/cache"
	"vectis/internal/config"
	"vectis/internal/dal"

	"golang.org/x/crypto/bcrypt"
)

// dummyBcryptHash is a valid bcrypt hash used for constant-time login failure paths
// when the username does not exist. It ensures timing does not leak username enumeration.
// Generated with: bcrypt.GenerateFromPassword([]byte("dummy"), bcrypt.DefaultCost)
var dummyBcryptHash = "$2a$10$RgvvFjOSrsWHTjz69BrUGOXOjgsfHXpxy0wLzBRDoIYPRlpTl/Xly"

type loginRequest struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	ReturnToken bool   `json:"return_token,omitempty"`
}

type loginResponse struct {
	Token     string     `json:"token,omitempty"`
	CSRFToken string     `json:"csrf_token,omitempty"`
	UserID    int64      `json:"user_id"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func (s *APIServer) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIErrorCode(w, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
		return
	}

	if !config.APIAuthEnabled() {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrAuthUnavailable)
		return
	}

	body, ok := readRequestBody(w, r, maxLoginBodyBytes)
	if !ok {
		return
	}

	var req loginRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	if req.Username == "" || req.Password == "" {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrMissingCredentials)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	if !s.requireAuthRepo(w) {
		return
	}

	complete, err := s.authRepo.IsSetupComplete(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if !complete {
		writeAPIError(w, http.StatusServiceUnavailable, string(apiErrSetupRequired), "complete initial setup before logging in", nil)
		return
	}

	s.mu.RLock()
	cacheService := s.cacheService
	s.mu.RUnlock()
	if cacheService == nil {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrAuthUnavailable)
		return
	}

	allowed, retryAfter, err := s.allowLoginForUsername(ctx, cacheService, req.Username)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Cache error checking login throttle: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if !allowed {
		s.recordSecurityRejection(r, securityReasonRateLimitExceeded, http.StatusTooManyRequests)
		writeRateLimitExceeded(w, retryAfter)
		return
	}

	uid, passHash, enabled, err := s.authRepo.GetLocalUserByUsername(ctx, req.Username)
	if err != nil {
		if dal.IsNotFound(err) {
			// Constant-time path: perform a dummy bcrypt compare so the timing
			// matches a wrong-password response and does not leak username existence.
			_ = bcrypt.CompareHashAndPassword([]byte(dummyBcryptHash), []byte(req.Password))
			s.auditLog(r.Context(), audit.EventAuthFailure, 0, 0, map[string]any{
				"reason":   "invalid_credentials",
				"username": req.Username,
			})

			writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error looking up user: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if !enabled {
		// Constant-time path: perform bcrypt compare even for disabled users.
		_ = bcrypt.CompareHashAndPassword([]byte(passHash), []byte(req.Password))
		s.auditLog(r.Context(), audit.EventAuthFailure, uid, 0, map[string]any{
			"reason":   "user_disabled",
			"username": req.Username,
		})

		writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(passHash), []byte(req.Password)); err != nil {
		s.auditLog(r.Context(), audit.EventAuthFailure, uid, 0, map[string]any{
			"reason":   "invalid_credentials",
			"username": req.Username,
		})

		writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
		return
	}

	plainToken, err := randomHexToken(apiTokenRandomBytes)
	if err != nil {
		s.logger.Error("Failed to generate login token: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	tokenHash := hashAPIToken(plainToken)
	csrfToken, err := randomHexToken(apiTokenRandomBytes)
	if err != nil {
		s.logger.Error("Failed to generate login csrf token: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	expiresAt := time.Now().UTC().Add(config.APISessionTTL())

	if err := cacheService.CreateSession(ctx, cache.Session{
		TokenHash:     tokenHash,
		CSRFTokenHash: hashAPIToken(csrfToken),
		LocalUserID:   uid,
		ExpiresAt:     expiresAt,
	}); err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Cache error creating login session: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	s.markDBRecovered()

	s.auditLog(r.Context(), audit.EventAuthSuccess, uid, 0, map[string]any{
		"method":          "password",
		"username":        req.Username,
		"credential_type": "session",
	})

	resp := loginResponse{
		CSRFToken: csrfToken,
		UserID:    uid,
		ExpiresAt: &expiresAt,
	}

	if req.ReturnToken {
		resp.Token = plainToken
	}

	setSessionCookies(w, r, plainToken, csrfToken, expiresAt)
	setNoStore(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode login response: %v", err)
	}
}

func (s *APIServer) allowLoginForUsername(ctx context.Context, cacheService cache.Service, username string) (bool, time.Duration, error) {
	keyMaterial := strings.ToLower(strings.TrimSpace(username))
	if keyMaterial == "" {
		keyMaterial = "<empty>"
	}

	decision, err := cacheService.TakeRateLimitToken(ctx, "login:user:"+hashAPIToken(keyMaterial), cache.RateLimitRule{
		RefillRate: config.RateLimitAuthRefillRate(),
		BurstSize:  config.RateLimitAuthBurstSize(),
	})

	if err != nil {
		return false, 0, err
	}

	return decision.Allowed, decision.RetryAfter, nil
}

func (s *APIServer) Logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIErrorCode(w, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
		return
	}

	raw, _, ok := requestCredential(r)
	if !ok || len(raw) > maxBearerTokenBytes {
		writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
		return
	}

	s.mu.RLock()
	cacheService := s.cacheService
	s.mu.RUnlock()

	if cacheService == nil {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrAuthUnavailable)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	if err := cacheService.DeleteSession(ctx, hashAPIToken(raw)); err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Cache error deleting login session: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	clearSessionCookies(w, r)
	w.WriteHeader(http.StatusNoContent)
}
