package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"vectis/internal/api/audit"
	"vectis/internal/api/authn"
	"vectis/internal/cache"
	"vectis/internal/config"
	"vectis/internal/dal"
	sdkauth "vectis/sdk/auth"

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

type loginPrincipal struct {
	LocalUserID int64
	Username    string
	Method      string
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

	ctx, cancel := s.handlerDBCtx(r.Context())
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

	principal, ok := s.authenticateLogin(ctx, w, r, req)
	if !ok {
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
		LocalUserID:   principal.LocalUserID,
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

	s.auditLog(r.Context(), audit.EventAuthSuccess, principal.LocalUserID, 0, map[string]any{
		"method":          principal.Method,
		"username":        principal.Username,
		"credential_type": "session",
	})

	resp := loginResponse{
		CSRFToken: csrfToken,
		UserID:    principal.LocalUserID,
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

func (s *APIServer) authenticateLogin(ctx context.Context, w http.ResponseWriter, r *http.Request, req loginRequest) (loginPrincipal, bool) {
	uid, passHash, enabled, err := s.authRepo.GetLocalUserByUsername(ctx, req.Username)
	localUserFound := false
	if err == nil {
		localUserFound = true
		if !enabled {
			_ = bcrypt.CompareHashAndPassword([]byte(passHash), []byte(req.Password))
			s.auditLog(r.Context(), audit.EventAuthFailure, uid, 0, map[string]any{
				"reason":   "user_disabled",
				"username": req.Username,
			})

			writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
			return loginPrincipal{}, false
		}

		if err := bcrypt.CompareHashAndPassword([]byte(passHash), []byte(req.Password)); err == nil {
			return loginPrincipal{LocalUserID: uid, Username: req.Username, Method: "password"}, true
		}
	} else if dal.IsNotFound(err) {
		_ = bcrypt.CompareHashAndPassword([]byte(dummyBcryptHash), []byte(req.Password))
	} else {
		if s.handleDBUnavailableError(w, err) {
			return loginPrincipal{}, false
		}

		s.logger.Error("Database error looking up user: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return loginPrincipal{}, false
	}

	s.mu.RLock()
	providers := append([]sdkauth.LoginProvider(nil), s.loginProviders...)
	s.mu.RUnlock()

	if len(providers) > 0 {
		principal, authenticated, handled := s.authenticateExternalLogin(ctx, w, r, req, providers)
		if handled {
			return principal, authenticated
		}
	}

	auditActorID := int64(0)
	if localUserFound {
		auditActorID = uid
	}

	s.auditLog(r.Context(), audit.EventAuthFailure, auditActorID, 0, map[string]any{
		"reason":   "invalid_credentials",
		"username": req.Username,
	})

	writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
	return loginPrincipal{}, false
}

func (s *APIServer) authenticateExternalLogin(ctx context.Context, w http.ResponseWriter, r *http.Request, req loginRequest, providers []sdkauth.LoginProvider) (loginPrincipal, bool, bool) {
	for _, provider := range providers {
		if provider == nil {
			continue
		}

		identity, err := provider.Authenticate(ctx, req.Username, req.Password)
		if err == nil {
			principal, ok := s.localPrincipalForExternalIdentity(ctx, w, r, identity)
			return principal, ok, true
		}

		if errors.Is(err, sdkauth.ErrInvalidCredentials) || errors.Is(err, sdkauth.ErrIdentityNotAllowed) {
			continue
		}

		if s.handleDBUnavailableError(w, err) {
			return loginPrincipal{}, false, true
		}

		if errors.Is(err, sdkauth.ErrUnavailable) {
			s.logger.Error("Login provider unavailable: %v", err)
			writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrAuthUnavailable)
			return loginPrincipal{}, false, true
		}

		s.logger.Error("Login provider failed: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return loginPrincipal{}, false, true
	}

	return loginPrincipal{}, false, false
}

func (s *APIServer) localPrincipalForExternalIdentity(ctx context.Context, w http.ResponseWriter, r *http.Request, identity sdkauth.Identity) (loginPrincipal, bool) {
	username := strings.TrimSpace(identity.Username)
	if !validExternalLoginUsername(username) {
		s.auditLog(r.Context(), audit.EventAuthFailure, 0, 0, map[string]any{
			"reason":   "invalid_external_identity",
			"provider": identity.Provider,
			"subject":  identity.Subject,
		})

		writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
		return loginPrincipal{}, false
	}

	uid, _, enabled, err := s.authRepo.GetLocalUserByUsername(ctx, username)
	if err == nil {
		if !enabled {
			s.auditLog(r.Context(), audit.EventAuthFailure, uid, 0, map[string]any{
				"reason":   "user_disabled",
				"username": username,
				"provider": identity.Provider,
				"subject":  identity.Subject,
			})

			writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
			return loginPrincipal{}, false
		}

		return loginPrincipal{LocalUserID: uid, Username: username, Method: externalLoginMethod(identity.Provider)}, true
	}

	if !dal.IsNotFound(err) {
		if s.handleDBUnavailableError(w, err) {
			return loginPrincipal{}, false
		}

		s.logger.Error("Database error looking up external user: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return loginPrincipal{}, false
	}

	s.mu.RLock()
	autoProvision := s.externalLoginAutoProvision
	s.mu.RUnlock()

	if !autoProvision {
		s.auditLog(r.Context(), audit.EventAuthFailure, 0, 0, map[string]any{
			"reason":   "external_user_not_provisioned",
			"username": username,
			"provider": identity.Provider,
			"subject":  identity.Subject,
		})

		writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
		return loginPrincipal{}, false
	}

	password, err := generateRandomPassword()
	if err != nil {
		s.logger.Error("Failed to generate external user placeholder password: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return loginPrincipal{}, false
	}

	passHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		s.logger.Error("Failed to hash external user placeholder password: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return loginPrincipal{}, false
	}

	uid, err = s.authRepo.CreateLocalUser(ctx, username, string(passHash))
	if err != nil {
		if dal.IsConflict(err) {
			var lookupErr error
			uid, _, enabled, lookupErr = s.authRepo.GetLocalUserByUsername(ctx, username)
			if lookupErr == nil {
				if enabled {
					return loginPrincipal{LocalUserID: uid, Username: username, Method: externalLoginMethod(identity.Provider)}, true
				}

				s.auditLog(r.Context(), audit.EventAuthFailure, uid, 0, map[string]any{
					"reason":   "user_disabled",
					"username": username,
					"provider": identity.Provider,
					"subject":  identity.Subject,
				})

				writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
				return loginPrincipal{}, false
			}

			err = lookupErr
		}

		if s.handleDBUnavailableError(w, err) {
			return loginPrincipal{}, false
		}

		s.logger.Error("Database error provisioning external user: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return loginPrincipal{}, false
	}

	return loginPrincipal{LocalUserID: uid, Username: username, Method: externalLoginMethod(identity.Provider)}, true
}

func validExternalLoginUsername(username string) bool {
	return len(username) >= adminUsernameMinLen &&
		len(username) <= adminUsernameMaxLen &&
		utf8.ValidString(username) &&
		!strings.ContainsAny(username, "\x00\r\n")
}

func externalLoginMethod(provider string) string {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return "external"
	}

	return provider
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

	raw, source, ok := requestCredential(r)
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

	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	if err := cacheService.DeleteSession(ctx, hashAPIToken(raw)); err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Cache error deleting login session: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	actorID := int64(0)
	if p, ok := authn.PrincipalFromContext(r.Context()); ok {
		actorID = p.LocalUserID
	}

	s.auditLog(r.Context(), audit.EventAuthLogout, actorID, 0, map[string]any{
		"credential_source": string(source),
	})

	clearSessionCookies(w, r)
	clearLogoutSiteData(w)
	w.WriteHeader(http.StatusNoContent)
}
