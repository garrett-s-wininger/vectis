package api

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"vectis/internal/api/audit"
	"vectis/internal/config"
	"vectis/internal/dal"

	"golang.org/x/crypto/bcrypt"
)

const loginTokenDefaultExpiry = 7 * 24 * time.Hour

// dummyBcryptHash is a valid bcrypt hash used for constant-time login failure paths
// when the username does not exist. It ensures timing does not leak username enumeration.
// Generated with: bcrypt.GenerateFromPassword([]byte("dummy"), bcrypt.DefaultCost)
var dummyBcryptHash = "$2a$10$RgvvFjOSrsWHTjz69BrUGOXOjgsfHXpxy0wLzBRDoIYPRlpTl/Xly"

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token     string     `json:"token"`
	UserID    int64      `json:"user_id"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func (s *APIServer) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIErrorCode(w, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
		return
	}

	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	if !config.APIAuthEnabled() {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrAuthUnavailable)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxLoginBodyBytes+1))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	if len(body) > maxLoginBodyBytes {
		writeAPIErrorCode(w, http.StatusRequestEntityTooLarge, apiErrRequestBodyTooLarge)
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
	expiresAt := time.Now().UTC().Add(loginTokenDefaultExpiry)

	tokenID, err := s.authRepo.CreateAPIToken(ctx, uid, tokenHash, "login", &expiresAt)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error creating login token: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	s.markDBRecovered()

	s.auditLog(r.Context(), audit.EventAuthSuccess, uid, tokenID, map[string]any{
		"method":   "password",
		"username": req.Username,
	})

	resp := loginResponse{
		Token:     plainToken,
		UserID:    uid,
		ExpiresAt: &expiresAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode login response: %v", err)
	}
}
