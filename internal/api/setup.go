package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"vectis/internal/api/audit"
	"vectis/internal/config"
	"vectis/internal/dal"

	"golang.org/x/crypto/bcrypt"
)

const (
	adminPasswordMinLen = 8
	adminUsernameMinLen = 1
	apiTokenRandomBytes = 32
)

type setupStatusResponse struct {
	SetupComplete bool `json:"setup_complete"`
}

type setupCompleteRequest struct {
	BootstrapToken string `json:"bootstrap_token"`
	AdminUsername  string `json:"admin_username"`
	AdminPassword  string `json:"admin_password"`
}

type setupCompleteResponse struct {
	APIToken string `json:"api_token"`
	Username string `json:"username"`
}

func (s *APIServer) GetSetupStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIErrorCode(w, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	if s.authRepo == nil {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrAuthUnavailable)
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

	writeJSON(w, http.StatusOK, setupStatusResponse{SetupComplete: complete})
}

func (s *APIServer) PostSetupComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIErrorCode(w, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
		return
	}

	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	if s.authRepo == nil {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrAuthUnavailable)
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

	if complete {
		writeAPIErrorCode(w, http.StatusConflict, apiErrSetupAlreadyComplete)

		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxSetupCompleteBodyBytes+1))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if len(body) > maxSetupCompleteBodyBytes {
		writeAPIErrorCode(w, http.StatusRequestEntityTooLarge, apiErrRequestBodyTooLarge)
		return
	}

	var req setupCompleteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	expected := strings.TrimSpace(config.APIAuthBootstrapToken())
	if len(expected) < config.MinBootstrapTokenLen {
		writeAPIError(w, http.StatusServiceUnavailable, string(apiErrBootstrapNotConfigured), apiErrBootstrapNotConfigured.message(), map[string]any{
			"setting": "api.auth.bootstrap_token",
		})

		return
	}

	a := strings.TrimSpace(req.BootstrapToken)
	if subtle.ConstantTimeCompare([]byte(a), []byte(expected)) != 1 {
		_ = s.auditLog(r.Context(), audit.EventSetupBootstrapFailed, 0, 0, map[string]any{
			"reason": "invalid_bootstrap_token",
		})

		writeAPIErrorCode(w, http.StatusUnauthorized, apiErrInvalidBootstrapToken)
		return
	}

	username := strings.TrimSpace(req.AdminUsername)
	if len(username) < adminUsernameMinLen {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrMissingAdminUsername)
		return
	}

	if len(username) > adminUsernameMaxLen || !utf8.ValidString(username) || strings.ContainsAny(username, "\x00\r\n") {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidAdminUsername)
		return
	}

	if len(req.AdminPassword) < adminPasswordMinLen {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrAdminPasswordTooShort)
		return
	}

	if len(req.AdminPassword) > adminPasswordMaxLen || !utf8.ValidString(req.AdminPassword) {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidAdminPassword)
		return
	}

	passHash, err := bcrypt.GenerateFromPassword([]byte(req.AdminPassword), bcrypt.DefaultCost)
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	plainToken, err := randomHexToken(apiTokenRandomBytes)
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	tokenHash := hashAPIToken(plainToken)

	localUserID, err := s.authRepo.CompleteInitialSetup(ctx, username, string(passHash), tokenHash, "initial-admin")
	if err != nil {
		if errors.Is(err, dal.ErrSetupAlreadyComplete) {
			writeAPIErrorCode(w, http.StatusConflict, apiErrSetupAlreadyComplete)
			return
		}

		if dal.IsConflict(err) {
			writeAPIErrorCode(w, http.StatusConflict, apiErrUsernameAlreadyExists)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	_ = s.auditLog(ctx, audit.EventSetupCompleted, localUserID, localUserID, map[string]any{
		"username": username,
	})

	writeJSON(w, http.StatusOK, setupCompleteResponse{
		APIToken: plainToken,
		Username: username,
	})
}

func randomHexToken(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
