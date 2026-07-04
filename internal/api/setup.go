package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"

	"vectis/internal/api/audit"
	"vectis/internal/config"
	"vectis/internal/dal"
)

const (
	adminPasswordMinLen = 8
	adminUsernameMinLen = 1
	apiTokenRandomBytes = 32
)

type setupStatusResponse struct {
	SetupComplete bool `json:"setup_complete"`
	AuthEnabled   bool `json:"auth_enabled"`
}

type setupCompleteRequest struct {
	BootstrapToken      string                        `json:"bootstrap_token"`
	AdminUsername       string                        `json:"admin_username"`
	AdminPassword       string                        `json:"admin_password,omitempty"`
	PasswordAuthEnabled *bool                         `json:"password_auth_enabled,omitempty"`
	ExternalIdentity    *setupExternalIdentityRequest `json:"external_identity,omitempty"`
}

type setupExternalIdentityRequest struct {
	ProviderID  string `json:"provider_id"`
	Subject     string `json:"subject"`
	Username    string `json:"username,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}

type setupCompleteResponse struct {
	APIToken            string                    `json:"api_token"`
	Username            string                    `json:"username"`
	PasswordAuthEnabled bool                      `json:"password_auth_enabled"`
	ExternalIdentity    *externalIdentityResponse `json:"external_identity,omitempty"`
}

func (s *APIServer) GetSetupStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIErrorCode(w, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
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

	writeJSON(w, http.StatusOK, setupStatusResponse{
		SetupComplete: complete,
		AuthEnabled:   config.APIAuthEnabled(),
	})
}

func (s *APIServer) PostSetupComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIErrorCode(w, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
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

	body, ok := readRequestBody(w, r, maxSetupCompleteBodyBytes)
	if !ok {
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
		s.auditLog(r.Context(), audit.EventSetupBootstrapFailed, 0, 0, map[string]any{
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

	passwordAuthEnabled := true
	if req.PasswordAuthEnabled != nil {
		passwordAuthEnabled = *req.PasswordAuthEnabled
	}

	var setupIdentity *dal.InitialSetupExternalIdentity
	if req.ExternalIdentity != nil {
		req.ExternalIdentity.ProviderID = strings.TrimSpace(req.ExternalIdentity.ProviderID)
		req.ExternalIdentity.Subject = strings.TrimSpace(req.ExternalIdentity.Subject)
		req.ExternalIdentity.Username = strings.TrimSpace(req.ExternalIdentity.Username)
		req.ExternalIdentity.DisplayName = strings.TrimSpace(req.ExternalIdentity.DisplayName)
		if req.ExternalIdentity.Username == "" {
			req.ExternalIdentity.Username = username
		}

		if !validExternalProviderID(req.ExternalIdentity.ProviderID) ||
			!validExternalSubject(req.ExternalIdentity.Subject) ||
			!validExternalLoginUsername(req.ExternalIdentity.Username) {
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidExternalIdentity)
			return
		}

		setupIdentity = &dal.InitialSetupExternalIdentity{
			ProviderID:  req.ExternalIdentity.ProviderID,
			Subject:     req.ExternalIdentity.Subject,
			Username:    req.ExternalIdentity.Username,
			DisplayName: req.ExternalIdentity.DisplayName,
		}
	}

	if !passwordAuthEnabled && setupIdentity == nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidExternalIdentity)
		return
	}

	password := req.AdminPassword
	if password == "" && !passwordAuthEnabled {
		var err error
		password, err = generateRandomPassword()
		if err != nil {
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}
	}

	if len(password) < adminPasswordMinLen {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrAdminPasswordTooShort)
		return
	}

	if len(password) > adminPasswordMaxLen || !utf8.ValidString(password) {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidAdminPassword)
		return
	}

	passHash, err := generatePasswordHash(password)
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

	localUserID, err := s.authRepo.CompleteInitialSetupWithOptions(ctx, dal.CompleteInitialSetupOptions{
		Username:            username,
		PasswordHash:        string(passHash),
		PasswordAuthEnabled: passwordAuthEnabled,
		TokenHash:           tokenHash,
		TokenLabel:          "initial-admin",
		ExternalIdentity:    setupIdentity,
	})

	if err != nil {
		if errors.Is(err, dal.ErrSetupAlreadyComplete) {
			writeAPIErrorCode(w, http.StatusConflict, apiErrSetupAlreadyComplete)
			return
		}

		if dal.IsConflict(err) {
			if setupIdentity != nil {
				writeAPIErrorCode(w, http.StatusConflict, apiErrExternalIdentityAlreadyExists)
			} else {
				writeAPIErrorCode(w, http.StatusConflict, apiErrUsernameAlreadyExists)
			}

			return
		}

		if dal.IsNotFound(err) && setupIdentity != nil {
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrAuthProviderNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if !s.auditLogOrFail(w, ctx, audit.EventSetupCompleted, localUserID, localUserID, map[string]any{
		"username":              username,
		"password_auth_enabled": passwordAuthEnabled,
	}) {
		return
	}

	var externalIdentity *externalIdentityResponse
	if setupIdentity != nil {
		if identity, err := s.authRepo.GetExternalIdentity(ctx, setupIdentity.ProviderID, setupIdentity.Subject); err == nil {
			resp := externalIdentityRecordToResponse(identity)
			externalIdentity = &resp
		} else {
			s.logger.Error("Database error loading setup external identity: %v", err)
		}
	}

	setNoStore(w)
	writeJSON(w, http.StatusOK, setupCompleteResponse{
		APIToken:            plainToken,
		Username:            username,
		PasswordAuthEnabled: passwordAuthEnabled,
		ExternalIdentity:    externalIdentity,
	})
}

func randomHexToken(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
