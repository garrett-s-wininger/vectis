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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	if s.authRepo == nil {
		writeAuthJSON(w, http.StatusServiceUnavailable, authAPIError{
			Error:  AuthJSONUnavailable,
			Detail: "authentication persistence is not available",
		})
		return
	}

	complete, err := s.authRepo.IsSetupComplete(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	writeAuthJSON(w, http.StatusOK, setupStatusResponse{SetupComplete: complete})
}

func (s *APIServer) PostSetupComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !requestContentTypeIsJSON(r) {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	if s.authRepo == nil {
		writeAuthJSON(w, http.StatusServiceUnavailable, authAPIError{
			Error:  AuthJSONUnavailable,
			Detail: "authentication persistence is not available",
		})
		return
	}

	complete, err := s.authRepo.IsSetupComplete(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	if complete {
		writeAuthJSON(w, http.StatusConflict, authAPIError{
			Error:  AuthJSONSetupAlreadyComplete,
			Detail: "initial setup has already been performed",
		})

		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxSetupCompleteBodyBytes+1))
	if err != nil {
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	if len(body) > maxSetupCompleteBodyBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	var req setupCompleteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	expected := strings.TrimSpace(config.APIAuthBootstrapToken())
	if len(expected) < config.MinBootstrapTokenLen {
		writeAuthJSON(w, http.StatusServiceUnavailable, authAPIError{
			Error:  AuthJSONBootstrapNotConfigured,
			Detail: "server is missing a bootstrap token of sufficient length (see api.auth.bootstrap_token)",
		})

		return
	}

	a := strings.TrimSpace(req.BootstrapToken)
	if subtle.ConstantTimeCompare([]byte(a), []byte(expected)) != 1 {
		writeAuthJSON(w, http.StatusUnauthorized, authAPIError{Error: AuthJSONInvalidBootstrapToken})
		return
	}

	username := strings.TrimSpace(req.AdminUsername)
	if len(username) < adminUsernameMinLen {
		http.Error(w, "admin_username is required", http.StatusBadRequest)
		return
	}

	if len(username) > adminUsernameMaxLen || !utf8.ValidString(username) || strings.ContainsAny(username, "\x00\r\n") {
		http.Error(w, "invalid admin_username", http.StatusBadRequest)
		return
	}

	if len(req.AdminPassword) < adminPasswordMinLen {
		http.Error(w, "admin_password must be at least 8 characters", http.StatusBadRequest)
		return
	}

	if len(req.AdminPassword) > adminPasswordMaxLen || !utf8.ValidString(req.AdminPassword) {
		http.Error(w, "invalid admin_password", http.StatusBadRequest)
		return
	}

	passHash, err := bcrypt.GenerateFromPassword([]byte(req.AdminPassword), bcrypt.DefaultCost)
	if err != nil {
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	plainToken, err := randomHexToken(apiTokenRandomBytes)
	if err != nil {
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	tokenHash := hashAPIToken(plainToken)

	_, err = s.authRepo.CompleteInitialSetup(ctx, username, string(passHash), tokenHash, "initial-admin")
	if err != nil {
		if errors.Is(err, dal.ErrSetupAlreadyComplete) {
			writeAuthJSON(w, http.StatusConflict, authAPIError{Error: AuthJSONSetupAlreadyComplete})
			return
		}

		if dal.IsConflict(err) {
			writeAuthJSON(w, http.StatusConflict, authAPIError{Error: AuthJSONUsernameExists})
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	writeAuthJSON(w, http.StatusOK, setupCompleteResponse{
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
