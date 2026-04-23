package api

import (
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"

	"vectis/internal/dal"

	"golang.org/x/crypto/bcrypt"
)

type changePasswordRequest struct {
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
	UserID          *int64 `json:"user_id,omitempty"`
}

func (s *APIServer) ChangePassword(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxChangePasswordBodyBytes+1))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	if len(body) > maxChangePasswordBodyBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	var req changePasswordRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.NewPassword == "" {
		http.Error(w, "new_password is required", http.StatusBadRequest)
		return
	}

	if len(req.NewPassword) < adminPasswordMinLen {
		http.Error(w, "new_password must be at least 8 characters", http.StatusBadRequest)
		return
	}

	if len(req.NewPassword) > adminPasswordMaxLen || !utf8.ValidString(req.NewPassword) {
		http.Error(w, "invalid new_password", http.StatusBadRequest)
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
	isAdmin := false

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

			isAdmin = true
		}

		targetUserID = *req.UserID
	}

	if !isAdmin {
		if req.CurrentPassword == "" {
			http.Error(w, "current_password is required", http.StatusBadRequest)
			return
		}

		currentHash, err := s.authRepo.GetUserPasswordHash(ctx, targetUserID)
		if err != nil {
			if dal.IsNotFound(err) {
				http.Error(w, "user not found", http.StatusNotFound)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error getting password hash: %v", err)
			writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
			return
		}

		if err := bcrypt.CompareHashAndPassword([]byte(currentHash), []byte(req.CurrentPassword)); err != nil {
			writeAuthJSON(w, http.StatusUnauthorized, authAPIError{Error: AuthJSONInvalidPassword})
			return
		}
	}

	newHash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		s.logger.Error("Failed to hash password: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	if err := s.authRepo.ChangePasswordAndRevokeTokens(ctx, targetUserID, string(newHash)); err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "user not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error changing password: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	s.markDBRecovered()
	w.WriteHeader(http.StatusNoContent)
}
