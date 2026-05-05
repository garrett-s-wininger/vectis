package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"vectis/internal/api/audit"
	"vectis/internal/api/authz"
	"vectis/internal/dal"

	"golang.org/x/crypto/bcrypt"
)

const (
	generatedPasswordBytes = 32
)

type createUserRequest struct {
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

type updateUserRequest struct {
	Enabled *bool `json:"enabled"`
}

type userResponse struct {
	ID        int64     `json:"id"`
	Username  string    `json:"username"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

type createUserResponse struct {
	ID              int64     `json:"id"`
	Username        string    `json:"username"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"created_at"`
	InitialPassword string    `json:"initial_password,omitempty"`
}

func localUserRecordToResponse(rec *dal.LocalUserRecord) userResponse {
	resp := userResponse{
		ID:       rec.ID,
		Username: rec.Username,
		Enabled:  rec.Enabled,
	}

	if rec.CreatedAt.Valid {
		resp.CreatedAt = rec.CreatedAt.Time
	}

	return resp
}

func generateRandomPassword() (string, error) {
	b := make([]byte, generatedPasswordBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}

func (s *APIServer) CreateUser(w http.ResponseWriter, r *http.Request) {
	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxUserBodyBytes+1))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	if len(body) > maxUserBodyBytes {
		writeAPIErrorCode(w, http.StatusRequestEntityTooLarge, apiErrRequestBodyTooLarge)
		return
	}

	var req createUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	req.Username = strings.TrimSpace(req.Username)

	if req.Username == "" {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrMissingUsername)
		return
	}

	if len(req.Username) < adminUsernameMinLen || len(req.Username) > adminUsernameMaxLen || !utf8.ValidString(req.Username) || strings.ContainsAny(req.Username, "\x00\r\n") {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidUsername)
		return
	}

	password := req.Password
	generated := false
	if password == "" {
		var err error
		password, err = generateRandomPassword()
		if err != nil {
			s.logger.Error("Failed to generate password: %v", err)
			writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
			return
		}

		generated = true
	}

	if len(password) < adminPasswordMinLen {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrPasswordTooShort)
		return
	}

	if len(password) > adminPasswordMaxLen || !utf8.ValidString(password) {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidPassword)
		return
	}

	passHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		s.logger.Error("Failed to hash password: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireAuthRepo(w) {
		return
	}

	id, err := s.authRepo.CreateLocalUser(ctx, req.Username, string(passHash))
	if err != nil {
		if dal.IsConflict(err) {
			writeAPIErrorCode(w, http.StatusConflict, apiErrUsernameAlreadyExists)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error creating user: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventUserCreated, actorID, id, map[string]any{
		"username":           req.Username,
		"generated_password": generated,
	})

	resp := createUserResponse{
		ID:        id,
		Username:  req.Username,
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	}
	if generated {
		resp.InitialPassword = password
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode user response: %v", err)
	}
}

func (s *APIServer) ListUsers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	_, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireAuthRepo(w) {
		return
	}

	users, err := s.authRepo.ListLocalUsers(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error listing users: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	s.markDBRecovered()

	resp := make([]userResponse, len(users))
	for i, u := range users {
		resp[i] = localUserRecordToResponse(u)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode users: %v", err)
	}
}

func (s *APIServer) GetUser(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidID)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	_, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireAuthRepo(w) {
		return
	}

	user, err := s.authRepo.GetLocalUser(ctx, id)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIErrorCode(w, http.StatusNotFound, apiErrUserNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error getting user: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	s.markDBRecovered()

	resp := localUserRecordToResponse(user)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode user: %v", err)
	}
}

func (s *APIServer) UpdateUser(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidID)
		return
	}

	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxUserBodyBytes+1))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	if len(body) > maxUserBodyBytes {
		writeAPIErrorCode(w, http.StatusRequestEntityTooLarge, apiErrRequestBodyTooLarge)
		return
	}

	var req updateUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	if req.Enabled == nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrMissingEnabled)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireAuthRepo(w) {
		return
	}

	if !*req.Enabled {
		if p != nil && id == p.LocalUserID {
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrSelfDisableForbidden)
			return
		}

		isAdmin, err := s.authRepo.IsUserRootAdmin(ctx, id)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error checking admin status: %v", err)
			writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
			return
		}

		if isAdmin {
			adminCount, err := s.authRepo.CountEnabledRootAdmins(ctx)
			if err != nil {
				if s.handleDBUnavailableError(w, err) {
					return
				}

				s.logger.Error("Database error counting admins: %v", err)
				writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
				return
			}

			if adminCount <= 1 {
				writeAPIErrorCode(w, http.StatusBadRequest, apiErrLastAdminDisableForbidden)
				return
			}
		}
	}

	if err := s.authRepo.UpdateLocalUserEnabled(ctx, id, *req.Enabled); err != nil {
		if dal.IsNotFound(err) {
			writeAPIErrorCode(w, http.StatusNotFound, apiErrUserNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error updating user: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventUserUpdated, actorID, id, map[string]any{
		"enabled": *req.Enabled,
	})

	w.WriteHeader(http.StatusNoContent)
}

func (s *APIServer) DeleteUser(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidID)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireAuthRepo(w) {
		return
	}

	if p != nil && id == p.LocalUserID {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrSelfDeleteForbidden)
		return
	}

	isAdmin, err := s.authRepo.IsUserRootAdmin(ctx, id)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error checking admin status: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	if isAdmin {
		adminCount, err := s.authRepo.CountEnabledRootAdmins(ctx)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error counting admins: %v", err)
			writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
			return
		}

		if adminCount <= 1 {
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrLastAdminDeleteForbidden)
			return
		}
	}

	if err := s.authRepo.DeleteLocalUser(ctx, id); err != nil {
		if dal.IsNotFound(err) {
			writeAPIErrorCode(w, http.StatusNotFound, apiErrUserNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error deleting user: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventUserDeleted, actorID, id, nil)
	w.WriteHeader(http.StatusNoContent)
}

type changePasswordRequest struct {
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
	UserID          *int64 `json:"user_id,omitempty"`
}

func (s *APIServer) ChangePassword(w http.ResponseWriter, r *http.Request) {
	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxChangePasswordBodyBytes+1))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}
	if len(body) > maxChangePasswordBodyBytes {
		writeAPIErrorCode(w, http.StatusRequestEntityTooLarge, apiErrRequestBodyTooLarge)
		return
	}

	var req changePasswordRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	if req.NewPassword == "" {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrMissingNewPassword)
		return
	}
	if len(req.NewPassword) < adminPasswordMinLen {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrNewPasswordTooShort)
		return
	}
	if len(req.NewPassword) > adminPasswordMaxLen || !utf8.ValidString(req.NewPassword) {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidNewPassword)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}
	if !s.requireAuthRepo(w) {
		return
	}

	targetUserID := int64(0)
	if p != nil {
		targetUserID = p.LocalUserID
	}

	isAdmin := false

	if req.UserID != nil {
		if p == nil || *req.UserID != p.LocalUserID {
			if !s.authorizeAction(ctx, w, p, authz.ActionUserAdmin, authz.Resource{}) {
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
				writeAPIErrorCode(w, http.StatusBadRequest, apiErrUserNotFoundOrDisabled)
				return
			}

			isAdmin = true
		}
		targetUserID = *req.UserID
	}

	if !isAdmin {
		if req.CurrentPassword == "" {
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrMissingCurrentPassword)
			return
		}

		currentHash, err := s.authRepo.GetUserPasswordHash(ctx, targetUserID)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAPIErrorCode(w, http.StatusNotFound, apiErrUserNotFound)
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
			writeAPIErrorCode(w, http.StatusNotFound, apiErrUserNotFound)
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

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventPasswordChanged, actorID, targetUserID, map[string]any{
		"admin_override": isAdmin,
	})
	w.WriteHeader(http.StatusNoContent)
}
