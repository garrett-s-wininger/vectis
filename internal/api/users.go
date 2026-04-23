package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxUserBodyBytes+1))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	if len(body) > maxUserBodyBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	var req createUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Username == "" {
		http.Error(w, "username is required", http.StatusBadRequest)
		return
	}

	if len(req.Username) < adminUsernameMinLen || len(req.Username) > adminUsernameMaxLen || !utf8.ValidString(req.Username) || strings.ContainsAny(req.Username, "\x00\r\n") {
		http.Error(w, "invalid username", http.StatusBadRequest)
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
		http.Error(w, "password must be at least 8 characters", http.StatusBadRequest)
		return
	}

	if len(password) > adminPasswordMaxLen || !utf8.ValidString(password) {
		http.Error(w, "invalid password", http.StatusBadRequest)
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

	if p == nil {
		writeAuthJSON(w, http.StatusUnauthorized, authAPIError{Error: AuthJSONAuthenticationRequired})
		return
	}

	if !s.requireAuthRepo(w) {
		return
	}

	id, err := s.authRepo.CreateLocalUser(ctx, req.Username, string(passHash))
	if err != nil {
		if dal.IsConflict(err) {
			http.Error(w, "username already exists", http.StatusConflict)
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

	user, err := s.authRepo.GetLocalUser(ctx, id)
	if err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "user not found", http.StatusNotFound)
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
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxUserBodyBytes+1))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	if len(body) > maxUserBodyBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	var req updateUserRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Enabled == nil {
		http.Error(w, "enabled is required", http.StatusBadRequest)
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

	if !*req.Enabled {
		if id == p.LocalUserID {
			http.Error(w, "cannot disable yourself", http.StatusBadRequest)
			return
		}

		isAdmin, err := s.authRepo.IsUserAdmin(ctx, id)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error checking admin status: %v", err)
			writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
			return
		}

		if isAdmin {
			adminCount, err := s.authRepo.CountEnabledAdmins(ctx)
			if err != nil {
				if s.handleDBUnavailableError(w, err) {
					return
				}

				s.logger.Error("Database error counting admins: %v", err)
				writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
				return
			}

			if adminCount <= 1 {
				http.Error(w, "cannot disable the last admin", http.StatusBadRequest)
				return
			}
		}
	}

	if err := s.authRepo.UpdateLocalUserEnabled(ctx, id, *req.Enabled); err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "user not found", http.StatusNotFound)
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
	w.WriteHeader(http.StatusNoContent)
}

func (s *APIServer) DeleteUser(w http.ResponseWriter, r *http.Request) {
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

	if id == p.LocalUserID {
		http.Error(w, "cannot delete yourself", http.StatusBadRequest)
		return
	}

	if id == 1 {
		http.Error(w, "cannot delete the break-glass admin", http.StatusBadRequest)
		return
	}

	isAdmin, err := s.authRepo.IsUserAdmin(ctx, id)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error checking admin status: %v", err)
		writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
		return
	}

	if isAdmin {
		adminCount, err := s.authRepo.CountEnabledAdmins(ctx)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error counting admins: %v", err)
			writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
			return
		}

		if adminCount <= 1 {
			http.Error(w, "cannot delete the last admin", http.StatusBadRequest)
			return
		}
	}

	if err := s.authRepo.DeleteLocalUser(ctx, id); err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "user not found", http.StatusNotFound)
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
	w.WriteHeader(http.StatusNoContent)
}

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
