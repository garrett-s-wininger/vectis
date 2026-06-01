package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	uiSessionCookieName  = "vectis_ui_session"
	uiSessionRandomBytes = 32
	defaultUISessionTTL  = 7 * 24 * time.Hour
	maxBFFBodyBytes      = 1 << 20
)

type uiBackend struct {
	apiURL     *url.URL
	httpClient *http.Client
	sessions   *sessionStore
}

type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]uiSession
	now      func() time.Time
}

type uiSession struct {
	APIToken  string
	Username  string
	UserID    int64
	ExpiresAt time.Time
}

type apiLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type apiLoginResponse struct {
	Token     string     `json:"token"`
	UserID    int64      `json:"user_id"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

type apiSetupCompleteRequest struct {
	BootstrapToken string `json:"bootstrap_token"`
	AdminUsername  string `json:"admin_username"`
	AdminPassword  string `json:"admin_password"`
}

type apiSetupCompleteResponse struct {
	APIToken string `json:"api_token"`
	Username string `json:"username"`
}

type apiSetupStatusResponse struct {
	SetupComplete bool `json:"setup_complete"`
	AuthEnabled   bool `json:"auth_enabled"`
}

type uiSessionResponse struct {
	UserID    int64      `json:"user_id,omitempty"`
	Username  string     `json:"username"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func newUIBackend(rawURL string) (*uiBackend, error) {
	target, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, err
	}

	if target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("must include scheme and host")
	}

	return &uiBackend{
		apiURL: target,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		sessions: newSessionStore(),
	}, nil
}

func newSessionStore() *sessionStore {
	return &sessionStore{
		sessions: make(map[string]uiSession),
		now:      time.Now,
	}
}

func (s *sessionStore) create(session uiSession) (string, error) {
	id, err := randomHex(uiSessionRandomBytes)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[id] = session
	return id, nil
}

func (s *sessionStore) get(id string) (uiSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.sessions[id]
	if !ok {
		return uiSession{}, false
	}

	if !session.ExpiresAt.IsZero() && !session.ExpiresAt.After(s.now()) {
		delete(s.sessions, id)
		return uiSession{}, false
	}

	return session, true
}

func (s *sessionStore) delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
}

func (b *uiBackend) apiProxyHandler() http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(b.apiURL)
	if b.httpClient.Transport != nil {
		proxy.Transport = b.httpClient.Transport
	}

	originalDirector := proxy.Director
	proxy.Director = func(r *http.Request) {
		originalDirector(r)
		r.Header.Del("Authorization")

		if session, ok := b.sessionFromRequest(r); ok {
			r.Header.Set("Authorization", "Bearer "+session.APIToken)
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if bffOnlyAPIPath(r.URL.Path) {
			writeBFFError(w, http.StatusNotFound, "bff_route_required", "use the UI session endpoint for this browser flow")
			return
		}

		proxy.ServeHTTP(w, r)
	})
}

func (b *uiBackend) spaGate(next http.Handler) http.Handler {
	return b.spaGateWithDevAssets(next, false)
}

func (b *uiBackend) spaGateWithDevAssets(next http.Handler, devAssets bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isStaticAssetPath(r.URL.Path) || (devAssets && isViteDevRequest(r)) {
			next.ServeHTTP(w, r)
			return
		}

		status, ok := b.setupStatus(w, r)
		if !ok {
			return
		}

		if !status.AuthEnabled {
			next.ServeHTTP(w, r)
			return
		}

		if !status.SetupComplete {
			if r.URL.Path != "/setup" {
				redirectToUIRoute(w, r, "/setup")
				return
			}

			next.ServeHTTP(w, r)
			return
		}

		_, hasSession := b.sessionFromRequest(r)
		if !hasSession {
			if r.URL.Path != "/login" {
				redirectToUIRoute(w, r, "/login")
				return
			}

			next.ServeHTTP(w, r)
			return
		}

		if r.URL.Path == "/login" || r.URL.Path == "/setup" {
			http.Redirect(w, r, safeNextPath(r, "/"), http.StatusFound)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func isStaticAssetPath(path string) bool {
	if strings.HasPrefix(path, "/assets/") {
		return true
	}

	base := path
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		base = base[idx+1:]
	}

	return strings.Contains(base, ".")
}

func isViteDevAssetPath(path string) bool {
	switch {
	case strings.HasPrefix(path, "/@vite/"):
		return true
	case strings.HasPrefix(path, "/@id/"):
		return true
	case path == "/@react-refresh":
		return true
	case strings.HasPrefix(path, "/@fs/"):
		return true
	case strings.HasPrefix(path, "/src/"):
		return true
	case strings.HasPrefix(path, "/node_modules/"):
		return true
	default:
		return false
	}
}

func isViteDevRequest(r *http.Request) bool {
	return isViteDevAssetPath(r.URL.Path) || isWebSocketUpgrade(r)
}

func isWebSocketUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}

	for _, value := range r.Header.Values("Connection") {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "upgrade") {
				return true
			}
		}
	}

	return false
}

func (b *uiBackend) setupStatus(w http.ResponseWriter, r *http.Request) (apiSetupStatusResponse, bool) {
	target := b.apiURL.ResolveReference(&url.URL{Path: "/api/v1/setup/status"})
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target.String(), nil)
	if err != nil {
		writeBFFError(w, http.StatusInternalServerError, "request_create_failed", "failed to create upstream request")
		return apiSetupStatusResponse{}, false
	}

	req.Header.Set("Accept", "application/json")
	resp, err := b.httpClient.Do(req)
	if err != nil {
		writeBFFError(w, http.StatusBadGateway, "api_unreachable", "unable to reach Vectis API")
		return apiSetupStatusResponse{}, false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		copyAPIError(w, resp)
		return apiSetupStatusResponse{}, false
	}

	var status apiSetupStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		writeBFFError(w, http.StatusBadGateway, "invalid_api_response", "invalid API response")
		return apiSetupStatusResponse{}, false
	}

	return status, true
}

func redirectToUIRoute(w http.ResponseWriter, r *http.Request, route string) {
	target := route
	if next := originalPath(r); next != "" && next != route {
		v := url.Values{}
		v.Set("next", next)
		target += "?" + v.Encode()
	}

	http.Redirect(w, r, target, http.StatusFound)
}

func originalPath(r *http.Request) string {
	if r.URL == nil {
		return ""
	}

	u := &url.URL{
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
	}

	return u.String()
}

func safeNextPath(r *http.Request, fallback string) string {
	next := strings.TrimSpace(r.URL.Query().Get("next"))
	if next == "" {
		return fallback
	}

	parsed, err := url.Parse(next)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || !strings.HasPrefix(parsed.Path, "/") {
		return fallback
	}

	if parsed.Path == "/login" || parsed.Path == "/setup" {
		return fallback
	}

	return parsed.String()
}

func bffOnlyAPIPath(path string) bool {
	switch path {
	case "/api/v1/login", "/api/v1/setup/complete":
		return true
	}

	return path == "/api/v1/tokens" || strings.HasPrefix(path, "/api/v1/tokens/")
}

func (b *uiBackend) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeBFFError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	body, err := readBFFBody(r)
	if err != nil {
		writeBFFError(w, http.StatusRequestEntityTooLarge, "request_body_too_large", "request body too large")
		return
	}

	var req apiLoginRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeBFFError(w, http.StatusBadRequest, "invalid_request_body", "invalid request body")
		return
	}

	var apiResp apiLoginResponse
	if ok := b.forwardJSON(w, r, http.MethodPost, "/api/v1/login", body, &apiResp); !ok {
		return
	}

	expiresAt := time.Now().UTC().Add(defaultUISessionTTL)
	if apiResp.ExpiresAt != nil {
		expiresAt = apiResp.ExpiresAt.UTC()
	}

	sessionID, err := b.sessions.create(uiSession{
		APIToken:  apiResp.Token,
		Username:  strings.TrimSpace(req.Username),
		UserID:    apiResp.UserID,
		ExpiresAt: expiresAt,
	})

	if err != nil {
		writeBFFError(w, http.StatusInternalServerError, "session_create_failed", "failed to create session")
		return
	}

	setUISessionCookie(w, r, sessionID, expiresAt)
	writeBFFJSON(w, http.StatusOK, uiSessionResponse{
		UserID:    apiResp.UserID,
		Username:  strings.TrimSpace(req.Username),
		ExpiresAt: &expiresAt,
	})
}

func (b *uiBackend) completeSetup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeBFFError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	body, err := readBFFBody(r)
	if err != nil {
		writeBFFError(w, http.StatusRequestEntityTooLarge, "request_body_too_large", "request body too large")
		return
	}

	var req apiSetupCompleteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeBFFError(w, http.StatusBadRequest, "invalid_request_body", "invalid request body")
		return
	}

	var apiResp apiSetupCompleteResponse
	if ok := b.forwardJSON(w, r, http.MethodPost, "/api/v1/setup/complete", body, &apiResp); !ok {
		return
	}

	expiresAt := time.Now().UTC().Add(defaultUISessionTTL)
	sessionID, err := b.sessions.create(uiSession{
		APIToken:  apiResp.APIToken,
		Username:  apiResp.Username,
		ExpiresAt: expiresAt,
	})

	if err != nil {
		writeBFFError(w, http.StatusInternalServerError, "session_create_failed", "failed to create session")
		return
	}

	setUISessionCookie(w, r, sessionID, expiresAt)
	writeBFFJSON(w, http.StatusOK, uiSessionResponse{
		Username:  apiResp.Username,
		ExpiresAt: &expiresAt,
	})
}

func (b *uiBackend) logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeBFFError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	if cookie, err := r.Cookie(uiSessionCookieName); err == nil {
		b.sessions.delete(cookie.Value)
	}

	clearUISessionCookie(w, r)
	w.WriteHeader(http.StatusNoContent)
}

func (b *uiBackend) sessionFromRequest(r *http.Request) (uiSession, bool) {
	cookie, err := r.Cookie(uiSessionCookieName)
	if err != nil || strings.TrimSpace(cookie.Value) == "" {
		return uiSession{}, false
	}

	return b.sessions.get(cookie.Value)
}

func (b *uiBackend) forwardJSON(w http.ResponseWriter, r *http.Request, method, apiPath string, body []byte, out any) bool {
	target := b.apiURL.ResolveReference(&url.URL{Path: apiPath})
	req, err := http.NewRequestWithContext(r.Context(), method, target.String(), bytes.NewReader(body))
	if err != nil {
		writeBFFError(w, http.StatusInternalServerError, "request_create_failed", "failed to create upstream request")
		return false
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		writeBFFError(w, http.StatusBadGateway, "api_unreachable", "unable to reach Vectis API")
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		copyAPIError(w, resp)
		return false
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		writeBFFError(w, http.StatusBadGateway, "invalid_api_response", "invalid API response")
		return false
	}

	return true
}

func readBFFBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBFFBodyBytes+1))
	if err != nil {
		return nil, err
	}

	if len(body) > maxBFFBodyBytes {
		return nil, fmt.Errorf("request body too large")
	}

	return body, nil
}

func copyAPIError(w http.ResponseWriter, resp *http.Response) {
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(resp.Body, maxBFFBodyBytes))
}

func writeBFFJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeBFFError(w http.ResponseWriter, status int, code, message string) {
	writeBFFJSON(w, status, map[string]string{
		"code":    code,
		"message": message,
	})
}

func setUISessionCookie(w http.ResponseWriter, r *http.Request, sessionID string, expiresAt time.Time) {
	maxAge := int(time.Until(expiresAt).Seconds())
	if maxAge <= 0 {
		maxAge = -1
	}

	http.SetCookie(w, &http.Cookie{
		Name:     uiSessionCookieName,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil,
		Expires:  expiresAt,
		MaxAge:   maxAge,
	})
}

func clearUISessionCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     uiSessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil,
		Expires:  time.Unix(0, 0).UTC(),
		MaxAge:   -1,
	})
}

func randomHex(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
