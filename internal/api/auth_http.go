package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"vectis/internal/api/audit"
	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/cache"
	"vectis/internal/config"
	"vectis/internal/dal"
)

type credentialSource string

const (
	credentialSourceBearer credentialSource = "bearer"
	credentialSourceCookie credentialSource = "cookie"
)

type routeAuthMode int

const (
	routeAuthProtected routeAuthMode = iota
	routeAuthPublic
)

type routeAuthPolicy struct {
	mode   routeAuthMode
	Action authz.Action
}

func (p routeAuthPolicy) normalized() routeAuthPolicy {
	if p.mode == routeAuthProtected && p.Action == "" {
		p.Action = authz.ActionAdmin
	}

	return p
}

func (p routeAuthPolicy) isPublic() bool {
	return p.normalized().mode == routeAuthPublic
}

func (p routeAuthPolicy) validate() error {
	switch p.mode {
	case routeAuthProtected:
		return nil
	case routeAuthPublic:
		if p.Action != "" {
			return fmt.Errorf("public routes must not set an auth action")
		}
		return nil
	default:
		return fmt.Errorf("unknown route auth mode %d", p.mode)
	}
}

func hashAPIToken(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

func bearerToken(h string) (string, bool) {
	const p = "Bearer "
	if len(h) < len(p) || !strings.EqualFold(h[:len(p)], p) {
		return "", false
	}

	t := strings.TrimSpace(h[len(p):])
	if t == "" {
		return "", false
	}

	return t, true
}

func requestCredential(r *http.Request) (string, credentialSource, bool) {
	if raw, ok := bearerToken(r.Header.Get("Authorization")); ok {
		return raw, credentialSourceBearer, true
	}

	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return "", "", false
	}

	raw := strings.TrimSpace(cookie.Value)
	if raw == "" {
		return "", "", false
	}

	return raw, credentialSourceCookie, true
}

func csrfRequired(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return false
	default:
		return true
	}
}

func validCSRFToken(raw, expectedHash string) bool {
	if raw == "" || expectedHash == "" {
		return false
	}

	got := hashAPIToken(raw)
	return subtle.ConstantTimeCompare([]byte(got), []byte(expectedHash)) == 1
}

func validCSRFOrigin(r *http.Request) bool {
	if origin := strings.TrimSpace(r.Header.Get("Origin")); origin != "" {
		return originMatchesRequestHost(origin, r.Host)
	}

	if referer := strings.TrimSpace(r.Header.Get("Referer")); referer != "" {
		return originMatchesRequestHost(referer, r.Host)
	}

	return false
}

func validFetchMetadata(r *http.Request) bool {
	site := strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")))
	switch site {
	case "", "same-origin", "same-site", "none":
		return true
	case "cross-site":
		return false
	default:
		return true
	}
}

func originMatchesRequestHost(rawOrigin, requestHost string) bool {
	u, err := url.Parse(rawOrigin)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}

	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return false
	}

	return canonicalOriginHost(u.Host, u.Scheme) == canonicalOriginHost(requestHost, u.Scheme)
}

func canonicalOriginHost(host, scheme string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return ""
	}

	h, p, err := net.SplitHostPort(host)
	if err == nil {
		h = strings.Trim(strings.ToLower(h), "[]")
		if (scheme == "http" && p == "80") || (scheme == "https" && p == "443") {
			return h
		}

		return net.JoinHostPort(h, p)
	}

	return strings.Trim(host, "[]")
}

func (s *APIServer) accessControlledHandler(policy routeAuthPolicy, next http.Handler) http.Handler {
	if err := policy.validate(); err != nil {
		panic(fmt.Sprintf("invalid route auth policy: %v", err))
	}

	policy = policy.normalized()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Inject request into context for audit logging
		ctx := context.WithValue(r.Context(), httpRequestKey{}, r)
		r = r.WithContext(ctx)

		if policy.isPublic() || !config.APIAuthEnabled() {
			next.ServeHTTP(w, r)
			return
		}

		if s.authRepo == nil {
			writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrAuthUnavailable)
			return
		}

		ctx, cancel := s.handlerDBCtx(r)
		defer cancel()

		complete, err := s.authRepo.IsSetupComplete(ctx)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}

		z := s.effectiveAuthorizer(complete)
		action := policy.Action

		if !complete {
			if z.Allow(ctx, nil, action, authz.Resource{}) {
				next.ServeHTTP(w, r)
				return
			}

			writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrSetupRequired)
			return
		}

		if action == authz.ActionSetupStatus || action == authz.ActionSetupComplete {
			if !z.Allow(ctx, nil, action, authz.Resource{}) {
				writeAPIErrorCode(w, http.StatusForbidden, apiErrAuthorizationDenied)
				return
			}

			next.ServeHTTP(w, r)
			return
		}

		raw, source, ok := requestCredential(r)
		if !ok {
			writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
			return
		}

		if len(raw) > maxBearerTokenBytes {
			writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
			return
		}

		if source == credentialSourceCookie && !validFetchMetadata(r) {
			if csrfRequired(r.Method) {
				s.recordSecurityRejection(r, securityReasonCSRFFetchMetadataBlocked, http.StatusForbidden)
				writeAPIErrorCode(w, http.StatusForbidden, apiErrCSRFOriginForbidden)
			} else {
				s.recordSecurityRejection(r, securityReasonFetchMetadataForbidden, http.StatusForbidden)
				writeAPIErrorCode(w, http.StatusForbidden, apiErrFetchMetadataForbidden)
			}

			return
		}

		tokenKey := hashAPIToken(raw)
		s.mu.RLock()
		cacheService := s.cacheService
		s.mu.RUnlock()

		if cacheService != nil {
			now := time.Now().UTC()
			session, err := cacheService.ResolveSession(ctx, tokenKey, now, config.APISessionIdleTTL())
			if err == nil {
				if source == credentialSourceCookie && csrfRequired(r.Method) {
					if !validCSRFToken(r.Header.Get(csrfHeaderName), session.CSRFTokenHash) {
						s.recordSecurityRejection(r, securityReasonCSRFTokenRequired, http.StatusForbidden)
						writeAPIErrorCode(w, http.StatusForbidden, apiErrCSRFTokenRequired)
						return
					}

					if !validCSRFOrigin(r) {
						s.recordSecurityRejection(r, securityReasonCSRFOriginForbidden, http.StatusForbidden)
						writeAPIErrorCode(w, http.StatusForbidden, apiErrCSRFOriginForbidden)
						return
					}
				}

				s.auditLog(r.Context(), audit.EventAuthSuccess, session.LocalUserID, 0, map[string]any{
					"credential_type":   "session",
					"credential_source": string(source),
				})

				// Best-effort; ignore errors so a metrics/update failure does not block the request.
				_ = cacheService.TouchSession(ctx, tokenKey, now)

				p := &authn.Principal{
					LocalUserID: session.LocalUserID,
					Username:    session.Username,
					Kind:        authn.KindLocalUser,
				}

				if !z.Allow(ctx, p, action, authz.Resource{}) {
					writeAPIErrorCode(w, http.StatusForbidden, apiErrAuthorizationDenied)
					return
				}

				next.ServeHTTP(w, r.WithContext(authn.WithPrincipal(r.Context(), p)))
				return
			}

			if !cache.IsNotFound(err) {
				if s.handleDBUnavailableError(w, err) {
					return
				}

				s.logger.Error("Cache error resolving session: %v", err)
				writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
				return
			}

			if source == credentialSourceCookie {
				s.auditLog(r.Context(), audit.EventAuthFailure, 0, 0, map[string]any{
					"reason":            "invalid_session",
					"credential_source": string(source),
				})

				writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
				return
			}
		}

		if source == credentialSourceCookie {
			writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrAuthUnavailable)
			return
		}

		uid, uname, tokenID, err := s.authRepo.ResolveAPIToken(ctx, tokenKey)
		if err != nil {
			if dal.IsNotFound(err) {
				s.auditLog(r.Context(), audit.EventAuthFailure, 0, 0, map[string]any{
					"reason": "invalid_token",
				})

				writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}

		s.auditLog(r.Context(), audit.EventAuthSuccess, uid, 0, map[string]any{
			"credential_type": "api_token",
			"token_id":        tokenID,
		})

		// Best-effort; ignore errors so a metrics/update failure does not block the request.
		_ = s.authRepo.TouchAPITokenUsed(ctx, tokenKey)

		p := &authn.Principal{
			LocalUserID: uid,
			TokenID:     tokenID,
			Username:    uname,
			Kind:        authn.KindLocalUser,
		}

		// Load token scopes if any exist
		if tokenID > 0 {
			scopes, err := s.authRepo.GetTokenScopes(ctx, tokenID)
			if err != nil {
				if s.handleDBUnavailableError(w, err) {
					return
				}

				s.auditLog(r.Context(), audit.EventAuthFailure, uid, 0, map[string]any{
					"reason":   "token_scope_load_error",
					"token_id": tokenID,
				})

				s.logger.Error("Database error loading token scopes: %v", err)
				writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
				return
			}

			if len(scopes) > 0 {
				p.TokenScopes = make([]authn.TokenScope, len(scopes))
				for i, s := range scopes {
					p.TokenScopes[i] = authn.TokenScope{
						Action:      s.Action,
						NamespaceID: s.NamespaceID.Int64,
						Propagate:   s.Propagate,
					}
				}
			}
		}

		if !z.Allow(ctx, p, action, authz.Resource{}) {
			writeAPIErrorCode(w, http.StatusForbidden, apiErrAuthorizationDenied)
			return
		}

		next.ServeHTTP(w, r.WithContext(authn.WithPrincipal(r.Context(), p)))
	})
}

func (s *APIServer) accessControlMiddleware(next http.Handler) http.Handler {
	return s.accessControlledHandler(routeAuthPolicy{}, next)
}

func (s *APIServer) effectiveAuthorizer(setupComplete bool) authz.Authorizer {
	if s.authzOverride != nil {
		return s.authzOverride
	}

	if !config.APIAuthEnabled() {
		return authz.AllowAll{}
	}

	var namespaces dal.NamespacesRepository
	var roleBindings dal.RoleBindingsRepository
	if s.namespaces != nil {
		namespaces = s.namespaces
	}

	if s.roleBindings != nil {
		roleBindings = s.roleBindings
	}

	return authz.SelectAuthorizer(setupComplete, config.APIAuthzEngine(), namespaces, roleBindings)
}
