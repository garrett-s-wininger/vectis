package api

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"

	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/config"
	"vectis/internal/dal"
)

func isAuthExcludedPath(path string) bool {
	switch path {
	case "/metrics", "/health/live", "/health/ready":
		return true
	default:
		return false
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

func (s *APIServer) accessControlMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !config.APIAuthEnabled() {
			next.ServeHTTP(w, r)
			return
		}

		if isAuthExcludedPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		if s.authRepo == nil {
			writeAuthJSON(w, http.StatusServiceUnavailable, authAPIError{
				Error:  AuthJSONUnavailable,
				Detail: "authentication persistence is not available",
			})
			return
		}

		ctx, cancel := s.handlerDBCtx(r)
		defer cancel()

		complete, err := s.authRepo.IsSetupComplete(ctx)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
			return
		}

		z := s.effectiveAuthorizer(complete)
		action := authz.ActionForRequest(r)

		if !complete {
			if z.Allow(ctx, nil, action, authz.Resource{}) {
				next.ServeHTTP(w, r)
				return
			}

			writeAuthJSON(w, http.StatusServiceUnavailable, authAPIError{
				Error:  AuthJSONSetupRequired,
				Detail: "complete initial setup before using the API",
			})
			return
		}

		if action == authz.ActionSetupStatus || action == authz.ActionSetupComplete {
			if !z.Allow(ctx, nil, action, authz.Resource{}) {
				writeAuthJSON(w, http.StatusForbidden, authAPIError{Error: AuthJSONAuthorizationDenied})
				return
			}

			next.ServeHTTP(w, r)
			return
		}

		raw, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			writeAuthJSON(w, http.StatusUnauthorized, authAPIError{Error: AuthJSONAuthenticationRequired})
			return
		}

		if len(raw) > maxBearerTokenBytes {
			writeAuthJSON(w, http.StatusUnauthorized, authAPIError{Error: AuthJSONAuthenticationRequired})
			return
		}

		tokenKey := hashAPIToken(raw)
		uid, uname, err := s.authRepo.ResolveAPIToken(ctx, tokenKey)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAuthJSON(w, http.StatusUnauthorized, authAPIError{Error: AuthJSONAuthenticationRequired})
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
			return
		}

		// Best-effort; ignore errors so a metrics/update failure does not block the request.
		_ = s.authRepo.TouchAPITokenUsed(ctx, tokenKey)

		p := &authn.Principal{
			LocalUserID: uid,
			Username:    uname,
			Kind:        authn.KindLocalUser,
		}

		if !z.Allow(ctx, p, action, authz.Resource{}) {
			writeAuthJSON(w, http.StatusForbidden, authAPIError{Error: AuthJSONAuthorizationDenied})
			return
		}

		next.ServeHTTP(w, r.WithContext(authn.WithPrincipal(r.Context(), p)))
	})
}

func (s *APIServer) effectiveAuthorizer(setupComplete bool) authz.Authorizer {
	if s.authzOverride != nil {
		return s.authzOverride
	}

	return authz.SelectAuthorizer(setupComplete)
}
