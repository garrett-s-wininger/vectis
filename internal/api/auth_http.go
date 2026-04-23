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

type routeAuthPolicy struct {
	Public bool
	Action authz.Action
}

func (p routeAuthPolicy) normalized() routeAuthPolicy {
	if !p.Public && p.Action == "" {
		p.Action = authz.ActionAdmin
	}

	return p
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

func (s *APIServer) accessControlledHandler(policy routeAuthPolicy, next http.Handler) http.Handler {
	policy = policy.normalized()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if policy.Public || !config.APIAuthEnabled() {
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
		action := policy.Action

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
		uid, uname, tokenID, err := s.authRepo.ResolveAPIToken(ctx, tokenKey)
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

				s.logger.Error("Database error loading token scopes: %v", err)
				writeAuthJSON(w, http.StatusInternalServerError, authAPIError{Error: AuthJSONInternal})
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
			writeAuthJSON(w, http.StatusForbidden, authAPIError{Error: AuthJSONAuthorizationDenied})
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

	return authz.SelectAuthorizer(setupComplete, namespaces, roleBindings)
}
