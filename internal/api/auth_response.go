package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"
)

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

type apiErrorCode string

const (
	apiErrAuthNotConfigured              apiErrorCode = "auth_not_configured"
	apiErrAuthUnavailable                apiErrorCode = "auth_unavailable"
	apiErrAuthenticationRequired         apiErrorCode = "authentication_required"
	apiErrAuthorizationDenied            apiErrorCode = "authorization_denied"
	apiErrBindingAlreadyExists           apiErrorCode = "binding_already_exists"
	apiErrBindingNotFound                apiErrorCode = "binding_not_found"
	apiErrBootstrapNotConfigured         apiErrorCode = "bootstrap_not_configured"
	apiErrCSRFOriginForbidden            apiErrorCode = "csrf_origin_forbidden"
	apiErrCSRFTokenRequired              apiErrorCode = "csrf_token_required"
	apiErrDatabaseNotReady               apiErrorCode = "database_not_ready"
	apiErrFetchMetadataForbidden         apiErrorCode = "fetch_metadata_forbidden"
	apiErrInternal                       apiErrorCode = "internal_error"
	apiErrInvalidHostHeader              apiErrorCode = "invalid_host_header"
	apiErrInvalidAdminPassword           apiErrorCode = "invalid_admin_password"
	apiErrInvalidAdminUsername           apiErrorCode = "invalid_admin_username"
	apiErrInvalidBootstrapToken          apiErrorCode = "invalid_bootstrap_token"
	apiErrInvalidExpiresIn               apiErrorCode = "invalid_expires_in"
	apiErrInvalidID                      apiErrorCode = "invalid_id"
	apiErrInvalidNamespaceID             apiErrorCode = "invalid_namespace_id"
	apiErrInvalidNamespaceName           apiErrorCode = "invalid_namespace_name"
	apiErrInvalidNewPassword             apiErrorCode = "invalid_new_password"
	apiErrInvalidPassword                apiErrorCode = "invalid_password"
	apiErrInvalidRequestBody             apiErrorCode = "invalid_request_body"
	apiErrInvalidRole                    apiErrorCode = "invalid_role"
	apiErrInvalidScopeAction             apiErrorCode = "invalid_scope_action"
	apiErrInvalidUserID                  apiErrorCode = "invalid_user_id"
	apiErrInvalidUsername                apiErrorCode = "invalid_username"
	apiErrLastAdminDeleteForbidden       apiErrorCode = "last_admin_delete_forbidden"
	apiErrLastAdminDisableForbidden      apiErrorCode = "last_admin_disable_forbidden"
	apiErrMethodNotAllowed               apiErrorCode = "method_not_allowed"
	apiErrMissingCredentials             apiErrorCode = "missing_credentials"
	apiErrMissingAdminUsername           apiErrorCode = "missing_admin_username"
	apiErrAdminPasswordTooShort          apiErrorCode = "admin_password_too_short"
	apiErrMissingCurrentPassword         apiErrorCode = "missing_current_password"
	apiErrMissingEnabled                 apiErrorCode = "missing_enabled"
	apiErrMissingLabel                   apiErrorCode = "missing_label"
	apiErrMissingLocalUserID             apiErrorCode = "missing_local_user_id"
	apiErrMissingName                    apiErrorCode = "missing_name"
	apiErrMissingNamespacePath           apiErrorCode = "missing_namespace_path"
	apiErrMissingNewPassword             apiErrorCode = "missing_new_password"
	apiErrNewPasswordTooShort            apiErrorCode = "new_password_too_short"
	apiErrMissingRole                    apiErrorCode = "missing_role"
	apiErrMissingUsername                apiErrorCode = "missing_username"
	apiErrNamespaceAlreadyExists         apiErrorCode = "namespace_already_exists"
	apiErrNamespaceHasChildren           apiErrorCode = "namespace_has_children"
	apiErrNamespaceHasJobs               apiErrorCode = "namespace_has_jobs"
	apiErrNamespaceNotEmpty              apiErrorCode = "namespace_not_empty"
	apiErrNamespaceNotFound              apiErrorCode = "namespace_not_found"
	apiErrNamespacePathForbidden         apiErrorCode = "namespace_path_forbidden"
	apiErrNamespaceRepositoryUnavailable apiErrorCode = "namespace_repository_unavailable"
	apiErrNamespacesNotConfigured        apiErrorCode = "namespaces_not_configured"
	apiErrParentNamespaceNotFound        apiErrorCode = "parent_namespace_not_found"
	apiErrPasswordTooShort               apiErrorCode = "password_too_short"
	apiErrQueueNotReady                  apiErrorCode = "queue_not_ready"
	apiErrRateLimitExceeded              apiErrorCode = "rate_limit_exceeded"
	apiErrRequestBodyNotAllowed          apiErrorCode = "request_body_not_allowed"
	apiErrRequestBodyTooLarge            apiErrorCode = "request_body_too_large"
	apiErrRequestReadFailed              apiErrorCode = "request_read_failed"
	apiErrRoleBindingsNotConfigured      apiErrorCode = "role_bindings_not_configured"
	apiErrRouteNotFound                  apiErrorCode = "route_not_found"
	apiErrRootNamespaceDeleteForbidden   apiErrorCode = "root_namespace_delete_forbidden"
	apiErrScopedTokenScopeRequired       apiErrorCode = "scoped_token_scope_required"
	apiErrSelfDeleteForbidden            apiErrorCode = "self_delete_forbidden"
	apiErrSelfDisableForbidden           apiErrorCode = "self_disable_forbidden"
	apiErrServerShuttingDown             apiErrorCode = "server_shutting_down"
	apiErrSetupAlreadyComplete           apiErrorCode = "setup_already_complete"
	apiErrSetupRequired                  apiErrorCode = "setup_required"
	apiErrStreamingUnsupported           apiErrorCode = "streaming_unsupported"
	apiErrTokenNotFound                  apiErrorCode = "token_not_found"
	apiErrUnsupportedMediaType           apiErrorCode = "unsupported_media_type"
	apiErrUserNotFound                   apiErrorCode = "user_not_found"
	apiErrUserNotFoundOrDisabled         apiErrorCode = "user_not_found_or_disabled"
	apiErrUsernameAlreadyExists          apiErrorCode = "username_already_exists"
)

func (c apiErrorCode) message() string {
	switch c {
	case apiErrAuthNotConfigured:
		return "auth not configured"
	case apiErrAuthUnavailable:
		return "authentication persistence is not available"
	case apiErrAuthenticationRequired:
		return "authentication required"
	case apiErrAuthorizationDenied:
		return "authorization denied"
	case apiErrBindingAlreadyExists:
		return "binding already exists"
	case apiErrBindingNotFound:
		return "binding not found"
	case apiErrBootstrapNotConfigured:
		return "server is missing a bootstrap token of sufficient length"
	case apiErrCSRFOriginForbidden:
		return "csrf origin forbidden"
	case apiErrCSRFTokenRequired:
		return "csrf token required"
	case apiErrDatabaseNotReady:
		return "database not ready"
	case apiErrFetchMetadataForbidden:
		return "fetch metadata forbidden"
	case apiErrInternal:
		return "internal server error"
	case apiErrInvalidHostHeader:
		return "invalid host header"
	case apiErrInvalidAdminPassword:
		return "invalid admin_password"
	case apiErrInvalidAdminUsername:
		return "invalid admin_username"
	case apiErrInvalidBootstrapToken:
		return "invalid bootstrap token"
	case apiErrInvalidExpiresIn:
		return "invalid expires_in"
	case apiErrInvalidID:
		return "invalid id"
	case apiErrInvalidNamespaceID:
		return "invalid namespace id"
	case apiErrInvalidNamespaceName:
		return "invalid namespace name"
	case apiErrInvalidNewPassword:
		return "invalid new_password"
	case apiErrInvalidPassword:
		return "invalid password"
	case apiErrInvalidRequestBody:
		return "invalid request body"
	case apiErrInvalidRole:
		return "invalid role"
	case apiErrInvalidScopeAction:
		return "invalid scope action"
	case apiErrInvalidUserID:
		return "invalid user id"
	case apiErrInvalidUsername:
		return "invalid username"
	case apiErrLastAdminDeleteForbidden:
		return "cannot delete the last admin"
	case apiErrLastAdminDisableForbidden:
		return "cannot disable the last admin"
	case apiErrMethodNotAllowed:
		return "method not allowed"
	case apiErrMissingCredentials:
		return "username and password are required"
	case apiErrMissingAdminUsername:
		return "admin_username is required"
	case apiErrAdminPasswordTooShort:
		return "admin_password must be at least 8 characters"
	case apiErrMissingCurrentPassword:
		return "current_password is required"
	case apiErrMissingEnabled:
		return "enabled is required"
	case apiErrMissingLabel:
		return "label is required"
	case apiErrMissingLocalUserID:
		return "local_user_id is required"
	case apiErrMissingName:
		return "name is required"
	case apiErrMissingNamespacePath:
		return "namespace_path is required for namespaced actions"
	case apiErrMissingNewPassword:
		return "new_password is required"
	case apiErrNewPasswordTooShort:
		return "new_password must be at least 8 characters"
	case apiErrMissingRole:
		return "role is required"
	case apiErrMissingUsername:
		return "username is required"
	case apiErrNamespaceAlreadyExists:
		return "namespace already exists"
	case apiErrNamespaceHasChildren:
		return "namespace has children"
	case apiErrNamespaceHasJobs:
		return "namespace has jobs"
	case apiErrNamespaceNotEmpty:
		return "namespace has children or jobs"
	case apiErrNamespaceNotFound:
		return "namespace not found"
	case apiErrNamespacePathForbidden:
		return "namespace_path is not allowed for global actions"
	case apiErrNamespaceRepositoryUnavailable:
		return "namespace repository unavailable"
	case apiErrNamespacesNotConfigured:
		return "namespaces not configured"
	case apiErrParentNamespaceNotFound:
		return "parent namespace not found"
	case apiErrPasswordTooShort:
		return "password must be at least 8 characters"
	case apiErrQueueNotReady:
		return "queue not ready"
	case apiErrRateLimitExceeded:
		return "rate limit exceeded"
	case apiErrRequestBodyNotAllowed:
		return "request body is not allowed for this route"
	case apiErrRequestBodyTooLarge:
		return "request body too large"
	case apiErrRequestReadFailed:
		return "failed to read request body"
	case apiErrRoleBindingsNotConfigured:
		return "role bindings not configured"
	case apiErrRouteNotFound:
		return "route not found"
	case apiErrRootNamespaceDeleteForbidden:
		return "cannot delete root namespace"
	case apiErrScopedTokenScopeRequired:
		return "scoped tokens must create explicitly scoped tokens"
	case apiErrSelfDeleteForbidden:
		return "cannot delete yourself"
	case apiErrSelfDisableForbidden:
		return "cannot disable yourself"
	case apiErrServerShuttingDown:
		return "server is shutting down"
	case apiErrSetupAlreadyComplete:
		return "initial setup has already been performed"
	case apiErrSetupRequired:
		return "complete initial setup before using the API"
	case apiErrStreamingUnsupported:
		return "streaming unsupported"
	case apiErrTokenNotFound:
		return "token not found"
	case apiErrUnsupportedMediaType:
		return "content type must be application/json"
	case apiErrUserNotFound:
		return "user not found"
	case apiErrUserNotFoundOrDisabled:
		return "user not found or disabled"
	case apiErrUsernameAlreadyExists:
		return "username already exists"
	default:
		return string(c)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")

	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeAPIError(w http.ResponseWriter, status int, code, message string, details any) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")

	if status == http.StatusUnauthorized {
		h.Set("WWW-Authenticate", "Bearer")
	}

	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiError{
		Code:    code,
		Message: message,
		Details: details,
	})
}

func writeAPIErrorCode(w http.ResponseWriter, status int, code apiErrorCode) {
	writeAPIError(w, status, string(code), code.message(), nil)
}

func writeMethodNotAllowed(w http.ResponseWriter, allow string) {
	if allow != "" {
		w.Header().Set("Allow", allow)
	}

	setNoStore(w)
	writeAPIErrorCode(w, http.StatusMethodNotAllowed, apiErrMethodNotAllowed)
}

func setNoStore(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Cache-Control", "no-store")
	h.Set("Pragma", "no-cache")
	h.Set("Expires", "0")
}

func writeRateLimitExceeded(w http.ResponseWriter, retryAfter time.Duration) {
	retrySeconds := max(int(math.Ceil(retryAfter.Seconds())), 1)
	w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
	writeAPIErrorCode(w, http.StatusTooManyRequests, apiErrRateLimitExceeded)
}
