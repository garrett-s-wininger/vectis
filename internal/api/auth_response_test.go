package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteJSON_setsHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, map[string]string{"ok": "true"})

	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type=%q", ct)
	}

	if cto := rec.Header().Get("X-Content-Type-Options"); cto != "nosniff" {
		t.Fatalf("X-Content-Type-Options=%q", cto)
	}
}

func TestWriteAPIError_wwwAuthenticateOn401(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAPIErrorCode(rec, http.StatusUnauthorized, apiErrAuthenticationRequired)

	if www := rec.Header().Get("WWW-Authenticate"); www != "Bearer" {
		t.Fatalf("WWW-Authenticate=%q", www)
	}
}

func TestWriteAPIError_noWWWAuthenticateOnOtherStatuses(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAPIErrorCode(rec, http.StatusForbidden, apiErrAuthorizationDenied)

	if www := rec.Header().Get("WWW-Authenticate"); www != "" {
		t.Fatalf("unexpected WWW-Authenticate=%q", www)
	}
}

func TestWriteAPIError_encodesContract(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAPIError(rec, http.StatusConflict, "run_requeue_conflict", "run cannot be requeued from current status", map[string]any{
		"status": "running",
	})

	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d", rec.Code)
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type=%q", ct)
	}

	if cto := rec.Header().Get("X-Content-Type-Options"); cto != "nosniff" {
		t.Fatalf("X-Content-Type-Options=%q", cto)
	}

	body := rec.Body.String()
	for _, want := range []string{
		`"code":"run_requeue_conflict"`,
		`"message":"run cannot be requeued from current status"`,
		`"details":{"status":"running"}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected %s in body, got: %s", want, body)
		}
	}
}

func TestWriteAPIError_omitsEmptyDetails(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAPIErrorCode(rec, http.StatusBadRequest, apiErrInvalidRequestBody)

	if strings.Contains(rec.Body.String(), "details") {
		t.Fatalf("expected no details field, got: %s", rec.Body.String())
	}
}

func TestWriteAPIErrorCode_statusMatrix(t *testing.T) {
	tests := []struct {
		name   string
		status int
		code   apiErrorCode
	}{
		{name: "bad request", status: http.StatusBadRequest, code: apiErrInvalidRequestBody},
		{name: "invalid host", status: http.StatusBadRequest, code: apiErrInvalidHostHeader},
		{name: "auth required", status: http.StatusUnauthorized, code: apiErrAuthenticationRequired},
		{name: "auth denied", status: http.StatusForbidden, code: apiErrAuthorizationDenied},
		{name: "conflict", status: http.StatusConflict, code: apiErrUsernameAlreadyExists},
		{name: "not acceptable", status: http.StatusNotAcceptable, code: apiErrNotAcceptable},
		{name: "method override", status: http.StatusBadRequest, code: apiErrMethodOverrideForbidden},
		{name: "body not allowed", status: http.StatusBadRequest, code: apiErrRequestBodyNotAllowed},
		{name: "too large", status: http.StatusRequestEntityTooLarge, code: apiErrRequestBodyTooLarge},
		{name: "unsupported media", status: http.StatusUnsupportedMediaType, code: apiErrUnsupportedMediaType},
		{name: "rate limited", status: http.StatusTooManyRequests, code: apiErrRateLimitExceeded},
		{name: "route not found", status: http.StatusNotFound, code: apiErrRouteNotFound},
		{name: "internal", status: http.StatusInternalServerError, code: apiErrInternal},
		{name: "unavailable", status: http.StatusServiceUnavailable, code: apiErrQueueNotReady},
		{name: "setup required", status: http.StatusServiceUnavailable, code: apiErrSetupRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeAPIErrorCode(rec, tt.status, tt.code)

			if rec.Code != tt.status {
				t.Fatalf("status=%d, want %d", rec.Code, tt.status)
			}

			var body apiError
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode body: %v; body=%s", err, rec.Body.String())
			}

			if body.Code != string(tt.code) {
				t.Fatalf("code=%q, want %q", body.Code, tt.code)
			}

			if body.Message != tt.code.message() {
				t.Fatalf("message=%q, want %q", body.Message, tt.code.message())
			}
		})
	}
}

func TestAPIErrorCodeMessages_areNonEmpty(t *testing.T) {
	codes := []apiErrorCode{
		apiErrAuthNotConfigured,
		apiErrAuthUnavailable,
		apiErrAuthenticationRequired,
		apiErrAuthorizationDenied,
		apiErrBindingAlreadyExists,
		apiErrBindingNotFound,
		apiErrBootstrapNotConfigured,
		apiErrCORSOriginForbidden,
		apiErrCSRFOriginForbidden,
		apiErrCSRFTokenRequired,
		apiErrDatabaseNotReady,
		apiErrFetchMetadataForbidden,
		apiErrInternal,
		apiErrInvalidHostHeader,
		apiErrInvalidAdminPassword,
		apiErrInvalidAdminUsername,
		apiErrInvalidBootstrapToken,
		apiErrInvalidExpiresIn,
		apiErrInvalidID,
		apiErrInvalidNamespaceID,
		apiErrInvalidNamespaceName,
		apiErrInvalidNewPassword,
		apiErrInvalidPassword,
		apiErrInvalidRequestBody,
		apiErrInvalidRole,
		apiErrInvalidScopeAction,
		apiErrInvalidUserID,
		apiErrInvalidUsername,
		apiErrLastAdminDeleteForbidden,
		apiErrLastAdminDisableForbidden,
		apiErrMethodNotAllowed,
		apiErrMethodOverrideForbidden,
		apiErrMissingCredentials,
		apiErrMissingAdminUsername,
		apiErrAdminPasswordTooShort,
		apiErrMissingCurrentPassword,
		apiErrMissingEnabled,
		apiErrMissingLabel,
		apiErrMissingLocalUserID,
		apiErrMissingName,
		apiErrMissingNamespacePath,
		apiErrMissingNewPassword,
		apiErrNewPasswordTooShort,
		apiErrMissingRole,
		apiErrMissingUsername,
		apiErrNamespaceAlreadyExists,
		apiErrNamespaceHasChildren,
		apiErrNamespaceHasJobs,
		apiErrNamespaceNotEmpty,
		apiErrNamespaceNotFound,
		apiErrNamespacePathForbidden,
		apiErrNamespaceRepositoryUnavailable,
		apiErrNamespacesNotConfigured,
		apiErrNotAcceptable,
		apiErrParentNamespaceNotFound,
		apiErrPasswordTooShort,
		apiErrQueueNotReady,
		apiErrRateLimitExceeded,
		apiErrRequestBodyNotAllowed,
		apiErrRequestBodyTooLarge,
		apiErrRequestReadFailed,
		apiErrRoleBindingsNotConfigured,
		apiErrRouteNotFound,
		apiErrRootNamespaceDeleteForbidden,
		apiErrScopedTokenScopeRequired,
		apiErrSelfDeleteForbidden,
		apiErrSelfDisableForbidden,
		apiErrServerShuttingDown,
		apiErrSetupAlreadyComplete,
		apiErrSetupRequired,
		apiErrStreamingUnsupported,
		apiErrTokenNotFound,
		apiErrUnsupportedMediaType,
		apiErrUserNotFound,
		apiErrUserNotFoundOrDisabled,
		apiErrUsernameAlreadyExists,
	}

	seen := make(map[apiErrorCode]bool, len(codes))
	for _, code := range codes {
		if seen[code] {
			t.Fatalf("duplicate api error code in test inventory: %q", code)
		}
		seen[code] = true

		if msg := code.message(); strings.TrimSpace(msg) == "" {
			t.Fatalf("empty message for api error code %q", code)
		}
	}
}
