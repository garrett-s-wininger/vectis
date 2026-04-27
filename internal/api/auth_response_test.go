package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteAuthJSON_setsHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAuthJSON(rec, http.StatusBadRequest, authAPIError{Error: "test_error"})

	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type=%q", ct)
	}

	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control=%q", cc)
	}

	if cto := rec.Header().Get("X-Content-Type-Options"); cto != "nosniff" {
		t.Fatalf("X-Content-Type-Options=%q", cto)
	}
}

func TestWriteAuthJSON_wwwAuthenticateOn401(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAuthJSON(rec, http.StatusUnauthorized, authAPIError{Error: AuthJSONAuthenticationRequired})

	if www := rec.Header().Get("WWW-Authenticate"); www != "Bearer" {
		t.Fatalf("WWW-Authenticate=%q", www)
	}
}

func TestWriteAuthJSON_noWWWAuthenticateOnOtherStatuses(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAuthJSON(rec, http.StatusForbidden, authAPIError{Error: AuthJSONAuthorizationDenied})

	if www := rec.Header().Get("WWW-Authenticate"); www != "" {
		t.Fatalf("unexpected WWW-Authenticate=%q", www)
	}
}

func TestWriteAuthJSON_encodesBody(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAuthJSON(rec, http.StatusConflict, authAPIError{Error: "setup_already_complete", Detail: "done"})

	body := rec.Body.String()
	if !strings.Contains(body, `"error":"setup_already_complete"`) {
		t.Fatalf("expected error in body, got: %s", body)
	}

	if !strings.Contains(body, `"detail":"done"`) {
		t.Fatalf("expected detail in body, got: %s", body)
	}
}

func TestWriteAuthJSON_omitemptyDetail(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAuthJSON(rec, http.StatusOK, authAPIError{Error: "ok"})

	body := rec.Body.String()
	if strings.Contains(body, "detail") {
		t.Fatalf("expected no detail field, got: %s", body)
	}
}
