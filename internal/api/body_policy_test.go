package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type unknownLengthReader struct {
	io.Reader
}

func TestRouteBodyMiddlewareRejectsBodyOnDefaultPolicy(t *testing.T) {
	handler := routeBodyMiddleware(routeBodyPolicy{}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/version", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusBadRequest)
	}

	var body apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v; body=%s", err, rec.Body.String())
	}

	if body.Code != string(apiErrRequestBodyNotAllowed) {
		t.Fatalf("code=%q, want %q", body.Code, apiErrRequestBodyNotAllowed)
	}
}

func TestRouteBodyMiddlewareAllowsEmptyDefaultPolicy(t *testing.T) {
	handler := routeBodyMiddleware(routeBodyPolicy{}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestRouteBodyMiddlewareRejectsKnownOversizeBody(t *testing.T) {
	handler := routeBodyMiddleware(routeBodyJSONPolicy(4), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader("12345"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestReadRequestBodyRejectsStreamingOversizeBody(t *testing.T) {
	handler := routeBodyMiddleware(routeBodyJSONPolicy(4), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := readRequestBody(w, r, 4); !ok {
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", unknownLengthReader{Reader: strings.NewReader("12345")})
	if req.ContentLength != -1 {
		t.Fatalf("test request ContentLength=%d, want unknown length", req.ContentLength)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}

	var body apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v; body=%s", err, rec.Body.String())
	}

	if body.Code != string(apiErrRequestBodyTooLarge) {
		t.Fatalf("code=%q, want %q", body.Code, apiErrRequestBodyTooLarge)
	}
}

func TestRouteBodyPolicyValidation(t *testing.T) {
	tests := []struct {
		name    string
		policy  routeBodyPolicy
		wantErr bool
	}{
		{name: "no body", policy: routeBodyPolicy{}},
		{name: "json", policy: routeBodyJSONPolicy(1)},
		{name: "optional json", policy: routeBodyOptionalJSONPolicy(1)},
		{name: "no body with max", policy: routeBodyPolicy{mode: routeBodyNone, maxBytes: 1}, wantErr: true},
		{name: "json without max", policy: routeBodyPolicy{mode: routeBodyJSON}, wantErr: true},
		{name: "unknown mode", policy: routeBodyPolicy{mode: routeBodyMode(99)}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.policy.validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validate err=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}
