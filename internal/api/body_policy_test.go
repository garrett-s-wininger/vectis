package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vectis/internal/interfaces/mocks"
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
	req.Header.Set("Content-Type", "application/json")

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

func TestAPIServerRecordsNoBodyRouteSecurityRejection(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/version", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusBadRequest)
	}

	requireSecurityRejection(t, metrics, securityReasonRequestBodyNotAllowed, "GET /api/v1/version", http.StatusBadRequest)
}

func TestAPIServerRecordsStreamingOversizeBodySecurityRejection(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)
	handler := routeBodyMiddleware(routeBodyJSONPolicy(4), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := readRequestBody(w, r, 4); !ok {
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}), s.recordSecurityRejection)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", unknownLengthReader{Reader: strings.NewReader("12345")})
	req.Pattern = "POST /api/v1/login"
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}

	requireSecurityRejection(t, metrics, securityReasonRequestBodyTooLarge, "POST /api/v1/login", http.StatusRequestEntityTooLarge)
}

func TestRouteBodyMiddlewareRejectsMissingJSONContentType(t *testing.T) {
	called := false
	handler := routeBodyMiddleware(routeBodyJSONPolicy(128), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if called {
		t.Fatal("handler should not be called for missing JSON content type")
	}

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusUnsupportedMediaType)
	}

	var body apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v; body=%s", err, rec.Body.String())
	}

	if body.Code != string(apiErrUnsupportedMediaType) {
		t.Fatalf("code=%q, want %q", body.Code, apiErrUnsupportedMediaType)
	}
}

func TestRouteBodyMiddlewareOptionalJSONContentType(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		contentType string
		wantStatus  int
	}{
		{name: "empty without content type", wantStatus: http.StatusNoContent},
		{name: "body without content type", body: "{}", wantStatus: http.StatusUnsupportedMediaType},
		{name: "body with json content type", body: "{}", contentType: "application/json; charset=utf-8", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := routeBodyMiddleware(routeBodyOptionalJSONPolicy(128), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))

			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}

			req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/1/replay", body)
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status=%d, want %d; body=%s", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestAPIServerRecordsUnsupportedMediaTypeSecurityRejection(t *testing.T) {
	metrics := &fakeSecurityRejectionMetrics{}
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	s.SetAPISecurityMetrics(metrics)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusUnsupportedMediaType)
	}

	requireSecurityRejection(t, metrics, securityReasonUnsupportedMediaType, "POST /api/v1/login", http.StatusUnsupportedMediaType)
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
