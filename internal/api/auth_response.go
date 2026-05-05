package api

import (
	"encoding/json"
	"net/http"
)

// authAPIError is the standard JSON envelope for /api/v1/setup/* and auth middleware errors.
type authAPIError struct {
	Error  string `json:"error"`
	Detail string `json:"detail,omitempty"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func writeAuthJSON(w http.ResponseWriter, status int, v any) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")

	if status == http.StatusUnauthorized {
		h.Set("WWW-Authenticate", "Bearer")
	}

	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeAPIError(w http.ResponseWriter, status int, code, message string, details any) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")

	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiError{
		Code:    code,
		Message: message,
		Details: details,
	})
}
