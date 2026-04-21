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

func writeAuthJSON(w http.ResponseWriter, status int, v any) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
