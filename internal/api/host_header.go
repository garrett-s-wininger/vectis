package api

import (
	"net/http"

	"vectis/internal/config"
)

func hostHeaderMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !config.APIHostAllowed(r.Host) {
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidHostHeader)
			return
		}

		next.ServeHTTP(w, r)
	})
}
