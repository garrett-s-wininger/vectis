package api

import (
	"net/http"

	"vectis/internal/config"
)

func hostHeaderMiddleware(next http.Handler) http.Handler {
	return (*APIServer)(nil).hostHeaderMiddleware(next)
}

func (s *APIServer) hostHeaderMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !config.APIHostAllowed(r.Host) {
			if s != nil {
				s.recordSecurityRejection(r, securityReasonInvalidHostHeader, http.StatusBadRequest)
			}

			writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidHostHeader)
			return
		}

		next.ServeHTTP(w, r)
	})
}
