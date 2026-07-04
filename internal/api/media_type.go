package api

import (
	"net/http"

	"vectis/internal/httpsecurity"
)

func requestContentTypeIsJSON(r *http.Request) bool {
	return httpsecurity.RequestContentTypeIsJSON(r)
}
