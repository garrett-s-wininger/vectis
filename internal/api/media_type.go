package api

import (
	"mime"
	"net/http"
	"strings"
)

func requestContentTypeIsJSON(r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && strings.EqualFold(mediaType, "application/json")
}
