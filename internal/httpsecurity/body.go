package httpsecurity

import (
	"mime"
	"net/http"
	"strings"
)

// RequestHasBody reports whether a request carries a body that a route policy
// should explicitly allow before handler code runs.
func RequestHasBody(r *http.Request) bool {
	if r == nil {
		return false
	}

	if r.ContentLength > 0 {
		return true
	}

	if len(r.TransferEncoding) > 0 {
		return true
	}

	return r.ContentLength < 0 && r.Body != nil && r.Body != http.NoBody
}

func RequestContentTypeIsJSON(r *http.Request) bool {
	if r == nil {
		return false
	}

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && strings.EqualFold(mediaType, MediaTypeJSON)
}
