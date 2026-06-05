package api

import (
	"net/http"
	"strings"
)

const (
	idempotencyKeyHeaderName = "Idempotency-Key"
	maxIdempotencyKeyBytes   = 255
)

var singletonRequestHeaders = []string{
	"Authorization",
	"Content-Type",
	csrfHeaderName,
	"Origin",
	"Referer",
	idempotencyKeyHeaderName,
}

type routeHeaderPolicy struct {
	allowIdempotencyKey bool
}

func routeHeadersIdempotencyPolicy() routeHeaderPolicy {
	return routeHeaderPolicy{allowIdempotencyKey: true}
}

func (p routeHeaderPolicy) validate() error {
	return nil
}

func (p routeHeaderPolicy) allows(r *http.Request) bool {
	if r == nil {
		return true
	}

	for _, name := range singletonRequestHeaders {
		if len(r.Header.Values(name)) > 1 {
			return false
		}
	}

	values := r.Header.Values(idempotencyKeyHeaderName)
	if len(values) == 0 {
		return true
	}

	if !p.allowIdempotencyKey {
		return false
	}

	return validIdempotencyKey(values[0])
}

func validIdempotencyKey(value string) bool {
	if value == "" || len(value) > maxIdempotencyKeyBytes {
		return false
	}

	if value != strings.TrimSpace(value) {
		return false
	}

	for _, r := range value {
		if r < 0x21 || r > 0x7e || r == ',' {
			return false
		}
	}

	return true
}

func routeHeaderMiddleware(policy routeHeaderPolicy, next http.Handler, recorders ...securityRejectionRecorder) http.Handler {
	var record securityRejectionRecorder
	if len(recorders) > 0 {
		record = recorders[0]
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !policy.allows(r) {
			if record != nil {
				record(r, securityReasonInvalidRequestHeader, http.StatusBadRequest)
			}

			setNoStore(w)
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestHeader)
			return
		}

		next.ServeHTTP(w, r)
	})
}
