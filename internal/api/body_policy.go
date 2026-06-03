package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

type routeBodyMode int

const (
	routeBodyNone routeBodyMode = iota
	routeBodyJSON
	routeBodyOptionalJSON
)

type routeBodyPolicy struct {
	mode     routeBodyMode
	maxBytes int64
}

func routeBodyJSONPolicy(maxBytes int64) routeBodyPolicy {
	return routeBodyPolicy{mode: routeBodyJSON, maxBytes: maxBytes}
}

func routeBodyOptionalJSONPolicy(maxBytes int64) routeBodyPolicy {
	return routeBodyPolicy{mode: routeBodyOptionalJSON, maxBytes: maxBytes}
}

func (p routeBodyPolicy) validate() error {
	switch p.mode {
	case routeBodyNone:
		if p.maxBytes != 0 {
			return fmt.Errorf("no-body route must not set max body bytes")
		}
	case routeBodyJSON, routeBodyOptionalJSON:
		if p.maxBytes <= 0 {
			return fmt.Errorf("body route must set positive max body bytes")
		}
	default:
		return fmt.Errorf("unknown body mode %d", p.mode)
	}

	return nil
}

func (p routeBodyPolicy) allowsBody() bool {
	return p.mode == routeBodyJSON || p.mode == routeBodyOptionalJSON
}

func routeBodyMiddleware(policy routeBodyPolicy, next http.Handler, recorders ...securityRejectionRecorder) http.Handler {
	var record securityRejectionRecorder
	if len(recorders) > 0 {
		record = recorders[0]
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !policy.allowsBody() {
			if requestHasBody(r) {
				if record != nil {
					record(r, securityReasonRequestBodyNotAllowed, http.StatusBadRequest)
				}
				writeAPIErrorCode(w, http.StatusBadRequest, apiErrRequestBodyNotAllowed)
				return
			}

			next.ServeHTTP(w, r)
			return
		}

		if r.ContentLength > policy.maxBytes {
			if record != nil {
				record(r, securityReasonRequestBodyTooLarge, http.StatusRequestEntityTooLarge)
			}
			writeAPIErrorCode(w, http.StatusRequestEntityTooLarge, apiErrRequestBodyTooLarge)
			return
		}

		r = withSecurityRejectionRecorder(r, record)
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, policy.maxBytes)
		}

		next.ServeHTTP(w, r)
	})
}

func requestHasBody(r *http.Request) bool {
	if r.ContentLength > 0 {
		return true
	}

	if len(r.TransferEncoding) > 0 {
		return true
	}

	return r.ContentLength < 0 && r.Body != nil && r.Body != http.NoBody
}

func readRequestBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, bool) {
	if maxBytes <= 0 {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return nil, false
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBytes+1))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			recordSecurityRejectionFromRequest(r, securityReasonRequestBodyTooLarge, http.StatusRequestEntityTooLarge)
			writeAPIErrorCode(w, http.StatusRequestEntityTooLarge, apiErrRequestBodyTooLarge)
			return nil, false
		}

		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return nil, false
	}

	if int64(len(body)) > maxBytes {
		recordSecurityRejectionFromRequest(r, securityReasonRequestBodyTooLarge, http.StatusRequestEntityTooLarge)
		writeAPIErrorCode(w, http.StatusRequestEntityTooLarge, apiErrRequestBodyTooLarge)
		return nil, false
	}

	return body, true
}
