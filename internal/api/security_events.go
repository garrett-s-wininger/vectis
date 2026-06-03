package api

import (
	"context"
	"net/http"
	"strings"
	"unicode/utf8"

	"vectis/internal/config"
)

const (
	securityRejectionUnknownRoute = "unknown"

	securityReasonInvalidHostHeader        = "invalid_host_header"
	securityReasonCORSPreflightForbidden   = "cors_preflight_forbidden"
	securityReasonCSRFFetchMetadataBlocked = "csrf_fetch_metadata_forbidden"
	securityReasonCSRFOriginForbidden      = "csrf_origin_forbidden"
	securityReasonCSRFTokenRequired        = "csrf_token_required"
	securityReasonMethodNotAllowed         = "method_not_allowed"
	securityReasonRateLimitExceeded        = "rate_limit_exceeded"
	securityReasonRequestBodyNotAllowed    = "request_body_not_allowed"
	securityReasonRequestBodyTooLarge      = "request_body_too_large"
	securityReasonUnsupportedHTTPMethod    = "unsupported_http_method"

	maxSecurityLogValueBytes = 160
)

type securityRejectionMetrics interface {
	RecordSecurityRejection(ctx context.Context, reason, route string, status int)
}

type securityRejectionRecorder func(*http.Request, string, int)

type securityRejectionRecorderKey struct{}

func (s *APIServer) recordSecurityRejection(r *http.Request, reason string, status int) {
	s.recordSecurityRejectionForRoute(r, reason, securityRoute(r), status)
}

func (s *APIServer) recordSecurityRejectionForRoute(r *http.Request, reason, route string, status int) {
	if r == nil {
		return
	}

	if strings.TrimSpace(route) == "" {
		route = securityRejectionUnknownRoute
	}

	if s.apiSecurityMetrics != nil {
		s.apiSecurityMetrics.RecordSecurityRejection(r.Context(), reason, route, status)
	}

	if s.logger == nil {
		return
	}

	s.logger.Warn(
		"API security rejection: reason=%s status=%d route=%s client_ip=%s host=%s origin=%s fetch_site=%s",
		securityLogValue(reason),
		status,
		securityLogValue(route),
		securityLogValue(config.HTTPClientIP(r)),
		securityLogValue(r.Host),
		securityLogValue(r.Header.Get("Origin")),
		securityLogValue(r.Header.Get("Sec-Fetch-Site")),
	)
}

func securityRoute(r *http.Request) string {
	if r != nil {
		if route := strings.TrimSpace(r.Pattern); route != "" {
			return route
		}
	}

	return securityRejectionUnknownRoute
}

func securityLogValue(value string) string {
	value = strings.TrimSpace(strings.ToValidUTF8(value, "?"))
	if value == "" {
		return ""
	}

	var b strings.Builder
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			r = '?'
		}

		size := utf8.RuneLen(r)
		if size < 0 {
			size = len("?")
			r = '?'
		}

		if b.Len()+size > maxSecurityLogValueBytes {
			break
		}

		b.WriteRune(r)
	}

	return b.String()
}

func withSecurityRejectionRecorder(r *http.Request, record securityRejectionRecorder) *http.Request {
	if r == nil || record == nil {
		return r
	}

	ctx := context.WithValue(r.Context(), securityRejectionRecorderKey{}, record)
	return r.WithContext(ctx)
}

func recordSecurityRejectionFromRequest(r *http.Request, reason string, status int) {
	if r == nil {
		return
	}

	record, ok := r.Context().Value(securityRejectionRecorderKey{}).(securityRejectionRecorder)
	if !ok || record == nil {
		return
	}

	record(r, reason, status)
}
