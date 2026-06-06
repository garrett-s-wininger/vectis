package api

import (
	"net"
	"net/http"
	"net/url"
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

var earlyRequestSingletonHeaders = []string{
	"Origin",
	"Access-Control-Request-Method",
	"Access-Control-Request-Headers",
	"Sec-Fetch-Site",
	"Sec-Fetch-Mode",
	"Sec-Fetch-Dest",
	"Sec-Fetch-User",
	"X-Forwarded-For",
	"X-Forwarded-Proto",
	"X-Real-IP",
	"Forwarded",
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

func requestHeaderGuardMiddleware(next http.Handler) http.Handler {
	return (*APIServer)(nil).requestHeaderGuardMiddleware(next)
}

func (s *APIServer) requestHeaderGuardMiddleware(next http.Handler) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if earlyRequestHeadersAllowed(r) {
			next.ServeHTTP(w, r)
			return
		}

		if s != nil {
			s.recordSecurityRejection(r, securityReasonInvalidRequestHeader, http.StatusBadRequest)
		}

		setNoStore(w)
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestHeader)
	})
}

func earlyRequestHeadersAllowed(r *http.Request) bool {
	if r == nil {
		return true
	}

	for _, name := range earlyRequestSingletonHeaders {
		values := r.Header.Values(name)
		if len(values) == 0 {
			continue
		}

		if len(values) > 1 {
			return false
		}

		if !validEarlyRequestHeaderValue(name, values[0]) {
			return false
		}
	}

	return true
}

func validEarlyRequestHeaderValue(name, value string) bool {
	switch name {
	case "Origin":
		return validOriginHeaderValue(value)
	case "Access-Control-Request-Method":
		return validHeaderTokenValue(value, 64)
	case "Access-Control-Request-Headers":
		return validCommaSeparatedHeaderTokens(value, 2048)
	case "Sec-Fetch-Site", "Sec-Fetch-Mode", "Sec-Fetch-Dest":
		return validHeaderTokenValue(value, 128)
	case "Sec-Fetch-User":
		return value == "?1"
	case "X-Forwarded-For":
		return validForwardedForHeaderValue(value)
	case "X-Forwarded-Proto":
		return validSingleForwardedProtoHeaderValue(value)
	case "X-Real-IP":
		return validXRealIPHeaderValue(value)
	case "Forwarded":
		return validForwardedHeaderValue(value)
	default:
		return true
	}
}

func validForwardedForHeaderValue(value string) bool {
	if value == "" || len(value) > 4096 || value != strings.TrimSpace(value) {
		return false
	}

	for part := range strings.SplitSeq(value, ",") {
		if !validForwardedIPPart(strings.TrimSpace(part), true) {
			return false
		}
	}

	return true
}

func validXRealIPHeaderValue(value string) bool {
	if value == "" || len(value) > 256 || value != strings.TrimSpace(value) || strings.Contains(value, ",") {
		return false
	}

	return validForwardedIPPart(value, false)
}

func validForwardedIPPart(value string, allowPort bool) bool {
	if value == "" || value != strings.TrimSpace(value) || strings.ContainsAny(value, " \t\r\n\"") {
		return false
	}

	if allowPort {
		if host, _, err := net.SplitHostPort(value); err == nil {
			value = host
		}
	}

	if strings.HasPrefix(value, "[") || strings.HasSuffix(value, "]") {
		if !strings.HasPrefix(value, "[") || !strings.HasSuffix(value, "]") {
			return false
		}

		value = strings.TrimPrefix(strings.TrimSuffix(value, "]"), "[")
		if strings.ContainsAny(value, "[]") {
			return false
		}
	}

	return net.ParseIP(value) != nil
}

func validSingleForwardedProtoHeaderValue(value string) bool {
	proto, ok := normalizedSingleForwardedProto(value)
	return ok && (proto == "http" || proto == "https")
}

func normalizedSingleForwardedProto(value string) (string, bool) {
	if value == "" || len(value) > 64 || value != strings.TrimSpace(value) || strings.Contains(value, ",") {
		return "", false
	}

	if value[0] == '"' || value[len(value)-1] == '"' {
		if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
			return "", false
		}

		value = strings.TrimSpace(value[1 : len(value)-1])
		if value == "" || strings.ContainsAny(value, "\"\r\n\t ") {
			return "", false
		}
	}

	return strings.ToLower(value), true
}

func validForwardedHeaderValue(value string) bool {
	if value == "" || len(value) > 4096 || value != strings.TrimSpace(value) || strings.Contains(value, ",") {
		return false
	}

	seen := map[string]bool{}
	for part := range strings.SplitSeq(value, ";") {
		key, rawValue, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return false
		}

		key = strings.ToLower(strings.TrimSpace(key))
		rawValue = strings.TrimSpace(rawValue)
		if !validHeaderToken(key) || rawValue == "" || seen[key] {
			return false
		}
		seen[key] = true

		if key == "proto" {
			if !validSingleForwardedProtoHeaderValue(rawValue) {
				return false
			}
			continue
		}

		if !validForwardedHeaderParamValue(rawValue) {
			return false
		}
	}

	return true
}

func validForwardedHeaderParamValue(value string) bool {
	if value == "" {
		return false
	}

	if value[0] == '"' || value[len(value)-1] == '"' {
		if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
			return false
		}

		value = value[1 : len(value)-1]
	}

	if value == "" {
		return false
	}

	for i := range len(value) {
		c := value[i]
		if c < 0x21 || c > 0x7e || c == ',' || c == ';' {
			return false
		}
	}

	return true
}

func validOriginHeaderValue(value string) bool {
	if value == "" || len(value) > 2048 || value != strings.TrimSpace(value) {
		return false
	}

	if value == "null" {
		return true
	}

	u, err := url.Parse(value)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}

	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return false
	}

	return u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == "" && u.Opaque == ""
}

func validCommaSeparatedHeaderTokens(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes || value != strings.TrimSpace(value) {
		return false
	}

	seen := map[string]bool{}
	for part := range strings.SplitSeq(value, ",") {
		token := strings.TrimSpace(part)
		if !validHeaderToken(token) {
			return false
		}

		key := strings.ToLower(token)
		if seen[key] {
			return false
		}

		seen[key] = true
	}

	return true
}

func validHeaderTokenValue(value string, maxBytes int) bool {
	return value != "" && len(value) <= maxBytes && value == strings.TrimSpace(value) && validHeaderToken(value)
}

func validHeaderToken(value string) bool {
	if value == "" {
		return false
	}

	for i := range len(value) {
		if !validHeaderTokenChar(value[i]) {
			return false
		}
	}

	return true
}

func validHeaderTokenChar(c byte) bool {
	if c >= '0' && c <= '9' {
		return true
	}

	if c >= 'A' && c <= 'Z' {
		return true
	}

	if c >= 'a' && c <= 'z' {
		return true
	}

	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	default:
		return false
	}
}
