package httpsecurity

import (
	"net/http"
	"sort"
	"strings"
)

var methodAllowHeaderOrder = []string{
	http.MethodGet,
	http.MethodHead,
	http.MethodPost,
	http.MethodPut,
	http.MethodDelete,
	http.MethodOptions,
}

func DangerousHTTPMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodTrace, "TRACK", http.MethodConnect:
		return true
	default:
		return false
	}
}

func MethodAllowed(method string, allowed ...string) bool {
	method = strings.TrimSpace(method)
	for _, candidate := range allowed {
		candidate = strings.TrimSpace(candidate)
		if method == candidate {
			return true
		}

		if method == http.MethodHead && candidate == http.MethodGet {
			return true
		}
	}

	return false
}

func AllowHeader(allowed ...string) string {
	allow := make(map[string]bool, len(allowed)+1)
	for _, method := range allowed {
		method = strings.TrimSpace(method)
		if method == "" {
			continue
		}

		allow[method] = true
	}

	if allow[http.MethodGet] {
		allow[http.MethodHead] = true
	}

	var ordered []string
	for _, method := range methodAllowHeaderOrder {
		if allow[method] {
			ordered = append(ordered, method)
			delete(allow, method)
		}
	}

	var extra []string
	for method := range allow {
		extra = append(extra, method)
	}

	sort.Strings(extra)
	ordered = append(ordered, extra...)

	return strings.Join(ordered, ", ")
}
