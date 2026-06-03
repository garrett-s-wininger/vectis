package api

import (
	"net/http"
	"sort"
	"strings"
)

var apiAllowedMethodOrder = []string{
	http.MethodGet,
	http.MethodHead,
	http.MethodPost,
	http.MethodPut,
	http.MethodDelete,
	http.MethodOptions,
}

type apiRouteIndex struct {
	routes []apiRoutePattern
}

type apiRoutePattern struct {
	method      string
	pathPattern string
	segments    []string
}

type apiRouteMatch struct {
	found       bool
	pathPattern string
	methods     map[string]bool
}

func newAPIRouteIndex(specs []routeSpec) *apiRouteIndex {
	routes := make([]apiRoutePattern, 0, len(specs))
	for _, spec := range specs {
		method, pathPattern, ok := splitRoutePattern(spec.Pattern)
		if !ok {
			continue
		}

		routes = append(routes, apiRoutePattern{
			method:      method,
			pathPattern: pathPattern,
			segments:    routePathSegments(pathPattern),
		})
	}

	return &apiRouteIndex{routes: routes}
}

func splitRoutePattern(pattern string) (method, pathPattern string, ok bool) {
	method, pathPattern, ok = strings.Cut(strings.TrimSpace(pattern), " ")
	if !ok {
		return "", "", false
	}

	method = strings.ToUpper(strings.TrimSpace(method))
	pathPattern = strings.TrimSpace(pathPattern)
	return method, pathPattern, method != "" && strings.HasPrefix(pathPattern, "/")
}

func (idx *apiRouteIndex) match(requestPath string) apiRouteMatch {
	if idx == nil {
		return apiRouteMatch{}
	}

	requestSegments := routePathSegments(requestPath)
	var matched apiRouteMatch
	for _, route := range idx.routes {
		if !routeSegmentsMatch(route.segments, requestSegments) {
			continue
		}

		if !matched.found {
			matched = apiRouteMatch{
				found:       true,
				pathPattern: route.pathPattern,
				methods:     make(map[string]bool),
			}
		}

		matched.methods[route.method] = true
	}

	return matched
}

func routePathSegments(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}

	return strings.Split(path, "/")
}

func routeSegmentsMatch(pattern, request []string) bool {
	if len(pattern) != len(request) {
		return false
	}

	for i, segment := range pattern {
		if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			continue
		}

		if segment != request[i] {
			return false
		}
	}

	return true
}

func (m apiRouteMatch) methodAllowed(method string) bool {
	method = strings.TrimSpace(method)
	if m.methods[method] {
		return true
	}

	return method == http.MethodHead && m.methods[http.MethodGet]
}

func (m apiRouteMatch) allowHeader() string {
	if !m.found {
		return ""
	}

	allowed := make(map[string]bool, len(m.methods)+1)
	for method := range m.methods {
		allowed[method] = true
	}

	if allowed[http.MethodGet] {
		allowed[http.MethodHead] = true
	}

	var methods []string
	for _, method := range apiAllowedMethodOrder {
		if allowed[method] {
			methods = append(methods, method)
			delete(allowed, method)
		}
	}

	var extra []string
	for method := range allowed {
		extra = append(extra, method)
	}

	sort.Strings(extra)
	methods = append(methods, extra...)

	return strings.Join(methods, ", ")
}

func (m apiRouteMatch) securityRoute() string {
	if !m.found || m.pathPattern == "" {
		return securityRejectionUnknownRoute
	}

	return m.pathPattern
}

func disallowedHTTPMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodTrace, "TRACK", http.MethodConnect:
		return true
	default:
		return false
	}
}

func (s *APIServer) routeGuardMiddleware(index *apiRouteIndex, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		match := index.match(r.URL.Path)

		if disallowedHTTPMethod(r.Method) {
			s.recordSecurityRejectionForRoute(r, securityReasonUnsupportedHTTPMethod, match.securityRoute(), http.StatusMethodNotAllowed)
			writeMethodNotAllowed(w, match.allowHeader())
			return
		}

		if !match.found {
			setNoStore(w)
			writeAPIErrorCode(w, http.StatusNotFound, apiErrRouteNotFound)
			return
		}

		if !match.methodAllowed(r.Method) {
			s.recordSecurityRejectionForRoute(r, securityReasonMethodNotAllowed, match.securityRoute(), http.StatusMethodNotAllowed)
			writeMethodNotAllowed(w, match.allowHeader())
			return
		}

		next.ServeHTTP(w, r)
	})
}
