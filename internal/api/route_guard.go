package api

import (
	"net/http"
	pathpkg "path"
	"strings"

	"vectis/internal/httpsecurity"
)

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
	return httpsecurity.MethodAllowed(method, m.allowedMethods()...)
}

func (m apiRouteMatch) allowHeader() string {
	if !m.found {
		return ""
	}

	return httpsecurity.AllowHeader(m.allowedMethods()...)
}

func (m apiRouteMatch) allowedMethods() []string {
	allowed := make([]string, 0, len(m.methods))
	for method := range m.methods {
		allowed = append(allowed, method)
	}

	return allowed
}

func (m apiRouteMatch) securityRoute() string {
	if !m.found || m.pathPattern == "" {
		return securityRejectionUnknownRoute
	}

	return m.pathPattern
}

func (s *APIServer) routeGuardMiddleware(index *apiRouteIndex, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		match := index.match(apiRequestPath(r))

		if !apiRequestTargetAllowed(r) {
			s.recordSecurityRejectionForRoute(r, securityReasonRequestTargetInvalid, match.securityRoute(), http.StatusBadRequest)
			setNoStore(w)
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestTarget)
			return
		}

		if httpsecurity.DangerousHTTPMethod(r.Method) {
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

func apiRequestPath(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}

	return r.URL.Path
}

func apiRequestTargetAllowed(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}

	if r.URL.IsAbs() || r.URL.Host != "" || r.URL.Opaque != "" {
		return false
	}

	requestPath := r.URL.Path
	if requestPath == "" || requestPath[0] != '/' || requestPath == "*" {
		return false
	}

	return pathpkg.Clean(requestPath) == requestPath
}
