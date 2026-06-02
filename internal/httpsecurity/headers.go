package httpsecurity

import "net/http"

const (
	apiContentSecurityPolicy  = "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
	docsContentSecurityPolicy = "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; " +
		"img-src 'self' data:; font-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self'"
	defaultHSTS = "max-age=31536000"
)

// Policy describes browser-facing HTTP response headers.
type Policy struct {
	ContentSecurityPolicy string
}

// APIHeaderPolicy returns strict defaults for JSON/API responses.
func APIHeaderPolicy() Policy {
	return Policy{ContentSecurityPolicy: apiContentSecurityPolicy}
}

// DocsHeaderPolicy returns defaults that still allow the static docs app to run.
func DocsHeaderPolicy() Policy {
	return Policy{ContentSecurityPolicy: docsContentSecurityPolicy}
}

// HeaderMiddleware applies baseline browser security headers. Existing handler
// values win so narrower routes can set stricter or more specific policies.
func HeaderMiddleware(policy Policy, next http.Handler) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		setHeaderIfEmpty(h, "X-Content-Type-Options", "nosniff")
		setHeaderIfEmpty(h, "X-Frame-Options", "DENY")
		setHeaderIfEmpty(h, "Referrer-Policy", "no-referrer")
		setHeaderIfEmpty(h, "Permissions-Policy", "camera=(), geolocation=(), microphone=(), payment=(), usb=()")

		if policy.ContentSecurityPolicy != "" {
			setHeaderIfEmpty(h, "Content-Security-Policy", policy.ContentSecurityPolicy)
		}

		if r != nil && r.TLS != nil {
			setHeaderIfEmpty(h, "Strict-Transport-Security", defaultHSTS)
		}

		next.ServeHTTP(w, r)
	})
}

func setHeaderIfEmpty(h http.Header, key, value string) {
	if h.Get(key) == "" {
		h.Set(key, value)
	}
}
