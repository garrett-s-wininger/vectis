package main

import (
	"net/http"
	"vectis/internal/httpsecurity"
)

func (b *uiBackend) handler(assets http.Handler, devAssets bool) http.Handler {
	mux := http.NewServeMux()
	api := b.apiProxyHandler()
	mux.Handle("GET /ui/api/context", http.HandlerFunc(b.context))
	mux.Handle("POST /ui/api/setup/complete", http.HandlerFunc(b.completeSetup))
	mux.Handle("POST /ui/api/login", http.HandlerFunc(b.login))
	mux.Handle("POST /ui/api/logout", http.HandlerFunc(b.logout))
	mux.Handle("GET /api/", api)
	mux.Handle("POST /api/", api)
	mux.Handle("PUT /api/", api)
	mux.Handle("PATCH /api/", api)
	mux.Handle("DELETE /api/", api)
	mux.Handle("GET /", b.spaGateWithDevAssets(assets, devAssets))
	mux.Handle("GET /health/live", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}))

	return httpsecurity.HeaderMiddleware(httpsecurity.Policy{
		StrictTransportSecurity: "max-age=31536000",
		RequestSecure: func(r *http.Request) bool {
			return r.TLS != nil || (b.publicURL != nil && b.publicURL.Scheme == "https")
		},
	}, mux)
}
