package api

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func instrumentHTTPServer(next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "",
		otelhttp.WithFilter(apiHTTPInstrumentationFilter),
		otelhttp.WithSpanNameFormatter(apiHTTPSpanName),
	)
}

func apiHTTPInstrumentationFilter(r *http.Request) bool {
	switch r.URL.Path {
	case "/metrics", "/health/live", "/health/ready":
		return false
	default:
		return true
	}
}

func apiHTTPSpanName(_ string, r *http.Request) string {
	if pat := r.Pattern; pat != "" {
		return r.Method + " " + pat
	}

	return r.Method + " " + r.URL.Path
}
