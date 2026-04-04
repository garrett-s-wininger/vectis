package api

import (
	"log/slog"
	"net/http"
	"time"

	"vectis/internal/observability"
)

type statusResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.status = http.StatusOK
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(b)
}

func accessLogMiddleware(log *slog.Logger, skip func(*http.Request) bool, next http.Handler) http.Handler {
	if log == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skip(r) {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		sw := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)

		status := sw.status
		if !sw.wroteHeader {
			status = http.StatusOK
		}

		route := r.Pattern
		if route == "" {
			route = r.URL.Path
		}

		log.LogAttrs(r.Context(), slog.LevelInfo, "http_request",
			slog.String("component", "vectis-api"),
			slog.String("correlation_id", observability.CorrelationID(r.Context())),
			slog.String("method", r.Method),
			slog.String("http_route", route),
			slog.Int("status", status),
			slog.Duration("duration", time.Since(start)),
		)
	})
}
