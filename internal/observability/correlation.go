package observability

import (
	"context"
	"net/http"
	"strings"
	"unicode"

	"github.com/google/uuid"
)

const maxCorrelationIDLen = 128

type correlationIDKey struct{}

func CorrelationID(ctx context.Context) string {
	v, _ := ctx.Value(correlationIDKey{}).(string)
	return v
}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, id)
}

func CorrelationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := pickCorrelationID(r.Header)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(WithCorrelationID(r.Context(), id)))
	})
}

func pickCorrelationID(h http.Header) string {
	for _, key := range []string{"X-Request-ID", "X-Correlation-ID"} {
		v := strings.TrimSpace(h.Get(key))
		if validCorrelationID(v) {
			return v
		}
	}

	return uuid.NewString()
}

func validCorrelationID(s string) bool {
	if s == "" || len(s) > maxCorrelationIDLen {
		return false
	}

	for _, r := range s {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return false
		}
	}

	return true
}
