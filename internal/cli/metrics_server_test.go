package cli

import (
	"net/http"
	"testing"

	"vectis/internal/httpsecurity"
)

func TestNewMetricsHTTPServerSetsMaxHeaderBytes(t *testing.T) {
	srv := newMetricsHTTPServer("127.0.0.1:0", http.NotFoundHandler())
	if srv.MaxHeaderBytes != httpsecurity.DefaultMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", srv.MaxHeaderBytes, httpsecurity.DefaultMaxHeaderBytes)
	}
}
