package api

import (
	"testing"

	"vectis/internal/httpsecurity"
)

func TestAPIServerHTTPServerSetsMaxHeaderBytes(t *testing.T) {
	srv := (&APIServer{}).newHTTPServer("127.0.0.1:0")
	if srv.MaxHeaderBytes != httpsecurity.DefaultMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", srv.MaxHeaderBytes, httpsecurity.DefaultMaxHeaderBytes)
	}
}
