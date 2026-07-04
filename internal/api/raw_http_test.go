package api

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"vectis/internal/httpsecurity"
)

func TestAPIServerRawHTTPRejectsMalformedFramingBeforeHandler(t *testing.T) {
	tests := []struct {
		name       string
		request    string
		wantStatus string
	}{
		{
			name: "conflicting content length",
			request: "GET /health/live HTTP/1.1\r\n" +
				"Host: localhost\r\n" +
				"Connection: close\r\n" +
				"Content-Length: 1\r\n" +
				"Content-Length: 2\r\n" +
				"\r\n",
			wantStatus: "HTTP/1.1 400",
		},
		{
			name: "invalid content length",
			request: "GET /health/live HTTP/1.1\r\n" +
				"Host: localhost\r\n" +
				"Connection: close\r\n" +
				"Content-Length: not-a-number\r\n" +
				"\r\n",
			wantStatus: "HTTP/1.1 400",
		},
		{
			name: "unsupported transfer encoding",
			request: "GET /health/live HTTP/1.1\r\n" +
				"Host: localhost\r\n" +
				"Connection: close\r\n" +
				"Transfer-Encoding: gzip\r\n" +
				"\r\n",
			wantStatus: "HTTP/1.1 501",
		},
		{
			name: "duplicate transfer encoding with unsupported value",
			request: "GET /health/live HTTP/1.1\r\n" +
				"Host: localhost\r\n" +
				"Connection: close\r\n" +
				"Transfer-Encoding: chunked\r\n" +
				"Transfer-Encoding: gzip\r\n" +
				"\r\n",
			wantStatus: "HTTP/1.1 501",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, hits := startRawHTTPParserTestServer(t)

			statusLine := rawHTTPStatusLine(t, addr, tt.request)
			if !strings.HasPrefix(statusLine, tt.wantStatus) {
				t.Fatalf("status line=%q, want prefix %q", statusLine, tt.wantStatus)
			}

			if got := hits.Load(); got != 0 {
				t.Fatalf("handler hit count=%d, want 0", got)
			}
		})
	}
}

func TestAPIServerRawHTTPRejectsOversizedHeadersBeforeHandler(t *testing.T) {
	addr, hits := startRawHTTPParserTestServer(t)
	request := "GET /health/live HTTP/1.1\r\n" +
		"Host: localhost\r\n" +
		"Connection: close\r\n" +
		"X-Fill: " + strings.Repeat("a", httpsecurity.DefaultMaxHeaderBytes*2) + "\r\n" +
		"\r\n"

	statusLine := rawHTTPStatusLine(t, addr, request)
	if !strings.HasPrefix(statusLine, "HTTP/1.1 431") {
		t.Fatalf("status line=%q, want 431", statusLine)
	}

	if got := hits.Load(); got != 0 {
		t.Fatalf("handler hit count=%d, want 0", got)
	}
}

func startRawHTTPParserTestServer(t *testing.T) (string, *atomic.Int32) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var hits atomic.Int32
	srv := (&APIServer{}).newHTTPServer(ln.Addr().String())
	srv.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()

	t.Cleanup(func() {
		_ = srv.Close()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("Serve returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("timeout waiting for raw HTTP test server shutdown")
		}
	})

	return ln.Addr().String(), &hits
}

func rawHTTPStatusLine(t *testing.T, addr, request string) string {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}

	statusLine, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read status line: %v", err)
	}

	return strings.TrimSpace(statusLine)
}
