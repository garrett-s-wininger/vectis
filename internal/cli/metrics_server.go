package cli

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"vectis/internal/config"
	"vectis/internal/httpsecurity"
	"vectis/internal/interfaces"
)

const metricsHTTPShutdownTimeout = 5 * time.Second

type MetricsHTTPServer struct {
	server *http.Server
	logger interfaces.Logger
}

func StartMetricsHTTPServer(
	handler http.Handler,
	addr string,
	serviceName string,
	logger interfaces.Logger,
) (*MetricsHTTPServer, error) {
	bindHost, err := metricsBindHost(addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listen address: %w", err)
	}

	if err := config.ValidateMetricsHostConfig(bindHost); err != nil {
		return nil, fmt.Errorf("metrics host validation config: %w", err)
	}

	srv := newMetricsHTTPServer(addr, metricsServerHandlerForHost(bindHost, handler))

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for metrics: %w", err)
	}

	ln, err = config.MetricsHTTPSListener(ln)
	if err != nil {
		return nil, fmt.Errorf("metrics tls: %w", err)
	}

	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server: %v", err)
		}
	}()

	if !config.MetricsTLSInsecure() {
		logger.Info("%s metrics listening on %s (HTTPS /metrics)", serviceName, addr)
	} else {
		logger.Info("%s metrics listening on %s (/metrics)", serviceName, addr)
	}

	return &MetricsHTTPServer{server: srv, logger: logger}, nil
}

func metricsBindHost(addr string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	return host, nil
}

func newMetricsHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    httpsecurity.DefaultMaxHeaderBytes,
	}
}

func metricsServerHandler(handler http.Handler) http.Handler {
	return metricsServerHandlerForHost("localhost", handler)
}

func metricsServerHandlerForHost(bindHost string, handler http.Handler) http.Handler {
	if handler == nil {
		handler = http.NotFoundHandler()
	}

	return httpsecurity.HeaderMiddleware(httpsecurity.APIHeaderPolicy(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setMetricsNoStore(w)

		if !config.MetricsHostAllowed(bindHost, r.Host) {
			http.Error(w, "invalid host header", http.StatusBadRequest)
			return
		}

		if !httpsecurity.SafeRequestTarget(r) {
			http.Error(w, "invalid request target", http.StatusBadRequest)
			return
		}

		if _, ok := httpsecurity.MethodOverrideHeader(r); ok {
			http.Error(w, "method override headers are not allowed", http.StatusBadRequest)
			return
		}

		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}

		if !httpsecurity.MethodAllowed(r.Method, http.MethodGet) {
			w.Header().Set("Allow", httpsecurity.AllowHeader(http.MethodGet))
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if httpsecurity.RequestHasBody(r) {
			http.Error(w, "request body is not allowed", http.StatusBadRequest)
			return
		}

		if !httpsecurity.AcceptsAny(r.Header.Get("Accept"), httpsecurity.MediaTypePlainText, httpsecurity.MediaTypeOpenMetricsText) {
			http.Error(w, "not acceptable", http.StatusNotAcceptable)
			return
		}

		handler.ServeHTTP(w, r)
	}))
}

func setMetricsNoStore(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Cache-Control", "no-store")
	h.Set("Pragma", "no-cache")
	h.Set("Expires", "0")
}

func (s *MetricsHTTPServer) Shutdown() {
	if s == nil || s.server == nil {
		return
	}

	DeferShutdownWithTimeout(s.logger, "Metrics HTTP", s.server.Shutdown, metricsHTTPShutdownTimeout)()
}
