package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"vectis/internal/config"
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
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", handler)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

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

func (s *MetricsHTTPServer) Shutdown() {
	if s == nil || s.server == nil {
		return
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), metricsHTTPShutdownTimeout)
	defer cancel()
	if err := s.server.Shutdown(shutCtx); err != nil {
		s.logger.Warn("Metrics HTTP shutdown: %v", err)
	}
}
