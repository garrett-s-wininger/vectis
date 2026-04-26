package resolver

import (
	"context"
	"fmt"
	"net"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/registry"
)

// ResolveLogSSEAddress returns the HTTP SSE endpoint for vectis-log.
// It first checks for a pinned address; if none is set, it resolves
// the log gRPC address via the registry and derives the SSE port.
func ResolveLogSSEAddress(ctx context.Context, logger interfaces.Logger, registryDialAddr string) (string, error) {
	if pinned := config.APILogSSEAddress(); pinned != "" {
		logger.Debug("resolver: using pinned log SSE address: %s", pinned)
		return pinned, nil
	}

	if registryDialAddr == "" {
		return "", fmt.Errorf("resolver: no pinned log SSE address and no registry dial address configured")
	}

	logger.Debug("resolver: looking up log SSE address via registry at %s", registryDialAddr)

	regClient, err := registry.New(ctx, registryDialAddr, logger, interfaces.SystemClock{})
	if err != nil {
		return "", fmt.Errorf("resolver: connect to registry: %w", err)
	}
	defer regClient.Close()

	grpcAddr, err := regClient.Address(ctx, api.Component_COMPONENT_LOG)
	if err != nil {
		return "", fmt.Errorf("resolver: lookup log address from registry: %w", err)
	}

	host, _, err := net.SplitHostPort(grpcAddr)
	if err != nil {
		// grpcAddr might not have a port (e.g., unix socket or bare hostname)
		// Fall back to using the whole address as the host.
		host = grpcAddr
	}

	ssePort := config.LogWebSocketPort()
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", ssePort))
	logger.Debug("resolver: derived log SSE address %s from registry gRPC address %s", addr, grpcAddr)
	return addr, nil
}

// ResolveLogSSEAddressWithTimeout wraps ResolveLogSSEAddress with a bounded timeout.
func ResolveLogSSEAddressWithTimeout(logger interfaces.Logger, registryDialAddr string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ResolveLogSSEAddress(ctx, logger, registryDialAddr)
}
