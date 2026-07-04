package resolver

import (
	"context"

	"vectis/internal/backoff"
	"vectis/internal/interfaces"

	"google.golang.org/grpc"
)

func DialOrchestrator(ctx context.Context, logger interfaces.Logger, pinnedOrchestratorAddr, registryDialAddr string, retryMetrics backoff.RetryMetrics) (*grpc.ClientConn, func(), error) {
	dial, cleanup, err := DialOrchestratorWithOwner(ctx, logger, pinnedOrchestratorAddr, registryDialAddr, "", "", retryMetrics)
	if err != nil {
		return nil, nil, err
	}

	return dial.Conn, cleanup, nil
}
