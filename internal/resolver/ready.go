package resolver

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const defaultWaitForReadyTimeout = 2 * time.Minute

func waitForConnReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()

	waitCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		waitCtx, cancel = context.WithTimeout(ctx, defaultWaitForReadyTimeout)
	} else {
		cancel = func() {}
	}
	defer cancel()

	for {
		switch state := conn.GetState(); state {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return fmt.Errorf("connection closed before ready")
		default:
			if !conn.WaitForStateChange(waitCtx, state) {
				if err := waitCtx.Err(); err != nil {
					return fmt.Errorf("waiting for ready: %w", err)
				}
				return fmt.Errorf("waiting for ready: state change channel closed")
			}
		}
	}
}
