package registry

import (
	"context"
	"math/rand/v2"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
)

const registrationHeartbeatRPCTimeout = 30 * time.Second

func StartRegistrationHeartbeat(ctx context.Context, r *Registry, comp api.Component, address string, interval time.Duration, logger interfaces.Logger) (stop func()) {
	if r == nil || interval <= 0 || logger == nil {
		return func() {}
	}

	loopCtx, cancel := context.WithCancel(ctx)
	go registrationHeartbeatLoop(loopCtx, r, comp, address, interval, logger)
	return cancel
}

func registrationHeartbeatLoop(ctx context.Context, r *Registry, comp api.Component, address string, interval time.Duration, logger interfaces.Logger) {
	for {
		wait := intervalWithJitter(interval)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		hbCtx, cancel := context.WithTimeout(ctx, registrationHeartbeatRPCTimeout)
		err := r.RegisterOnce(hbCtx, comp, address)
		cancel()
		if err != nil {
			logger.Debug("Registry registration heartbeat failed for %s: %v", comp.String(), err)
		}
	}
}

func intervalWithJitter(interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}

	maxJitter := interval / 4
	if maxJitter <= 0 {
		return interval
	}

	return interval + time.Duration(rand.Int64N(int64(maxJitter)))
}
