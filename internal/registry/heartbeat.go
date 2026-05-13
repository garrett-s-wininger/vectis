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
	return StartRegistrationHeartbeatWithMetadata(ctx, r, comp, address, nil, interval, logger)
}

func StartRegistrationHeartbeatWithMetadata(ctx context.Context, r *Registry, comp api.Component, address string, metadata map[string]string, interval time.Duration, logger interfaces.Logger) (stop func()) {
	return StartInstanceRegistrationHeartbeatWithMetadata(ctx, r, comp, "", address, metadata, interval, logger)
}

func StartInstanceRegistrationHeartbeat(ctx context.Context, r *Registry, comp api.Component, instanceID, address string, interval time.Duration, logger interfaces.Logger) (stop func()) {
	return StartInstanceRegistrationHeartbeatWithMetadata(ctx, r, comp, instanceID, address, nil, interval, logger)
}

func StartInstanceRegistrationHeartbeatWithMetadata(ctx context.Context, r *Registry, comp api.Component, instanceID, address string, metadata map[string]string, interval time.Duration, logger interfaces.Logger) (stop func()) {
	if r == nil || interval <= 0 || logger == nil {
		return func() {}
	}

	metadata = cloneMetadata(metadata)
	loopCtx, cancel := context.WithCancel(ctx)
	go registrationHeartbeatLoop(loopCtx, r, comp, instanceID, address, metadata, interval, logger)
	return cancel
}

func registrationHeartbeatLoop(ctx context.Context, r *Registry, comp api.Component, instanceID, address string, metadata map[string]string, interval time.Duration, logger interfaces.Logger) {
	for {
		wait := intervalWithJitter(interval)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		hbCtx, cancel := context.WithTimeout(ctx, registrationHeartbeatRPCTimeout)
		err := r.RegisterInstanceOnceWithMetadata(hbCtx, comp, instanceID, address, metadata)
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
