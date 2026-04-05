package queue

import (
	api "vectis/api/gen/go"
)

func MetricsSnapshot(svc api.QueueServiceServer) (pending int64, inflight int64) {
	qs, ok := svc.(*queueServer)
	if !ok {
		return 0, 0
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()

	return int64(qs.size), int64(len(qs.inflight))
}
