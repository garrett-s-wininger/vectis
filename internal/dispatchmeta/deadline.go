package dispatchmeta

import (
	"strconv"
	"time"

	api "vectis/api/gen/go"
)

const StartDeadlineUnixNanoKey = "vectis.dispatch.start_deadline_unix_nano"

func DeadlineUnixNano(now time.Time, ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}

	return now.UTC().Add(ttl).UnixNano()
}

func StampStartDeadline(req *api.JobRequest, deadlineUnixNano int64) {
	if req == nil || deadlineUnixNano <= 0 {
		return
	}

	if req.Metadata == nil {
		req.Metadata = map[string]string{}
	}

	req.Metadata[StartDeadlineUnixNanoKey] = strconv.FormatInt(deadlineUnixNano, 10)
}

func StartDeadlineFromMetadata(metadata map[string]string) (int64, bool) {
	if len(metadata) == 0 {
		return 0, false
	}

	raw := metadata[StartDeadlineUnixNanoKey]
	if raw == "" {
		return 0, false
	}

	deadline, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || deadline <= 0 {
		return 0, false
	}

	return deadline, true
}

func IsExpired(req *api.JobRequest, now time.Time) bool {
	deadline, ok := StartDeadlineFromMetadata(req.GetMetadata())
	return ok && deadline <= now.UTC().UnixNano()
}
