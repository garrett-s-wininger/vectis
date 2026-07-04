package logspool

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsPermanentReplayError reports whether a spooled log payload can no longer be
// delivered by retrying the same file.
func IsPermanentReplayError(err error) bool {
	if err == nil {
		return false
	}

	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound:
		return true
	default:
	}

	return containsAny(err.Error(), []string{
		"crc mismatch",
		"unmarshal chunk",
		"unsupported spool version",
		"invalid spool magic",
		"spool batch count",
		"read magic",
		"record payload length",
		"record length suffix",
		"run id is required",
		"not found: run ",
		"has no assigned shard after update",
	})
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}

	return false
}
