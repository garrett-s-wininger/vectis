package logspool

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsPermanentReplayError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "transient", err: errors.New("temporary unavailable"), want: false},
		{name: "grpc not found", err: status.Error(codes.NotFound, "run missing"), want: true},
		{name: "wrapped invalid argument", err: fmt.Errorf("send chunk: %w", status.Error(codes.InvalidArgument, "bad run")), want: true},
		{name: "missing run id", err: errors.New("send chunk: run id is required"), want: true},
		{name: "stale run", err: errors.New("create stream: not found: run stale-run"), want: true},
		{name: "assignment lost", err: errors.New("assign log shard: run stale-run has no assigned shard after update"), want: true},
		{name: "unavailable assigned endpoint", err: errors.New("assigned log endpoint \"log-a\" is not available"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanentReplayError(tt.err); got != tt.want {
				t.Fatalf("IsPermanentReplayError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
