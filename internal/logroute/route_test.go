package logroute

import (
	"strings"
	"testing"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/proto"
)

func TestStreamRouteBindRejectsMixedRuns(t *testing.T) {
	var route StreamRoute
	if _, err := route.Bind(&api.LogChunk{RunId: proto.String("run-1")}); err != nil {
		t.Fatalf("bind first chunk: %v", err)
	}

	_, err := route.Bind(&api.LogChunk{RunId: proto.String("run-2")})
	if err == nil || !strings.Contains(err.Error(), "run") {
		t.Fatalf("bind mixed run error = %v, want run mismatch", err)
	}
}

func TestStreamRouteBindRejectsConflictingShardHints(t *testing.T) {
	var route StreamRoute
	if _, err := route.Bind(&api.LogChunk{
		RunId:      proto.String("run-1"),
		LogShardId: proto.String("log-a"),
	}); err != nil {
		t.Fatalf("bind first chunk: %v", err)
	}

	_, err := route.Bind(&api.LogChunk{
		RunId:      proto.String("run-1"),
		LogShardId: proto.String("log-b"),
	})

	if err == nil || !strings.Contains(err.Error(), "shard") {
		t.Fatalf("bind shard mismatch error = %v, want shard mismatch", err)
	}
}

func TestStreamRouteBindRequiresRunID(t *testing.T) {
	var route StreamRoute
	_, err := route.Bind(&api.LogChunk{})
	if err == nil || !strings.Contains(err.Error(), "run_id") {
		t.Fatalf("bind missing run error = %v, want run_id requirement", err)
	}
}
