package logroute

import (
	"fmt"
	"strings"

	api "vectis/api/gen/go"
)

type Route struct {
	RunID      string
	LogShardID string
}

type StreamRoute struct {
	route Route
	bound bool
}

func FromChunk(chunk *api.LogChunk) Route {
	if chunk == nil {
		return Route{}
	}

	return Route{
		RunID:      chunk.GetRunId(),
		LogShardID: chunk.GetLogShardId(),
	}
}

func (r Route) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return fmt.Errorf("log chunk run_id is required")
	}

	if r.RunID != strings.TrimSpace(r.RunID) {
		return fmt.Errorf("log chunk run_id must not contain leading or trailing whitespace")
	}

	if r.LogShardID != strings.TrimSpace(r.LogShardID) {
		return fmt.Errorf("log chunk log_shard_id must not contain leading or trailing whitespace")
	}

	return nil
}

func (s *StreamRoute) Bind(chunk *api.LogChunk) (Route, error) {
	route := FromChunk(chunk)
	if err := route.Validate(); err != nil {
		return Route{}, err
	}

	if !s.bound {
		s.route = route
		s.bound = true
		return s.route, nil
	}

	if route.RunID != s.route.RunID {
		return Route{}, fmt.Errorf("log stream already routed to run %q, got chunk for run %q", s.route.RunID, route.RunID)
	}

	if s.route.LogShardID == "" {
		s.route.LogShardID = route.LogShardID
		return s.route, nil
	}

	if route.LogShardID != "" && route.LogShardID != s.route.LogShardID {
		return Route{}, fmt.Errorf("log stream already routed to shard %q, got chunk for shard %q", s.route.LogShardID, route.LogShardID)
	}

	return s.route, nil
}
