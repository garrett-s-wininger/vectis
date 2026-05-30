package logclient

import (
	"context"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
)

type recordingLogClient struct {
	endpointID string
	chunks     []*api.LogChunk
}

func (c *recordingLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return &recordingLogStream{client: c}, nil
}

func (c *recordingLogClient) Close() error {
	return nil
}

type recordingLogStream struct {
	client *recordingLogClient
}

func (s *recordingLogStream) Send(chunk *api.LogChunk) error {
	s.client.chunks = append(s.client.chunks, chunk)
	return nil
}

func (s *recordingLogStream) CloseSend() error {
	return nil
}

type memoryAssignmentStore struct {
	shardID string
	set     bool
}

type recordingRoutingMetrics struct {
	assignments []string
	failures    []routingFailure
}

type routingFailure struct {
	operation string
	reason    string
}

func (m *recordingRoutingMetrics) RecordShardAssignment(_ context.Context, outcome string) {
	m.assignments = append(m.assignments, outcome)
}

func (m *recordingRoutingMetrics) RecordShardRouteFailure(_ context.Context, operation, reason string) {
	m.failures = append(m.failures, routingFailure{operation: operation, reason: reason})
}

func (s *memoryAssignmentStore) GetLogShard(context.Context, string) (string, bool, error) {
	return s.shardID, s.set, nil
}

func (s *memoryAssignmentStore) AssignLogShard(_ context.Context, _ string, shardID string) (string, error) {
	if !s.set {
		s.shardID = shardID
		s.set = true
	}

	return s.shardID, nil
}

func TestLogPoolRoutesRunToStableShard(t *testing.T) {
	a := &recordingLogClient{endpointID: "log-a"}
	b := &recordingLogClient{endpointID: "log-b"}
	c := &recordingLogClient{endpointID: "log-c"}
	p := &logPool{
		logger: mocks.NopLogger{},
		active: []*logEndpoint{
			{id: a.endpointID, writer: a, writable: true},
			{id: b.endpointID, writer: b, writable: true},
			{id: c.endpointID, writer: c, writable: true},
		},
	}

	expected, err := p.chooseEndpoint("run-1")
	if err != nil {
		t.Fatalf("choose endpoint: %v", err)
	}

	stream, err := p.streamLogs(context.Background())
	if err != nil {
		t.Fatalf("stream logs: %v", err)
	}

	runID := "run-1"
	if err := stream.Send(&api.LogChunk{RunId: &runID}); err != nil {
		t.Fatalf("send first chunk: %v", err)
	}

	var got string
	for _, client := range []*recordingLogClient{a, b, c} {
		if len(client.chunks) > 0 {
			got = client.endpointID
		}
	}

	if got != expected.id {
		t.Fatalf("expected run routed to %s, got %s", expected.id, got)
	}

	otherRunID := "run-2"
	if err := stream.Send(&api.LogChunk{RunId: &otherRunID}); err == nil {
		t.Fatal("expected mixed-run stream to be rejected")
	}
}

func TestLogPoolAssignsWritableShardForNewRun(t *testing.T) {
	readOnly := &recordingLogClient{endpointID: "log-read-only"}
	writable := &recordingLogClient{endpointID: "log-writable"}
	assignments := &memoryAssignmentStore{}
	metrics := &recordingRoutingMetrics{}
	p := &logPool{
		logger: mocks.NopLogger{},
		opts:   PoolOptions{AssignmentStore: assignments, Metrics: metrics},
		active: []*logEndpoint{
			{id: readOnly.endpointID, writer: readOnly, writable: false},
			{id: writable.endpointID, writer: writable, writable: true},
		},
	}

	runID := "run-new"
	stream, err := p.streamLogsForRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("stream logs for run: %v", err)
	}

	if err := stream.Send(&api.LogChunk{RunId: &runID}); err != nil {
		t.Fatalf("send chunk: %v", err)
	}

	if assignments.shardID != writable.endpointID {
		t.Fatalf("assigned shard = %q, want %q", assignments.shardID, writable.endpointID)
	}

	if len(writable.chunks) != 1 {
		t.Fatalf("expected writable shard to receive chunk, got %d", len(writable.chunks))
	}

	if len(readOnly.chunks) != 0 {
		t.Fatalf("expected read-only shard to receive no chunks, got %d", len(readOnly.chunks))
	}

	if len(metrics.assignments) != 1 || metrics.assignments[0] != ShardAssignmentNew {
		t.Fatalf("assignment metrics = %v, want [%s]", metrics.assignments, ShardAssignmentNew)
	}
}

func TestLogPoolAssignLogShardForRunStoresWritableChoice(t *testing.T) {
	readOnly := &recordingLogClient{endpointID: "log-read-only"}
	writable := &recordingLogClient{endpointID: "log-writable"}
	assignments := &memoryAssignmentStore{}
	p := &logPool{
		logger: mocks.NopLogger{},
		opts:   PoolOptions{AssignmentStore: assignments},
		active: []*logEndpoint{
			{id: readOnly.endpointID, writer: readOnly, writable: false},
			{id: writable.endpointID, writer: writable, writable: true},
		},
	}

	shardID, err := p.assignLogShardForRun(context.Background(), "run-new")
	if err != nil {
		t.Fatalf("assign log shard for run: %v", err)
	}

	if shardID != writable.endpointID {
		t.Fatalf("assigned shard = %q, want %q", shardID, writable.endpointID)
	}

	if assignments.shardID != writable.endpointID {
		t.Fatalf("stored shard = %q, want %q", assignments.shardID, writable.endpointID)
	}
}

func TestLogPoolStreamsAssignedRunToShard(t *testing.T) {
	a := &recordingLogClient{endpointID: "log-a"}
	b := &recordingLogClient{endpointID: "log-b"}
	p := &logPool{
		logger: mocks.NopLogger{},
		active: []*logEndpoint{
			{id: a.endpointID, writer: a, writable: true},
			{id: b.endpointID, writer: b, writable: true},
		},
	}

	runID := "run-1"
	stream, err := p.streamLogsForAssignedRun(context.Background(), runID, b.endpointID)
	if err != nil {
		t.Fatalf("stream logs for assigned run: %v", err)
	}

	if err := stream.Send(&api.LogChunk{RunId: &runID}); err != nil {
		t.Fatalf("send chunk: %v", err)
	}

	if len(b.chunks) != 1 {
		t.Fatalf("expected assigned shard to receive chunk, got %d", len(b.chunks))
	}

	if len(a.chunks) != 0 {
		t.Fatalf("expected unassigned shard to receive no chunks, got %d", len(a.chunks))
	}
}

func TestLogPoolRecordsAssignedShardUnavailable(t *testing.T) {
	writable := &recordingLogClient{endpointID: "log-writable"}
	assignments := &memoryAssignmentStore{shardID: "log-missing", set: true}
	metrics := &recordingRoutingMetrics{}
	p := &logPool{
		logger: mocks.NopLogger{},
		opts:   PoolOptions{AssignmentStore: assignments, Metrics: metrics},
		active: []*logEndpoint{
			{id: writable.endpointID, writer: writable, writable: true},
		},
	}

	if _, err := p.streamLogsForRun(context.Background(), "run-existing"); err == nil {
		t.Fatal("expected unavailable assigned shard error")
	}

	if len(metrics.failures) != 1 {
		t.Fatalf("failure metrics = %v, want one failure", metrics.failures)
	}

	got := metrics.failures[0]
	if got.operation != ShardRouteOperationWrite || got.reason != ShardRouteFailureAssignedUnavailable {
		t.Fatalf("failure metric = %+v, want %s/%s", got, ShardRouteOperationWrite, ShardRouteFailureAssignedUnavailable)
	}
}

func TestLogPoolUsesAssignedReadOnlyShard(t *testing.T) {
	readOnly := &recordingLogClient{endpointID: "log-read-only"}
	writable := &recordingLogClient{endpointID: "log-writable"}
	assignments := &memoryAssignmentStore{shardID: readOnly.endpointID, set: true}
	p := &logPool{
		logger: mocks.NopLogger{},
		opts:   PoolOptions{AssignmentStore: assignments},
		active: []*logEndpoint{
			{id: readOnly.endpointID, writer: readOnly, writable: false},
			{id: writable.endpointID, writer: writable, writable: true},
		},
	}

	runID := "run-existing"
	stream, err := p.streamLogsForRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("stream logs for run: %v", err)
	}

	if err := stream.Send(&api.LogChunk{RunId: &runID}); err != nil {
		t.Fatalf("send chunk: %v", err)
	}

	if len(readOnly.chunks) != 1 {
		t.Fatalf("expected assigned read-only shard to receive chunk, got %d", len(readOnly.chunks))
	}

	if len(writable.chunks) != 0 {
		t.Fatalf("expected writable shard to receive no chunks, got %d", len(writable.chunks))
	}
}
