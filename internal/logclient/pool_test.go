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

func TestLogPoolRoutesRunToStableShard(t *testing.T) {
	a := &recordingLogClient{endpointID: "log-a"}
	b := &recordingLogClient{endpointID: "log-b"}
	c := &recordingLogClient{endpointID: "log-c"}
	p := &logPool{
		logger: mocks.NopLogger{},
		active: []*logEndpoint{
			{id: a.endpointID, writer: a},
			{id: b.endpointID, writer: b},
			{id: c.endpointID, writer: c},
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
