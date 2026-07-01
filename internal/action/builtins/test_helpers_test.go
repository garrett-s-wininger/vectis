package builtins

import (
	"context"
	"errors"
	"strings"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces"

	"google.golang.org/grpc/metadata"
)

type mockLogStream struct {
	mu      sync.Mutex
	chunks  []*api.LogChunk
	sendErr error
}

func (m *mockLogStream) Send(chunk *api.LogChunk) error {
	if m.sendErr != nil {
		return m.sendErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.chunks = append(m.chunks, chunk)
	return nil
}

func (m *mockLogStream) CloseAndRecv() (*api.Empty, error) {
	return &api.Empty{}, nil
}

func (m *mockLogStream) GetChunks() []*api.LogChunk {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*api.LogChunk, len(m.chunks))
	copy(result, m.chunks)
	return result
}

func (m *mockLogStream) Context() context.Context {
	return context.Background()
}

func (m *mockLogStream) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (m *mockLogStream) Trailer() metadata.MD {
	return nil
}

func (m *mockLogStream) CloseSend() error {
	return nil
}

func (m *mockLogStream) RecvMsg(msg any) error {
	return errors.New("not implemented")
}

func (m *mockLogStream) SendMsg(msg any) error {
	return errors.New("not implemented")
}

var _ api.LogService_StreamLogsClient = (*mockLogStream)(nil)

func createTestState(logStream api.LogService_StreamLogsClient) *action.ExecutionState {
	return &action.ExecutionState{
		JobID:      "test-job",
		Workspace:  "/tmp/vectis-test-workspace",
		ProcessEnv: action.SanitizedProcessEnv("/tmp/vectis-test-workspace", []string{"PATH=/usr/bin", "VECTIS_DATABASE_DSN=secret"}),
		Logger:     interfaces.NewLogger("test"),
		LogStream:  logStream,
	}
}

func testEnvLookup(env []string, key string) (string, bool) {
	for _, entry := range env {
		k, v, ok := strings.Cut(entry, "=")
		if ok && k == key {
			return v, true
		}
	}

	return "", false
}
