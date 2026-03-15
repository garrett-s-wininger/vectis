package interfaces_test

import (
	"context"
	"errors"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
)

func TestMockLogClient_StreamLogs(t *testing.T) {
	client := mocks.NewMockLogClient()

	stream, err := client.StreamLogs(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if stream == nil {
		t.Fatal("expected stream to be non-nil")
	}

	if client.GetStreamCount() != 1 {
		t.Errorf("expected stream count to be 1, got %d", client.GetStreamCount())
	}
}

func TestMockLogClient_StreamLogsError(t *testing.T) {
	client := mocks.NewMockLogClient()
	expectedErr := errors.New("failed to create stream")
	client.SetStreamError(expectedErr)

	_, err := client.StreamLogs(context.Background())
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if client.GetStreamCount() != 0 {
		t.Errorf("expected stream count to be 0, got %d", client.GetStreamCount())
	}
}

func TestMockLogClient_SendChunk(t *testing.T) {
	client := mocks.NewMockLogClient()

	stream, err := client.StreamLogs(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	jobID := "test-job"
	seq := int64(1)
	chunk := &api.LogChunk{
		JobId:    &jobID,
		Data:     []byte("test log data"),
		Sequence: &seq,
	}

	err = stream.Send(chunk)
	if err != nil {
		t.Errorf("expected no error sending chunk, got %v", err)
	}

	chunks := client.GetChunks()
	if len(chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(chunks))
	}

	if chunks[0].GetJobId() != "test-job" {
		t.Errorf("expected job id 'test-job', got '%s'", chunks[0].GetJobId())
	}

	if string(chunks[0].GetData()) != "test log data" {
		t.Errorf("expected data 'test log data', got '%s'", string(chunks[0].GetData()))
	}
}

func TestMockLogClient_SendMultipleChunks(t *testing.T) {
	client := mocks.NewMockLogClient()

	stream, _ := client.StreamLogs(context.Background())

	for i := 0; i < 3; i++ {
		jobID := "test-job"
		seq := int64(i)
		chunk := &api.LogChunk{
			JobId:    &jobID,
			Data:     []byte("log line"),
			Sequence: &seq,
		}
		stream.Send(chunk)
	}

	chunks := client.GetChunks()
	if len(chunks) != 3 {
		t.Errorf("expected 3 chunks, got %d", len(chunks))
	}
}

func TestMockLogClient_SendError(t *testing.T) {
	client := mocks.NewMockLogClient()
	stream, _ := client.StreamLogs(context.Background())

	mockStream := stream.(*mocks.MockLogStream)
	expectedErr := errors.New("send failed")
	mockStream.SetSendError(expectedErr)

	jobID := "test-job"
	seq := int64(1)
	chunk := &api.LogChunk{
		JobId:    &jobID,
		Data:     []byte("test"),
		Sequence: &seq,
	}

	err := stream.Send(chunk)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if client.GetChunks() != nil && len(client.GetChunks()) > 0 {
		t.Error("expected no chunks to be stored when send fails")
	}
}

func TestMockLogClient_CloseSend(t *testing.T) {
	client := mocks.NewMockLogClient()
	stream, _ := client.StreamLogs(context.Background())

	mockStream := stream.(*mocks.MockLogStream)

	if mockStream.CloseSendCalled() {
		t.Error("expected CloseSend to not be called initially")
	}

	err := stream.CloseSend()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if !mockStream.CloseSendCalled() {
		t.Error("expected CloseSend to be called")
	}

	if !mockStream.IsClosed() {
		t.Error("expected stream to be closed")
	}
}

func TestMockLogClient_CloseSendError(t *testing.T) {
	client := mocks.NewMockLogClient()
	stream, _ := client.StreamLogs(context.Background())

	mockStream := stream.(*mocks.MockLogStream)
	expectedErr := errors.New("close send failed")
	mockStream.SetCloseSendError(expectedErr)

	err := stream.CloseSend()
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMockLogClient_SendAfterClose(t *testing.T) {
	client := mocks.NewMockLogClient()
	stream, _ := client.StreamLogs(context.Background())

	stream.CloseSend()

	jobID := "test-job"
	seq := int64(1)
	chunk := &api.LogChunk{
		JobId:    &jobID,
		Data:     []byte("test"),
		Sequence: &seq,
	}

	err := stream.Send(chunk)
	if err == nil {
		t.Error("expected error when sending to closed stream")
	}
}

func TestMockLogClient_Close(t *testing.T) {
	client := mocks.NewMockLogClient()

	if client.IsClosed() {
		t.Error("expected client to be open initially")
	}

	err := client.Close()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if !client.IsClosed() {
		t.Error("expected client to be closed")
	}
}

func TestMockLogClient_CloseError(t *testing.T) {
	client := mocks.NewMockLogClient()
	expectedErr := errors.New("close failed")
	client.SetCloseError(expectedErr)

	err := client.Close()
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if client.IsClosed() {
		t.Error("expected client to remain open when close fails")
	}
}

func TestMockLogClient_MultipleStreams(t *testing.T) {
	client := mocks.NewMockLogClient()

	for i := 0; i < 3; i++ {
		_, err := client.StreamLogs(context.Background())
		if err != nil {
			t.Errorf("failed to create stream %d: %v", i, err)
		}
	}

	if client.GetStreamCount() != 3 {
		t.Errorf("expected 3 streams, got %d", client.GetStreamCount())
	}
}
