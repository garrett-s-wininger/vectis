package observability

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestAsyncSlogHandler_CloseFlushesQueuedRecords(t *testing.T) {
	var buf bytes.Buffer
	handler := NewAsyncSlogHandler(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}), 8)
	logger := slog.New(handler)

	logger.InfoContext(context.Background(), "queued_record", slog.String("key", "value"))
	if err := handler.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "queued_record") || !strings.Contains(out, "key=value") {
		t.Fatalf("expected queued slog record to flush on close, got: %s", out)
	}
}
