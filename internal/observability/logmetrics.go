package observability

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type LogMetrics struct {
	grpcChunksReceived metric.Int64Counter
	appendFailures     metric.Int64Counter
	memBufferDrops     metric.Int64Counter
	sseChannelDrops    metric.Int64Counter
	sseActive          atomic.Int64
}

func NewLogMetrics() (*LogMetrics, error) {
	m := otel.Meter("vectis/log")

	chunks, err := m.Int64Counter("vectis_log_grpc_chunks_received_total",
		metric.WithDescription("Log line chunks received on gRPC StreamLogs"),
		metric.WithUnit("{chunk}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_log_grpc_chunks_received_total: %w", err)
	}

	appendFail, err := m.Int64Counter("vectis_log_storage_append_failures_total",
		metric.WithDescription("Failed durable Append operations (storage errors)"),
		metric.WithUnit("{error}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_log_storage_append_failures_total: %w", err)
	}

	memDrops, err := m.Int64Counter("vectis_log_memory_buffer_drops_total",
		metric.WithDescription("Log lines dropped because the in-memory buffer for a run was full"),
		metric.WithUnit("{line}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_log_memory_buffer_drops_total: %w", err)
	}

	sseDrops, err := m.Int64Counter("vectis_log_sse_channel_drops_total",
		metric.WithDescription("Stdout/stderr lines dropped because an SSE subscriber channel was full"),
		metric.WithUnit("{line}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_log_sse_channel_drops_total: %w", err)
	}

	lm := &LogMetrics{
		grpcChunksReceived: chunks,
		appendFailures:     appendFail,
		memBufferDrops:     memDrops,
		sseChannelDrops:    sseDrops,
	}

	activeG, err := m.Int64ObservableGauge("vectis_log_sse_connections_active",
		metric.WithDescription("Currently connected SSE clients (one per HTTP request)"),
		metric.WithUnit("{connection}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_log_sse_connections_active: %w", err)
	}

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(activeG, lm.sseActive.Load())
		return nil
	}, activeG)

	if err != nil {
		return nil, fmt.Errorf("register log SSE gauge callback: %w", err)
	}

	return lm, nil
}

func (lm *LogMetrics) RecordGRPCChunk(ctx context.Context) {
	if lm == nil {
		return
	}

	lm.grpcChunksReceived.Add(ctx, 1)
}

func (lm *LogMetrics) RecordAppendFailure(ctx context.Context) {
	if lm == nil {
		return
	}

	lm.appendFailures.Add(ctx, 1)
}

func (lm *LogMetrics) RecordMemoryBufferDrop(ctx context.Context) {
	if lm == nil {
		return
	}

	lm.memBufferDrops.Add(ctx, 1)
}

func (lm *LogMetrics) RecordSSEChannelDrop(ctx context.Context) {
	if lm == nil {
		return
	}

	lm.sseChannelDrops.Add(ctx, 1)
}

func (lm *LogMetrics) SSEConnectionOpened() {
	if lm == nil {
		return
	}

	lm.sseActive.Add(1)
}

func (lm *LogMetrics) SSEConnectionClosed() {
	if lm == nil {
		return
	}

	lm.sseActive.Add(-1)
}
