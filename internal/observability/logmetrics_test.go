package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestNewLogMetrics_appearsOnScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-log")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	lm, err := NewLogMetrics()
	if err != nil {
		t.Fatal(err)
	}

	lm.RecordGRPCChunk(ctx)
	lm.RecordAppendFailure(ctx)
	lm.RecordMemoryBufferDrop(ctx)
	lm.RecordSSEChannelDrop(ctx)
	lm.SSEConnectionOpened()
	lm.SSEConnectionClosed()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeOpenMetrics)))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}

	names, err := metricFamilyNames(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"vectis_log_grpc_chunks_received_total",
		"vectis_log_storage_append_failures_total",
		"vectis_log_memory_buffer_drops_total",
		"vectis_log_sse_channel_drops_total",
		"vectis_log_sse_connections_active",
	} {
		if _, ok := names[want]; !ok {
			t.Fatalf("missing metric %q; got: %v", want, sortedFamilyNames(names))
		}
	}
}
