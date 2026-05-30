package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/expfmt"
)

func TestLogForwarderMetrics_ExposeCountersAndSpoolGauges(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-log-forwarder")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	metrics, err := NewLogForwarderMetrics()
	if err != nil {
		t.Fatal(err)
	}

	metrics.RecordChunkReceived(ctx, "hinted")
	metrics.RecordBatch(ctx, "spooled")

	if err := RegisterLogForwarderSpoolGauges(func() (int64, int64) {
		return 2, 30
	}); err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeOpenMetrics)))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}

	families, err := metricFamilies(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if !metricFamilyHasLabels(families["vectis_log_forwarder_chunks_received_total"], map[string]string{"route": "hinted"}) {
		t.Fatal("missing log-forwarder chunk route labels")
	}

	if !metricFamilyHasLabels(families["vectis_log_forwarder_batches_total"], map[string]string{"outcome": "spooled"}) {
		t.Fatal("missing log-forwarder batch outcome labels")
	}

	for _, name := range []string{
		"vectis_log_forwarder_spool_files",
		"vectis_log_forwarder_spool_oldest_age_seconds",
	} {
		if families[name] == nil {
			t.Fatalf("missing metric %q", name)
		}
	}
}
