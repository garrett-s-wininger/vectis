package observability

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func TestRetryMetrics_ExposeStableServiceAndOperationLabels(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-api")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	rm, err := NewRetryMetrics()
	if err != nil {
		t.Fatal(err)
	}

	rm.RecordAttempt(ctx, "api.enqueue")
	rm.RecordExhausted(ctx, "api.enqueue")
	rm.RecordDelay(ctx, "api.enqueue", 150*time.Millisecond)

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

	for _, family := range []string{"vectis_retries_total", "vectis_retries_exhausted_total", "vectis_retry_delay_seconds"} {
		if !metricFamilyHasLabels(families[family], map[string]string{
			"component": "api.enqueue",
			"service":   "api",
			"operation": "enqueue",
		}) {
			t.Fatalf("metric family %s missing retry labels", family)
		}
	}
}

func TestSplitRetryComponent(t *testing.T) {
	tests := []struct {
		name          string
		component     string
		wantService   string
		wantOperation string
	}{
		{name: "dot", component: "worker.finalize", wantService: "worker", wantOperation: "finalize"},
		{name: "slash", component: "queue/enqueue", wantService: "queue", wantOperation: "enqueue"},
		{name: "colon", component: "registry:register", wantService: "registry", wantOperation: "register"},
		{name: "legacy", component: "resolver", wantService: "resolver", wantOperation: "retry"},
		{name: "empty", component: "", wantService: "unknown", wantOperation: "retry"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, operation := splitRetryComponent(tt.component)
			if service != tt.wantService || operation != tt.wantOperation {
				t.Fatalf("splitRetryComponent(%q) = %q/%q, want %q/%q", tt.component, service, operation, tt.wantService, tt.wantOperation)
			}
		})
	}
}

func metricFamilies(payload []byte) (map[string]*dto.MetricFamily, error) {
	var lastErr error
	for _, format := range []expfmt.Format{
		expfmt.NewFormat(expfmt.TypeOpenMetrics),
		expfmt.NewFormat(expfmt.TypeTextPlain),
	} {
		families, err := decodeMetricFamilies(bytes.NewReader(payload), format)
		if err == nil && len(families) > 0 {
			return families, nil
		}

		lastErr = err
	}

	if lastErr == nil {
		lastErr = errors.New("no metric families decoded")
	}
	return nil, lastErr
}

func decodeMetricFamilies(r io.Reader, format expfmt.Format) (map[string]*dto.MetricFamily, error) {
	dec := expfmt.NewDecoder(r, format)
	out := make(map[string]*dto.MetricFamily)
	for {
		var mf dto.MetricFamily
		err := dec.Decode(&mf)
		if errors.Is(err, io.EOF) {
			return out, nil
		}

		if err != nil {
			return nil, err
		}

		if name := mf.GetName(); name != "" {
			copied := mf
			out[name] = &copied
		}
	}
}

func metricFamilyHasLabels(family *dto.MetricFamily, labels map[string]string) bool {
	if family == nil {
		return false
	}

	for _, m := range family.GetMetric() {
		got := map[string]string{}
		for _, label := range m.GetLabel() {
			got[label.GetName()] = label.GetValue()
		}

		matches := true
		for name, want := range labels {
			if got[name] != want {
				matches = false
				break
			}
		}

		if matches {
			return true
		}
	}

	return false
}
