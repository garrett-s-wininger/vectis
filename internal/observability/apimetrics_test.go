package observability

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func TestInitAPIMetrics_servesScrapeEndpoint(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitAPIMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeOpenMetrics)))
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status %d, body: %s", rr.Code, rr.Body.String())
	}

	names, err := metricFamilyNames(rr.Body.Bytes())
	if err != nil {
		t.Fatalf("parse metrics: %v\nbody:\n%s", err, rr.Body.String())
	}

	if len(names) == 0 {
		t.Fatal("expected at least one metric family")
	}

	if _, ok := names["target_info"]; !ok {
		t.Fatalf("missing target_info metric; got families: %v", sortedFamilyNames(names))
	}
}

func TestInitAPIMetrics_otelHTTPHistogramAfterRequest(t *testing.T) {
	ctx := context.Background()
	metricsHandler, shutdown, err := InitAPIMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/jobs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	api := otelhttp.NewHandler(mux, "")
	api.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/jobs", http.NoBody))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeOpenMetrics)))
	metricsHandler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}

	names, err := metricFamilyNames(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := names["http_server_request_duration_seconds"]; !ok {
		t.Fatalf("missing http_server_request_duration_seconds; got: %v", sortedFamilyNames(names))
	}
}

func metricFamilyNames(payload []byte) (map[string]struct{}, error) {
	var lastErr error
	for _, format := range []expfmt.Format{
		expfmt.NewFormat(expfmt.TypeOpenMetrics),
		expfmt.NewFormat(expfmt.TypeTextPlain),
	} {
		names, err := decodeMetricFamilyNames(bytes.NewReader(payload), format)
		if err == nil && len(names) > 0 {
			return names, nil
		}

		lastErr = err
	}

	if lastErr == nil {
		lastErr = errors.New("no metric families decoded")
	}

	return nil, lastErr
}

func decodeMetricFamilyNames(r io.Reader, format expfmt.Format) (map[string]struct{}, error) {
	dec := expfmt.NewDecoder(r, format)
	out := make(map[string]struct{})
	for {
		var mf dto.MetricFamily
		err := dec.Decode(&mf)
		if errors.Is(err, io.EOF) {
			return out, nil
		}

		if err != nil {
			return nil, err
		}

		if n := mf.GetName(); n != "" {
			out[n] = struct{}{}
		}
	}
}

func sortedFamilyNames(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)
	return keys
}
