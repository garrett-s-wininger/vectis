package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/catalog"
	"vectis/internal/cell"

	"github.com/prometheus/common/expfmt"
)

func TestCatalogCounters_AppearOnScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-catalog")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	cm, err := NewCatalogMetrics()
	if err != nil {
		t.Fatal(err)
	}

	cm.RecordProcessResult(ctx, cell.CatalogInboxProcessResult{Read: 3, Applied: 2, Failed: 1})
	cm.RecordProcessError(ctx)
	cm.RecordFanInSourceResult(ctx, catalog.FanInSourceResult{CellID: "iad-a", Backfilled: 1, Read: 2, Copied: 1})

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
		"vectis_catalog_events_read_total",
		"vectis_catalog_events_applied_total",
		"vectis_catalog_events_failed_total",
		"vectis_catalog_process_errors_total",
		"vectis_catalog_fanin_events_read_total",
		"vectis_catalog_fanin_events_copied_total",
		"vectis_catalog_fanin_events_backfilled_total",
	} {
		if _, ok := names[want]; !ok {
			t.Fatalf("missing metric %q; got: %v", want, sortedFamilyNames(names))
		}
	}
}
