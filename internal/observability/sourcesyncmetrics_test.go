package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
)

func TestSourceSyncMetrics_RecordSourceRepositorySync(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitAPIMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	m, err := NewSourceSyncMetrics()
	if err != nil {
		t.Fatal(err)
	}

	m.RecordSourceRepositorySync(ctx, SourceSyncTriggerManual, "local_checkout", "managed", SourceSyncOutcomeSucceeded, SourceSyncReasonNone, 25*time.Millisecond)
	m.RecordSourceRepositorySync(ctx, SourceSyncTriggerPeriodic, "local_checkout", "managed", SourceSyncOutcomeFailed, SourceSyncReasonFromErrorCode("git_credentials_unavailable"), 10*time.Millisecond)
	m.RecordSourceRefHydration(ctx, "local_checkout", "managed", SourceSyncOutcomeSucceeded, SourceSyncReasonNone, "fallback-2", SourceRefHydrationCacheHit, 15*time.Millisecond)
	m.RecordSourceRepositoryObjectStore(ctx, "managed-repo", "local_checkout", "managed", "warning", 64, 128<<20, 6000, []SourceRepositoryObjectStoreWarning{
		{Code: "many_pack_files", Severity: "warning"},
	})

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

	for _, family := range []string{"vectis_source_repository_syncs_total", "vectis_source_repository_sync_duration_seconds"} {
		if !metricFamilyHasLabels(families[family], map[string]string{
			"trigger":       SourceSyncTriggerManual,
			"source_kind":   "local_checkout",
			"checkout_mode": "managed",
			"outcome":       SourceSyncOutcomeSucceeded,
			"reason":        SourceSyncReasonNone,
		}) {
			t.Fatalf("metric family %s missing success labels: %v", family, families[family])
		}

		if !metricFamilyHasLabels(families[family], map[string]string{
			"trigger":       SourceSyncTriggerPeriodic,
			"source_kind":   "local_checkout",
			"checkout_mode": "managed",
			"outcome":       SourceSyncOutcomeFailed,
			"reason":        "git_credentials_unavailable",
		}) {
			t.Fatalf("metric family %s missing credential failure labels: %v", family, families[family])
		}
	}

	for _, family := range []string{"vectis_source_ref_hydrations_total", "vectis_source_ref_hydration_duration_seconds"} {
		if !metricFamilyHasLabels(families[family], map[string]string{
			"source_kind":   "local_checkout",
			"checkout_mode": "managed",
			"outcome":       SourceSyncOutcomeSucceeded,
			"reason":        SourceSyncReasonNone,
			"tier":          "fallback-2",
			"cache":         SourceRefHydrationCacheHit,
		}) {
			t.Fatalf("metric family %s missing hydration labels: %v", family, families[family])
		}
	}

	for _, family := range []string{
		"vectis_source_repository_object_store_pack_files",
		"vectis_source_repository_object_store_pack_bytes",
		"vectis_source_repository_object_store_loose_objects",
	} {
		if !metricFamilyHasLabels(families[family], map[string]string{
			"repository_id": "managed-repo",
			"source_kind":   "local_checkout",
			"checkout_mode": "managed",
		}) {
			t.Fatalf("metric family %s missing object-store labels: %v", family, families[family])
		}
	}

	if !metricFamilyHasLabels(families["vectis_source_repository_object_store_pressure"], map[string]string{
		"repository_id": "managed-repo",
		"source_kind":   "local_checkout",
		"checkout_mode": "managed",
		"pressure":      "warning",
	}) {
		t.Fatalf("pressure metric missing object-store labels: %v", families["vectis_source_repository_object_store_pressure"])
	}

	if !metricFamilyHasLabels(families["vectis_source_repository_object_store_warnings"], map[string]string{
		"repository_id": "managed-repo",
		"source_kind":   "local_checkout",
		"checkout_mode": "managed",
		"code":          "many_pack_files",
		"severity":      "warning",
	}) {
		t.Fatalf("warning metric missing object-store labels: %v", families["vectis_source_repository_object_store_warnings"])
	}
}

func TestSourceSyncReasonFromErrorCode(t *testing.T) {
	tests := []struct {
		name string
		code string
		want string
	}{
		{name: "empty", code: "", want: SourceSyncReasonNone},
		{name: "git code", code: "git_fetch_failed", want: "git_fetch_failed"},
		{name: "deadline", code: "context deadline exceeded", want: SourceSyncReasonContextDeadline},
		{name: "message", code: "unsupported source kind: nope", want: "unsupported_source_kind_nope"},
		{name: "symbols", code: "../no thank you", want: "no_thank_you"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SourceSyncReasonFromErrorCode(tt.code); got != tt.want {
				t.Fatalf("SourceSyncReasonFromErrorCode(%q) = %q, want %q", tt.code, got, tt.want)
			}
		})
	}
}
