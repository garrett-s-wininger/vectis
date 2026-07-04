package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"

	"vectis/internal/artifact"
)

func TestRegisterArtifactStorageMetrics_nilStore(t *testing.T) {
	t.Parallel()

	err := RegisterArtifactStorageMetrics(nil)
	if err == nil {
		t.Fatal("expected error for nil artifact storage provider")
	}
}

func TestRegisterArtifactStorageMetrics_appearsOnScrape(t *testing.T) {
	ctx := context.Background()
	h, shutdown, err := InitServiceMetrics(ctx, "vectis-artifact")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	store, err := artifact.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	if _, err := store.Put(ctx, strings.NewReader("artifact metrics"), artifact.PutOptions{}); err != nil {
		t.Fatalf("put artifact blob: %v", err)
	}

	if err := RegisterArtifactStorageMetrics(store); err != nil {
		t.Fatal(err)
	}

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
		"vectis_artifact_storage_blobs",
		"vectis_artifact_storage_bytes",
		"vectis_artifact_storage_free_bytes",
		"vectis_artifact_storage_free_inodes",
		"vectis_artifact_storage_new_blob_writable",
	} {
		if _, ok := names[want]; !ok {
			t.Fatalf("missing metric %q; got: %v", want, sortedFamilyNames(names))
		}
	}
}
