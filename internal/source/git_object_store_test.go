package source

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGitCheckoutObjectStoreStatusClassifiesPressure(t *testing.T) {
	status := GitCheckoutObjectStoreStatus{
		PackFiles:                 gitObjectStorePackFilesWarning,
		PackKeepFiles:             1,
		LooseObjects:              gitObjectStoreLooseObjectsWarning,
		LooseObjectScanLimit:      gitObjectLooseScanLimit,
		MaintenanceIndicatorFiles: []string{"gc.pid"},
	}

	status.classifyPressure()

	if status.Pressure != gitObjectStorePressureWarning {
		t.Fatalf("pressure=%q, want warning; warnings=%+v", status.Pressure, status.Warnings)
	}

	for _, code := range []string{
		gitObjectStoreWarningManyPacks,
		gitObjectStoreWarningManyLoose,
		gitObjectStoreWarningKeepFiles,
		gitObjectStoreWarningMaintenance,
		gitObjectStoreWarningMissingCommit,
		gitObjectStoreWarningMissingMultiPack,
	} {
		if !objectStoreWarningsInclude(status.Warnings, code) {
			t.Fatalf("missing warning %q in %+v", code, status.Warnings)
		}
	}
}

func TestGitCheckoutObjectStoreStatusCountsHydratedRefsFromFiles(t *testing.T) {
	commonDir := t.TempDir()
	looseRef := filepath.Join(commonDir, "refs", "vectis", "hydrated", "aaa")
	if err := os.MkdirAll(filepath.Dir(looseRef), 0o755); err != nil {
		t.Fatalf("create loose ref dir: %v", err)
	}

	if err := os.WriteFile(looseRef, []byte("1111111111111111111111111111111111111111\n"), 0o644); err != nil {
		t.Fatalf("write loose ref: %v", err)
	}

	packedRefs := "" +
		"# pack-refs with: peeled fully-peeled sorted\n" +
		"1111111111111111111111111111111111111111 refs/vectis/hydrated/aaa\n" +
		"2222222222222222222222222222222222222222 refs/vectis/hydrated/bbb\n" +
		"3333333333333333333333333333333333333333 refs/heads/main\n"
	if err := os.WriteFile(filepath.Join(commonDir, "packed-refs"), []byte(packedRefs), 0o644); err != nil {
		t.Fatalf("write packed refs: %v", err)
	}

	status := GitCheckoutObjectStoreStatus{}
	status.countHydratedRefs(commonDir)

	if got, want := status.HydratedRefs, 2; got != want {
		t.Fatalf("HydratedRefs got %d, want %d", got, want)
	}
	
	if status.HydratedRefsTruncated {
		t.Fatalf("HydratedRefsTruncated got true, want false")
	}
}

func TestGitCheckoutObjectStoreStatusClassifiesCriticalPressure(t *testing.T) {
	status := GitCheckoutObjectStoreStatus{
		PackFiles:             gitObjectStorePackFilesCritical,
		LooseObjectsTruncated: true,
		LooseObjectScanLimit:  gitObjectLooseScanLimit,
	}

	status.classifyPressure()

	if status.Pressure != gitObjectStorePressureCritical {
		t.Fatalf("pressure=%q, want critical; warnings=%+v", status.Pressure, status.Warnings)
	}

	if !objectStoreWarningsInclude(status.Warnings, gitObjectStoreWarningManyPacks) ||
		!objectStoreWarningsInclude(status.Warnings, gitObjectStoreWarningLooseTruncated) {
		t.Fatalf("critical warnings mismatch: %+v", status.Warnings)
	}
}

func TestGitCheckoutObjectStoreStatusClassifiesHydratedRefPressure(t *testing.T) {
	status := GitCheckoutObjectStoreStatus{
		HydratedRefs: gitObjectStoreHydratedRefsWarning,
	}

	status.classifyPressure()

	if status.Pressure != gitObjectStorePressureWarning {
		t.Fatalf("pressure=%q, want warning; warnings=%+v", status.Pressure, status.Warnings)
	}
	if !objectStoreWarningsInclude(status.Warnings, gitObjectStoreWarningManyHydratedRefs) {
		t.Fatalf("missing hydrated ref warning: %+v", status.Warnings)
	}

	status = GitCheckoutObjectStoreStatus{
		HydratedRefs:          gitObjectStoreHydratedRefsCritical,
		HydratedRefsTruncated: true,
	}

	status.classifyPressure()

	if status.Pressure != gitObjectStorePressureCritical {
		t.Fatalf("pressure=%q, want critical; warnings=%+v", status.Pressure, status.Warnings)
	}
	if !objectStoreWarningsInclude(status.Warnings, gitObjectStoreWarningHydratedRefsScan) {
		t.Fatalf("missing hydrated ref scan warning: %+v", status.Warnings)
	}
}

func objectStoreWarningsInclude(warnings []GitCheckoutObjectStoreWarning, code string) bool {
	for _, warning := range warnings {
		if warning.Code == code {
			return true
		}
	}

	return false
}
