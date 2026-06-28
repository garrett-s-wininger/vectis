package source

import "testing"

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
