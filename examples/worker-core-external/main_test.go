package main

import (
	"testing"

	sdk "vectis/sdk/workercore"
	"vectis/sdk/workercore/conformance"
)

func TestSampleCoreConformance(t *testing.T) {
	conformance.RunCoreSuite(t, func(t *testing.T) sdk.Core {
		t.Helper()
		return sampleCore{}
	}, conformance.Options{
		RequireLogCallback:      true,
		RequireArtifactCallback: true,
	})
}
