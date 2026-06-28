package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	sdk "vectis/sdk/workercore"
	"vectis/sdk/workercore/conformance"

	"google.golang.org/grpc"
)

func TestSampleCoreConformance(t *testing.T) {
	conformance.RunCoreSuite(t, func(t *testing.T) sdk.Core {
		t.Helper()
		return newSampleCore()
	}, conformance.Options{
		RequireLogCallback:      true,
		RequireArtifactCallback: true,
	})
}

func TestSampleCoreServerConformance(t *testing.T) {
	conformance.RunCoreServerSuite(t, func(t *testing.T) string {
		t.Helper()

		socketPath := shortSocketPath(t, "worker-core-example.sock")
		server, listener, err := sdk.NewUnixCoreServer(socketPath, newSampleCore(), sdk.ServiceOptions{})
		if err != nil {
			t.Fatalf("NewUnixCoreServer: %v", err)
		}
		t.Cleanup(server.Stop)

		go func() {
			if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("worker core example server: %v", err)
			}
		}()

		return socketPath
	}, conformance.Options{
		RequireLogCallback:      true,
		RequireArtifactCallback: true,
	})
}

func shortSocketPath(t *testing.T, name string) string {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "vectis-worker-core-example-") //nolint:usetesting // Keep Unix socket paths short on platforms with long test temp roots.
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return filepath.Join(dir, name)
}
