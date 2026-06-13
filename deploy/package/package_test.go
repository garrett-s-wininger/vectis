package packaging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadManifestIncludesVectisCLI(t *testing.T) {
	manifest, err := LoadManifest(DefaultManifestPath)
	if err != nil {
		t.Fatal(err)
	}

	pkg, err := manifest.resolve(BuildOptions{
		PackageID: "vectis-cli",
		Version:   "v1.2.3",
		Release:   "2",
		Arch:      "arm64",
		Inputs:    map[string]string{"vectis-cli": "/tmp/vectis-cli"},
	})

	if err != nil {
		t.Fatal(err)
	}

	if pkg.Name != "vectis-cli" {
		t.Fatalf("package name = %q", pkg.Name)
	}

	if pkg.Version != "1.2.3" {
		t.Fatalf("version = %q, want 1.2.3", pkg.Version)
	}

	if pkg.Files[0].Source != "/tmp/vectis-cli" {
		t.Fatalf("source = %q", pkg.Files[0].Source)
	}

	if pkg.Files[0].Destination != "/usr/bin/vectis-cli" {
		t.Fatalf("destination = %q", pkg.Files[0].Destination)
	}
}

func TestBuildDebPackage(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "vectis-cli")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\necho vectis\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	result, err := Build(BuildOptions{
		PackageID: "vectis-cli",
		Format:    "deb",
		OutputDir: t.TempDir(),
		Version:   "v0.1.0-test",
		Release:   "1",
		Arch:      "amd64",
		Inputs:    map[string]string{"vectis-cli": bin},
	})

	if err != nil {
		t.Fatal(err)
	}

	if result.Status != "packaged" || result.Format != "deb" || result.Files != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}

	if !strings.HasSuffix(result.Path, "vectis-cli_0.1.0-test-1_amd64.deb") {
		t.Fatalf("package path = %q", result.Path)
	}

	if info, err := os.Stat(result.Path); err != nil {
		t.Fatal(err)
	} else if info.Size() == 0 {
		t.Fatal("package is empty")
	}
}

func TestBuildRejectsRPMUntilLaneExists(t *testing.T) {
	_, err := Build(BuildOptions{
		PackageID: "vectis-cli",
		Format:    "rpm",
		OutputDir: t.TempDir(),
		Inputs:    map[string]string{"vectis-cli": "/tmp/vectis-cli"},
	})

	if err == nil {
		t.Fatal("expected rpm build to fail until implemented")
	}

	if !strings.Contains(err.Error(), "RPM VM lane") && !strings.Contains(err.Error(), "rpm") {
		t.Fatalf("error = %v", err)
	}
}
