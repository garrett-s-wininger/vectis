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

func TestLoadManifestIncludesVectisServices(t *testing.T) {
	manifest, err := LoadManifest(DefaultManifestPath)
	if err != nil {
		t.Fatal(err)
	}

	artifactRoot := t.TempDir()
	for _, path := range []string{
		"systemd/vectis.target",
		"systemd/vectis-api.service",
		"sysusers.d/vectis.conf",
		"tmpfiles.d/vectis.conf",
		"env/vectis.env.example",
		"env/vectis-api.env.example",
	} {
		writeTestFile(t, filepath.Join(artifactRoot, filepath.FromSlash(path)), "test\n")
	}

	inputs := map[string]string{
		"linux-artifacts": artifactRoot,
	}

	for _, name := range []string{
		"vectis-api",
		"vectis-catalog",
		"vectis-cell-ingress",
		"vectis-cron",
		"vectis-docs",
		"vectis-log",
		"vectis-log-forwarder",
		"vectis-queue",
		"vectis-reconciler",
		"vectis-registry",
		"vectis-worker",
	} {
		inputs[name] = "/tmp/" + name
	}

	pkg, err := manifest.resolve(BuildOptions{
		PackageID: "vectis-services",
		Version:   "v1.2.3",
		Release:   "2",
		Arch:      "arm64",
		Inputs:    inputs,
	})

	if err != nil {
		t.Fatal(err)
	}

	if pkg.Name != "vectis-services" {
		t.Fatalf("package name = %q", pkg.Name)
	}

	if !containsString(pkg.Depends, "vectis-cli") {
		t.Fatalf("vectis-services dependencies missing vectis-cli: %v", pkg.Depends)
	}

	requireResolvedFile(t, pkg, "/usr/bin/vectis-api", "/tmp/vectis-api", 0o755)
	requireResolvedFile(t, pkg, "/usr/lib/systemd/system/vectis.target", filepath.Join(artifactRoot, "systemd", "vectis.target"), 0o644)
	requireResolvedFile(t, pkg, "/usr/lib/systemd/system/vectis-api.service", filepath.Join(artifactRoot, "systemd", "vectis-api.service"), 0o644)
	requireResolvedFile(t, pkg, "/usr/lib/sysusers.d/vectis.conf", filepath.Join(artifactRoot, "sysusers.d", "vectis.conf"), 0o644)
	requireResolvedFile(t, pkg, "/usr/share/doc/vectis-services/examples/vectis-api.env.example", filepath.Join(artifactRoot, "env", "vectis-api.env.example"), 0o644)

	for _, file := range pkg.Files {
		if strings.HasPrefix(file.Destination, "/etc/vectis/") {
			t.Fatalf("vectis-services package should not install live env config: %+v", file)
		}
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
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

func requireResolvedFile(t *testing.T, pkg resolvedPackage, destination, source string, mode int64) {
	t.Helper()

	for _, file := range pkg.Files {
		if file.Destination != destination {
			continue
		}

		if file.Source != source {
			t.Fatalf("%s source = %q, want %q", destination, file.Source, source)
		}

		if file.Mode != mode {
			t.Fatalf("%s mode = %#o, want %#o", destination, file.Mode, mode)
		}

		return
	}

	t.Fatalf("package %s missing %s", pkg.ID, destination)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}

	return false
}

func TestBuildRPMPackage(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "vectis-cli")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\necho vectis\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	result, err := Build(BuildOptions{
		PackageID: "vectis-cli",
		Format:    "rpm",
		OutputDir: t.TempDir(),
		Version:   "v0.1.0-test",
		Release:   "1",
		Arch:      "arm64",
		Inputs:    map[string]string{"vectis-cli": bin},
	})

	if err != nil {
		t.Fatal(err)
	}

	if result.Status != "packaged" || result.Format != "rpm" || result.Files != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}

	if !strings.HasSuffix(result.Path, "vectis-cli-0.1.0_test-1.aarch64.rpm") {
		t.Fatalf("package path = %q", result.Path)
	}

	if info, err := os.Stat(result.Path); err != nil {
		t.Fatal(err)
	} else if info.Size() == 0 {
		t.Fatal("package is empty")
	}
}
