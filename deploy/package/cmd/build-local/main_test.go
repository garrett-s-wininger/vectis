package main

import (
	"io"
	"slices"
	"strings"
	"testing"
)

func TestUseNativeBuild(t *testing.T) {
	tests := []struct {
		name          string
		hostOS        string
		allowCrossCGO bool
		want          bool
	}{
		{name: "linux", hostOS: "linux", want: true},
		{name: "darwin", hostOS: "darwin", want: false},
		{name: "darwin cross override", hostOS: "darwin", allowCrossCGO: true, want: true},
		{name: "windows", hostOS: "windows", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := useNativeBuild(tt.hostOS, tt.allowCrossCGO); got != tt.want {
				t.Fatalf("useNativeBuild(%q, %v) = %v, want %v", tt.hostOS, tt.allowCrossCGO, got, tt.want)
			}
		})
	}
}

func TestNativeTarget(t *testing.T) {
	if got, want := nativeTarget("deb", "arm64"), "package-local-native-deb-arm64"; got != want {
		t.Fatalf("target = %q, want %q", got, want)
	}
}

func TestParseOptions(t *testing.T) {
	clearPackageLocalEnv(t)

	opts, err := parseOptions([]string{
		"--format", "rpm",
		"--arch", "arm64",
		"--workdir", ".",
		"--env", "PACKAGE_OUT=artifacts/packages",
		"--env", "PACKAGE_LOCAL_APPS=local api worker",
	}, io.Discard)

	if err != nil {
		t.Fatal(err)
	}

	if opts.Format != "rpm" {
		t.Fatalf("format = %q, want rpm", opts.Format)
	}

	if opts.Arch != "arm64" {
		t.Fatalf("arch = %q, want arm64", opts.Arch)
	}

	if opts.Provider != "lima" {
		t.Fatalf("provider = %q, want lima", opts.Provider)
	}

	if opts.Instance != defaultBuildVMInstance {
		t.Fatalf("instance = %q, want %q", opts.Instance, defaultBuildVMInstance)
	}

	if !slices.Contains(opts.Env, "PACKAGE_LOCAL_APPS=local api worker") {
		t.Fatalf("env did not preserve spaces: %v", opts.Env)
	}
}

func TestParseOptionsRejectsInvalidFormat(t *testing.T) {
	clearPackageLocalEnv(t)

	_, err := parseOptions([]string{"--format", "apk", "--arch", "arm64"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "format must be deb or rpm") {
		t.Fatalf("expected format error, got %v", err)
	}
}

func TestVMBuildEnvDefaultsCanBeOverridden(t *testing.T) {
	got := vmBuildEnv([]string{"GOCACHE=/custom/cache", "PACKAGE_ARCH=arm64"}, "go", "make", "/var/tmp/cache")
	if !slices.Contains(got, "PATH=/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin") {
		t.Fatalf("default path missing: %v", got)
	}

	if got[len(got)-2] != "GOCACHE=/custom/cache" {
		t.Fatalf("override should be appended after defaults, got %v", got)
	}
}

func TestRelativePackageOut(t *testing.T) {
	opts := options{
		WorkDir: "/work/vectis",
		Env:     []string{"PACKAGE_OUT=artifacts/packages"},
	}

	got, err := relativePackageOut(opts)
	if err != nil {
		t.Fatal(err)
	}

	if got != "artifacts/packages" {
		t.Fatalf("relative output = %q, want artifacts/packages", got)
	}

	opts.Env = []string{"PACKAGE_OUT=/work/vectis/out/packages"}
	got, err = relativePackageOut(opts)
	if err != nil {
		t.Fatal(err)
	}

	if got != "out/packages" {
		t.Fatalf("absolute in-worktree output = %q, want out/packages", got)
	}
}

func TestRelativePackageOutRejectsOutsideWorktree(t *testing.T) {
	_, err := relativePackageOut(options{
		WorkDir: "/work/vectis",
		Env:     []string{"PACKAGE_OUT=/tmp/packages"},
	})

	if err == nil || !strings.Contains(err.Error(), "PACKAGE_OUT must be relative") {
		t.Fatalf("expected outside worktree error, got %v", err)
	}
}

func TestSafeGuestName(t *testing.T) {
	if got, want := safeGuestName("vectis deb/arm64"), "vectis-deb-arm64"; got != want {
		t.Fatalf("safe guest name = %q, want %q", got, want)
	}
}

func clearPackageLocalEnv(t *testing.T) {
	t.Helper()

	for _, name := range []string{
		"PACKAGE_LOCAL_ALLOW_CROSS_CGO",
		"PACKAGE_LOCAL_MAKE",
		"PACKAGE_LOCAL_VM_CACHE_ROOT",
		"PACKAGE_LOCAL_VM_GO",
		"PACKAGE_LOCAL_VM_INSTANCE",
		"PACKAGE_LOCAL_VM_KEEP",
		"PACKAGE_LOCAL_VM_PRESERVE_ENV",
		"PACKAGE_LOCAL_VM_PROVIDER",
		"PACKAGE_LOCAL_VM_PROVIDER_PATH",
		"PACKAGE_LOCAL_VM_TIMEOUT",
		"PACKAGE_LOCAL_VM_WORKSPACE_ROOT",
	} {
		t.Setenv(name, "")
	}
}
