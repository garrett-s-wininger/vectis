//go:build mage

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestProtoWithWorkspaceHome(t *testing.T) {
	for _, tool := range []string{"protoc", "protoc-gen-go", "protoc-gen-go-grpc"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("protobuf toolchain unavailable: %v", err)
		}
	}

	workspace := t.TempDir()
	t.Chdir(workspace)
	t.Setenv("HOME", workspace)
	t.Setenv("USERPROFILE", workspace)
	t.Setenv("GOPATH", "")
	t.Setenv("GOTOOLCHAIN", "local")
	t.Setenv("PROTOC_GEN_GO", "")
	t.Setenv("PROTOC_GEN_GO_GRPC", "")

	protoDir := filepath.Join("api", "proto")
	if err := os.MkdirAll(protoDir, 0o755); err != nil {
		t.Fatal(err)
	}

	definition := `syntax = "proto3";
package probe;
option go_package = "vectis/api/gen/go;api";
message Check {}
service Probe { rpc CheckStatus(Check) returns (Check); }
`

	if err := os.WriteFile(filepath.Join(protoDir, "probe.proto"), []byte(definition), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := Proto(); err != nil {
		t.Fatalf("generate protobuf with workspace HOME: %v", err)
	}

	for _, name := range []string{"probe.pb.go", "probe_grpc.pb.go"} {
		if _, err := os.Stat(filepath.Join("api", "gen", "go", name)); err != nil {
			t.Fatalf("generated file %s: %v", name, err)
		}
	}
}

func TestProtocPluginResolution(t *testing.T) {
	goBinary, goErr := exec.LookPath(goCommand())
	for _, plugin := range []struct {
		envName string
		tool    string
	}{
		{"PROTOC_GEN_GO", "protoc-gen-go"},
		{"PROTOC_GEN_GO_GRPC", "protoc-gen-go-grpc"},
	} {
		t.Run(plugin.tool, func(t *testing.T) {
			binDir := t.TempDir()
			installed := filepath.Join(binDir, exeName(plugin.tool))

			if err := os.WriteFile(installed, []byte("test executable"), 0o755); err != nil {
				t.Fatal(err)
			}

			t.Setenv("PATH", binDir)
			t.Setenv(plugin.envName, "")

			if goErr == nil {
				t.Setenv("GO", goBinary)
			}

			t.Setenv("GOTOOLCHAIN", "local")
			t.Setenv("GOPATH", "")
			t.Setenv("HOME", t.TempDir())
			t.Setenv("USERPROFILE", os.Getenv("HOME"))

			t.Run("workspace home uses installed tool", func(t *testing.T) {
				if got := protocPlugin(plugin.envName, plugin.tool); got != installed {
					t.Fatalf("plugin = %q, want installed executable %q", got, installed)
				}
			})

			t.Run("explicit override wins", func(t *testing.T) {
				custom := filepath.Join(t.TempDir(), exeName(plugin.tool))
				t.Setenv(plugin.envName, custom)
				if got := protocPlugin(plugin.envName, plugin.tool); got != custom {
					t.Fatalf("plugin = %q, want explicit override %q", got, custom)
				}
			})

			t.Run("gopath fallback", func(t *testing.T) {
				if goErr != nil {
					t.Skipf("Go executable unavailable for GOPATH lookup: %v", goErr)
				}

				t.Setenv("PATH", t.TempDir())
				gopath := t.TempDir()
				t.Setenv("GOPATH", gopath)

				want := filepath.Join(gopath, "bin", exeName(plugin.tool))
				if got := protocPlugin(plugin.envName, plugin.tool); got != want {
					t.Fatalf("plugin = %q, want GOPATH fallback %q", got, want)
				}
			})
		})
	}
}
