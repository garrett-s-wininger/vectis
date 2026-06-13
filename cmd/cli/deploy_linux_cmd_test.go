package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/spf13/cobra"
	linuxdeploy "vectis/deploy/linux"
	"vectis/internal/platform"
)

func TestRunLinuxDeployUsesProviderOptionsAndCapturesJSONOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	restoreLinuxDeployCommandOptions(t)

	linuxDeployProvider = "test-provider"
	linuxDeployProviderPath = "/usr/local/bin/test-provider"
	linuxDeployInstance = "test-instance"
	linuxDeployTemplate = "test-template"
	linuxDeployArtifactDir = t.TempDir()
	linuxManifestPath = "deploy/linux/services.toml"
	linuxDeployKeepArtifacts = true

	var got linuxdeploy.VMSmokeOptions
	result, stdout, stderr, err := runLinuxDeploy(testCommandContext(), "", func(ctx context.Context, opts linuxdeploy.VMSmokeOptions) (linuxdeploy.VMSmokeResult, error) {
		got = opts
		fmt.Fprint(opts.Stdout, "guest stdout")
		fmt.Fprint(opts.Stderr, "guest stderr")
		return linuxdeploy.VMSmokeResult{
			Status:   "verified",
			Provider: opts.Provider,
			Instance: opts.Instance,
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != "verified" || result.Provider != "test-provider" || result.Instance != "test-instance" {
		t.Fatalf("unexpected result: %+v", result)
	}

	if stdout != "guest stdout" || stderr != "guest stderr" {
		t.Fatalf("captured stdout=%q stderr=%q", stdout, stderr)
	}

	if got.Provider != linuxDeployProvider ||
		got.ProviderPath != linuxDeployProviderPath ||
		got.Instance != linuxDeployInstance ||
		got.Template != linuxDeployTemplate ||
		got.ArtifactDir != linuxDeployArtifactDir ||
		got.ManifestPath != linuxManifestPath ||
		!got.KeepArtifacts {
		t.Fatalf("unexpected deploy options: %+v", got)
	}
}

func TestRunLinuxDeployDefaultsProviderToAuto(t *testing.T) {
	restoreLinuxDeployCommandOptions(t)

	linuxDeployInstance = "auto-instance"

	var gotProvider string
	_, _, _, err := runLinuxDeploy(testCommandContext(), "", func(ctx context.Context, opts linuxdeploy.VMSmokeOptions) (linuxdeploy.VMSmokeResult, error) {
		gotProvider = opts.Provider
		return linuxdeploy.VMSmokeResult{
			Status:   "verified",
			Provider: opts.Provider,
			Instance: opts.Instance,
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if gotProvider != platform.VirtualMachineProviderAuto {
		t.Fatalf("provider = %q, want %q", gotProvider, platform.VirtualMachineProviderAuto)
	}
}

func TestRunLinuxDeployForcedProviderOverridesDefault(t *testing.T) {
	restoreLinuxDeployCommandOptions(t)

	linuxDeployProvider = "not-lima"
	linuxDeployInstance = "lima-instance"

	var gotProvider string
	result, _, _, err := runLinuxDeploy(testCommandContext(), platform.VirtualMachineProviderLima, func(ctx context.Context, opts linuxdeploy.VMSmokeOptions) (linuxdeploy.VMSmokeResult, error) {
		gotProvider = opts.Provider
		return linuxdeploy.VMSmokeResult{
			Status:   "cleaned",
			Provider: opts.Provider,
			Instance: opts.Instance,
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if gotProvider != platform.VirtualMachineProviderLima {
		t.Fatalf("provider = %q, want %q", gotProvider, platform.VirtualMachineProviderLima)
	}

	if result.Provider != platform.VirtualMachineProviderLima || result.Instance != "lima-instance" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestLinuxDeployVerifyTextUsesProviderDisplayName(t *testing.T) {
	got := linuxDeployVerifyText(linuxdeploy.VMSmokeResult{
		Provider:     platform.VirtualMachineProviderLima,
		Instance:     "smoke",
		GuestCleaned: true,
		LocalCleaned: true,
	})
	want := "Verified Linux systemd artifacts in Lima instance smoke. Guest smoke artifacts were cleaned. Local rendered artifacts were removed."
	if got != want {
		t.Fatalf("verify text = %q, want %q", got, want)
	}
}

func restoreLinuxDeployCommandOptions(t *testing.T) {
	t.Helper()
	oldManifestPath := linuxManifestPath
	oldProvider := linuxDeployProvider
	oldProviderPath := linuxDeployProviderPath
	oldInstance := linuxDeployInstance
	oldTemplate := linuxDeployTemplate
	oldArtifactDir := linuxDeployArtifactDir
	oldKeepArtifacts := linuxDeployKeepArtifacts

	t.Cleanup(func() {
		linuxManifestPath = oldManifestPath
		linuxDeployProvider = oldProvider
		linuxDeployProviderPath = oldProviderPath
		linuxDeployInstance = oldInstance
		linuxDeployTemplate = oldTemplate
		linuxDeployArtifactDir = oldArtifactDir
		linuxDeployKeepArtifacts = oldKeepArtifacts
	})
}

func testCommandContext() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	return cmd
}
