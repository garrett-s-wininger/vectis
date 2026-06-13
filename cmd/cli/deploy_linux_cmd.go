package main

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	linuxdeploy "vectis/deploy/linux"
	"vectis/internal/platform"
)

var (
	linuxManifestPath        = linuxdeploy.DefaultManifestPath
	linuxRenderOut           = linuxdeploy.DefaultArtifactDir
	linuxDeployProvider      = platform.VirtualMachineProviderAuto
	linuxDeployProviderPath  string
	linuxDeployInstance      string
	linuxDeployTemplate      string
	linuxDeployArtifactDir   string
	linuxDeployKeepArtifacts bool
)

type linuxDeployCommandResult struct {
	Status        string `json:"status"`
	Provider      string `json:"provider,omitempty"`
	Instance      string `json:"instance"`
	Template      string `json:"template,omitempty"`
	ArtifactDir   string `json:"artifact_dir,omitempty"`
	ManifestPath  string `json:"manifest_path,omitempty"`
	Files         int    `json:"files,omitempty"`
	GuestCleaned  bool   `json:"guest_cleaned,omitempty"`
	LocalCleaned  bool   `json:"local_cleaned,omitempty"`
	CommandStdout string `json:"command_stdout,omitempty"`
	CommandStderr string `json:"command_stderr,omitempty"`
}

func runDeployLinuxRender(cmd *cobra.Command, args []string) {
	result, err := linuxdeploy.RenderToDir(linuxdeploy.RenderOptions{
		ManifestPath: linuxManifestPath,
		OutDir:       linuxRenderOut,
	})
	runCLIError(err)

	if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, result))
		return
	}

	fmt.Printf("Rendered Linux artifacts: %s (%d files)\n", result.OutputDir, result.Files)
}

func runDeployLinuxVerify(cmd *cobra.Command, args []string) {
	result, stdout, stderr, err := runLinuxDeploy(cmd, "", linuxdeploy.RunVMSmokeVerify)
	runCLIError(err)
	writeLinuxDeployResult(result, stdout, stderr, linuxDeployVerifyText(result))
}

func runDeployLinuxClean(cmd *cobra.Command, args []string) {
	result, stdout, stderr, err := runLinuxDeploy(cmd, "", linuxdeploy.RunVMSmokeClean)
	runCLIError(err)
	writeLinuxDeployResult(result, stdout, stderr, fmt.Sprintf("Cleaned Linux systemd artifacts from %s instance %s.", linuxDeployProviderDisplay(result.Provider), result.Instance))
}

func runDeployLinuxDown(cmd *cobra.Command, args []string) {
	result, stdout, stderr, err := runLinuxDeploy(cmd, "", linuxdeploy.RunVMSmokeDown)
	runCLIError(err)
	writeLinuxDeployResult(result, stdout, stderr, fmt.Sprintf("Stopped %s instance %s.", linuxDeployProviderDisplay(result.Provider), result.Instance))
}

func runDeployLinuxDelete(cmd *cobra.Command, args []string) {
	result, stdout, stderr, err := runLinuxDeploy(cmd, "", linuxdeploy.RunVMSmokeDelete)
	runCLIError(err)
	writeLinuxDeployResult(result, stdout, stderr, fmt.Sprintf("Deleted %s instance %s.", linuxDeployProviderDisplay(result.Provider), result.Instance))
}

func runLinuxDeploy(cmd *cobra.Command, provider string, run func(ctx context.Context, opts linuxdeploy.VMSmokeOptions) (linuxdeploy.VMSmokeResult, error)) (linuxdeploy.VMSmokeResult, string, string, error) {
	var stdout, stderr bytes.Buffer
	if provider == "" {
		provider = linuxDeployProvider
	}

	opts := linuxdeploy.VMSmokeOptions{
		Provider:      provider,
		ProviderPath:  linuxDeployProviderPath,
		Instance:      linuxDeployInstance,
		Template:      linuxDeployTemplate,
		ArtifactDir:   linuxDeployArtifactDir,
		ManifestPath:  linuxManifestPath,
		KeepArtifacts: linuxDeployKeepArtifacts,
	}

	if outputIsJSON() {
		opts.Stdout = &stdout
		opts.Stderr = &stderr
	} else {
		opts.Stdout = os.Stdout
		opts.Stderr = os.Stderr
	}

	result, err := run(cmd.Context(), opts)
	if outputIsJSON() {
		return result, stdout.String(), stderr.String(), err
	}

	return result, "", "", err
}

func linuxDeployVerifyText(result linuxdeploy.VMSmokeResult) string {
	text := fmt.Sprintf("Verified Linux systemd artifacts in %s instance %s.", linuxDeployProviderDisplay(result.Provider), result.Instance)
	if result.GuestCleaned {
		text += " Guest smoke artifacts were cleaned."
	}
	if result.LocalCleaned {
		text += " Local rendered artifacts were removed."
	}

	return text
}

func linuxDeployProviderDisplay(provider string) string {
	switch provider {
	case platform.VirtualMachineProviderLima:
		return "Lima"
	default:
		return provider
	}
}

func writeLinuxDeployResult(result linuxdeploy.VMSmokeResult, stdout, stderr, text string) {
	if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, linuxDeployCommandResult{
			Status:        result.Status,
			Provider:      result.Provider,
			Instance:      result.Instance,
			Template:      result.Template,
			ArtifactDir:   result.ArtifactDir,
			ManifestPath:  result.ManifestPath,
			Files:         result.Files,
			GuestCleaned:  result.GuestCleaned,
			LocalCleaned:  result.LocalCleaned,
			CommandStdout: stdout,
			CommandStderr: stderr,
		}))
		return
	}

	fmt.Println(text)
}

func configureDeployLinuxBackendFlags(cmd *cobra.Command, includeProvider bool) {
	if includeProvider {
		cmd.Flags().StringVar(&linuxDeployProvider, "provider", linuxDeployProvider, "Backend provider for Linux deploy smoke tests: auto or lima")
	}

	cmd.Flags().StringVar(&linuxDeployProviderPath, "provider-path", linuxDeployProviderPath, "Path to the backend provider command")
	cmd.Flags().StringVar(&linuxDeployInstance, "instance", linuxDeployInstance, "Backend instance name for Linux deploy smoke tests")
}

func configureDeployLinuxVerifyFlags(cmd *cobra.Command, includeProvider bool) {
	configureDeployLinuxBackendFlags(cmd, includeProvider)
	cmd.Flags().StringVar(&linuxDeployTemplate, "template", linuxDeployTemplate, "Backend template used when creating the Linux deploy smoke instance")
	cmd.Flags().StringVar(&linuxDeployArtifactDir, "artifacts", linuxDeployArtifactDir, "Optional directory where rendered Linux artifacts are written before copying into the backend; defaults to a temporary directory")
	cmd.Flags().BoolVar(&linuxDeployKeepArtifacts, "keep-artifacts", false, "Leave rendered artifacts and installed smoke files in place after verification")
}

var deployLinuxCmd = &cobra.Command{
	Use:   "linux",
	Short: "Render and test Linux service artifacts",
	Run:   showCommandHelp,
}

var deployLinuxRenderCmd = &cobra.Command{
	Use:   "render",
	Short: "Render Linux systemd, env, sysusers, and tmpfiles artifacts",
	Args:  cobra.NoArgs,
	Run:   runDeployLinuxRender,
}

var deployLinuxVerifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify Linux service artifacts on a real systemd host",
	Args:  cobra.NoArgs,
	Run:   runDeployLinuxVerify,
}

var deployLinuxCleanCmd = &cobra.Command{
	Use:   "clean",
	Short: "Remove Vectis Linux smoke artifacts from the selected backend",
	Args:  cobra.NoArgs,
	Run:   runDeployLinuxClean,
}

var deployLinuxDownCmd = &cobra.Command{
	Use:   "down",
	Short: "Stop the Linux deploy backend",
	Args:  cobra.NoArgs,
	Run:   runDeployLinuxDown,
}

var deployLinuxDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete the Linux deploy backend",
	Args:  cobra.NoArgs,
	Run:   runDeployLinuxDelete,
}
