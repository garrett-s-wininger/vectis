package main

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	linuxdeploy "vectis/deploy/linux"
)

var (
	linuxManifestPath      = linuxdeploy.DefaultManifestPath
	linuxRenderOut         = linuxdeploy.DefaultArtifactDir
	linuxLimaInstance      = linuxdeploy.DefaultLimaInstance
	linuxLimaTemplate      = linuxdeploy.DefaultLimaTemplate
	linuxLimaArtifactDir   string
	linuxLimaKeepArtifacts bool
)

type linuxLimaCommandResult struct {
	Status        string `json:"status"`
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

func runDeployLinuxLimaVerify(cmd *cobra.Command, args []string) {
	result, stdout, stderr, err := runLinuxLima(cmd, linuxdeploy.RunLimaVerify)
	runCLIError(err)
	text := fmt.Sprintf("Verified Linux systemd artifacts in Lima instance %s.", result.Instance)
	if result.GuestCleaned {
		text += " Guest smoke artifacts were cleaned."
	}
	if result.LocalCleaned {
		text += " Local rendered artifacts were removed."
	}
	writeLinuxLimaResult(result, stdout, stderr, text)
}

func runDeployLinuxLimaClean(cmd *cobra.Command, args []string) {
	result, stdout, stderr, err := runLinuxLima(cmd, linuxdeploy.RunLimaClean)
	runCLIError(err)
	writeLinuxLimaResult(result, stdout, stderr, fmt.Sprintf("Cleaned Linux systemd artifacts from Lima instance %s.", result.Instance))
}

func runDeployLinuxLimaDown(cmd *cobra.Command, args []string) {
	result, stdout, stderr, err := runLinuxLima(cmd, linuxdeploy.RunLimaDown)
	runCLIError(err)
	writeLinuxLimaResult(result, stdout, stderr, fmt.Sprintf("Stopped Lima instance %s.", result.Instance))
}

func runDeployLinuxLimaDelete(cmd *cobra.Command, args []string) {
	result, stdout, stderr, err := runLinuxLima(cmd, linuxdeploy.RunLimaDelete)
	runCLIError(err)
	writeLinuxLimaResult(result, stdout, stderr, fmt.Sprintf("Deleted Lima instance %s.", result.Instance))
}

func runLinuxLima(cmd *cobra.Command, run func(ctx context.Context, opts linuxdeploy.LimaOptions) (linuxdeploy.LimaResult, error)) (linuxdeploy.LimaResult, string, string, error) {
	var stdout, stderr bytes.Buffer
	opts := linuxdeploy.LimaOptions{
		Instance:      linuxLimaInstance,
		Template:      linuxLimaTemplate,
		ArtifactDir:   linuxLimaArtifactDir,
		ManifestPath:  linuxManifestPath,
		KeepArtifacts: linuxLimaKeepArtifacts,
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

func writeLinuxLimaResult(result linuxdeploy.LimaResult, stdout, stderr, text string) {
	if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, linuxLimaCommandResult{
			Status:        result.Status,
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

var deployLinuxLimaCmd = &cobra.Command{
	Use:   "lima",
	Short: "Run Linux artifact smoke tests in a Lima VM",
	Run:   showCommandHelp,
}

var deployLinuxLimaVerifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify Linux service artifacts on a real systemd host in Lima",
	Args:  cobra.NoArgs,
	Run:   runDeployLinuxLimaVerify,
}

var deployLinuxLimaCleanCmd = &cobra.Command{
	Use:   "clean",
	Short: "Remove Vectis smoke artifacts from the Lima guest",
	Args:  cobra.NoArgs,
	Run:   runDeployLinuxLimaClean,
}

var deployLinuxLimaDownCmd = &cobra.Command{
	Use:   "down",
	Short: "Stop the Linux deploy Lima instance",
	Args:  cobra.NoArgs,
	Run:   runDeployLinuxLimaDown,
}

var deployLinuxLimaDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete the Linux deploy Lima instance",
	Args:  cobra.NoArgs,
	Run:   runDeployLinuxLimaDelete,
}
