package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	linuxdeploy "vectis/deploy/linux"
)

var (
	linuxManifestPath = linuxdeploy.DefaultManifestPath
	linuxRenderOut    = linuxdeploy.DefaultArtifactDir
)

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

var deployLinuxCmd = &cobra.Command{
	Use:   "linux",
	Short: "Render Linux service artifacts",
	Run:   showCommandHelp,
}

var deployLinuxRenderCmd = &cobra.Command{
	Use:   "render",
	Short: "Render Linux systemd, env, sysusers, and tmpfiles artifacts",
	Args:  cobra.NoArgs,
	Run:   runDeployLinuxRender,
}
