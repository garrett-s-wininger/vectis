package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	kubernetesdeploy "vectis/deploy/kubernetes"
)

var (
	kubernetesManifestPath     = kubernetesdeploy.DefaultManifestPath
	kubernetesRenderOut        = "-"
	kubernetesNamespace        = kubernetesdeploy.DefaultNamespace
	kubernetesImageRegistry    string
	kubernetesImageTag         = kubernetesdeploy.DefaultImageTag
	kubernetesPostgresPassword string
	kubernetesBootstrapToken   string
	kubernetesEncryptedFSKey   string
)

type kubernetesCommandResult struct {
	kubernetesdeploy.RenderResult
	OutputPath string `json:"output_path,omitempty"`
	Manifest   string `json:"manifest,omitempty"`
}

func kubernetesRenderOptions() kubernetesdeploy.RenderOptions {
	return kubernetesdeploy.RenderOptions{
		ManifestPath:     kubernetesManifestPath,
		Namespace:        kubernetesNamespace,
		ImageRegistry:    kubernetesImageRegistry,
		ImageTag:         kubernetesImageTag,
		PostgresPassword: kubernetesPostgresPassword,
		BootstrapToken:   kubernetesBootstrapToken,
		EncryptedFSKey:   kubernetesEncryptedFSKey,
	}
}

func runDeployKubernetesRender(cmd *cobra.Command, args []string) {
	opts := kubernetesRenderOptions()
	manifest, result, err := kubernetesdeploy.Render(opts)
	runCLIError(err)

	if kubernetesRenderOut == "" || kubernetesRenderOut == "-" {
		if outputIsJSON() {
			runCLIError(writeJSON(os.Stdout, kubernetesCommandResult{
				RenderResult: result,
				Manifest:     string(manifest),
			}))

			return
		}

		_, _ = os.Stdout.Write(manifest)
		return
	}

	result, err = kubernetesdeploy.RenderToFile(opts, kubernetesRenderOut)
	runCLIError(err)

	if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, kubernetesCommandResult{
			RenderResult: result,
			OutputPath:   kubernetesRenderOut,
		}))

		return
	}

	fmt.Printf("Rendered Kubernetes manifest: %s\n", kubernetesRenderOut)
}

var deployKubernetesCmd = &cobra.Command{
	Use:   "kubernetes",
	Short: "Render Kubernetes reference deployment artifacts",
	Run:   showCommandHelp,
}

var deployKubernetesRenderCmd = &cobra.Command{
	Use:   "render",
	Short: "Render the Kubernetes reference deployment manifest",
	Args:  cobra.NoArgs,
	Run:   runDeployKubernetesRender,
}
