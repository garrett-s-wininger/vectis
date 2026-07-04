package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	kubernetesdeploy "vectis/deploy/kubernetes"
)

func main() {
	var opts kubernetesdeploy.ValidateOptions
	var outputJSON bool
	seedSecret := kubernetesdeploy.DefaultSmokeSeedSecret

	flag.StringVar(&opts.Kubectl, "kubectl", "kubectl", "kubectl command path")
	flag.StringVar(&opts.Context, "context", kubernetesdeploy.DefaultValidateContext, "Kubernetes context to use; empty uses kubectl's current context")
	flag.StringVar(&opts.Namespace, "namespace", kubernetesdeploy.DefaultNamespace, "Kubernetes namespace")
	flag.StringVar(&opts.ManifestPath, "manifest", kubernetesdeploy.DefaultManifestPath, "Kubernetes manifest template path")
	flag.StringVar(&opts.OutputPath, "output", kubernetesdeploy.DefaultOutputPath, "Rendered manifest output path")
	flag.StringVar(&opts.ImageRegistry, "image-registry", "", "Optional image registry prefix for Vectis component images")
	flag.StringVar(&opts.ImageTag, "image-tag", kubernetesdeploy.DefaultImageTag, "Image tag for Vectis component images")
	flag.StringVar(&opts.PostgresPassword, "postgres-password", "", "Postgres password embedded in the rendered Secret")
	flag.StringVar(&opts.BootstrapToken, "bootstrap-token", "", "API bootstrap token embedded in the rendered Secret")
	flag.StringVar(&opts.EncryptedFSKey, "encryptedfs-key", "", "EncryptedFS key embedded in the rendered Secret")
	flag.DurationVar(&opts.Wait, "wait", kubernetesdeploy.DefaultValidateWait, "Maximum time to wait for each validation phase")
	flag.BoolVar(&opts.SkipSmoke, "skip-smoke", false, "Skip the canonical workload smoke after rollout readiness")
	flag.StringVar(&opts.SmokeOptions.JobPath, "job", kubernetesdeploy.DefaultSmokeJobPath, "Kubernetes smoke job definition path")
	flag.StringVar(&opts.SmokeOptions.CLIImage, "cli-image", kubernetesdeploy.DefaultSmokeCLIImage, "vectis-cli image used to seed smoke secrets")
	flag.BoolVar(&seedSecret, "seed-secret", kubernetesdeploy.DefaultSmokeSeedSecret, "Seed the canonical encryptedfs smoke secret before submitting the job")
	flag.IntVar(&opts.SmokeOptions.APILocalPort, "api-local-port", kubernetesdeploy.DefaultSmokeAPIPort, "Local port used for the API port-forward")
	flag.StringVar(&opts.SmokeOptions.APIToken, "api-token", os.Getenv("VECTIS_API_TOKEN"), "Optional API bearer token")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after successful validation")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.SmokeOptions.SeedSecret = &seedSecret
	opts.Stdout = os.Stdout
	if outputJSON {
		opts.Stdout = os.Stderr
	}

	result, err := kubernetesdeploy.RunValidate(ctx, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if outputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: write JSON result: %v\n", err)
			os.Exit(1)
		}

		return
	}

	writeTextSummary(os.Stdout, result)
}

func writeTextSummary(out io.Writer, result kubernetesdeploy.ValidateResult) {
	if result.Smoke != nil {
		fmt.Fprintf(out, "Validated Kubernetes deployment: %s (run_id=%s)\n", result.ManifestPath, result.Smoke.RunID)
		return
	}

	fmt.Fprintf(out, "Validated Kubernetes deployment: %s\n", result.ManifestPath)
}
