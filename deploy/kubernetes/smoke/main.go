package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	kubernetesdeploy "vectis/deploy/kubernetes"
)

func main() {
	var opts kubernetesdeploy.SmokeOptions
	var outputJSON bool

	flag.StringVar(&opts.Kubectl, "kubectl", "kubectl", "kubectl command path")
	flag.StringVar(&opts.Context, "context", kubernetesdeploy.DefaultSmokeContext, "Kubernetes context to use; empty uses kubectl's current context")
	flag.StringVar(&opts.Namespace, "namespace", kubernetesdeploy.DefaultNamespace, "Kubernetes namespace")
	flag.StringVar(&opts.JobPath, "job", kubernetesdeploy.DefaultSmokeJobPath, "Kubernetes smoke job definition path")
	flag.IntVar(&opts.APILocalPort, "api-local-port", kubernetesdeploy.DefaultSmokeAPIPort, "Local port used for the API port-forward")
	flag.DurationVar(&opts.Wait, "wait", kubernetesdeploy.DefaultSmokeWait, "Maximum time to wait for each smoke phase")
	flag.StringVar(&opts.APIToken, "api-token", os.Getenv("VECTIS_API_TOKEN"), "Optional API bearer token")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after a successful smoke")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.Stdout = os.Stdout
	result, err := kubernetesdeploy.RunSmoke(ctx, opts)
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
	}
}
