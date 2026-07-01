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
	seedSecret := kubernetesdeploy.DefaultSmokeSeedSecret

	flag.StringVar(&opts.Kubectl, "kubectl", "kubectl", "kubectl command path")
	flag.StringVar(&opts.Context, "context", kubernetesdeploy.DefaultSmokeContext, "Kubernetes context to use; empty uses kubectl's current context")
	flag.StringVar(&opts.Namespace, "namespace", kubernetesdeploy.DefaultNamespace, "Kubernetes namespace")
	flag.StringVar(&opts.JobPath, "job", kubernetesdeploy.DefaultSmokeJobPath, "Kubernetes smoke job definition path")
	flag.StringVar(&opts.WorkerCoreJobPath, "worker-core-job", kubernetesdeploy.DefaultSmokeWorkerCoreJobPath, "Kubernetes worker-core provider smoke job definition path")
	flag.StringVar(&opts.CancelJobPath, "cancel-job", kubernetesdeploy.DefaultSmokeCancelJobPath, "Kubernetes worker-control cancel smoke job definition path")
	flag.StringVar(&opts.ScaleJobPath, "scale-job", kubernetesdeploy.DefaultSmokeScaleJobPath, "Kubernetes worker scaling smoke job definition path")
	flag.StringVar(&opts.OrphanJobPath, "orphan-job", kubernetesdeploy.DefaultSmokeOrphanJobPath, "Kubernetes worker pod-loss orphan smoke job definition path")
	flag.StringVar(&opts.RepairJobPath, "repair-job", kubernetesdeploy.DefaultSmokeRepairJobPath, "Kubernetes explicit orphan repair smoke job definition path")
	flag.BoolVar(&opts.WorkerCoreOnly, "worker-core-only", false, "Run only the Kubernetes worker-core provider smoke")
	flag.BoolVar(&opts.CancelOnly, "cancel-only", false, "Run only the worker-control cancel smoke")
	flag.BoolVar(&opts.ScaleOnly, "scale-only", false, "Run only the worker scaling smoke")
	flag.BoolVar(&opts.OrphanOnly, "orphan-only", false, "Run only the worker pod-loss orphan smoke")
	flag.BoolVar(&opts.RepairOnly, "repair-only", false, "Run only the explicit orphan repair smoke")
	flag.BoolVar(&opts.GerritStreamOnly, "gerrit-stream-only", false, "Run only the Kubernetes Gerrit stream bridge smoke")
	flag.IntVar(&opts.ScaleWorkerReplicas, "scale-worker-replicas", kubernetesdeploy.DefaultSmokeScaleWorkerReplicas, "Worker deployment replica count used during the scaling smoke")
	flag.IntVar(&opts.ScaleMinWorkers, "scale-min-workers", kubernetesdeploy.DefaultSmokeScaleMinWorkers, "Minimum distinct worker lease owners required by the scaling smoke")
	flag.DurationVar(&opts.OrphanLeaseTTL, "orphan-lease-ttl", kubernetesdeploy.DefaultSmokeOrphanLeaseTTL, "Temporary worker execution lease TTL used during the orphan smoke")
	flag.DurationVar(&opts.OrphanStability, "orphan-stability", kubernetesdeploy.DefaultSmokeOrphanStability, "How long the orphan smoke requires the run to remain orphaned")
	flag.DurationVar(&opts.RepairLeaseTTL, "repair-lease-ttl", kubernetesdeploy.DefaultSmokeRepairLeaseTTL, "Temporary worker execution lease TTL used during the repair smoke")
	flag.DurationVar(&opts.RepairReadyAfter, "repair-ready-after", kubernetesdeploy.DefaultSmokeRepairReadyAfter, "Delay after repair-job submission before its retry may take the success path")
	flag.StringVar(&opts.CLIImage, "cli-image", kubernetesdeploy.DefaultSmokeCLIImage, "vectis-cli image used to seed smoke secrets")
	flag.StringVar(&opts.WorkerCoreImage, "worker-core-image", kubernetesdeploy.DefaultSmokeWorkerCoreImage, "vectis-worker-core-kubernetes image used during the worker-core provider smoke")
	flag.StringVar(&opts.WorkerCoreTaskImage, "worker-core-task-image", kubernetesdeploy.DefaultSmokeWorkerCoreTaskImage, "Task container image used by the Kubernetes worker-core provider smoke")
	flag.StringVar(&opts.GerritImage, "gerrit-image", kubernetesdeploy.DefaultSmokeGerritImage, "Gerrit image used by the Kubernetes Gerrit stream smoke")
	flag.IntVar(&opts.GerritHTTPPort, "gerrit-http-local-port", kubernetesdeploy.DefaultSmokeGerritHTTPPort, "Local port used for the Gerrit HTTP port-forward")
	flag.IntVar(&opts.GerritSSHLocalPort, "gerrit-ssh-local-port", kubernetesdeploy.DefaultSmokeGerritSSHLocalPort, "Local port used for the Gerrit SSH port-forward")
	flag.StringVar(&opts.GerritClusterURL, "gerrit-cluster-url", kubernetesdeploy.DefaultSmokeGerritClusterURL, "In-cluster Gerrit base URL used by Vectis SCM triggers")
	flag.StringVar(&opts.GerritSSHHost, "gerrit-ssh-host", kubernetesdeploy.DefaultSmokeGerritSSHHost, "In-cluster Gerrit SSH host used by the stream bridge")
	flag.IntVar(&opts.GerritSSHPort, "gerrit-ssh-port", kubernetesdeploy.DefaultSmokeGerritSSHPort, "In-cluster Gerrit SSH port used by the stream bridge")
	flag.StringVar(&opts.GerritAccountID, "gerrit-account-id", kubernetesdeploy.DefaultSmokeGerritAccountID, "Development Gerrit account id used by the stream smoke")
	flag.StringVar(&opts.GerritUsername, "gerrit-username", kubernetesdeploy.DefaultSmokeGerritUsername, "Gerrit username used by the stream smoke")
	flag.StringVar(&opts.GerritProject, "gerrit-project", "", "Gerrit project name; generated when empty")
	flag.StringVar(&opts.GerritProjectPrefix, "gerrit-project-prefix", kubernetesdeploy.DefaultSmokeGerritProjectPrefix, "Generated Gerrit project prefix")
	flag.StringVar(&opts.GerritGitBin, "gerrit-git", kubernetesdeploy.DefaultSmokeGerritGitBin, "git executable used to push the Gerrit smoke change")
	flag.BoolVar(&seedSecret, "seed-secret", kubernetesdeploy.DefaultSmokeSeedSecret, "Seed the canonical encryptedfs smoke secret before submitting the job")
	flag.IntVar(&opts.APILocalPort, "api-local-port", kubernetesdeploy.DefaultSmokeAPIPort, "Local port used for the API port-forward")
	flag.DurationVar(&opts.Wait, "wait", kubernetesdeploy.DefaultSmokeWait, "Maximum time to wait for each smoke phase")
	flag.StringVar(&opts.APIToken, "api-token", os.Getenv("VECTIS_API_TOKEN"), "Optional API bearer token")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after a successful smoke")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.SeedSecret = &seedSecret
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
