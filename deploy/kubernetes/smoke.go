package kubernetes

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	api "vectis/api/gen/go"
)

const (
	DefaultSmokeContext              = "kind-vectis"
	DefaultSmokeJobPath              = "examples/e2e-canonical.json"
	DefaultSmokeWorkerCoreJobPath    = "examples/e2e-kubernetes-worker-core.json"
	DefaultSmokeCancelJobPath        = "examples/e2e-kubernetes-cancel.json"
	DefaultSmokeKnoxJobPath          = "examples/e2e-kubernetes-knox.json"
	DefaultSmokeScaleJobPath         = "examples/e2e-kubernetes-scale.json"
	DefaultSmokeOrphanJobPath        = "examples/e2e-kubernetes-orphan.json"
	DefaultSmokeRepairJobPath        = "examples/e2e-kubernetes-repair.json"
	DefaultSmokeScaleWorkerReplicas  = 3
	DefaultSmokeScaleMinWorkers      = 2
	DefaultSmokeOrphanLeaseTTL       = 30 * time.Second
	DefaultSmokeOrphanStability      = 20 * time.Second
	DefaultSmokeRepairLeaseTTL       = 30 * time.Second
	DefaultSmokeRepairReadyAfter     = 75 * time.Second
	DefaultSmokeCLIImage             = "localhost/vectis-cli:dev-local"
	DefaultSmokeWorkerCoreImage      = "localhost/vectis-worker-core-kubernetes:dev-local"
	DefaultSmokeWorkerCoreTaskImage  = "localhost/vectis-worker:dev-local"
	DefaultSmokeSeedSecret           = true
	DefaultSmokeAPIPort              = 18080
	DefaultSmokeGerritImage          = "docker.io/gerritcodereview/gerrit:3.14.1-ubuntu24"
	DefaultSmokeGerritHTTPPort       = 18086
	DefaultSmokeGerritSSHLocalPort   = 29419
	DefaultSmokeGerritClusterURL     = "http://vectis-gerrit:8080"
	DefaultSmokeGerritSSHHost        = "vectis-gerrit"
	DefaultSmokeGerritSSHPort        = 29418
	DefaultSmokeGerritAccountID      = "1000000"
	DefaultSmokeGerritUsername       = "admin"
	DefaultSmokeGerritProjectPrefix  = "vectis-k8s-gerrit-stream"
	DefaultSmokeGerritGitBin         = "git"
	DefaultSmokeS3Image              = "docker.io/chrislusf/seaweedfs:4.36"
	DefaultSmokeS3LocalPort          = 18335
	DefaultSmokeS3ClusterEndpoint    = "http://vectis-s3-artifact:8333"
	DefaultSmokeS3Bucket             = "vectis-artifacts"
	DefaultSmokeS3Prefix             = "kubernetes-smoke"
	DefaultSmokeS3AccessKeyID        = "vectis-smoke"
	DefaultSmokeS3SecretAccessKey    = "vectis-smoke-secret"
	DefaultSmokeS3TempDir            = "/data/vectis/artifact/s3-tmp"
	DefaultSmokeKnoxImage            = "localhost/vectis-knox-smoke:dev-local"
	DefaultSmokeKnoxLocalPort        = 19001
	DefaultSmokeKnoxClusterURL       = "https://vectis-knox:9000"
	DefaultSmokeKnoxCertMountPath    = "/run/vectis/knox-smoke"
	DefaultSmokeKnoxAuthToken        = "0tknox-smoke"
	DefaultSmokeKnoxWrongAuthToken   = "0twrong-knox-smoke"
	DefaultSmokeKnoxKeyID            = "team:smoke_token"
	DefaultSmokeKnoxRef              = "knox://team/smoke_token"
	DefaultSmokeKnoxMissingRef       = "knox://team/missing_token"
	DefaultSmokeKnoxSecret           = "knox-smoke-secret"
	DefaultSmokeWait                 = 180 * time.Second
	smokeAPIServiceName              = "service/vectis-api"
	smokeAPIRemotePort               = 8080
	smokeWorkerDeploymentName        = "deployment/vectis-worker"
	smokeWorkerPodSelector           = "app.kubernetes.io/name=vectis,app.kubernetes.io/component=worker"
	smokeWorkerContainerName         = "worker"
	smokeWorkerCoreContainerName     = "worker-core"
	smokeWorkerCoreServiceAccount    = "vectis-worker-core-kubernetes"
	smokeWorkerCoreTaskJobSelector   = "app.kubernetes.io/name=vectis-worker-core-task"
	smokeWorkerCoreTaskRunAnnotation = "vectis.dev/run-id"
	smokeWorkerLeaseTTLEnv           = "VECTIS_WORKER_EXECUTION_LEASE_TTL"
	smokeRepairReadyAfterPlaceholder = "__VECTIS_REPAIR_READY_AFTER_UNIX__"
	smokeHTTPClientDelay             = 30 * time.Second
	smokeSecretRef                   = "encryptedfs://team/smoke-token"
	smokeSecretPlaintext             = "spiffe-secret"
	smokeSecretRoot                  = "/data/vectis/secrets/encryptedfs"
	smokeSecretKeyFile               = "/run/vectis/secrets/encryptedfs.key"
)

type SmokeOptions struct {
	Kubectl             string
	Context             string
	Namespace           string
	JobPath             string
	WorkerCoreJobPath   string
	CancelJobPath       string
	ScaleJobPath        string
	OrphanJobPath       string
	RepairJobPath       string
	WorkerCoreOnly      bool
	CancelOnly          bool
	ScaleOnly           bool
	OrphanOnly          bool
	RepairOnly          bool
	GerritStreamOnly    bool
	S3ArtifactOnly      bool
	KnoxSecretsOnly     bool
	ScaleWorkerReplicas int
	ScaleMinWorkers     int
	OrphanLeaseTTL      time.Duration
	OrphanStability     time.Duration
	RepairLeaseTTL      time.Duration
	RepairReadyAfter    time.Duration
	CLIImage            string
	WorkerCoreImage     string
	WorkerCoreTaskImage string
	GerritImage         string
	GerritHTTPPort      int
	GerritSSHLocalPort  int
	GerritClusterURL    string
	GerritSSHHost       string
	GerritSSHPort       int
	GerritAccountID     string
	GerritUsername      string
	GerritProject       string
	GerritProjectPrefix string
	GerritGitBin        string
	GerritKeepFixture   bool
	S3Image             string
	S3LocalPort         int
	S3ClusterEndpoint   string
	S3Bucket            string
	S3Prefix            string
	S3AccessKeyID       string
	S3SecretAccessKey   string
	S3TempDir           string
	S3KeepFixture       bool
	KnoxImage           string
	KnoxLocalPort       int
	KnoxClusterURL      string
	KnoxCertMountPath   string
	KnoxAuthToken       string
	KnoxWrongAuthToken  string
	KnoxKeyID           string
	KnoxRef             string
	KnoxMissingRef      string
	KnoxExpectedData    string
	KnoxKeepFixture     bool
	SeedSecret          *bool
	APILocalPort        int
	Wait                time.Duration
	APIToken            string
	Stdout              io.Writer
}

type SmokeResult struct {
	Status                string               `json:"status"`
	Context               string               `json:"context,omitempty"`
	Namespace             string               `json:"namespace"`
	JobPath               string               `json:"job_path"`
	JobID                 string               `json:"job_id,omitempty"`
	RunID                 string               `json:"run_id"`
	RunStatus             string               `json:"run_status"`
	TaskJob               string               `json:"task_job,omitempty"`
	WorkerOwners          []string             `json:"worker_owners,omitempty"`
	DeletedPod            string               `json:"deleted_pod,omitempty"`
	RepairChecks          []SmokeRepairCheck   `json:"repair_checks,omitempty"`
	GerritProject         string               `json:"gerrit_project,omitempty"`
	GerritChange          string               `json:"gerrit_change,omitempty"`
	TriggerSourceInstance string               `json:"trigger_source_instance,omitempty"`
	S3Endpoint            string               `json:"s3_endpoint,omitempty"`
	S3Bucket              string               `json:"s3_bucket,omitempty"`
	S3Prefix              string               `json:"s3_prefix,omitempty"`
	KnoxURL               string               `json:"knox_url,omitempty"`
	KnoxRef               string               `json:"knox_ref,omitempty"`
	Artifacts             []SmokeArtifactCheck `json:"artifacts"`
}

type SmokeArtifactCheck struct {
	Name  string `json:"name"`
	Bytes int    `json:"bytes"`
}

type SmokeRepairCheck struct {
	Action     string `json:"action"`
	RunID      string `json:"run_id"`
	RunStatus  string `json:"run_status"`
	DeletedPod string `json:"deleted_pod,omitempty"`
	Attempts   int    `json:"attempts,omitempty"`
}

type smokeJobRunResult struct {
	JobID    string `json:"job_id,omitempty"`
	ID       string `json:"id,omitempty"`
	RunID    string `json:"run_id,omitempty"`
	RunIndex int    `json:"run_index,omitempty"`
}

type smokeRunDetail struct {
	RunID                 string               `json:"run_id"`
	RunIndex              int                  `json:"run_index,omitempty"`
	Status                string               `json:"status"`
	TriggerInvocationID   *string              `json:"trigger_invocation_id,omitempty"`
	TriggerID             *int64               `json:"trigger_id,omitempty"`
	TriggerKey            *string              `json:"trigger_key,omitempty"`
	TriggerName           *string              `json:"trigger_name,omitempty"`
	TriggerType           *string              `json:"trigger_type,omitempty"`
	TriggerSourceInstance *string              `json:"trigger_source_instance,omitempty"`
	TriggerPayloadHash    *string              `json:"trigger_payload_hash,omitempty"`
	NextAction            string               `json:"next_action,omitempty"`
	DispatchEvents        []smokeDispatchEvent `json:"dispatch_events,omitempty"`
}

type smokeDispatchEvent struct {
	Source         string `json:"source"`
	SourceInstance string `json:"source_instance,omitempty"`
	EventType      string `json:"event_type"`
	Message        string `json:"message,omitempty"`
	CreatedAt      int64  `json:"created_at,omitempty"`
}

type smokeLogEntry struct {
	Stream   int    `json:"stream"`
	Sequence int64  `json:"sequence"`
	Data     string `json:"data"`
}

type smokeDeployment struct {
	Spec struct {
		Replicas *int `json:"replicas"`
		Template struct {
			Spec struct {
				Containers []smokeContainer `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}

type smokeContainer struct {
	Name string        `json:"name"`
	Env  []smokeEnvVar `json:"env,omitempty"`
}

type smokeEnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
}

type smokePodList struct {
	Items []smokePod `json:"items"`
}

type smokePod struct {
	Metadata struct {
		Name              string  `json:"name"`
		DeletionTimestamp *string `json:"deletionTimestamp,omitempty"`
	} `json:"metadata"`
	Status struct {
		Phase             string `json:"phase"`
		ContainerStatuses []struct {
			Name  string `json:"name"`
			Ready bool   `json:"ready"`
		} `json:"containerStatuses,omitempty"`
	} `json:"status"`
}

type smokeKubernetesJobList struct {
	Items []smokeKubernetesJob `json:"items"`
}

type smokeKubernetesJob struct {
	Metadata struct {
		Name        string            `json:"name"`
		Annotations map[string]string `json:"annotations,omitempty"`
	} `json:"metadata"`
	Status struct {
		Conditions []struct {
			Type    string `json:"type,omitempty"`
			Status  string `json:"status,omitempty"`
			Reason  string `json:"reason,omitempty"`
			Message string `json:"message,omitempty"`
		} `json:"conditions,omitempty"`
	} `json:"status"`
}

type smokeRunTasksResponse struct {
	Data []smokeRunTask `json:"data"`
}

type smokeRunTask struct {
	TaskKey  string                `json:"task_key"`
	Attempts []smokeRunTaskAttempt `json:"attempts"`
}

type smokeRunTaskAttempt struct {
	ExecutionStatus string  `json:"execution_status"`
	LeaseOwner      *string `json:"lease_owner,omitempty"`
}

type smokePortForward struct {
	cancel context.CancelFunc
	done   <-chan error
}

type smokeLineBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func RunSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	opts = normalizeSmokeOptions(opts)
	if opts.WorkerCoreOnly {
		return runWorkerCoreSmoke(ctx, opts)
	}

	if opts.CancelOnly {
		return runCancelSmoke(ctx, opts)
	}

	if opts.ScaleOnly {
		return runScaleSmoke(ctx, opts)
	}

	if opts.OrphanOnly {
		return runOrphanSmoke(ctx, opts)
	}

	if opts.RepairOnly {
		return runRepairSmoke(ctx, opts)
	}

	if opts.GerritStreamOnly {
		return runGerritStreamSmoke(ctx, opts)
	}

	if opts.S3ArtifactOnly {
		return runS3ArtifactSmoke(ctx, opts)
	}
	if opts.KnoxSecretsOnly {
		return runKnoxSecretsSmoke(ctx, opts)
	}

	return runCanonicalSmoke(ctx, opts)
}

func runCanonicalSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	out := opts.Stdout
	if smokeSeedSecretEnabled(opts) {
		fmt.Fprintf(out, "Seeding %s with %s\n", smokeSecretRef, opts.CLIImage)
		seedCtx, seedCancel := context.WithTimeout(ctx, opts.Wait)
		err := seedSmokeSecret(seedCtx, opts)
		seedCancel()

		if err != nil {
			return SmokeResult{}, err
		}
	}

	fmt.Fprintf(out, "Starting API port-forward on 127.0.0.1:%d\n", opts.APILocalPort)
	pf, err := startSmokeAPIPortForward(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}
	defer pf.stop()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", opts.APILocalPort)
	client := &http.Client{Timeout: smokeHTTPClientDelay}

	fmt.Fprintf(out, "Submitting %s\n", opts.JobPath)
	run, err := submitSmokeJob(client, baseURL, opts)
	if err != nil {
		return SmokeResult{}, err
	}
	fmt.Fprintf(out, "Submitted job_id=%s run_id=%s\n", smokeJobID(run), run.RunID)

	runCtx, runCancel := context.WithTimeout(ctx, opts.Wait)
	detail, err := waitForSmokeRunStatus(runCtx, client, baseURL, opts.APIToken, run.RunID, out, "succeeded")
	runCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	markers := []string{
		"canonical-control-start",
		"canonical-artifact-written",
		"canonical-fanout-ok",
		"canonical-registry-retry-succeeded",
	}
	if smokeSeedSecretEnabled(opts) {
		markers = append(markers, "canonical-secret-ok")
	}
	fmt.Fprintln(out, "Verifying run logs")
	logCtx, logCancel := context.WithTimeout(ctx, opts.Wait)
	err = waitForSmokeLogMarkers(logCtx, baseURL, opts.APIToken, run.RunID, markers, out)
	logCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	artifactChecks, err := verifySmokeArtifacts(client, baseURL, opts.APIToken, run.RunID)
	if err != nil {
		return SmokeResult{}, err
	}

	result := SmokeResult{
		Status:    "ok",
		Context:   strings.TrimSpace(opts.Context),
		Namespace: opts.Namespace,
		JobPath:   opts.JobPath,
		JobID:     smokeJobID(run),
		RunID:     run.RunID,
		RunStatus: detail.Status,
		Artifacts: artifactChecks,
	}

	fmt.Fprintf(out, "Kubernetes smoke succeeded: run_id=%s\n", run.RunID)
	return result, nil
}

func runWorkerCoreSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	if strings.TrimSpace(opts.WorkerCoreJobPath) == "" {
		return SmokeResult{}, fmt.Errorf("worker-core job path is required")
	}

	if strings.TrimSpace(opts.WorkerCoreImage) == "" {
		return SmokeResult{}, fmt.Errorf("worker-core image is required")
	}

	if strings.TrimSpace(opts.WorkerCoreTaskImage) == "" {
		return SmokeResult{}, fmt.Errorf("worker-core task image is required")
	}

	out := opts.Stdout
	setupCtx, setupCancel := context.WithTimeout(ctx, opts.Wait)
	if err := applySmokeWorkerCoreRBAC(setupCtx, opts); err != nil {
		setupCancel()
		return SmokeResult{}, err
	}

	setupCancel()
	defer cleanupSmokeWorkerCoreRBAC(opts)

	patchCtx, patchCancel := context.WithTimeout(ctx, opts.Wait)
	if err := patchSmokeWorkerForKubernetesCore(patchCtx, opts); err != nil {
		patchCancel()
		return SmokeResult{}, err
	}

	patchCancel()
	defer restoreSmokeWorkerAfterKubernetesCore(opts)

	fmt.Fprintf(out, "Starting API port-forward on 127.0.0.1:%d\n", opts.APILocalPort)
	pf, err := startSmokeAPIPortForward(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}
	defer pf.stop()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", opts.APILocalPort)
	client := &http.Client{Timeout: smokeHTTPClientDelay}

	submitOpts := opts
	submitOpts.JobPath = opts.WorkerCoreJobPath
	fmt.Fprintf(out, "Submitting %s through Kubernetes worker core\n", submitOpts.JobPath)
	run, err := submitSmokeJob(client, baseURL, submitOpts)
	if err != nil {
		return SmokeResult{}, err
	}

	fmt.Fprintf(out, "Submitted job_id=%s run_id=%s\n", smokeJobID(run), run.RunID)
	taskJobCtx, taskJobCancel := context.WithTimeout(ctx, opts.Wait)
	taskJob, err := waitForSmokeWorkerCoreTaskJob(taskJobCtx, opts, run.RunID, out)
	taskJobCancel()
	if err != nil {
		return SmokeResult{}, err
	}
	defer cleanupSmokeWorkerCoreTaskJob(opts, taskJob)

	runCtx, runCancel := context.WithTimeout(ctx, opts.Wait)
	detail, err := waitForSmokeRunStatus(runCtx, client, baseURL, opts.APIToken, run.RunID, out, "succeeded")
	runCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	fmt.Fprintln(out, "Verifying worker-core smoke logs")
	logCtx, logCancel := context.WithTimeout(ctx, opts.Wait)
	err = waitForSmokeLogMarkers(logCtx, baseURL, opts.APIToken, run.RunID, []string{"kubernetes-worker-core-smoke-ok"}, out)
	logCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	result := SmokeResult{
		Status:    "ok",
		Context:   strings.TrimSpace(opts.Context),
		Namespace: opts.Namespace,
		JobPath:   opts.WorkerCoreJobPath,
		JobID:     smokeJobID(run),
		RunID:     run.RunID,
		RunStatus: detail.Status,
		TaskJob:   taskJob,
	}

	fmt.Fprintf(out, "Kubernetes worker-core smoke succeeded: run_id=%s task_job=%s\n", run.RunID, taskJob)
	return result, nil
}

func runCancelSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}
	if strings.TrimSpace(opts.CancelJobPath) == "" {
		return SmokeResult{}, fmt.Errorf("cancel job path is required")
	}

	out := opts.Stdout
	fmt.Fprintf(out, "Starting API port-forward on 127.0.0.1:%d\n", opts.APILocalPort)
	pf, err := startSmokeAPIPortForward(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}
	defer pf.stop()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", opts.APILocalPort)
	client := &http.Client{Timeout: smokeHTTPClientDelay}

	submitOpts := opts
	submitOpts.JobPath = opts.CancelJobPath
	fmt.Fprintf(out, "Submitting %s\n", submitOpts.JobPath)
	run, err := submitSmokeJob(client, baseURL, submitOpts)
	if err != nil {
		return SmokeResult{}, err
	}

	fmt.Fprintf(out, "Submitted job_id=%s run_id=%s\n", smokeJobID(run), run.RunID)
	fmt.Fprintln(out, "Waiting for cancellable run to start")
	logCtx, logCancel := context.WithTimeout(ctx, opts.Wait)
	err = waitForSmokeLogMarkers(logCtx, baseURL, opts.APIToken, run.RunID, []string{"canonical-cancel-started"}, out)
	logCancel()

	if err != nil {
		return SmokeResult{}, err
	}

	fmt.Fprintf(out, "Cancelling run_id=%s through worker-control\n", run.RunID)
	if err := cancelSmokeRun(client, baseURL, opts.APIToken, run.RunID); err != nil {
		return SmokeResult{}, err
	}

	runCtx, runCancel := context.WithTimeout(ctx, opts.Wait)
	detail, err := waitForSmokeRunStatus(runCtx, client, baseURL, opts.APIToken, run.RunID, out, "cancelled")
	runCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	result := SmokeResult{
		Status:    "ok",
		Context:   strings.TrimSpace(opts.Context),
		Namespace: opts.Namespace,
		JobPath:   opts.CancelJobPath,
		JobID:     smokeJobID(run),
		RunID:     run.RunID,
		RunStatus: detail.Status,
	}

	fmt.Fprintf(out, "Kubernetes cancel smoke succeeded: run_id=%s\n", run.RunID)
	return result, nil
}

func runScaleSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	if strings.TrimSpace(opts.ScaleJobPath) == "" {
		return SmokeResult{}, fmt.Errorf("scale job path is required")
	}

	if opts.ScaleWorkerReplicas < 2 {
		return SmokeResult{}, fmt.Errorf("scale worker replicas must be >= 2")
	}

	if opts.ScaleMinWorkers < 2 {
		return SmokeResult{}, fmt.Errorf("scale min workers must be >= 2")
	}

	if opts.ScaleMinWorkers > opts.ScaleWorkerReplicas {
		return SmokeResult{}, fmt.Errorf("scale min workers must be <= scale worker replicas")
	}

	out := opts.Stdout
	scaleCtx, scaleCancel := context.WithTimeout(ctx, opts.Wait)
	originalReplicas, err := currentSmokeWorkerReplicas(scaleCtx, opts)
	scaleCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	restore := false
	if originalReplicas != opts.ScaleWorkerReplicas {
		fmt.Fprintf(out, "Scaling %s from %d to %d replicas\n", smokeWorkerDeploymentName, originalReplicas, opts.ScaleWorkerReplicas)
		scaleCtx, scaleCancel = context.WithTimeout(ctx, opts.Wait)
		err = scaleSmokeWorkerDeployment(scaleCtx, opts, opts.ScaleWorkerReplicas)
		scaleCancel()

		if err != nil {
			return SmokeResult{}, err
		}

		restore = true
	}

	if restore {
		defer restoreSmokeWorkerDeployment(opts, originalReplicas)
	}

	fmt.Fprintf(out, "Starting API port-forward on 127.0.0.1:%d\n", opts.APILocalPort)
	pf, err := startSmokeAPIPortForward(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}
	defer pf.stop()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", opts.APILocalPort)
	client := &http.Client{Timeout: smokeHTTPClientDelay}

	submitOpts := opts
	submitOpts.JobPath = opts.ScaleJobPath
	fmt.Fprintf(out, "Submitting %s\n", submitOpts.JobPath)
	run, err := submitSmokeJob(client, baseURL, submitOpts)
	if err != nil {
		return SmokeResult{}, err
	}

	fmt.Fprintf(out, "Submitted job_id=%s run_id=%s\n", smokeJobID(run), run.RunID)
	fmt.Fprintln(out, "Waiting for distributed scale branches to be owned by multiple workers")
	scaleTaskKeys := []string{"scale-branch-a", "scale-branch-b", "scale-branch-c"}
	ownerCtx, ownerCancel := context.WithTimeout(ctx, opts.Wait)
	owners, err := waitForSmokeTaskLeaseOwners(ownerCtx, client, baseURL, opts.APIToken, run.RunID, scaleTaskKeys, opts.ScaleMinWorkers, out)
	ownerCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	runCtx, runCancel := context.WithTimeout(ctx, opts.Wait)
	detail, err := waitForSmokeRunStatus(runCtx, client, baseURL, opts.APIToken, run.RunID, out, "succeeded")
	runCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	result := SmokeResult{
		Status:       "ok",
		Context:      strings.TrimSpace(opts.Context),
		Namespace:    opts.Namespace,
		JobPath:      opts.ScaleJobPath,
		JobID:        smokeJobID(run),
		RunID:        run.RunID,
		RunStatus:    detail.Status,
		WorkerOwners: owners,
	}

	fmt.Fprintf(out, "Kubernetes scale smoke succeeded: run_id=%s workers=%s\n", run.RunID, strings.Join(owners, ","))
	return result, nil
}

type smokeJobSubmitFunc func(client *http.Client, baseURL string, opts SmokeOptions) (smokeJobRunResult, error)

type smokePodLossScenario struct {
	Name      string
	JobPath   string
	LeaseTTL  time.Duration
	Stability time.Duration
	Submit    smokeJobSubmitFunc
}

type smokePodLossResult struct {
	Run        smokeJobRunResult
	Detail     smokeRunDetail
	Owner      string
	DeletedPod string
}

func runOrphanSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	orphan, err := runPodLossOrphanScenario(ctx, opts, smokePodLossScenario{
		Name:      "orphan smoke",
		JobPath:   opts.OrphanJobPath,
		LeaseTTL:  opts.OrphanLeaseTTL,
		Stability: opts.OrphanStability,
	})

	if err != nil {
		return SmokeResult{}, err
	}

	result := SmokeResult{
		Status:       "ok",
		Context:      strings.TrimSpace(opts.Context),
		Namespace:    opts.Namespace,
		JobPath:      opts.OrphanJobPath,
		JobID:        smokeJobID(orphan.Run),
		RunID:        orphan.Run.RunID,
		RunStatus:    orphan.Detail.Status,
		WorkerOwners: []string{orphan.Owner},
		DeletedPod:   orphan.DeletedPod,
	}

	fmt.Fprintf(opts.Stdout, "Kubernetes orphan smoke succeeded: run_id=%s deleted_pod=%s owner=%s\n", orphan.Run.RunID, orphan.DeletedPod, orphan.Owner)
	return result, nil
}

func runRepairSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	if strings.TrimSpace(opts.RepairJobPath) == "" {
		return SmokeResult{}, fmt.Errorf("repair job path is required")
	}

	if strings.TrimSpace(opts.OrphanJobPath) == "" {
		return SmokeResult{}, fmt.Errorf("orphan job path is required")
	}

	if opts.RepairLeaseTTL <= 0 {
		return SmokeResult{}, fmt.Errorf("repair lease ttl must be > 0")
	}

	if opts.RepairReadyAfter <= 0 {
		return SmokeResult{}, fmt.Errorf("repair ready-after must be > 0")
	}

	out := opts.Stdout
	var readyAt time.Time
	repairSubmit := func(client *http.Client, baseURL string, submitOpts SmokeOptions) (smokeJobRunResult, error) {
		readyAt = time.Now().Add(opts.RepairReadyAfter)
		replacements := map[string]string{
			smokeRepairReadyAfterPlaceholder: strconv.FormatInt(readyAt.Unix(), 10),
		}

		return submitSmokeJobWithReplacements(client, baseURL, submitOpts, replacements)
	}

	requeueOrphan, err := runPodLossOrphanScenario(ctx, opts, smokePodLossScenario{
		Name:     "repair force-requeue smoke",
		JobPath:  opts.RepairJobPath,
		LeaseTTL: opts.RepairLeaseTTL,
		Submit:   repairSubmit,
	})

	if err != nil {
		return SmokeResult{}, err
	}

	if readyAt.IsZero() {
		return SmokeResult{}, fmt.Errorf("repair smoke did not record retry readiness cutoff")
	}

	if err := waitUntilSmokeTime(ctx, readyAt, out, "force-requeue retry success window"); err != nil {
		return SmokeResult{}, err
	}

	fmt.Fprintf(out, "Starting API port-forward on 127.0.0.1:%d\n", opts.APILocalPort)
	pf, err := startSmokeAPIPortForward(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}

	defer func() {
		if pf != nil {
			pf.stop()
		}
	}()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", opts.APILocalPort)
	client := &http.Client{Timeout: smokeHTTPClientDelay}

	fmt.Fprintf(out, "Force-requeueing orphaned run_id=%s\n", requeueOrphan.Run.RunID)
	if err := forceRequeueSmokeRun(client, baseURL, opts.APIToken, requeueOrphan.Run.RunID); err != nil {
		return SmokeResult{}, err
	}

	runCtx, runCancel := context.WithTimeout(ctx, opts.Wait)
	requeuedDetail, err := waitForSmokeRunStatus(runCtx, client, baseURL, opts.APIToken, requeueOrphan.Run.RunID, out, "succeeded")
	runCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	if err := assertSmokeRunTaskAttemptCount(client, baseURL, opts.APIToken, requeueOrphan.Run.RunID, "root", 2); err != nil {
		return SmokeResult{}, err
	}

	fmt.Fprintln(out, "Verifying force-requeue retry logs")
	logCtx, logCancel := context.WithTimeout(ctx, opts.Wait)
	err = waitForSmokeLogMarkers(logCtx, baseURL, opts.APIToken, requeueOrphan.Run.RunID, []string{"repair-task-requeued"}, out)
	logCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	pf.stop()
	pf = nil

	checks := []SmokeRepairCheck{{
		Action:     "force-requeue",
		RunID:      requeueOrphan.Run.RunID,
		RunStatus:  requeuedDetail.Status,
		DeletedPod: requeueOrphan.DeletedPod,
		Attempts:   2,
	}}

	fmt.Fprintln(out, "Creating orphaned run for repair mark-abandoned")
	markOrphan, err := runPodLossOrphanScenario(ctx, opts, smokePodLossScenario{
		Name:     "repair mark-abandoned smoke",
		JobPath:  opts.OrphanJobPath,
		LeaseTTL: opts.RepairLeaseTTL,
	})

	if err != nil {
		return SmokeResult{}, err
	}

	fmt.Fprintf(out, "Starting API port-forward on 127.0.0.1:%d\n", opts.APILocalPort)
	markPF, err := startSmokeAPIPortForward(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}
	defer markPF.stop()

	markBaseURL := fmt.Sprintf("http://127.0.0.1:%d", opts.APILocalPort)
	fmt.Fprintf(out, "Repair-marking orphaned run_id=%s abandoned\n", markOrphan.Run.RunID)
	if err := repairMarkSmokeRun(client, markBaseURL, opts.APIToken, markOrphan.Run.RunID, "abandoned", "kubernetes repair smoke mark-abandoned"); err != nil {
		return SmokeResult{}, err
	}

	markCtx, markCancel := context.WithTimeout(ctx, opts.Wait)
	markedDetail, err := waitForSmokeRunStatus(markCtx, client, markBaseURL, opts.APIToken, markOrphan.Run.RunID, out, "abandoned")
	markCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	if err := assertSmokeRunTaskAttemptCount(client, markBaseURL, opts.APIToken, markOrphan.Run.RunID, "root", 1); err != nil {
		return SmokeResult{}, err
	}

	checks = append(checks, SmokeRepairCheck{
		Action:     "mark-abandoned",
		RunID:      markOrphan.Run.RunID,
		RunStatus:  markedDetail.Status,
		DeletedPod: markOrphan.DeletedPod,
		Attempts:   1,
	})

	result := SmokeResult{
		Status:       "ok",
		Context:      strings.TrimSpace(opts.Context),
		Namespace:    opts.Namespace,
		JobPath:      opts.RepairJobPath,
		JobID:        smokeJobID(requeueOrphan.Run),
		RunID:        requeueOrphan.Run.RunID,
		RunStatus:    requeuedDetail.Status,
		WorkerOwners: []string{requeueOrphan.Owner, markOrphan.Owner},
		DeletedPod:   requeueOrphan.DeletedPod,
		RepairChecks: checks,
	}

	fmt.Fprintf(out, "Kubernetes repair smoke succeeded: force_requeue_run_id=%s mark_abandoned_run_id=%s\n", requeueOrphan.Run.RunID, markOrphan.Run.RunID)
	return result, nil
}

func runPodLossOrphanScenario(ctx context.Context, opts SmokeOptions, scenario smokePodLossScenario) (smokePodLossResult, error) {
	name := strings.TrimSpace(scenario.Name)
	if name == "" {
		name = "pod-loss smoke"
	}

	jobPath := strings.TrimSpace(scenario.JobPath)
	if jobPath == "" {
		return smokePodLossResult{}, fmt.Errorf("%s job path is required", name)
	}

	if scenario.LeaseTTL <= 0 {
		return smokePodLossResult{}, fmt.Errorf("%s lease ttl must be > 0", name)
	}

	if scenario.Stability < 0 {
		return smokePodLossResult{}, fmt.Errorf("%s stability must be >= 0", name)
	}

	submit := scenario.Submit
	if submit == nil {
		submit = submitSmokeJob
	}

	out := opts.Stdout
	setupCtx, setupCancel := context.WithTimeout(ctx, opts.Wait)
	deployment, err := fetchSmokeWorkerDeployment(setupCtx, opts)
	setupCancel()
	if err != nil {
		return smokePodLossResult{}, err
	}

	originalReplicas := smokeDeploymentReplicas(deployment)
	restoreReplicas := false
	if originalReplicas != 1 {
		fmt.Fprintf(out, "Scaling %s from %d to 1 replica for %s\n", smokeWorkerDeploymentName, originalReplicas, name)
		scaleCtx, scaleCancel := context.WithTimeout(ctx, opts.Wait)
		err = scaleSmokeWorkerDeployment(scaleCtx, opts, 1)
		scaleCancel()
		if err != nil {
			return smokePodLossResult{}, err
		}

		restoreReplicas = true
	}

	if restoreReplicas {
		defer restoreSmokeWorkerDeployment(opts, originalReplicas)
	}

	originalLeaseTTL, hadLeaseTTL := smokeDeploymentContainerEnv(deployment, smokeWorkerContainerName, smokeWorkerLeaseTTLEnv)
	desiredLeaseTTL := scenario.LeaseTTL.String()
	if originalLeaseTTL != desiredLeaseTTL {
		fmt.Fprintf(out, "Temporarily setting %s=%s on %s\n", smokeWorkerLeaseTTLEnv, desiredLeaseTTL, smokeWorkerDeploymentName)
		envCtx, envCancel := context.WithTimeout(ctx, opts.Wait)
		err = setSmokeWorkerEnv(envCtx, opts, smokeWorkerLeaseTTLEnv, desiredLeaseTTL)
		envCancel()

		if err != nil {
			return smokePodLossResult{}, err
		}
		defer restoreSmokeWorkerEnv(opts, smokeWorkerLeaseTTLEnv, originalLeaseTTL, hadLeaseTTL)
	}

	podCtx, podCancel := context.WithTimeout(ctx, opts.Wait)
	workerPod, err := waitForSingleReadySmokeWorkerPod(podCtx, opts, "")
	podCancel()
	if err != nil {
		return smokePodLossResult{}, err
	}

	fmt.Fprintf(out, "Using worker pod %s for %s\n", workerPod.Metadata.Name, name)
	fmt.Fprintf(out, "Starting API port-forward on 127.0.0.1:%d\n", opts.APILocalPort)
	pf, err := startSmokeAPIPortForward(ctx, opts)
	if err != nil {
		return smokePodLossResult{}, err
	}
	defer pf.stop()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", opts.APILocalPort)
	client := &http.Client{Timeout: smokeHTTPClientDelay}

	submitOpts := opts
	submitOpts.JobPath = jobPath
	fmt.Fprintf(out, "Submitting %s\n", submitOpts.JobPath)
	run, err := submit(client, baseURL, submitOpts)
	if err != nil {
		return smokePodLossResult{}, err
	}

	fmt.Fprintf(out, "Submitted job_id=%s run_id=%s\n", smokeJobID(run), run.RunID)
	ownerCtx, ownerCancel := context.WithTimeout(ctx, opts.Wait)
	owner, err := waitForSmokeTaskLeaseOwner(ownerCtx, client, baseURL, opts.APIToken, run.RunID, "root", out)
	ownerCancel()
	if err != nil {
		return smokePodLossResult{}, err
	}

	if err := assertSmokeRunTaskAttemptCount(client, baseURL, opts.APIToken, run.RunID, "root", 1); err != nil {
		return smokePodLossResult{}, err
	}

	fmt.Fprintf(out, "Deleting active worker pod %s with root owner %s\n", workerPod.Metadata.Name, owner)
	deleteCtx, deleteCancel := context.WithTimeout(ctx, opts.Wait)
	err = deleteSmokePod(deleteCtx, opts, workerPod.Metadata.Name)
	deleteCancel()
	if err != nil {
		return smokePodLossResult{}, err
	}

	replacementCtx, replacementCancel := context.WithTimeout(ctx, opts.Wait)
	replacementPod, err := waitForSingleReadySmokeWorkerPod(replacementCtx, opts, workerPod.Metadata.Name)
	replacementCancel()
	if err != nil {
		return smokePodLossResult{}, err
	}

	fmt.Fprintf(out, "Replacement worker pod ready: %s\n", replacementPod.Metadata.Name)
	runCtx, runCancel := context.WithTimeout(ctx, opts.Wait)
	detail, err := waitForSmokeRunStatus(runCtx, client, baseURL, opts.APIToken, run.RunID, out, "orphaned")
	runCancel()
	if err != nil {
		return smokePodLossResult{}, err
	}

	if err := assertSmokeRunTaskAttemptCount(client, baseURL, opts.APIToken, run.RunID, "root", 1); err != nil {
		return smokePodLossResult{}, err
	}

	if scenario.Stability > 0 {
		fmt.Fprintf(out, "Verifying run remains orphaned for %v\n", scenario.Stability)
		stabilityCtx, stabilityCancel := context.WithTimeout(ctx, scenario.Stability)
		err = assertSmokeRunRemainsOrphaned(stabilityCtx, client, baseURL, opts.APIToken, run.RunID, "root", 1, out)
		stabilityCancel()
		if err != nil {
			return smokePodLossResult{}, err
		}
	}

	return smokePodLossResult{
		Run:        run,
		Detail:     detail,
		Owner:      owner,
		DeletedPod: workerPod.Metadata.Name,
	}, nil
}

func normalizeSmokeOptions(opts SmokeOptions) SmokeOptions {
	opts.Kubectl = strings.TrimSpace(opts.Kubectl)
	if opts.Kubectl == "" {
		opts.Kubectl = "kubectl"
	}

	opts.Context = strings.TrimSpace(opts.Context)
	opts.Namespace = strings.TrimSpace(opts.Namespace)
	if opts.Namespace == "" {
		opts.Namespace = DefaultNamespace
	}

	opts.JobPath = strings.TrimSpace(opts.JobPath)
	if opts.JobPath == "" && opts.KnoxSecretsOnly {
		opts.JobPath = DefaultSmokeKnoxJobPath
	}

	if opts.JobPath == "" {
		opts.JobPath = DefaultSmokeJobPath
	}

	opts.WorkerCoreJobPath = strings.TrimSpace(opts.WorkerCoreJobPath)
	if opts.WorkerCoreJobPath == "" {
		opts.WorkerCoreJobPath = DefaultSmokeWorkerCoreJobPath
	}

	opts.CancelJobPath = strings.TrimSpace(opts.CancelJobPath)
	if opts.CancelJobPath == "" {
		opts.CancelJobPath = DefaultSmokeCancelJobPath
	}

	opts.ScaleJobPath = strings.TrimSpace(opts.ScaleJobPath)
	if opts.ScaleJobPath == "" {
		opts.ScaleJobPath = DefaultSmokeScaleJobPath
	}

	opts.OrphanJobPath = strings.TrimSpace(opts.OrphanJobPath)
	if opts.OrphanJobPath == "" {
		opts.OrphanJobPath = DefaultSmokeOrphanJobPath
	}

	opts.RepairJobPath = strings.TrimSpace(opts.RepairJobPath)
	if opts.RepairJobPath == "" {
		opts.RepairJobPath = DefaultSmokeRepairJobPath
	}

	if opts.ScaleWorkerReplicas == 0 {
		opts.ScaleWorkerReplicas = DefaultSmokeScaleWorkerReplicas
	}

	if opts.ScaleMinWorkers == 0 {
		opts.ScaleMinWorkers = DefaultSmokeScaleMinWorkers
	}

	if opts.OrphanLeaseTTL == 0 {
		opts.OrphanLeaseTTL = DefaultSmokeOrphanLeaseTTL
	}

	if opts.OrphanStability == 0 {
		opts.OrphanStability = DefaultSmokeOrphanStability
	}

	if opts.RepairLeaseTTL == 0 {
		opts.RepairLeaseTTL = DefaultSmokeRepairLeaseTTL
	}

	if opts.RepairReadyAfter == 0 {
		opts.RepairReadyAfter = DefaultSmokeRepairReadyAfter
	}

	opts.CLIImage = strings.TrimSpace(opts.CLIImage)
	if opts.CLIImage == "" {
		opts.CLIImage = DefaultSmokeCLIImage
	}

	opts.WorkerCoreImage = strings.TrimSpace(opts.WorkerCoreImage)
	if opts.WorkerCoreImage == "" {
		opts.WorkerCoreImage = DefaultSmokeWorkerCoreImage
	}

	opts.WorkerCoreTaskImage = strings.TrimSpace(opts.WorkerCoreTaskImage)
	if opts.WorkerCoreTaskImage == "" {
		opts.WorkerCoreTaskImage = DefaultSmokeWorkerCoreTaskImage
	}

	opts.GerritImage = strings.TrimSpace(opts.GerritImage)
	if opts.GerritImage == "" {
		opts.GerritImage = DefaultSmokeGerritImage
	}

	if opts.GerritHTTPPort == 0 {
		opts.GerritHTTPPort = DefaultSmokeGerritHTTPPort
	}

	if opts.GerritSSHLocalPort == 0 {
		opts.GerritSSHLocalPort = DefaultSmokeGerritSSHLocalPort
	}

	opts.GerritClusterURL = strings.TrimRight(strings.TrimSpace(opts.GerritClusterURL), "/")
	if opts.GerritClusterURL == "" {
		opts.GerritClusterURL = DefaultSmokeGerritClusterURL
	}

	opts.GerritSSHHost = strings.TrimSpace(opts.GerritSSHHost)
	if opts.GerritSSHHost == "" {
		opts.GerritSSHHost = DefaultSmokeGerritSSHHost
	}

	if opts.GerritSSHPort == 0 {
		opts.GerritSSHPort = DefaultSmokeGerritSSHPort
	}

	opts.GerritAccountID = strings.TrimSpace(opts.GerritAccountID)
	if opts.GerritAccountID == "" {
		opts.GerritAccountID = DefaultSmokeGerritAccountID
	}

	opts.GerritUsername = strings.TrimSpace(opts.GerritUsername)
	if opts.GerritUsername == "" {
		opts.GerritUsername = DefaultSmokeGerritUsername
	}

	opts.GerritProject = strings.Trim(strings.TrimSpace(opts.GerritProject), "/")
	opts.GerritProjectPrefix = strings.Trim(strings.TrimSpace(opts.GerritProjectPrefix), "/")
	if opts.GerritProjectPrefix == "" {
		opts.GerritProjectPrefix = DefaultSmokeGerritProjectPrefix
	}

	opts.GerritGitBin = strings.TrimSpace(opts.GerritGitBin)
	if opts.GerritGitBin == "" {
		opts.GerritGitBin = DefaultSmokeGerritGitBin
	}

	opts.S3Image = strings.TrimSpace(opts.S3Image)
	if opts.S3Image == "" {
		opts.S3Image = DefaultSmokeS3Image
	}

	if opts.S3LocalPort == 0 {
		opts.S3LocalPort = DefaultSmokeS3LocalPort
	}

	opts.S3ClusterEndpoint = strings.TrimRight(strings.TrimSpace(opts.S3ClusterEndpoint), "/")
	if opts.S3ClusterEndpoint == "" {
		opts.S3ClusterEndpoint = DefaultSmokeS3ClusterEndpoint
	}

	opts.S3Bucket = strings.TrimSpace(opts.S3Bucket)
	if opts.S3Bucket == "" {
		opts.S3Bucket = DefaultSmokeS3Bucket
	}

	opts.S3Prefix = strings.Trim(strings.TrimSpace(opts.S3Prefix), "/")
	if opts.S3Prefix == "" {
		opts.S3Prefix = DefaultSmokeS3Prefix
	}

	opts.S3AccessKeyID = strings.TrimSpace(opts.S3AccessKeyID)
	if opts.S3AccessKeyID == "" {
		opts.S3AccessKeyID = DefaultSmokeS3AccessKeyID
	}

	opts.S3SecretAccessKey = strings.TrimSpace(opts.S3SecretAccessKey)
	if opts.S3SecretAccessKey == "" {
		opts.S3SecretAccessKey = DefaultSmokeS3SecretAccessKey
	}

	opts.S3TempDir = strings.TrimSpace(opts.S3TempDir)
	if opts.S3TempDir == "" {
		opts.S3TempDir = DefaultSmokeS3TempDir
	}

	opts.KnoxImage = strings.TrimSpace(opts.KnoxImage)
	if opts.KnoxImage == "" {
		opts.KnoxImage = DefaultSmokeKnoxImage
	}

	if opts.KnoxLocalPort == 0 {
		opts.KnoxLocalPort = DefaultSmokeKnoxLocalPort
	}

	opts.KnoxClusterURL = strings.TrimRight(strings.TrimSpace(opts.KnoxClusterURL), "/")
	if opts.KnoxClusterURL == "" {
		opts.KnoxClusterURL = DefaultSmokeKnoxClusterURL
	}

	opts.KnoxCertMountPath = strings.TrimRight(strings.TrimSpace(opts.KnoxCertMountPath), "/")
	if opts.KnoxCertMountPath == "" {
		opts.KnoxCertMountPath = DefaultSmokeKnoxCertMountPath
	}

	opts.KnoxAuthToken = strings.TrimSpace(opts.KnoxAuthToken)
	if opts.KnoxAuthToken == "" {
		opts.KnoxAuthToken = DefaultSmokeKnoxAuthToken
	}

	opts.KnoxWrongAuthToken = strings.TrimSpace(opts.KnoxWrongAuthToken)
	if opts.KnoxWrongAuthToken == "" {
		opts.KnoxWrongAuthToken = DefaultSmokeKnoxWrongAuthToken
	}

	opts.KnoxKeyID = strings.TrimSpace(opts.KnoxKeyID)
	if opts.KnoxKeyID == "" {
		opts.KnoxKeyID = DefaultSmokeKnoxKeyID
	}

	opts.KnoxRef = strings.TrimSpace(opts.KnoxRef)
	if opts.KnoxRef == "" {
		opts.KnoxRef = DefaultSmokeKnoxRef
	}

	opts.KnoxMissingRef = strings.TrimSpace(opts.KnoxMissingRef)
	if opts.KnoxMissingRef == "" {
		opts.KnoxMissingRef = DefaultSmokeKnoxMissingRef
	}

	opts.KnoxExpectedData = strings.TrimSpace(opts.KnoxExpectedData)
	if opts.KnoxExpectedData == "" {
		opts.KnoxExpectedData = DefaultSmokeKnoxSecret
	}

	if opts.SeedSecret == nil {
		seedSecret := DefaultSmokeSeedSecret
		opts.SeedSecret = &seedSecret
	}

	if opts.APILocalPort == 0 {
		opts.APILocalPort = DefaultSmokeAPIPort
	}

	if opts.Wait == 0 {
		opts.Wait = DefaultSmokeWait
	}

	opts.APIToken = strings.TrimSpace(opts.APIToken)
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}

	return opts
}

func validateSmokeOptions(opts SmokeOptions) error {
	if opts.Kubectl == "" {
		return fmt.Errorf("kubectl command is required")
	}

	if opts.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	if opts.JobPath == "" {
		return fmt.Errorf("job path is required")
	}

	if smokeSeedSecretEnabled(opts) && opts.CLIImage == "" {
		return fmt.Errorf("cli image is required when secret seeding is enabled")
	}

	if opts.APILocalPort <= 0 || opts.APILocalPort > 65535 {
		return fmt.Errorf("api local port must be between 1 and 65535")
	}

	if opts.Wait <= 0 {
		return fmt.Errorf("wait must be > 0")
	}

	return nil
}

func smokeSeedSecretEnabled(opts SmokeOptions) bool {
	return opts.SeedSecret != nil && *opts.SeedSecret
}

func seedSmokeSecret(ctx context.Context, opts SmokeOptions) error {
	name := fmt.Sprintf("vectis-smoke-seed-%d", time.Now().UnixNano())
	manifest := smokeSeedManifest(name, opts.CLIImage)

	stdout, stderr, err := runSmokeKubectl(ctx, opts, strings.NewReader(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("apply smoke seed job: %w: %s%s", err, stdout, stderr)
	}
	defer cleanupSmokeSeedResources(opts, name)

	waitTimeout := opts.Wait.String()
	stdout, stderr, err = runSmokeKubectl(ctx, opts, nil, "wait", "--for=condition=complete", "job/"+name, "--timeout", waitTimeout)
	if err != nil {
		logs, logErr := smokeSeedJobLogs(ctx, opts, name)
		if logErr != nil {
			return fmt.Errorf("wait for smoke seed job: %w: %s%s; fetch logs: %v", err, stdout, stderr, logErr)
		}

		return fmt.Errorf("wait for smoke seed job: %w: %s%s%s", err, stdout, stderr, logs)
	}

	logs, err := smokeSeedJobLogs(ctx, opts, name)
	if err != nil {
		return fmt.Errorf("fetch smoke seed logs: %w", err)
	}

	if text := strings.TrimSpace(logs); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	return nil
}

func smokeSeedManifest(name, image string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: smoke-seed
type: Opaque
stringData:
  plaintext: %s
---
apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: smoke-seed
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 60
  template:
    metadata:
      labels:
        app.kubernetes.io/name: vectis
        app.kubernetes.io/component: smoke-seed
    spec:
      automountServiceAccountToken: false
      restartPolicy: Never
      containers:
        - name: seed
          image: %s
          imagePullPolicy: IfNotPresent
          args:
            - secrets
            - encryptedfs
            - put
            - %s
            - --root
            - %s
            - --key-file
            - %s
            - --from-file
            - /run/vectis/smoke/plaintext
            - --force
          volumeMounts:
            - name: secrets-data
              mountPath: /data/vectis/secrets
            - name: secrets-key
              mountPath: /run/vectis/secrets
              readOnly: true
            - name: smoke-plaintext
              mountPath: /run/vectis/smoke
              readOnly: true
      volumes:
        - name: secrets-data
          persistentVolumeClaim:
            claimName: vectis-secrets-data
        - name: secrets-key
          secret:
            secretName: vectis-secrets
            items:
              - key: encryptedfs.key
                path: encryptedfs.key
        - name: smoke-plaintext
          secret:
            secretName: %s
            items:
              - key: plaintext
                path: plaintext
`, name, yamlQuote(smokeSecretPlaintext), name, yamlQuote(image), yamlQuote(smokeSecretRef), yamlQuote(smokeSecretRoot), yamlQuote(smokeSecretKeyFile), name)
}

func smokeSeedJobLogs(ctx context.Context, opts SmokeOptions, name string) (string, error) {
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "logs", "job/"+name)
	if err != nil {
		return "", fmt.Errorf("%w: %s%s", err, stdout, stderr)
	}

	return stdout + stderr, nil
}

func cleanupSmokeSeedResources(opts SmokeOptions, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, _ = runSmokeKubectl(ctx, opts, nil, "delete", "job/"+name, "secret/"+name, "--ignore-not-found")
}

func fetchSmokeWorkerDeployment(ctx context.Context, opts SmokeOptions) (smokeDeployment, error) {
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "get", smokeWorkerDeploymentName, "-o", "json")
	if err != nil {
		return smokeDeployment{}, fmt.Errorf("get %s: %w: %s%s", smokeWorkerDeploymentName, err, stdout, stderr)
	}

	var deployment smokeDeployment
	if err := json.Unmarshal([]byte(stdout), &deployment); err != nil {
		return smokeDeployment{}, fmt.Errorf("parse %s: %w", smokeWorkerDeploymentName, err)
	}

	return deployment, nil
}

func currentSmokeWorkerReplicas(ctx context.Context, opts SmokeOptions) (int, error) {
	deployment, err := fetchSmokeWorkerDeployment(ctx, opts)
	if err != nil {
		return 0, fmt.Errorf("get %s replicas: %w", smokeWorkerDeploymentName, err)
	}

	return smokeDeploymentReplicas(deployment), nil
}

func smokeDeploymentReplicas(deployment smokeDeployment) int {
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas <= 0 {
		return 1
	}

	return *deployment.Spec.Replicas
}

func smokeDeploymentContainerEnv(deployment smokeDeployment, containerName, envName string) (string, bool) {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != containerName {
			continue
		}

		for _, env := range container.Env {
			if env.Name == envName {
				return env.Value, true
			}
		}
	}

	return "", false
}

func waitForSmokeWorkerCoreTaskJob(ctx context.Context, opts SmokeOptions, runID string, out io.Writer) (string, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		job, found, err := fetchSmokeWorkerCoreTaskJob(ctx, opts, runID)
		if err != nil {
			return "", err
		}
		if found {
			phase, message := smokeKubernetesJobPhase(job)
			switch phase {
			case "succeeded":
				return job.Metadata.Name, nil
			case "failed":
				return "", fmt.Errorf("worker-core task Job %s failed: %s", job.Metadata.Name, message)
			default:
				fmt.Fprintf(out, "Waiting for worker-core task Job %s phase=%s\n", job.Metadata.Name, phase)
			}
		} else {
			fmt.Fprintf(out, "Waiting for worker-core task Job for run_id=%s\n", runID)
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for worker-core task Job for run_id=%s: %w", runID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func fetchSmokeWorkerCoreTaskJob(ctx context.Context, opts SmokeOptions, runID string) (smokeKubernetesJob, bool, error) {
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "get", "jobs", "-l", smokeWorkerCoreTaskJobSelector, "-o", "json")
	if err != nil {
		return smokeKubernetesJob{}, false, fmt.Errorf("list worker-core task Jobs: %w: %s%s", err, stdout, stderr)
	}

	var jobs smokeKubernetesJobList
	if err := json.Unmarshal([]byte(stdout), &jobs); err != nil {
		return smokeKubernetesJob{}, false, fmt.Errorf("parse worker-core task Jobs: %w", err)
	}

	for _, job := range jobs.Items {
		if job.Metadata.Annotations[smokeWorkerCoreTaskRunAnnotation] == runID {
			return job, true, nil
		}
	}

	return smokeKubernetesJob{}, false, nil
}

func smokeKubernetesJobPhase(job smokeKubernetesJob) (string, string) {
	for _, condition := range job.Status.Conditions {
		if !strings.EqualFold(condition.Status, "true") {
			continue
		}

		switch condition.Type {
		case "Complete":
			return "succeeded", strings.TrimSpace(condition.Message)
		case "Failed":
			message := strings.TrimSpace(condition.Message)
			if message == "" {
				message = strings.TrimSpace(condition.Reason)
			}
			return "failed", message
		}
	}

	return "running", ""
}

func cleanupSmokeWorkerCoreTaskJob(opts SmokeOptions, name string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, _ = runSmokeKubectl(ctx, opts, nil, "delete", "job/"+name, "--ignore-not-found")
}

func scaleSmokeWorkerDeployment(ctx context.Context, opts SmokeOptions, replicas int) error {
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "scale", smokeWorkerDeploymentName, "--replicas", strconv.Itoa(replicas))
	if err != nil {
		return fmt.Errorf("scale %s to %d: %w: %s%s", smokeWorkerDeploymentName, replicas, err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	if err := waitForSmokeWorkerDeployment(ctx, opts); err != nil {
		return fmt.Errorf("wait for %s after scale to %d: %w", smokeWorkerDeploymentName, replicas, err)
	}

	return nil
}

func setSmokeWorkerEnv(ctx context.Context, opts SmokeOptions, name, value string) error {
	arg := name + "=" + value
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "set", "env", smokeWorkerDeploymentName, arg)
	if err != nil {
		return fmt.Errorf("set %s on %s: %w: %s%s", name, smokeWorkerDeploymentName, err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	if err := waitForSmokeWorkerDeployment(ctx, opts); err != nil {
		return fmt.Errorf("wait for %s after setting %s: %w", smokeWorkerDeploymentName, name, err)
	}

	return nil
}

func unsetSmokeWorkerEnv(ctx context.Context, opts SmokeOptions, name string) error {
	arg := name + "-"
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "set", "env", smokeWorkerDeploymentName, arg)
	if err != nil {
		return fmt.Errorf("unset %s on %s: %w: %s%s", name, smokeWorkerDeploymentName, err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	if err := waitForSmokeWorkerDeployment(ctx, opts); err != nil {
		return fmt.Errorf("wait for %s after unsetting %s: %w", smokeWorkerDeploymentName, name, err)
	}

	return nil
}

func waitForSmokeWorkerDeployment(ctx context.Context, opts SmokeOptions) error {
	timeout := opts.Wait.String()
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "rollout", "status", smokeWorkerDeploymentName, "--timeout", timeout)
	if err != nil {
		return fmt.Errorf("rollout status: %w: %s%s", err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	return nil
}

func applySmokeWorkerCoreRBAC(ctx context.Context, opts SmokeOptions) error {
	fmt.Fprintf(opts.Stdout, "Applying temporary Kubernetes worker-core RBAC\n")
	stdout, stderr, err := runSmokeKubectl(ctx, opts, strings.NewReader(smokeWorkerCoreRBACManifest()), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("apply worker-core smoke RBAC: %w: %s%s", err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	return nil
}

func smokeWorkerCoreRBACManifest() string {
	name := yamlQuote(smokeWorkerCoreServiceAccount)
	return fmt.Sprintf(`apiVersion: v1
kind: ServiceAccount
metadata:
  name: %s
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: worker-core-kubernetes-smoke
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: %s
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: worker-core-kubernetes-smoke
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "patch", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %s
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: worker-core-kubernetes-smoke
subjects:
  - kind: ServiceAccount
    name: %s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: %s
`, name, name, name, name, name)
}

func cleanupSmokeWorkerCoreRBAC(opts SmokeOptions) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, _, _ = runSmokeKubectl(ctx, opts, nil,
		"delete",
		"rolebinding/"+smokeWorkerCoreServiceAccount,
		"role/"+smokeWorkerCoreServiceAccount,
		"serviceaccount/"+smokeWorkerCoreServiceAccount,
		"--ignore-not-found",
	)
}

func patchSmokeWorkerForKubernetesCore(ctx context.Context, opts SmokeOptions) error {
	patch, err := smokeWorkerCoreDeploymentPatch(opts)
	if err != nil {
		return err
	}

	fmt.Fprintf(opts.Stdout, "Patching %s to use %s\n", smokeWorkerDeploymentName, opts.WorkerCoreImage)
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "patch", smokeWorkerDeploymentName, "--type=strategic", "--patch", patch)
	if err != nil {
		return fmt.Errorf("patch %s for Kubernetes worker core: %w: %s%s", smokeWorkerDeploymentName, err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	if err := waitForSmokeWorkerDeployment(ctx, opts); err != nil {
		return fmt.Errorf("wait for %s after Kubernetes worker core patch: %w", smokeWorkerDeploymentName, err)
	}

	return nil
}

func smokeWorkerCoreDeploymentPatch(opts SmokeOptions) (string, error) {
	patch := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"serviceAccountName":           smokeWorkerCoreServiceAccount,
					"automountServiceAccountToken": true,
					"containers": []map[string]any{
						{
							"name":  smokeWorkerCoreContainerName,
							"image": opts.WorkerCoreImage,
							"env": []map[string]string{
								{"name": "KUBERNETES_NAMESPACE", "value": opts.Namespace},
								{"name": "VECTIS_KUBERNETES_WORKER_CORE_IMAGE", "value": opts.WorkerCoreTaskImage},
								{"name": "VECTIS_KUBERNETES_WORKER_CORE_DELETE_AFTER", "value": "false"},
							},
						},
					},
				},
			},
		},
	}

	b, err := json.Marshal(patch)
	if err != nil {
		return "", fmt.Errorf("marshal Kubernetes worker-core deployment patch: %w", err)
	}

	return string(b), nil
}

func restoreSmokeWorkerAfterKubernetesCore(opts SmokeOptions) {
	ctx, cancel := context.WithTimeout(context.Background(), opts.Wait)
	defer cancel()

	fmt.Fprintf(opts.Stdout, "Restoring %s after Kubernetes worker-core smoke\n", smokeWorkerDeploymentName)
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "rollout", "undo", smokeWorkerDeploymentName)
	if err != nil {
		fmt.Fprintf(opts.Stdout, "Warning: restore %s failed: %v: %s%s\n", smokeWorkerDeploymentName, err, stdout, stderr)
		return
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	if err := waitForSmokeWorkerDeployment(ctx, opts); err != nil {
		fmt.Fprintf(opts.Stdout, "Warning: wait for restored %s failed: %v\n", smokeWorkerDeploymentName, err)
	}
}

func restoreSmokeWorkerDeployment(opts SmokeOptions, replicas int) {
	ctx, cancel := context.WithTimeout(context.Background(), opts.Wait)
	defer cancel()

	fmt.Fprintf(opts.Stdout, "Restoring %s to %d replicas\n", smokeWorkerDeploymentName, replicas)
	if err := scaleSmokeWorkerDeployment(ctx, opts, replicas); err != nil {
		fmt.Fprintf(opts.Stdout, "Warning: restore %s failed: %v\n", smokeWorkerDeploymentName, err)
	}
}

func restoreSmokeWorkerEnv(opts SmokeOptions, name, value string, hadValue bool) {
	ctx, cancel := context.WithTimeout(context.Background(), opts.Wait)
	defer cancel()

	if hadValue {
		fmt.Fprintf(opts.Stdout, "Restoring %s=%s on %s\n", name, value, smokeWorkerDeploymentName)
		if err := setSmokeWorkerEnv(ctx, opts, name, value); err != nil {
			fmt.Fprintf(opts.Stdout, "Warning: restore %s failed: %v\n", name, err)
		}

		return
	}

	fmt.Fprintf(opts.Stdout, "Removing temporary %s from %s\n", name, smokeWorkerDeploymentName)
	if err := unsetSmokeWorkerEnv(ctx, opts, name); err != nil {
		fmt.Fprintf(opts.Stdout, "Warning: remove %s failed: %v\n", name, err)
	}
}

func listSmokeWorkerPods(ctx context.Context, opts SmokeOptions) ([]smokePod, error) {
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "get", "pods", "-l", smokeWorkerPodSelector, "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("list worker pods: %w: %s%s", err, stdout, stderr)
	}

	var pods smokePodList
	if err := json.Unmarshal([]byte(stdout), &pods); err != nil {
		return nil, fmt.Errorf("parse worker pods: %w", err)
	}

	return pods.Items, nil
}

func waitForSingleReadySmokeWorkerPod(ctx context.Context, opts SmokeOptions, excludeName string) (smokePod, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		pods, err := listSmokeWorkerPods(ctx, opts)
		if err != nil {
			return smokePod{}, err
		}

		ready := make([]smokePod, 0, len(pods))
		for _, pod := range pods {
			if pod.Metadata.Name == "" || pod.Metadata.Name == excludeName || pod.Metadata.DeletionTimestamp != nil {
				continue
			}

			if smokePodReady(pod) {
				ready = append(ready, pod)
			}
		}

		if len(ready) == 1 {
			return ready[0], nil
		}

		fmt.Fprintf(opts.Stdout, "waiting for one ready worker pod; ready=%d total=%d\n", len(ready), len(pods))
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return smokePod{}, fmt.Errorf("timed out waiting for one ready worker pod: %w", ctx.Err())
		}
	}
}

func smokePodReady(pod smokePod) bool {
	if !strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "running") {
		return false
	}

	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}

	for _, status := range pod.Status.ContainerStatuses {
		if !status.Ready {
			return false
		}
	}

	return true
}

func deleteSmokePod(ctx context.Context, opts SmokeOptions, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("pod name is required")
	}

	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "delete", "pod", name, "--grace-period=0", "--force")
	if err != nil {
		return fmt.Errorf("delete pod %s: %w: %s%s", name, err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	return nil
}

func runSmokeKubectl(ctx context.Context, opts SmokeOptions, stdin io.Reader, args ...string) (string, string, error) {
	kubectlArgs := []string{}
	if opts.Context != "" {
		kubectlArgs = append(kubectlArgs, "--context", opts.Context)
	}

	kubectlArgs = append(kubectlArgs, "-n", opts.Namespace)
	kubectlArgs = append(kubectlArgs, args...)

	cmd := exec.CommandContext(ctx, opts.Kubectl, kubectlArgs...) // #nosec G204 -- kubectl path and args are smoke-harness controlled.
	cmd.Stdin = stdin
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	return stdout.String(), stderr.String(), err
}

func startSmokeAPIPortForward(ctx context.Context, opts SmokeOptions) (*smokePortForward, error) {
	return startSmokePortForward(ctx, opts, smokeAPIServiceName, []smokePortMapping{{
		LocalPort:  opts.APILocalPort,
		RemotePort: smokeAPIRemotePort,
	}}, "API")
}

type smokePortMapping struct {
	LocalPort  int
	RemotePort int
}

func startSmokePortForward(ctx context.Context, opts SmokeOptions, target string, mappings []smokePortMapping, label string) (*smokePortForward, error) {
	pfCtx, cancel := context.WithCancel(ctx)
	args := []string{}
	if opts.Context != "" {
		args = append(args, "--context", opts.Context)
	}

	args = append(args, "-n", opts.Namespace, "port-forward", "--address", "127.0.0.1", target)
	for _, mapping := range mappings {
		args = append(args, fmt.Sprintf("%d:%d", mapping.LocalPort, mapping.RemotePort))
	}

	if strings.TrimSpace(label) == "" {
		label = target
	}

	cmd := exec.CommandContext(pfCtx, opts.Kubectl, args...) // #nosec G204 -- kubectl path and args are smoke-harness controlled.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, err
	}

	lines := make(chan string, 32)
	var output smokeLineBuffer
	scan := func(r io.Reader) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			output.add(line)
			select {
			case lines <- line:
			case <-pfCtx.Done():
				return
			default:
			}
		}
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}

	go scan(stdout)
	go scan(stderr)

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	readyCtx, readyCancel := context.WithTimeout(ctx, opts.Wait)
	defer readyCancel()
	readyLines := 0
	readyTarget := len(mappings)
	if readyTarget == 0 {
		readyTarget = 1
	}

	for {
		select {
		case line := <-lines:
			fmt.Fprintln(opts.Stdout, line)
			if strings.Contains(line, "Forwarding from") {
				readyLines++
				if readyLines >= readyTarget {
					return &smokePortForward{cancel: cancel, done: done}, nil
				}
			}
		case err := <-done:
			cancel()
			if err != nil {
				return nil, fmt.Errorf("%s port-forward failed: %w: %s", label, err, strings.TrimSpace(output.String()))
			}

			return nil, fmt.Errorf("%s port-forward exited before becoming ready: %s", label, strings.TrimSpace(output.String()))
		case <-readyCtx.Done():
			cancel()
			return nil, fmt.Errorf("timed out waiting for %s port-forward: %s", label, strings.TrimSpace(output.String()))
		}
	}
}

func (p *smokePortForward) stop() {
	if p == nil || p.cancel == nil {
		return
	}
	p.cancel()

	if p.done == nil {
		return
	}

	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
}

func (b *smokeLineBuffer) add(line string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buf.WriteString(line)
	b.buf.WriteByte('\n')
}

func (b *smokeLineBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

func submitSmokeJob(client *http.Client, baseURL string, opts SmokeOptions) (smokeJobRunResult, error) {
	return submitSmokeJobWithReplacements(client, baseURL, opts, nil)
}

func submitSmokeJobWithReplacements(client *http.Client, baseURL string, opts SmokeOptions, replacements map[string]string) (smokeJobRunResult, error) {
	body, err := os.ReadFile(opts.JobPath)
	if err != nil {
		return smokeJobRunResult{}, fmt.Errorf("read smoke job %s: %w", opts.JobPath, err)
	}

	if len(replacements) > 0 {
		text := string(body)
		for placeholder, value := range replacements {
			text = strings.ReplaceAll(text, placeholder, value)
		}

		body = []byte(text)
	}

	return submitSmokeJobBody(client, baseURL, opts, body)
}

func submitSmokeJobBody(client *http.Client, baseURL string, opts SmokeOptions, body []byte) (smokeJobRunResult, error) {
	req, err := newSmokeAPIRequest(http.MethodPost, baseURL, "/api/v1/jobs/run", opts.APIToken, bytes.NewReader(body))
	if err != nil {
		return smokeJobRunResult{}, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return smokeJobRunResult{}, fmt.Errorf("submit smoke job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return smokeJobRunResult{}, smokeUnexpectedStatus("submit smoke job", resp)
	}

	var result smokeJobRunResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return smokeJobRunResult{}, fmt.Errorf("parse smoke job response: %w", err)
	}

	if strings.TrimSpace(result.RunID) == "" {
		return smokeJobRunResult{}, fmt.Errorf("smoke job response missing run_id")
	}

	return result, nil
}

func waitUntilSmokeTime(ctx context.Context, target time.Time, out io.Writer, reason string) error {
	wait := time.Until(target)
	if wait <= 0 {
		return nil
	}

	fmt.Fprintf(out, "Waiting %v for %s\n", wait.Round(time.Second), reason)
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cancelled waiting for %s: %w", reason, ctx.Err())
	}
}

func waitForSmokeRunStatus(ctx context.Context, client *http.Client, baseURL, token, runID string, out io.Writer, expectedStatuses ...string) (smokeRunDetail, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	expected := map[string]bool{}
	for _, status := range expectedStatuses {
		status = strings.ToLower(strings.TrimSpace(status))
		if status != "" {
			expected[status] = true
		}
	}

	lastDiagnostics := ""
	for {
		detail, err := fetchSmokeRunDetail(client, baseURL, token, runID)
		if err != nil {
			return smokeRunDetail{}, err
		}

		fmt.Fprintln(out, smokeRunProgressLine(detail))
		diagnostics := smokeRunDispatchDiagnostics(detail)
		if diagnostics != "" && diagnostics != lastDiagnostics {
			fmt.Fprintf(out, "run_id=%s dispatch=%s\n", detail.RunID, diagnostics)
			lastDiagnostics = diagnostics
		}

		status := strings.ToLower(strings.TrimSpace(detail.Status))
		if expected[status] {
			return detail, nil
		}

		switch status {
		case "succeeded", "failed", "orphaned", "cancelled", "abandoned", "aborted":
			return detail, fmt.Errorf("run %s reached terminal status %s%s", runID, detail.Status, smokeRunWaitSuffix(detail))
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return smokeRunDetail{}, fmt.Errorf("timed out waiting for run %s%s: %w", runID, smokeRunWaitSuffix(detail), ctx.Err())
		}
	}
}

func smokeRunProgressLine(detail smokeRunDetail) string {
	parts := []string{
		"run_id=" + strings.TrimSpace(detail.RunID),
		"status=" + strings.TrimSpace(detail.Status),
	}

	if nextAction := strings.TrimSpace(detail.NextAction); nextAction != "" {
		parts = append(parts, "next_action="+nextAction)
	}

	return strings.Join(parts, " ")
}

func smokeRunWaitSuffix(detail smokeRunDetail) string {
	parts := []string{}
	if status := strings.TrimSpace(detail.Status); status != "" {
		parts = append(parts, "last_status="+status)
	}

	if nextAction := strings.TrimSpace(detail.NextAction); nextAction != "" {
		parts = append(parts, "next_action="+nextAction)
	}

	if diagnostics := smokeRunDispatchDiagnostics(detail); diagnostics != "" {
		parts = append(parts, "dispatch="+diagnostics)
	}

	if len(parts) == 0 {
		return ""
	}

	return " (" + strings.Join(parts, "; ") + ")"
}

func smokeRunDispatchDiagnostics(detail smokeRunDetail) string {
	failures := []string{}
	for _, event := range detail.DispatchEvents {
		if !strings.EqualFold(strings.TrimSpace(event.EventType), "failure") {
			continue
		}

		source := strings.TrimSpace(event.Source)
		if instance := strings.TrimSpace(event.SourceInstance); instance != "" {
			if source == "" {
				source = instance
			} else {
				source += "/" + instance
			}
		}
		if source == "" {
			source = "unknown"
		}

		message := strings.TrimSpace(event.Message)
		if message == "" {
			message = "no message"
		}

		failures = append(failures, source+" failure: "+message)
	}

	const maxDispatchDiagnostics = 3
	if len(failures) > maxDispatchDiagnostics {
		failures = failures[len(failures)-maxDispatchDiagnostics:]
	}

	return strings.Join(failures, "; ")
}

func waitForSmokeTaskLeaseOwners(ctx context.Context, client *http.Client, baseURL, token, runID string, taskKeys []string, minOwners int, out io.Writer) ([]string, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	taskKeySet := map[string]bool{}
	for _, key := range taskKeys {
		key = strings.TrimSpace(key)
		if key != "" {
			taskKeySet[key] = true
		}
	}

	for {
		tasks, err := fetchSmokeRunTasks(client, baseURL, token, runID)
		if err != nil {
			return nil, err
		}

		ownersByTask := map[string]string{}
		ownerSet := map[string]bool{}
		for _, task := range tasks.Data {
			if len(taskKeySet) > 0 && !taskKeySet[task.TaskKey] {
				continue
			}

			for _, attempt := range task.Attempts {
				owner := ""
				if attempt.LeaseOwner != nil {
					owner = strings.TrimSpace(*attempt.LeaseOwner)
				}
				if owner == "" {
					continue
				}

				ownersByTask[task.TaskKey] = owner
				ownerSet[owner] = true
			}
		}

		owners := sortedSmokeMapKeys(ownerSet)
		fmt.Fprintf(out, "active scale task owners: %s\n", formatSmokeOwnersByTask(ownersByTask))
		if len(owners) >= minOwners {
			return owners, nil
		}

		detail, err := fetchSmokeRunDetail(client, baseURL, token, runID)
		if err != nil {
			return nil, err
		}

		switch strings.ToLower(strings.TrimSpace(detail.Status)) {
		case "succeeded", "failed", "orphaned", "cancelled", "abandoned", "aborted":
			return nil, fmt.Errorf("run %s reached terminal status %s before %d distinct worker owners were observed; saw %d: %s", runID, detail.Status, minOwners, len(owners), strings.Join(owners, ","))
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for %d distinct worker owners on run %s: %w", minOwners, runID, ctx.Err())
		}
	}
}

func waitForSmokeTaskLeaseOwner(ctx context.Context, client *http.Client, baseURL, token, runID, taskKey string, out io.Writer) (string, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	taskKey = strings.TrimSpace(taskKey)
	if taskKey == "" {
		return "", fmt.Errorf("task key is required")
	}

	for {
		tasks, err := fetchSmokeRunTasks(client, baseURL, token, runID)
		if err != nil {
			return "", err
		}

		owner := smokeTaskLeaseOwner(tasks, taskKey)
		if owner != "" {
			fmt.Fprintf(out, "active task owner: %s=%s\n", taskKey, owner)
			return owner, nil
		}

		fmt.Fprintf(out, "active task owner: %s=-\n", taskKey)
		detail, err := fetchSmokeRunDetail(client, baseURL, token, runID)
		if err != nil {
			return "", err
		}

		switch strings.ToLower(strings.TrimSpace(detail.Status)) {
		case "succeeded", "failed", "orphaned", "cancelled", "abandoned", "aborted":
			return "", fmt.Errorf("run %s reached terminal status %s before task %s had an active owner", runID, detail.Status, taskKey)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for active owner on task %s for run %s: %w", taskKey, runID, ctx.Err())
		}
	}
}

func smokeTaskLeaseOwner(tasks smokeRunTasksResponse, taskKey string) string {
	for _, task := range tasks.Data {
		if task.TaskKey != taskKey {
			continue
		}

		for _, attempt := range task.Attempts {
			owner := ""
			if attempt.LeaseOwner != nil {
				owner = strings.TrimSpace(*attempt.LeaseOwner)
			}
			if owner != "" && smokeExecutionActive(attempt.ExecutionStatus) {
				return owner
			}
		}
	}

	return ""
}

func smokeExecutionActive(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "accepted", "running":
		return true
	default:
		return false
	}
}

func assertSmokeRunTaskAttemptCount(client *http.Client, baseURL, token, runID, taskKey string, want int) error {
	tasks, err := fetchSmokeRunTasks(client, baseURL, token, runID)
	if err != nil {
		return err
	}

	got, found := smokeTaskAttemptCount(tasks, taskKey)
	if !found {
		return fmt.Errorf("run %s missing task %s", runID, taskKey)
	}

	if got != want {
		return fmt.Errorf("run %s task %s attempts = %d, want %d", runID, taskKey, got, want)
	}

	return nil
}

func assertSmokeRunRemainsOrphaned(ctx context.Context, client *http.Client, baseURL, token, runID, taskKey string, wantAttempts int, out io.Writer) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		detail, err := fetchSmokeRunDetail(client, baseURL, token, runID)
		if err != nil {
			return err
		}

		status := strings.ToLower(strings.TrimSpace(detail.Status))
		fmt.Fprintf(out, "run_id=%s stability_status=%s\n", detail.RunID, detail.Status)
		if status != "orphaned" {
			return fmt.Errorf("run %s left orphaned state during stability check: %s", runID, detail.Status)
		}

		if err := assertSmokeRunTaskAttemptCount(client, baseURL, token, runID, taskKey, wantAttempts); err != nil {
			return err
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return nil
			}
			return fmt.Errorf("orphan stability check cancelled for run %s: %w", runID, ctx.Err())
		}
	}
}

func smokeTaskAttemptCount(tasks smokeRunTasksResponse, taskKey string) (int, bool) {
	for _, task := range tasks.Data {
		if task.TaskKey == taskKey {
			return len(task.Attempts), true
		}
	}

	return 0, false
}

func fetchSmokeRunTasks(client *http.Client, baseURL, token, runID string) (smokeRunTasksResponse, error) {
	req, err := newSmokeAPIRequest(http.MethodGet, baseURL, fmt.Sprintf("/api/v1/runs/%s/tasks?limit=200", url.PathEscape(runID)), token, nil)
	if err != nil {
		return smokeRunTasksResponse{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return smokeRunTasksResponse{}, fmt.Errorf("fetch run %s tasks: %w", runID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return smokeRunTasksResponse{}, smokeUnexpectedStatus("fetch run "+runID+" tasks", resp)
	}

	var result smokeRunTasksResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return smokeRunTasksResponse{}, fmt.Errorf("parse run %s tasks: %w", runID, err)
	}

	return result, nil
}

func sortedSmokeMapKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}

func formatSmokeOwnersByTask(ownersByTask map[string]string) string {
	if len(ownersByTask) == 0 {
		return "-"
	}

	keys := make([]string, 0, len(ownersByTask))
	for key := range ownersByTask {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+ownersByTask[key])
	}

	return strings.Join(parts, " ")
}

func cancelSmokeRun(client *http.Client, baseURL, token, runID string) error {
	req, err := newSmokeAPIRequest(http.MethodPost, baseURL, fmt.Sprintf("/api/v1/runs/%s/cancel", url.PathEscape(runID)), token, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("cancel smoke run: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusAccepted:
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cancel smoke run used durable pending path instead of worker-control fast path: %s", strings.TrimSpace(string(body)))
	default:
		return smokeUnexpectedStatus("cancel smoke run", resp)
	}
}

func forceRequeueSmokeRun(client *http.Client, baseURL, token, runID string) error {
	req, err := newSmokeAPIRequest(http.MethodPost, baseURL, fmt.Sprintf("/api/v1/runs/%s/force-requeue", url.PathEscape(runID)), token, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("force-requeue smoke run: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return smokeUnexpectedStatus("force-requeue smoke run", resp)
	}

	return nil
}

func repairMarkSmokeRun(client *http.Client, baseURL, token, runID, status, reason string) error {
	status = strings.TrimSpace(status)
	if status == "" {
		return fmt.Errorf("repair mark status is required")
	}

	body, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		return err
	}

	path := fmt.Sprintf("/api/v1/runs/%s/repair/mark-%s", url.PathEscape(runID), url.PathEscape(status))
	req, err := newSmokeAPIRequest(http.MethodPost, baseURL, path, token, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("repair-mark smoke run: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return smokeUnexpectedStatus("repair-mark smoke run", resp)
	}

	return nil
}

func fetchSmokeRunDetail(client *http.Client, baseURL, token, runID string) (smokeRunDetail, error) {
	req, err := newSmokeAPIRequest(http.MethodGet, baseURL, fmt.Sprintf("/api/v1/runs/%s", url.PathEscape(runID)), token, nil)
	if err != nil {
		return smokeRunDetail{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return smokeRunDetail{}, fmt.Errorf("fetch run %s: %w", runID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return smokeRunDetail{}, smokeUnexpectedStatus("fetch run "+runID, resp)
	}

	var detail smokeRunDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		return smokeRunDetail{}, fmt.Errorf("parse run %s: %w", runID, err)
	}

	return detail, nil
}

func waitForSmokeLogMarkers(ctx context.Context, baseURL, token, runID string, markers []string, out io.Writer) error {
	state := smokeLogMarkerState{
		seen:    map[string]bool{},
		printed: map[int64]bool{},
	}
	for {
		if err := readSmokeLogMarkerStream(ctx, baseURL, token, runID, markers, out, &state); err != nil {
			if ctx.Err() != nil {
				return missingSmokeLogMarkers(markers, state.seen)
			}

			return err
		}

		if smokeLogMarkersComplete(markers, state.seen) {
			return nil
		}

		select {
		case <-ctx.Done():
			return missingSmokeLogMarkers(markers, state.seen)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

type smokeLogMarkerState struct {
	seen        map[string]bool
	printed     map[int64]bool
	maxSequence int64
}

func readSmokeLogMarkerStream(ctx context.Context, baseURL, token, runID string, markers []string, out io.Writer, state *smokeLogMarkerState) error {
	path := fmt.Sprintf("/api/v1/runs/%s/logs", url.PathEscape(runID))
	if state.maxSequence > 0 {
		path += "?since_sequence=" + strconv.FormatInt(state.maxSequence, 10)
	}

	req, err := newSmokeAPIRequest(http.MethodGet, baseURL, path, token, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Accept", "text/event-stream")
	req = req.WithContext(ctx)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("connect to run log stream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return smokeUnexpectedStatus("connect to run log stream", resp)
	}

	reader := bufio.NewReader(resp.Body)
	var dataBuf strings.Builder
	var eventSequence int64
	eventHasSequence := false

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}

			return err
		}

		line = strings.TrimRight(line, "\r\n")
		if line != "" {
			if after, ok := strings.CutPrefix(line, "id:"); ok {
				if n, err := strconv.ParseInt(strings.TrimSpace(after), 10, 64); err == nil {
					eventSequence = n
					eventHasSequence = true
				}
			}

			if after, ok := strings.CutPrefix(line, "data:"); ok {
				dataBuf.WriteString(strings.TrimSpace(after))
			}

			continue
		}

		if dataBuf.Len() == 0 {
			eventSequence = 0
			eventHasSequence = false
			continue
		}

		message := []byte(dataBuf.String())
		dataBuf.Reset()

		var entry smokeLogEntry
		if err := json.Unmarshal(message, &entry); err != nil {
			eventSequence = 0
			eventHasSequence = false
			continue
		}

		sequence := entry.Sequence
		if eventHasSequence && eventSequence >= 0 {
			sequence = eventSequence
		}
		if sequence > state.maxSequence {
			state.maxSequence = sequence
		}

		lineText := smokeLogEntryDisplayText(entry)
		if strings.TrimSpace(lineText) != "" && !state.printed[sequence] {
			fmt.Fprintln(out, lineText)
			if sequence >= 0 {
				state.printed[sequence] = true
			}
		}

		for _, marker := range markers {
			if !strings.HasPrefix(lineText, "$ ") && strings.Contains(lineText, marker) {
				state.seen[marker] = true
			}
		}

		eventSequence = 0
		eventHasSequence = false

		if smokeLogMarkersComplete(markers, state.seen) {
			return nil
		}
	}
}

func smokeLogMarkersComplete(markers []string, seen map[string]bool) bool {
	for _, marker := range markers {
		if !seen[marker] {
			return false
		}
	}

	return true
}

func smokeLogEntryDisplayText(entry smokeLogEntry) string {
	if entry.Stream == int(api.Stream_STREAM_CONTROL.Number()) {
		var meta struct {
			Event  string `json:"event"`
			Status string `json:"status,omitempty"`
		}

		if err := json.Unmarshal([]byte(entry.Data), &meta); err != nil {
			return ""
		}

		switch meta.Event {
		case "start":
			return "run started"
		case "completed":
			if meta.Status != "" {
				return "run completed: " + meta.Status
			}

			return "run completed"
		default:
			return ""
		}
	}

	if entry.Stream == int(api.Stream_STREAM_STDERR.Number()) {
		return "[stderr] " + entry.Data
	}

	return entry.Data
}

func missingSmokeLogMarkers(markers []string, seen map[string]bool) error {
	missing := make([]string, 0, len(markers))
	for _, marker := range markers {
		if !seen[marker] {
			missing = append(missing, marker)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	return fmt.Errorf("run logs missing markers: %s", strings.Join(missing, ", "))
}

func verifySmokeArtifacts(client *http.Client, baseURL, token, runID string) ([]SmokeArtifactCheck, error) {
	checks := []struct {
		name     string
		contains []string
		exact    string
	}{
		{name: "e2e-smoke-report", exact: "canonical-artifact-ok\n"},
		{name: "e2e-registry-retry", contains: []string{"canonical-registry-retry-attempt=2", "canonical-registry-retry-succeeded"}},
	}

	out := make([]SmokeArtifactCheck, 0, len(checks))
	for _, check := range checks {
		body, err := downloadSmokeArtifact(client, baseURL, token, runID, check.name)
		if err != nil {
			return nil, err
		}

		text := string(body)
		if check.exact != "" && text != check.exact {
			return nil, fmt.Errorf("artifact %s content mismatch", check.name)
		}

		for _, want := range check.contains {
			if !strings.Contains(text, want) {
				return nil, fmt.Errorf("artifact %s missing %q", check.name, want)
			}
		}

		out = append(out, SmokeArtifactCheck{Name: check.name, Bytes: len(body)})
	}

	return out, nil
}

func downloadSmokeArtifact(client *http.Client, baseURL, token, runID, name string) ([]byte, error) {
	req, err := newSmokeAPIRequest(
		http.MethodGet,
		baseURL,
		fmt.Sprintf("/api/v1/runs/%s/artifacts/%s/download", url.PathEscape(runID), url.PathEscape(name)),
		token,
		nil,
	)

	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "*/*")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download artifact %s: %w", name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, smokeUnexpectedStatus("download artifact "+name, resp)
	}

	return io.ReadAll(resp.Body)
}

func newSmokeAPIRequest(method, baseURL, path, token string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, strings.TrimRight(baseURL, "/")+path, body)
	if err != nil {
		return nil, err
	}

	if token = strings.TrimSpace(token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	return req, nil
}

func smokeUnexpectedStatus(action string, resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	if detail := strings.TrimSpace(string(body)); detail != "" {
		return fmt.Errorf("%s failed: %s: %s", action, resp.Status, detail)
	}

	return fmt.Errorf("%s failed: %s", action, resp.Status)
}

func smokeJobID(run smokeJobRunResult) string {
	if strings.TrimSpace(run.JobID) != "" {
		return strings.TrimSpace(run.JobID)
	}

	return strings.TrimSpace(run.ID)
}
