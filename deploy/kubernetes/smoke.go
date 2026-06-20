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
	DefaultSmokeContext             = "kind-vectis"
	DefaultSmokeJobPath             = "examples/e2e-canonical.json"
	DefaultSmokeCancelJobPath       = "examples/e2e-kubernetes-cancel.json"
	DefaultSmokeScaleJobPath        = "examples/e2e-kubernetes-scale.json"
	DefaultSmokeScaleWorkerReplicas = 3
	DefaultSmokeScaleMinWorkers     = 2
	DefaultSmokeCLIImage            = "localhost/vectis-cli:dev-local"
	DefaultSmokeSeedSecret          = true
	DefaultSmokeAPIPort             = 18080
	DefaultSmokeWait                = 180 * time.Second
	smokeAPIServiceName             = "service/vectis-api"
	smokeAPIRemotePort              = 8080
	smokeWorkerDeploymentName       = "deployment/vectis-worker"
	smokeHTTPClientDelay            = 30 * time.Second
	smokeSecretRef                  = "encryptedfs://team/smoke-token"
	smokeSecretPlaintext            = "spiffe-secret"
	smokeSecretRoot                 = "/data/vectis/secrets/encryptedfs"
	smokeSecretKeyFile              = "/run/vectis/secrets/encryptedfs.key"
)

type SmokeOptions struct {
	Kubectl             string
	Context             string
	Namespace           string
	JobPath             string
	CancelJobPath       string
	ScaleJobPath        string
	CancelOnly          bool
	ScaleOnly           bool
	ScaleWorkerReplicas int
	ScaleMinWorkers     int
	CLIImage            string
	SeedSecret          *bool
	APILocalPort        int
	Wait                time.Duration
	APIToken            string
	Stdout              io.Writer
}

type SmokeResult struct {
	Status       string               `json:"status"`
	Context      string               `json:"context,omitempty"`
	Namespace    string               `json:"namespace"`
	JobPath      string               `json:"job_path"`
	JobID        string               `json:"job_id,omitempty"`
	RunID        string               `json:"run_id"`
	RunStatus    string               `json:"run_status"`
	WorkerOwners []string             `json:"worker_owners,omitempty"`
	Artifacts    []SmokeArtifactCheck `json:"artifacts"`
}

type SmokeArtifactCheck struct {
	Name  string `json:"name"`
	Bytes int    `json:"bytes"`
}

type smokeJobRunResult struct {
	JobID    string `json:"job_id,omitempty"`
	ID       string `json:"id,omitempty"`
	RunID    string `json:"run_id,omitempty"`
	RunIndex int    `json:"run_index,omitempty"`
}

type smokeRunDetail struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type smokeLogEntry struct {
	Stream int    `json:"stream"`
	Data   string `json:"data"`
}

type smokeDeployment struct {
	Spec struct {
		Replicas *int `json:"replicas"`
	} `json:"spec"`
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
	if opts.CancelOnly {
		return runCancelSmoke(ctx, opts)
	}
	if opts.ScaleOnly {
		return runScaleSmoke(ctx, opts)
	}

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
	if opts.JobPath == "" {
		opts.JobPath = DefaultSmokeJobPath
	}

	opts.CancelJobPath = strings.TrimSpace(opts.CancelJobPath)
	if opts.CancelJobPath == "" {
		opts.CancelJobPath = DefaultSmokeCancelJobPath
	}

	opts.ScaleJobPath = strings.TrimSpace(opts.ScaleJobPath)
	if opts.ScaleJobPath == "" {
		opts.ScaleJobPath = DefaultSmokeScaleJobPath
	}

	if opts.ScaleWorkerReplicas == 0 {
		opts.ScaleWorkerReplicas = DefaultSmokeScaleWorkerReplicas
	}

	if opts.ScaleMinWorkers == 0 {
		opts.ScaleMinWorkers = DefaultSmokeScaleMinWorkers
	}

	opts.CLIImage = strings.TrimSpace(opts.CLIImage)
	if opts.CLIImage == "" {
		opts.CLIImage = DefaultSmokeCLIImage
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

func currentSmokeWorkerReplicas(ctx context.Context, opts SmokeOptions) (int, error) {
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "get", smokeWorkerDeploymentName, "-o", "json")
	if err != nil {
		return 0, fmt.Errorf("get %s replicas: %w: %s%s", smokeWorkerDeploymentName, err, stdout, stderr)
	}

	var deployment smokeDeployment
	if err := json.Unmarshal([]byte(stdout), &deployment); err != nil {
		return 0, fmt.Errorf("parse %s: %w", smokeWorkerDeploymentName, err)
	}

	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas <= 0 {
		return 1, nil
	}

	return *deployment.Spec.Replicas, nil
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

func restoreSmokeWorkerDeployment(opts SmokeOptions, replicas int) {
	ctx, cancel := context.WithTimeout(context.Background(), opts.Wait)
	defer cancel()

	fmt.Fprintf(opts.Stdout, "Restoring %s to %d replicas\n", smokeWorkerDeploymentName, replicas)
	if err := scaleSmokeWorkerDeployment(ctx, opts, replicas); err != nil {
		fmt.Fprintf(opts.Stdout, "Warning: restore %s failed: %v\n", smokeWorkerDeploymentName, err)
	}
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
	pfCtx, cancel := context.WithCancel(ctx)
	args := []string{}
	if opts.Context != "" {
		args = append(args, "--context", opts.Context)
	}

	args = append(args,
		"-n", opts.Namespace,
		"port-forward",
		"--address", "127.0.0.1",
		smokeAPIServiceName,
		fmt.Sprintf("%d:%d", opts.APILocalPort, smokeAPIRemotePort),
	)

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

	for {
		select {
		case line := <-lines:
			fmt.Fprintln(opts.Stdout, line)
			if strings.Contains(line, "Forwarding from") {
				return &smokePortForward{cancel: cancel, done: done}, nil
			}
		case err := <-done:
			cancel()
			if err != nil {
				return nil, fmt.Errorf("API port-forward failed: %w: %s", err, strings.TrimSpace(output.String()))
			}

			return nil, fmt.Errorf("API port-forward exited before becoming ready: %s", strings.TrimSpace(output.String()))
		case <-readyCtx.Done():
			cancel()
			return nil, fmt.Errorf("timed out waiting for API port-forward: %s", strings.TrimSpace(output.String()))
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
	body, err := os.ReadFile(opts.JobPath)
	if err != nil {
		return smokeJobRunResult{}, fmt.Errorf("read smoke job %s: %w", opts.JobPath, err)
	}

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

	for {
		detail, err := fetchSmokeRunDetail(client, baseURL, token, runID)
		if err != nil {
			return smokeRunDetail{}, err
		}

		fmt.Fprintf(out, "run_id=%s status=%s\n", detail.RunID, detail.Status)

		status := strings.ToLower(strings.TrimSpace(detail.Status))
		if expected[status] {
			return detail, nil
		}

		switch status {
		case "succeeded", "failed", "orphaned", "cancelled", "abandoned", "aborted":
			return detail, fmt.Errorf("run %s reached terminal status %s", runID, detail.Status)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return smokeRunDetail{}, fmt.Errorf("timed out waiting for run %s: %w", runID, ctx.Err())
		}
	}
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
	req, err := newSmokeAPIRequest(http.MethodGet, baseURL, fmt.Sprintf("/api/v1/runs/%s/logs", url.PathEscape(runID)), token, nil)
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

	seen := map[string]bool{}
	reader := bufio.NewReader(resp.Body)
	var dataBuf strings.Builder

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return missingSmokeLogMarkers(markers, seen)
			}

			return err
		}

		line = strings.TrimRight(line, "\r\n")
		if line != "" {
			if after, ok := strings.CutPrefix(line, "data:"); ok {
				dataBuf.WriteString(strings.TrimSpace(after))
			}

			continue
		}

		if dataBuf.Len() == 0 {
			continue
		}

		message := []byte(dataBuf.String())
		dataBuf.Reset()

		var entry smokeLogEntry
		if err := json.Unmarshal(message, &entry); err != nil {
			continue
		}

		lineText := smokeLogEntryDisplayText(entry)
		if strings.TrimSpace(lineText) != "" {
			fmt.Fprintln(out, lineText)
		}

		for _, marker := range markers {
			if !strings.HasPrefix(lineText, "$ ") && strings.Contains(lineText, marker) {
				seen[marker] = true
			}
		}

		if len(seen) == len(markers) {
			return nil
		}
	}
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
