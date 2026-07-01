package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	gerritsmoke "vectis/deploy/gerrit"
	"vectis/internal/dal"
)

const (
	smokeGerritDeploymentName          = "deployment/vectis-gerrit"
	smokeGerritPodSelector             = "app.kubernetes.io/name=vectis,app.kubernetes.io/component=gerrit-smoke"
	smokeGerritHTTPRemotePort          = 8080
	smokeGerritSSHRemotePort           = 29418
	smokeSCMPollerDeploymentName       = "deployment/vectis-scm-poller"
	smokeGerritStreamDeploymentName    = "deployment/vectis-scm-gerrit-stream"
	smokeGerritStreamContainerName     = "scm-gerrit-stream"
	smokeGerritStreamSSHSecretName     = "vectis-gerrit-stream-ssh"
	smokeGerritStreamSSHMountPath      = "/run/vectis/gerrit-stream"
	smokeGerritStreamReadyLog          = "Gerrit stream bridge reading managed SSH stream"
	smokeGerritStreamLogMarker         = "kubernetes-gerrit-stream-ok"
	smokeGerritStreamTriggerID         = "gerrit_stream"
	smokeGerritStreamTriggerName       = "Kubernetes Gerrit stream"
	smokeGerritStreamBranch            = "master"
	smokeGerritStreamQuery             = "status:open"
	smokeGerritStreamPollInterval      = 60
	smokeGerritStreamJobIDPrefix       = "kubernetes-gerrit-stream"
	smokeGerritStreamSourcePrefix      = "vectis-scm-gerrit-stream"
	smokeGerritStreamDeploymentPollLog = 200
)

func runGerritStreamSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}
	if err := validateGerritStreamSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	out := opts.Stdout
	setupCtx, setupCancel := context.WithTimeout(ctx, opts.Wait)
	originalReplicas, err := currentSmokeDeploymentReplicas(setupCtx, opts, smokeGerritStreamDeploymentName)
	setupCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	if originalReplicas != 0 {
		return SmokeResult{}, fmt.Errorf("%s must be scaled to 0 before the Gerrit stream smoke can manage it safely; current replicas=%d", smokeGerritStreamDeploymentName, originalReplicas)
	}

	defer restoreSmokeDeployment(opts, smokeGerritStreamDeploymentName, originalReplicas)

	pollerCtx, pollerCancel := context.WithTimeout(ctx, opts.Wait)
	originalPollerReplicas, err := currentSmokeDeploymentReplicas(pollerCtx, opts, smokeSCMPollerDeploymentName)
	pollerCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	if originalPollerReplicas > 0 {
		fmt.Fprintf(out, "Scaling %s from %d to 0 replicas for stream-source isolation\n", smokeSCMPollerDeploymentName, originalPollerReplicas)
		pollerCtx, pollerCancel = context.WithTimeout(ctx, opts.Wait)
		err = scaleSmokeDeployment(pollerCtx, opts, smokeSCMPollerDeploymentName, 0)
		pollerCancel()
		if err != nil {
			return SmokeResult{}, err
		}
		defer restoreSmokeDeployment(opts, smokeSCMPollerDeploymentName, originalPollerReplicas)
	}

	fmt.Fprintf(out, "Applying Gerrit smoke fixture %s\n", smokeGerritDeploymentName)
	setupCtx, setupCancel = context.WithTimeout(ctx, opts.Wait)
	if err := applySmokeGerritFixture(setupCtx, opts); err != nil {
		setupCancel()
		return SmokeResult{}, err
	}

	setupCancel()

	fmt.Fprintf(out, "Starting Gerrit port-forward on 127.0.0.1:%d and 127.0.0.1:%d\n", opts.GerritHTTPPort, opts.GerritSSHLocalPort)
	gerritPF, err := startSmokePortForward(ctx, opts, smokeGerritDeploymentName, []smokePortMapping{
		{LocalPort: opts.GerritHTTPPort, RemotePort: smokeGerritHTTPRemotePort},
		{LocalPort: opts.GerritSSHLocalPort, RemotePort: smokeGerritSSHRemotePort},
	}, "Gerrit")

	if err != nil {
		return SmokeResult{}, err
	}
	defer gerritPF.stop()

	fixtureCtx, fixtureCancel := context.WithTimeout(ctx, opts.Wait)
	fixture, err := gerritsmoke.PrepareStreamSmokeFixture(fixtureCtx, gerritsmoke.StreamSmokeOptions{
		URL:           fmt.Sprintf("http://127.0.0.1:%d", opts.GerritHTTPPort),
		AccountID:     opts.GerritAccountID,
		Username:      opts.GerritUsername,
		Project:       opts.GerritProject,
		ProjectPrefix: opts.GerritProjectPrefix,
		SSHHost:       "127.0.0.1",
		SSHPort:       opts.GerritSSHLocalPort,
		Timeout:       opts.Wait,
		GitBin:        opts.GerritGitBin,
		Stdout:        out,
	})

	fixtureCancel()
	if err != nil {
		return SmokeResult{}, err
	}
	defer func() { _ = fixture.Close() }()

	knownHosts, err := fixture.KnownHostsLine(opts.GerritSSHHost, opts.GerritSSHPort)
	if err != nil {
		return SmokeResult{}, err
	}

	secretCtx, secretCancel := context.WithTimeout(ctx, opts.Wait)
	if err := applySmokeGerritStreamSSHSecret(secretCtx, opts, fixture.PrivateKeyPEM, knownHosts); err != nil {
		secretCancel()
		return SmokeResult{}, err
	}
	secretCancel()

	fmt.Fprintf(out, "Configuring %s for %s\n", smokeGerritStreamDeploymentName, opts.GerritClusterURL)
	envCtx, envCancel := context.WithTimeout(ctx, opts.Wait)
	if err := setSmokeGerritStreamEnv(envCtx, opts); err != nil {
		envCancel()
		return SmokeResult{}, err
	}
	envCancel()

	fmt.Fprintf(out, "Starting API port-forward on 127.0.0.1:%d\n", opts.APILocalPort)
	apiPF, err := startSmokeAPIPortForward(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}
	defer apiPF.stop()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", opts.APILocalPort)
	client := &http.Client{Timeout: smokeHTTPClientDelay}
	jobID := smokeGerritStreamJobID()
	jobCommand := fmt.Sprintf("echo %s job=%s project=%s change=%s", smokeGerritStreamLogMarker, jobID, fixture.Project, fixture.ChangeID)

	fmt.Fprintf(out, "Creating Gerrit-triggered smoke job %s\n", jobID)
	if err := createSmokeGerritStreamJob(client, baseURL, opts, jobID, fixture.Project, jobCommand); err != nil {
		return SmokeResult{}, err
	}
	defer deleteSmokeJob(client, baseURL, opts.APIToken, jobID, out)

	fmt.Fprintf(out, "Scaling %s to 1 replica\n", smokeGerritStreamDeploymentName)
	scaleCtx, scaleCancel := context.WithTimeout(ctx, opts.Wait)
	if err := scaleSmokeDeployment(scaleCtx, opts, smokeGerritStreamDeploymentName, 1); err != nil {
		scaleCancel()
		return SmokeResult{}, err
	}
	scaleCancel()

	readyCtx, readyCancel := context.WithTimeout(ctx, opts.Wait)
	if err := waitForSmokeDeploymentLog(readyCtx, opts, smokeGerritStreamDeploymentName, smokeGerritStreamContainerName, smokeGerritStreamReadyLog, out); err != nil {
		readyCancel()
		return SmokeResult{}, err
	}
	readyCancel()

	fmt.Fprintf(out, "Pushing Gerrit change project=%s change=%s\n", fixture.Project, fixture.ChangeID)
	pushCtx, pushCancel := context.WithTimeout(ctx, opts.Wait)
	if err := fixture.PushChange(pushCtx); err != nil {
		pushCancel()
		return SmokeResult{}, err
	}
	pushCancel()

	runCtx, runCancel := context.WithTimeout(ctx, opts.Wait)
	triggered, err := waitForSmokeGerritStreamRun(runCtx, client, baseURL, opts.APIToken, jobID, out)
	runCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	runCtx, runCancel = context.WithTimeout(ctx, opts.Wait)
	detail, err := waitForSmokeRunStatus(runCtx, client, baseURL, opts.APIToken, triggered.RunID, out, "succeeded")
	runCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	if err := assertSmokeGerritStreamRun(detail); err != nil {
		return SmokeResult{}, err
	}

	fmt.Fprintln(out, "Verifying Gerrit stream smoke logs")
	logCtx, logCancel := context.WithTimeout(ctx, opts.Wait)
	err = waitForSmokeLogMarkers(logCtx, baseURL, opts.APIToken, detail.RunID, []string{smokeGerritStreamLogMarker}, out)
	logCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	sourceInstance := ""
	if detail.TriggerSourceInstance != nil {
		sourceInstance = *detail.TriggerSourceInstance
	}

	result := SmokeResult{
		Status:                "ok",
		Context:               strings.TrimSpace(opts.Context),
		Namespace:             opts.Namespace,
		JobID:                 jobID,
		RunID:                 detail.RunID,
		RunStatus:             detail.Status,
		GerritProject:         fixture.Project,
		GerritChange:          fmt.Sprintf("%s~%s~%s", fixture.Project, smokeGerritStreamBranch, fixture.ChangeID),
		TriggerSourceInstance: sourceInstance,
	}

	fmt.Fprintf(out, "Kubernetes Gerrit stream smoke succeeded: run_id=%s source_instance=%s\n", detail.RunID, sourceInstance)
	return result, nil
}

func validateGerritStreamSmokeOptions(opts SmokeOptions) error {
	if opts.GerritImage == "" {
		return fmt.Errorf("gerrit image is required")
	}

	if opts.GerritHTTPPort <= 0 || opts.GerritHTTPPort > 65535 {
		return fmt.Errorf("gerrit HTTP local port must be between 1 and 65535")
	}

	if opts.GerritSSHLocalPort <= 0 || opts.GerritSSHLocalPort > 65535 {
		return fmt.Errorf("gerrit SSH local port must be between 1 and 65535")
	}

	if opts.GerritClusterURL == "" {
		return fmt.Errorf("gerrit cluster URL is required")
	}

	if _, err := url.ParseRequestURI(opts.GerritClusterURL); err != nil {
		return fmt.Errorf("gerrit cluster URL must be absolute: %w", err)
	}

	if opts.GerritSSHHost == "" {
		return fmt.Errorf("gerrit SSH host is required")
	}

	if opts.GerritSSHPort <= 0 {
		return fmt.Errorf("gerrit SSH port must be > 0")
	}

	if opts.GerritAccountID == "" {
		return fmt.Errorf("gerrit account id is required")
	}

	if opts.GerritUsername == "" {
		return fmt.Errorf("gerrit username is required")
	}

	if opts.GerritGitBin == "" {
		return fmt.Errorf("git executable is required")
	}

	return nil
}

func applySmokeGerritFixture(ctx context.Context, opts SmokeOptions) error {
	stdout, stderr, err := runSmokeKubectl(ctx, opts, strings.NewReader(smokeGerritFixtureManifest(opts.GerritImage)), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("apply Gerrit smoke fixture: %w: %s%s", err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	if err := waitForSmokeDeployment(ctx, opts, smokeGerritDeploymentName); err != nil {
		return fmt.Errorf("wait for Gerrit smoke fixture: %w", err)
	}

	if _, err := waitForSingleReadySmokeGerritPod(ctx, opts); err != nil {
		return fmt.Errorf("wait for Gerrit smoke fixture pod: %w", err)
	}

	return nil
}

func smokeGerritFixtureManifest(image string) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: vectis-gerrit
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: gerrit-smoke
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: vectis
      app.kubernetes.io/component: gerrit-smoke
  template:
    metadata:
      labels:
        app.kubernetes.io/name: vectis
        app.kubernetes.io/component: gerrit-smoke
    spec:
      containers:
        - name: gerrit
          image: %s
          imagePullPolicy: IfNotPresent
          env:
            - name: CANONICAL_WEB_URL
              value: "http://vectis-gerrit:8080/"
            - name: HTTPD_LISTEN_URL
              value: "http://*:8080/"
          ports:
            - name: http
              containerPort: 8080
            - name: ssh
              containerPort: 29418
---
apiVersion: v1
kind: Service
metadata:
  name: vectis-gerrit
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: gerrit-smoke
spec:
  selector:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: gerrit-smoke
  ports:
    - name: http
      port: 8080
      targetPort: http
    - name: ssh
      port: 29418
      targetPort: ssh
`, yamlQuote(image))
}

func waitForSingleReadySmokeGerritPod(ctx context.Context, opts SmokeOptions) (smokePod, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "get", "pods", "-l", smokeGerritPodSelector, "-o", "json")
		if err != nil {
			return smokePod{}, fmt.Errorf("list Gerrit fixture pods: %w: %s%s", err, stdout, stderr)
		}

		var pods smokePodList
		if err := json.Unmarshal([]byte(stdout), &pods); err != nil {
			return smokePod{}, fmt.Errorf("parse Gerrit fixture pods: %w", err)
		}

		ready := make([]smokePod, 0, len(pods.Items))
		for _, pod := range pods.Items {
			if pod.Metadata.Name == "" || pod.Metadata.DeletionTimestamp != nil {
				continue
			}

			if smokePodReady(pod) {
				ready = append(ready, pod)
			}
		}

		if len(ready) == 1 {
			fmt.Fprintf(opts.Stdout, "Gerrit fixture pod ready: %s\n", ready[0].Metadata.Name)
			return ready[0], nil
		}

		fmt.Fprintf(opts.Stdout, "Waiting for one ready Gerrit fixture pod; ready=%d total=%d\n", len(ready), len(pods.Items))
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return smokePod{}, fmt.Errorf("timed out waiting for one ready Gerrit fixture pod: %w", ctx.Err())
		}
	}
}

func applySmokeGerritStreamSSHSecret(ctx context.Context, opts SmokeOptions, privateKey, knownHosts string) error {
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: scm-gerrit-stream
type: Opaque
stringData:
  id_rsa: %s
  known_hosts: %s
`, smokeGerritStreamSSHSecretName, yamlQuote(privateKey), yamlQuote(knownHosts+"\n"))

	stdout, stderr, err := runSmokeKubectl(ctx, opts, strings.NewReader(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("apply Gerrit stream SSH Secret: %w: %s%s", err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	return nil
}

func setSmokeGerritStreamEnv(ctx context.Context, opts SmokeOptions) error {
	env := map[string]string{
		"VECTIS_SCM_GERRIT_STREAM_URL":                  opts.GerritClusterURL,
		"VECTIS_SCM_GERRIT_STREAM_TRANSPORT":            "ssh",
		"VECTIS_SCM_GERRIT_STREAM_SSH_HOST":             opts.GerritSSHHost,
		"VECTIS_SCM_GERRIT_STREAM_SSH_PORT":             strconv.Itoa(opts.GerritSSHPort),
		"VECTIS_SCM_GERRIT_STREAM_SSH_USER":             opts.GerritUsername,
		"VECTIS_SCM_GERRIT_STREAM_SSH_KEY_FILE":         smokeGerritStreamSSHMountPath + "/id_rsa",
		"VECTIS_SCM_GERRIT_STREAM_SSH_KNOWN_HOSTS_FILE": smokeGerritStreamSSHMountPath + "/known_hosts",
		"VECTIS_SCM_GERRIT_STREAM_SSH_USE_AGENT":        "false",
	}

	return setSmokeDeploymentEnv(ctx, opts, smokeGerritStreamDeploymentName, smokeGerritStreamContainerName, env)
}

func setSmokeDeploymentEnv(ctx context.Context, opts SmokeOptions, deployment, container string, env map[string]string) error {
	if len(env) == 0 {
		return nil
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	args := []string{"set", "env", deployment, "--containers=" + container}
	for _, key := range keys {
		args = append(args, key+"="+env[key])
	}

	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, args...)
	if err != nil {
		return fmt.Errorf("set env on %s: %w: %s%s", deployment, err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	return nil
}

func createSmokeGerritStreamJob(client *http.Client, baseURL string, opts SmokeOptions, jobID, project, command string) error {
	payload, err := smokeGerritStreamJobPayload(opts, jobID, project, command)
	if err != nil {
		return err
	}

	req, err := newSmokeAPIRequest(http.MethodPost, baseURL, "/api/v1/jobs", opts.APIToken, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("create Gerrit stream smoke job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return smokeUnexpectedStatus("create Gerrit stream smoke job", resp)
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func smokeGerritStreamJobPayload(opts SmokeOptions, jobID, project, command string) ([]byte, error) {
	definition := map[string]any{
		"id": jobID,
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/shell",
			"with": map[string]any{
				"command": command,
			},
		},
		"triggers": []map[string]any{
			{
				"id":   smokeGerritStreamTriggerID,
				"name": smokeGerritStreamTriggerName,
				"scm_poll": map[string]any{
					"provider":         "gerrit",
					"base_url":         opts.GerritClusterURL,
					"project":          project,
					"branch":           smokeGerritStreamBranch,
					"query":            smokeGerritStreamQuery,
					"interval_seconds": smokeGerritStreamPollInterval,
				},
			},
		},
	}

	payload, err := json.Marshal(map[string]any{
		"namespace": "/",
		"job":       definition,
	})

	if err != nil {
		return nil, fmt.Errorf("marshal Gerrit stream smoke job: %w", err)
	}

	return payload, nil
}

func deleteSmokeJob(client *http.Client, baseURL, token, jobID string, out io.Writer) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return
	}

	req, err := newSmokeAPIRequest(http.MethodDelete, baseURL, "/api/v1/jobs/"+url.PathEscape(jobID), token, nil)
	if err != nil {
		fmt.Fprintf(out, "Warning: delete smoke job %s request failed: %v\n", jobID, err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(out, "Warning: delete smoke job %s failed: %v\n", jobID, err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}

func waitForSmokeGerritStreamRun(ctx context.Context, client *http.Client, baseURL, token, jobID string, out io.Writer) (smokeRunDetail, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		runs, err := listSmokeJobRuns(client, baseURL, token, jobID)
		if err != nil {
			return smokeRunDetail{}, err
		}

		for _, run := range runs.Data {
			source := smokeStringPtr(run.TriggerSourceInstance)
			triggerType := smokeStringPtr(run.TriggerType)
			fmt.Fprintf(out, "job_id=%s run_id=%s status=%s trigger_type=%s source_instance=%s\n", jobID, run.RunID, run.Status, triggerType, source)
			if triggerType == dal.TriggerTypeSCMPoll && strings.Contains(source, smokeGerritStreamSourcePrefix) {
				return run, nil
			}
		}

		fmt.Fprintf(out, "Waiting for Gerrit stream-triggered run for job_id=%s\n", jobID)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return smokeRunDetail{}, fmt.Errorf("timed out waiting for Gerrit stream-triggered run for job %s: %w", jobID, ctx.Err())
		}
	}
}

type smokeJobRunsResponse struct {
	Data []smokeRunDetail `json:"data"`
}

func listSmokeJobRuns(client *http.Client, baseURL, token, jobID string) (smokeJobRunsResponse, error) {
	req, err := newSmokeAPIRequest(http.MethodGet, baseURL, "/api/v1/jobs/"+url.PathEscape(jobID)+"/runs?limit=20", token, nil)
	if err != nil {
		return smokeJobRunsResponse{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return smokeJobRunsResponse{}, fmt.Errorf("list runs for job %s: %w", jobID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return smokeJobRunsResponse{}, smokeUnexpectedStatus("list runs for job "+jobID, resp)
	}

	var result smokeJobRunsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return smokeJobRunsResponse{}, fmt.Errorf("parse runs for job %s: %w", jobID, err)
	}

	return result, nil
}

func assertSmokeGerritStreamRun(detail smokeRunDetail) error {
	if smokeStringPtr(detail.TriggerType) != dal.TriggerTypeSCMPoll {
		return fmt.Errorf("Gerrit stream smoke run trigger_type = %q, want %q", smokeStringPtr(detail.TriggerType), dal.TriggerTypeSCMPoll)
	}

	if detail.TriggerInvocationID == nil || strings.TrimSpace(*detail.TriggerInvocationID) == "" {
		return fmt.Errorf("Gerrit stream smoke run missing trigger_invocation_id")
	}

	if detail.TriggerSourceInstance == nil || !strings.Contains(*detail.TriggerSourceInstance, smokeGerritStreamSourcePrefix) {
		return fmt.Errorf("Gerrit stream smoke run trigger_source_instance = %q, want %q prefix", smokeStringPtr(detail.TriggerSourceInstance), smokeGerritStreamSourcePrefix)
	}

	if smokeStringPtr(detail.TriggerKey) != smokeGerritStreamTriggerID {
		return fmt.Errorf("Gerrit stream smoke run trigger_key = %q, want %q", smokeStringPtr(detail.TriggerKey), smokeGerritStreamTriggerID)
	}

	for _, event := range detail.DispatchEvents {
		if event.Source == dal.DispatchSourceSCMGerritStream && strings.Contains(event.SourceInstance, smokeGerritStreamSourcePrefix) {
			return nil
		}
	}

	return fmt.Errorf("Gerrit stream smoke run missing %s dispatch event: %+v", dal.DispatchSourceSCMGerritStream, detail.DispatchEvents)
}

func fetchSmokeDeployment(ctx context.Context, opts SmokeOptions, name string) (smokeDeployment, error) {
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "get", name, "-o", "json")
	if err != nil {
		return smokeDeployment{}, fmt.Errorf("get %s: %w: %s%s", name, err, stdout, stderr)
	}

	var deployment smokeDeployment
	if err := json.Unmarshal([]byte(stdout), &deployment); err != nil {
		return smokeDeployment{}, fmt.Errorf("parse %s: %w", name, err)
	}

	return deployment, nil
}

func currentSmokeDeploymentReplicas(ctx context.Context, opts SmokeOptions, name string) (int, error) {
	deployment, err := fetchSmokeDeployment(ctx, opts, name)
	if err != nil {
		return 0, err
	}

	return smokeDeploymentReplicasExact(deployment), nil
}

func smokeDeploymentReplicasExact(deployment smokeDeployment) int {
	if deployment.Spec.Replicas == nil {
		return 1
	}

	if *deployment.Spec.Replicas < 0 {
		return 0
	}

	return *deployment.Spec.Replicas
}

func scaleSmokeDeployment(ctx context.Context, opts SmokeOptions, name string, replicas int) error {
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "scale", name, "--replicas", strconv.Itoa(replicas))
	if err != nil {
		return fmt.Errorf("scale %s to %d: %w: %s%s", name, replicas, err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	if err := waitForSmokeDeployment(ctx, opts, name); err != nil {
		return fmt.Errorf("wait for %s after scale to %d: %w", name, replicas, err)
	}

	return nil
}

func waitForSmokeDeployment(ctx context.Context, opts SmokeOptions, name string) error {
	timeout := opts.Wait.String()
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "rollout", "status", name, "--timeout", timeout)
	if err != nil {
		return fmt.Errorf("rollout status: %w: %s%s", err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	return nil
}

func restoreSmokeDeployment(opts SmokeOptions, name string, replicas int) {
	ctx, cancel := context.WithTimeout(context.Background(), opts.Wait)
	defer cancel()

	fmt.Fprintf(opts.Stdout, "Restoring %s to %d replicas\n", name, replicas)
	if err := scaleSmokeDeployment(ctx, opts, name, replicas); err != nil {
		fmt.Fprintf(opts.Stdout, "Warning: restore %s failed: %v\n", name, err)
	}
}

func waitForSmokeDeploymentLog(ctx context.Context, opts SmokeOptions, deployment, container, marker string, out io.Writer) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		stdout, stderr, err := runSmokeKubectl(ctx, opts, nil,
			"logs",
			deployment,
			"-c", container,
			"--tail", strconv.Itoa(smokeGerritStreamDeploymentPollLog),
		)

		if err == nil && strings.Contains(stdout+stderr, marker) {
			fmt.Fprintf(out, "%s log marker observed: %s\n", deployment, marker)
			return nil
		}

		if err != nil {
			fmt.Fprintf(out, "Waiting for %s logs: %v\n", deployment, err)
		} else {
			fmt.Fprintf(out, "Waiting for %s log marker %q\n", deployment, marker)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %s log marker %q: %w", deployment, marker, ctx.Err())
		}
	}
}

func smokeGerritStreamJobID() string {
	return fmt.Sprintf("%s-%d", smokeGerritStreamJobIDPrefix, time.Now().UTC().UnixNano())
}

func smokeStringPtr(value *string) string {
	if value == nil {
		return ""
	}

	return strings.TrimSpace(*value)
}
