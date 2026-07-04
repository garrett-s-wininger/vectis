package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	s3artifact "vectis/extensions/artifacts/s3"
)

const (
	smokeS3ArtifactDeploymentName  = "deployment/vectis-s3-artifact"
	smokeS3ArtifactServiceName     = "service/vectis-s3-artifact"
	smokeS3ArtifactRemotePort      = 8333
	smokeS3ArtifactContainerName   = "seaweedfs"
	smokeArtifactStatefulSetName   = "statefulset/vectis-artifact"
	smokeArtifactContainerName     = "artifact"
	smokeS3ArtifactNegativeTimeout = 5 * time.Second
)

func runS3ArtifactSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}
	if err := validateS3ArtifactSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	out := opts.Stdout
	fmt.Fprintf(out, "Applying S3 artifact smoke fixture %s\n", smokeS3ArtifactDeploymentName)
	setupCtx, setupCancel := context.WithTimeout(ctx, opts.Wait)
	if err := applySmokeS3ArtifactFixture(setupCtx, opts); err != nil {
		setupCancel()
		return SmokeResult{}, err
	}

	setupCancel()
	if !opts.S3KeepFixture {
		defer cleanupSmokeS3ArtifactFixture(opts)
	}

	fmt.Fprintf(out, "Starting S3 artifact port-forward on 127.0.0.1:%d\n", opts.S3LocalPort)
	s3PF, err := startSmokePortForward(ctx, opts, smokeS3ArtifactServiceName, []smokePortMapping{{
		LocalPort:  opts.S3LocalPort,
		RemotePort: smokeS3ArtifactRemotePort,
	}}, "S3 artifact")

	if err != nil {
		return SmokeResult{}, err
	}
	defer s3PF.stop()

	localEndpoint := fmt.Sprintf("http://127.0.0.1:%d", opts.S3LocalPort)
	seedCtx, seedCancel := context.WithTimeout(ctx, opts.Wait)
	if _, err := s3artifact.RunSmoke(seedCtx, s3artifact.SmokeOptions{
		Endpoint:        localEndpoint,
		Bucket:          opts.S3Bucket,
		Prefix:          opts.S3Prefix + "/seed",
		AccessKeyID:     opts.S3AccessKeyID,
		SecretAccessKey: opts.S3SecretAccessKey,
		PathStyle:       true,
		CreateBucket:    true,
		Timeout:         opts.Wait,
		Stdout:          out,
	}); err != nil {
		seedCancel()
		return SmokeResult{}, fmt.Errorf("seed S3 artifact smoke bucket: %w", err)
	}
	seedCancel()

	unsignedCtx, unsignedCancel := context.WithTimeout(ctx, smokeS3ArtifactNegativeTimeout)
	_, err = s3artifact.RunSmoke(unsignedCtx, s3artifact.SmokeOptions{
		Endpoint:     localEndpoint,
		Bucket:       opts.S3Bucket,
		Prefix:       opts.S3Prefix + "/unsigned-reject",
		PathStyle:    true,
		CreateBucket: false,
		Timeout:      smokeS3ArtifactNegativeTimeout,
		Stdout:       out,
	})

	unsignedCancel()
	if err == nil {
		return SmokeResult{}, fmt.Errorf("S3 artifact smoke unexpectedly accepted unsigned requests")
	}

	fmt.Fprintf(out, "S3 artifact fixture rejected unsigned requests: %v\n", err)

	artifactEnv := smokeArtifactS3Env(opts)
	snapshotCtx, snapshotCancel := context.WithTimeout(ctx, opts.Wait)
	artifactEnvSnapshot, err := snapshotSmokeWorkloadEnv(snapshotCtx, opts, smokeArtifactStatefulSetName, smokeArtifactContainerName, mapKeys(artifactEnv))
	snapshotCancel()
	if err != nil {
		return SmokeResult{}, err
	}
	defer restoreSmokeWorkloadEnv(opts, smokeArtifactStatefulSetName, smokeArtifactContainerName, artifactEnvSnapshot)

	patchCtx, patchCancel := context.WithTimeout(ctx, opts.Wait)
	if err := setSmokeWorkloadEnv(patchCtx, opts, smokeArtifactStatefulSetName, smokeArtifactContainerName, artifactEnv); err != nil {
		patchCancel()
		return SmokeResult{}, err
	}
	patchCancel()

	result, err := runCanonicalSmoke(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}

	result.S3Endpoint = opts.S3ClusterEndpoint
	result.S3Bucket = opts.S3Bucket
	result.S3Prefix = opts.S3Prefix
	fmt.Fprintf(out, "Kubernetes S3 artifact smoke succeeded: run_id=%s bucket=%s prefix=%s\n", result.RunID, opts.S3Bucket, opts.S3Prefix)
	return result, nil
}

func validateS3ArtifactSmokeOptions(opts SmokeOptions) error {
	if opts.S3Image == "" {
		return fmt.Errorf("S3 image is required")
	}

	if opts.S3LocalPort <= 0 || opts.S3LocalPort > 65535 {
		return fmt.Errorf("S3 local port must be between 1 and 65535")
	}

	if opts.S3ClusterEndpoint == "" {
		return fmt.Errorf("S3 cluster endpoint is required")
	}

	if opts.S3Bucket == "" {
		return fmt.Errorf("S3 bucket is required")
	}

	if opts.S3Prefix == "" {
		return fmt.Errorf("S3 prefix is required")
	}

	if opts.S3AccessKeyID == "" {
		return fmt.Errorf("S3 access key id is required")
	}

	if opts.S3SecretAccessKey == "" {
		return fmt.Errorf("S3 secret access key is required")
	}

	return nil
}

func applySmokeS3ArtifactFixture(ctx context.Context, opts SmokeOptions) error {
	return applySmokeManifest(ctx, opts, "S3 artifact smoke fixture", smokeS3ArtifactFixtureManifest(opts), smokeS3ArtifactDeploymentName)
}

func cleanupSmokeS3ArtifactFixture(opts SmokeOptions) {
	cleanupSmokeResources(opts, "S3 artifact smoke fixture", smokeS3ArtifactDeploymentName, smokeS3ArtifactServiceName, "configmap/vectis-s3-artifact-auth")
}

func smokeS3ArtifactFixtureManifest(opts SmokeOptions) string {
	return fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: vectis-s3-artifact-auth
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: s3-artifact-smoke
data:
  s3-auth.json: %s
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vectis-s3-artifact
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: s3-artifact-smoke
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: vectis
      app.kubernetes.io/component: s3-artifact-smoke
  template:
    metadata:
      labels:
        app.kubernetes.io/name: vectis
        app.kubernetes.io/component: s3-artifact-smoke
    spec:
      automountServiceAccountToken: false
      containers:
        - name: %s
          image: %s
          imagePullPolicy: IfNotPresent
          args:
            - server
            - -s3
            - -s3.port=8333
            - -s3.config=/etc/seaweedfs/s3-auth.json
            - -dir=/data
          ports:
            - name: s3
              containerPort: %d
          volumeMounts:
            - name: s3-auth
              mountPath: /etc/seaweedfs
              readOnly: true
          readinessProbe:
            tcpSocket:
              port: %d
            initialDelaySeconds: 3
            periodSeconds: 5
          livenessProbe:
            tcpSocket:
              port: %d
            initialDelaySeconds: 10
            periodSeconds: 10
      volumes:
        - name: s3-auth
          configMap:
            name: vectis-s3-artifact-auth
---
apiVersion: v1
kind: Service
metadata:
  name: vectis-s3-artifact
  labels:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: s3-artifact-smoke
spec:
  selector:
    app.kubernetes.io/name: vectis
    app.kubernetes.io/component: s3-artifact-smoke
  ports:
    - name: s3
      port: %d
      targetPort: %d
`, yamlQuote(smokeS3ArtifactAuthConfig(opts)), smokeS3ArtifactContainerName, yamlQuote(opts.S3Image), smokeS3ArtifactRemotePort, smokeS3ArtifactRemotePort, smokeS3ArtifactRemotePort, smokeS3ArtifactRemotePort, smokeS3ArtifactRemotePort)
}

func smokeS3ArtifactAuthConfig(opts SmokeOptions) string {
	config := struct {
		Identities []struct {
			Name        string `json:"name"`
			Credentials []struct {
				AccessKey string `json:"accessKey"`
				SecretKey string `json:"secretKey"`
			} `json:"credentials"`
			Actions []string `json:"actions"`
		} `json:"identities"`
	}{
		Identities: []struct {
			Name        string `json:"name"`
			Credentials []struct {
				AccessKey string `json:"accessKey"`
				SecretKey string `json:"secretKey"`
			} `json:"credentials"`
			Actions []string `json:"actions"`
		}{
			{
				Name: opts.S3AccessKeyID,
				Credentials: []struct {
					AccessKey string `json:"accessKey"`
					SecretKey string `json:"secretKey"`
				}{
					{AccessKey: opts.S3AccessKeyID, SecretKey: opts.S3SecretAccessKey},
				},
				Actions: []string{"Admin", "Read", "List", "Tagging", "Write"},
			},
		},
	}

	data, _ := json.Marshal(config)
	return string(data)
}

func smokeArtifactS3Env(opts SmokeOptions) map[string]string {
	return map[string]string{
		"VECTIS_ARTIFACT_STORAGE_BACKEND":              "s3",
		"VECTIS_ARTIFACT_STORAGE_S3_ENDPOINT":          opts.S3ClusterEndpoint,
		"VECTIS_ARTIFACT_STORAGE_S3_REGION":            s3artifact.DefaultSmokeRegion,
		"VECTIS_ARTIFACT_STORAGE_S3_BUCKET":            opts.S3Bucket,
		"VECTIS_ARTIFACT_STORAGE_S3_PREFIX":            opts.S3Prefix,
		"VECTIS_ARTIFACT_STORAGE_S3_ACCESS_KEY_ID":     opts.S3AccessKeyID,
		"VECTIS_ARTIFACT_STORAGE_S3_SECRET_ACCESS_KEY": opts.S3SecretAccessKey,
		"VECTIS_ARTIFACT_STORAGE_S3_PATH_STYLE":        "true",
		"VECTIS_ARTIFACT_STORAGE_S3_TEMP_DIR":          opts.S3TempDir,
	}
}

type smokeWorkloadEnvSnapshot struct {
	Value    string
	HadValue bool
}

func snapshotSmokeWorkloadEnv(ctx context.Context, opts SmokeOptions, workload, container string, names []string) (map[string]smokeWorkloadEnvSnapshot, error) {
	current, err := fetchSmokeWorkload(ctx, opts, workload)
	if err != nil {
		return nil, err
	}

	out := make(map[string]smokeWorkloadEnvSnapshot, len(names))
	for _, name := range names {
		value, hadValue := smokeDeploymentContainerEnv(current, container, name)
		out[name] = smokeWorkloadEnvSnapshot{
			Value:    value,
			HadValue: hadValue,
		}
	}

	return out, nil
}

func fetchSmokeWorkload(ctx context.Context, opts SmokeOptions, workload string) (smokeDeployment, error) {
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "get", workload, "-o", "json")
	if err != nil {
		return smokeDeployment{}, fmt.Errorf("get %s: %w: %s%s", workload, err, stdout, stderr)
	}

	var deployment smokeDeployment
	if err := json.Unmarshal([]byte(stdout), &deployment); err != nil {
		return smokeDeployment{}, fmt.Errorf("parse %s: %w", workload, err)
	}

	return deployment, nil
}

func setSmokeWorkloadEnv(ctx context.Context, opts SmokeOptions, workload, container string, env map[string]string) error {
	if len(env) == 0 {
		return nil
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	args := []string{"set", "env", workload, "--containers=" + container}
	for _, key := range keys {
		args = append(args, key+"="+env[key])
	}

	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, args...)
	if err != nil {
		return fmt.Errorf("set env on %s: %w: %s%s", workload, err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	if err := waitForSmokeWorkload(ctx, opts, workload); err != nil {
		return fmt.Errorf("wait for %s after setting env: %w", workload, err)
	}

	return nil
}

func restoreSmokeWorkloadEnv(opts SmokeOptions, workload, container string, snapshot map[string]smokeWorkloadEnvSnapshot) {
	if len(snapshot) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.Wait)
	defer cancel()

	fmt.Fprintf(opts.Stdout, "Restoring temporary env on %s\n", workload)
	args := []string{"set", "env", workload, "--containers=" + container}
	for _, key := range mapKeys(snapshot) {
		env := snapshot[key]
		if env.HadValue {
			args = append(args, key+"="+env.Value)
		} else {
			args = append(args, key+"-")
		}
	}

	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, args...)
	if err != nil {
		fmt.Fprintf(opts.Stdout, "Warning: restore env on %s failed: %v: %s%s\n", workload, err, stdout, stderr)
		return
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	if err := waitForSmokeWorkload(ctx, opts, workload); err != nil {
		fmt.Fprintf(opts.Stdout, "Warning: wait for restored %s failed: %v\n", workload, err)
	}
}

func waitForSmokeWorkload(ctx context.Context, opts SmokeOptions, workload string) error {
	timeout := opts.Wait.String()
	stdout, stderr, err := runSmokeKubectl(ctx, opts, nil, "rollout", "status", workload, "--timeout", timeout)
	if err != nil {
		return fmt.Errorf("rollout status: %w: %s%s", err, stdout, stderr)
	}

	if text := strings.TrimSpace(stdout + stderr); text != "" {
		fmt.Fprintln(opts.Stdout, text)
	}

	return nil
}

func mapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}
