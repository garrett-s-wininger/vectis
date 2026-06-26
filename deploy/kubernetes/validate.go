package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

const (
	DefaultValidateContext = DefaultSmokeContext
	DefaultValidateWait    = DefaultSmokeWait

	validatePodSelector = "app.kubernetes.io/name=vectis"
)

var (
	DefaultValidateStatefulSets = []string{
		"vectis-postgres",
		"vectis-queue",
		"vectis-log",
		"vectis-artifact",
		"vectis-secrets",
	}

	DefaultValidateDeployments = []string{
		"vectis-registry",
		"vectis-orchestrator",
		"vectis-api",
		"vectis-docs",
		"vectis-cron",
		"vectis-reconciler",
		"vectis-catalog",
		"vectis-worker",
	}

	runValidateKubectlCommand = defaultRunValidateKubectlCommand
	runValidateSmoke          = RunSmoke
)

type ValidateOptions struct {
	Kubectl          string
	Context          string
	Namespace        string
	ManifestPath     string
	OutputPath       string
	ImageRegistry    string
	ImageTag         string
	PostgresPassword string
	BootstrapToken   string
	EncryptedFSKey   string
	StatefulSets     []string
	Deployments      []string
	Wait             time.Duration
	SkipSmoke        bool
	SmokeOptions     SmokeOptions
	Stdout           io.Writer
}

type ValidateResult struct {
	Status       string             `json:"status"`
	ManifestPath string             `json:"manifest_path"`
	Render       RenderResult       `json:"render"`
	Workloads    []ValidateWorkload `json:"workloads"`
	Smoke        *SmokeResult       `json:"smoke,omitempty"`
}

type ValidateWorkload struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type validateKubectlScope string

const (
	validateKubectlCluster    validateKubectlScope = "cluster"
	validateKubectlNamespaced validateKubectlScope = "namespaced"
)

func RunValidate(ctx context.Context, opts ValidateOptions) (ValidateResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	opts = normalizeValidateOptions(opts)
	if err := validateValidateOptions(opts); err != nil {
		return ValidateResult{}, err
	}

	fmt.Fprintf(opts.Stdout, "Rendering Kubernetes manifest to %s\n", opts.OutputPath)
	renderResult, err := RenderToFile(RenderOptions{
		ManifestPath:     opts.ManifestPath,
		Namespace:        opts.Namespace,
		ImageRegistry:    opts.ImageRegistry,
		ImageTag:         opts.ImageTag,
		PostgresPassword: opts.PostgresPassword,
		BootstrapToken:   opts.BootstrapToken,
		EncryptedFSKey:   opts.EncryptedFSKey,
	}, opts.OutputPath)

	if err != nil {
		return ValidateResult{}, err
	}

	result := ValidateResult{
		Status:       "validated",
		ManifestPath: opts.OutputPath,
		Render:       renderResult,
	}

	fmt.Fprintf(opts.Stdout, "Applying Kubernetes manifest %s\n", opts.OutputPath)
	if err := runValidateKubectlPhase(ctx, opts, validateKubectlCluster, nil, "apply", "-f", opts.OutputPath); err != nil {
		return ValidateResult{}, fmt.Errorf("apply Kubernetes manifest %s: %w", opts.OutputPath, err)
	}

	for _, name := range opts.StatefulSets {
		if err := waitForValidateWorkload(ctx, opts, "statefulset", name); err != nil {
			return ValidateResult{}, err
		}

		result.Workloads = append(result.Workloads, ValidateWorkload{Kind: "statefulset", Name: name})
	}

	for _, name := range opts.Deployments {
		if err := waitForValidateWorkload(ctx, opts, "deployment", name); err != nil {
			return ValidateResult{}, err
		}

		result.Workloads = append(result.Workloads, ValidateWorkload{Kind: "deployment", Name: name})
	}

	fmt.Fprintln(opts.Stdout, "Checking Vectis pod readiness")
	if err := runValidateKubectlPhase(ctx, opts, validateKubectlNamespaced, nil, "get", "pods", "-o", "wide"); err != nil {
		return ValidateResult{}, fmt.Errorf("list Kubernetes pods: %w", err)
	}

	if err := runValidateKubectlPhase(ctx, opts, validateKubectlNamespaced, nil,
		"wait",
		"--for=condition=Ready",
		"pod",
		"-l", validatePodSelector,
		"--field-selector=status.phase!=Succeeded",
		"--timeout", opts.Wait.String(),
	); err != nil {
		return ValidateResult{}, fmt.Errorf("wait for Kubernetes pods: %w", err)
	}

	if !opts.SkipSmoke {
		smokeOpts := smokeOptionsForValidate(opts)
		smoke, err := runValidateSmoke(ctx, smokeOpts)
		if err != nil {
			return ValidateResult{}, fmt.Errorf("run Kubernetes smoke: %w", err)
		}

		result.Smoke = &smoke
	}

	fmt.Fprintln(opts.Stdout, "Kubernetes deployment validation succeeded")
	return result, nil
}

func normalizeValidateOptions(opts ValidateOptions) ValidateOptions {
	opts.Kubectl = strings.TrimSpace(opts.Kubectl)
	if opts.Kubectl == "" {
		opts.Kubectl = "kubectl"
	}

	opts.Context = strings.TrimSpace(opts.Context)
	opts.Namespace = strings.TrimSpace(opts.Namespace)
	if opts.Namespace == "" {
		opts.Namespace = DefaultNamespace
	}

	opts.ManifestPath = strings.TrimSpace(opts.ManifestPath)
	if opts.ManifestPath == "" {
		opts.ManifestPath = DefaultManifestPath
	}

	opts.OutputPath = strings.TrimSpace(opts.OutputPath)
	if opts.OutputPath == "" {
		opts.OutputPath = DefaultOutputPath
	}

	opts.ImageRegistry = strings.Trim(strings.TrimSpace(opts.ImageRegistry), "/")
	opts.ImageTag = strings.TrimSpace(opts.ImageTag)
	if opts.ImageTag == "" {
		opts.ImageTag = DefaultImageTag
	}

	if opts.PostgresPassword == "" {
		opts.PostgresPassword = defaultPostgresPassword
	}

	if opts.BootstrapToken == "" {
		opts.BootstrapToken = defaultBootstrapToken
	}

	if opts.EncryptedFSKey == "" {
		opts.EncryptedFSKey = defaultEncryptedFSKey
	}

	opts.StatefulSets = normalizeValidateWorkloadNames(opts.StatefulSets, DefaultValidateStatefulSets)
	opts.Deployments = normalizeValidateWorkloadNames(opts.Deployments, DefaultValidateDeployments)

	if opts.Wait == 0 {
		opts.Wait = DefaultValidateWait
	}

	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}

	return opts
}

func normalizeValidateWorkloadNames(names, defaults []string) []string {
	if len(names) == 0 {
		return append([]string(nil), defaults...)
	}

	normalized := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name != "" {
			normalized = append(normalized, name)
		}
	}

	return normalized
}

func validateValidateOptions(opts ValidateOptions) error {
	if opts.Kubectl == "" {
		return fmt.Errorf("kubectl command is required")
	}

	if opts.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	if opts.ManifestPath == "" {
		return fmt.Errorf("manifest path is required")
	}

	if opts.OutputPath == "" {
		return fmt.Errorf("output path is required")
	}

	if opts.ImageTag == "" {
		return fmt.Errorf("image tag is required")
	}

	if opts.Wait <= 0 {
		return fmt.Errorf("wait must be > 0")
	}

	if len(opts.StatefulSets) == 0 && len(opts.Deployments) == 0 {
		return fmt.Errorf("at least one workload is required")
	}

	return nil
}

func waitForValidateWorkload(ctx context.Context, opts ValidateOptions, kind, name string) error {
	resource := kind + "/" + name
	fmt.Fprintf(opts.Stdout, "Waiting for %s rollout\n", resource)

	waitCtx, cancel := context.WithTimeout(ctx, opts.Wait)
	defer cancel()

	if err := runValidateKubectlPhase(waitCtx, opts, validateKubectlNamespaced, nil, "rollout", "status", resource, "--timeout", opts.Wait.String()); err != nil {
		return fmt.Errorf("wait for Kubernetes %s rollout: %w", resource, err)
	}

	return nil
}

func smokeOptionsForValidate(opts ValidateOptions) SmokeOptions {
	smokeOpts := opts.SmokeOptions
	if strings.TrimSpace(smokeOpts.Kubectl) == "" {
		smokeOpts.Kubectl = opts.Kubectl
	}

	if strings.TrimSpace(smokeOpts.Context) == "" {
		smokeOpts.Context = opts.Context
	}

	if strings.TrimSpace(smokeOpts.Namespace) == "" {
		smokeOpts.Namespace = opts.Namespace
	}

	if smokeOpts.Wait == 0 {
		smokeOpts.Wait = opts.Wait
	}

	if smokeOpts.Stdout == nil {
		smokeOpts.Stdout = opts.Stdout
	}

	return smokeOpts
}

func runValidateKubectlPhase(ctx context.Context, opts ValidateOptions, scope validateKubectlScope, stdin io.Reader, args ...string) error {
	phaseCtx, cancel := context.WithTimeout(ctx, opts.Wait)
	defer cancel()

	stdout, stderr, err := runValidateKubectlCommand(phaseCtx, opts, scope, stdin, args...)
	writeValidateCommandOutput(opts.Stdout, stdout, stderr)
	if err != nil {
		return kubectlError(err, stdout, stderr)
	}

	return nil
}

func writeValidateCommandOutput(out io.Writer, stdout, stderr string) {
	for _, text := range []string{stdout, stderr} {
		text = strings.TrimSpace(text)
		if text != "" {
			fmt.Fprintln(out, text)
		}
	}
}

func kubectlError(err error, stdout, stderr string) error {
	text := strings.TrimSpace(stdout + stderr)
	if text == "" {
		return err
	}

	return fmt.Errorf("%w: %s", err, text)
}

func defaultRunValidateKubectlCommand(ctx context.Context, opts ValidateOptions, scope validateKubectlScope, stdin io.Reader, args ...string) (string, string, error) {
	kubectlArgs := validateKubectlArgs(opts.Context, opts.Namespace, scope, args...)
	cmd := exec.CommandContext(ctx, opts.Kubectl, kubectlArgs...) // #nosec G204 -- kubectl path and args are validation-harness controlled.
	cmd.Stdin = stdin

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	return stdout.String(), stderr.String(), err
}

func validateKubectlArgs(kubernetesContext, namespace string, scope validateKubectlScope, args ...string) []string {
	kubectlArgs := []string{}
	if strings.TrimSpace(kubernetesContext) != "" {
		kubectlArgs = append(kubectlArgs, "--context", strings.TrimSpace(kubernetesContext))
	}

	if scope == validateKubectlNamespaced {
		kubectlArgs = append(kubectlArgs, "-n", namespace)
	}

	kubectlArgs = append(kubectlArgs, args...)
	return kubectlArgs
}
