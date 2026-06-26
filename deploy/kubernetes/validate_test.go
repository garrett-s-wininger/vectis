package kubernetes

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type validateKubectlCall struct {
	scope validateKubectlScope
	args  []string
}

func TestRunValidateAppliesWaitsAndRunsSmoke(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "vectis.yaml")
	var calls []validateKubectlCall
	oldKubectl := runValidateKubectlCommand
	runValidateKubectlCommand = func(ctx context.Context, opts ValidateOptions, scope validateKubectlScope, stdin io.Reader, args ...string) (string, string, error) {
		calls = append(calls, validateKubectlCall{scope: scope, args: append([]string(nil), args...)})
		return "ok\n", "", nil
	}
	t.Cleanup(func() { runValidateKubectlCommand = oldKubectl })

	var smokeOpts SmokeOptions
	oldSmoke := runValidateSmoke
	runValidateSmoke = func(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
		smokeOpts = opts
		return SmokeResult{
			Status:    "ok",
			Namespace: opts.Namespace,
			JobPath:   opts.JobPath,
			RunID:     "run-1",
			RunStatus: "succeeded",
		}, nil
	}
	t.Cleanup(func() { runValidateSmoke = oldSmoke })

	seedSecret := false
	result, err := RunValidate(context.Background(), ValidateOptions{
		Kubectl:          "kubectl-test",
		Context:          "kind-ci",
		Namespace:        "ci-vectis",
		OutputPath:       outputPath,
		ImageRegistry:    "localhost",
		ImageTag:         "dev",
		PostgresPassword: "postgres-test",
		BootstrapToken:   "bootstrap-test",
		EncryptedFSKey:   "encryptedfs-test",
		Wait:             time.Minute,
		SmokeOptions: SmokeOptions{
			JobPath:      "examples/e2e-canonical.json",
			APILocalPort: 19090,
			APIToken:     "token-test",
			SeedSecret:   &seedSecret,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if result.Status != "validated" || result.ManifestPath != outputPath || result.Render.Namespace != "ci-vectis" {
		t.Fatalf("unexpected result: %+v", result)
	}

	if result.Smoke == nil || result.Smoke.RunID != "run-1" {
		t.Fatalf("expected smoke result, got %+v", result.Smoke)
	}

	manifest, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}

	if text := string(manifest); !strings.Contains(text, `name: "ci-vectis"`) || !strings.Contains(text, "image: localhost/vectis-api:dev") {
		t.Fatalf("rendered manifest does not contain validation settings:\n%s", text)
	}

	if len(calls) != 1+len(DefaultValidateStatefulSets)+len(DefaultValidateDeployments)+2 {
		t.Fatalf("unexpected kubectl call count: %d calls=%+v", len(calls), calls)
	}

	if calls[0].scope != validateKubectlCluster || !reflect.DeepEqual(calls[0].args, []string{"apply", "-f", outputPath}) {
		t.Fatalf("unexpected apply call: %+v", calls[0])
	}

	workloadCalls := calls[1 : 1+len(DefaultValidateStatefulSets)+len(DefaultValidateDeployments)]
	var workloads []ValidateWorkload
	for _, call := range workloadCalls {
		if call.scope != validateKubectlNamespaced {
			t.Fatalf("workload call should be namespaced: %+v", call)
		}

		if len(call.args) != 5 || call.args[0] != "rollout" || call.args[1] != "status" || call.args[3] != "--timeout" || call.args[4] != time.Minute.String() {
			t.Fatalf("unexpected rollout args: %+v", call.args)
		}

		kind, name, ok := strings.Cut(call.args[2], "/")
		if !ok {
			t.Fatalf("unexpected workload resource: %q", call.args[2])
		}
		workloads = append(workloads, ValidateWorkload{Kind: kind, Name: name})
	}

	if !reflect.DeepEqual(result.Workloads, workloads) {
		t.Fatalf("workloads mismatch\ngot:  %+v\nwant: %+v", result.Workloads, workloads)
	}

	getPods := calls[len(calls)-2]
	if getPods.scope != validateKubectlNamespaced || !reflect.DeepEqual(getPods.args, []string{"get", "pods", "-o", "wide"}) {
		t.Fatalf("unexpected get pods call: %+v", getPods)
	}

	waitPods := calls[len(calls)-1]
	wantWaitPods := []string{
		"wait",
		"--for=condition=Ready",
		"pod",
		"-l", validatePodSelector,
		"--field-selector=status.phase!=Succeeded",
		"--timeout", time.Minute.String(),
	}

	if waitPods.scope != validateKubectlNamespaced || !reflect.DeepEqual(waitPods.args, wantWaitPods) {
		t.Fatalf("unexpected wait pods call: %+v", waitPods)
	}

	if smokeOpts.Kubectl != "kubectl-test" || smokeOpts.Context != "kind-ci" || smokeOpts.Namespace != "ci-vectis" || smokeOpts.Wait != time.Minute {
		t.Fatalf("smoke did not inherit validation options: %+v", smokeOpts)
	}

	if smokeOpts.APILocalPort != 19090 || smokeOpts.APIToken != "token-test" || smokeOpts.SeedSecret == nil || *smokeOpts.SeedSecret {
		t.Fatalf("smoke-specific options were not preserved: %+v", smokeOpts)
	}
}

func TestRunValidateCanSkipSmoke(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "vectis.yaml")
	oldKubectl := runValidateKubectlCommand
	runValidateKubectlCommand = func(ctx context.Context, opts ValidateOptions, scope validateKubectlScope, stdin io.Reader, args ...string) (string, string, error) {
		return "", "", nil
	}
	t.Cleanup(func() { runValidateKubectlCommand = oldKubectl })

	oldSmoke := runValidateSmoke
	runValidateSmoke = func(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
		t.Fatal("smoke should not run when SkipSmoke is true")
		return SmokeResult{}, nil
	}
	t.Cleanup(func() { runValidateSmoke = oldSmoke })

	result, err := RunValidate(context.Background(), ValidateOptions{
		Namespace:    "skip-smoke",
		OutputPath:   outputPath,
		StatefulSets: []string{"vectis-postgres"},
		Deployments:  []string{"vectis-api"},
		SkipSmoke:    true,
	})

	if err != nil {
		t.Fatal(err)
	}

	if result.Smoke != nil {
		t.Fatalf("expected no smoke result: %+v", result.Smoke)
	}

	if len(result.Workloads) != 2 {
		t.Fatalf("expected two workload checks, got %+v", result.Workloads)
	}
}

func TestRunValidateIncludesKubectlOutputInErrors(t *testing.T) {
	oldKubectl := runValidateKubectlCommand
	runValidateKubectlCommand = func(ctx context.Context, opts ValidateOptions, scope validateKubectlScope, stdin io.Reader, args ...string) (string, string, error) {
		return "", "deployment unavailable", errors.New("exit status 1")
	}
	t.Cleanup(func() { runValidateKubectlCommand = oldKubectl })

	_, err := RunValidate(context.Background(), ValidateOptions{
		Namespace:    "broken",
		OutputPath:   filepath.Join(t.TempDir(), "vectis.yaml"),
		SkipSmoke:    true,
		StatefulSets: []string{"vectis-postgres"},
		Deployments:  []string{"vectis-api"},
	})

	if err == nil {
		t.Fatal("expected error")
	}

	if !strings.Contains(err.Error(), "deployment unavailable") {
		t.Fatalf("error did not include kubectl stderr: %v", err)
	}
}

func TestValidateKubectlArgs(t *testing.T) {
	got := validateKubectlArgs("kind-vectis", "vectis", validateKubectlNamespaced, "get", "pods")
	want := []string{"--context", "kind-vectis", "-n", "vectis", "get", "pods"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args=%v, want %v", got, want)
	}

	got = validateKubectlArgs("", "vectis", validateKubectlCluster, "apply", "-f", "manifest.yaml")
	want = []string{"apply", "-f", "manifest.yaml"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args=%v, want %v", got, want)
	}
}
