package action

import (
	"reflect"
	"strings"
	"testing"

	"vectis/internal/workloadidentity"
)

func TestSanitizedProcessEnvAllowsOnlyStableExecutionVariables(t *testing.T) {
	got := SanitizedProcessEnv("/work/run-1", []string{
		"PATH=/custom/bin",
		"VECTIS_ACTION_LAUNCHER_TOKEN=secret",
		"VECTIS_DATABASE_DSN=postgres://secret",
		"VECTIS_API_AUTH_BOOTSTRAP_TOKEN=secret",
		"SPIFFE_ENDPOINT_SOCKET=unix:///tmp/spire.sock",
		"AWS_SECRET_ACCESS_KEY=secret",
		"AWS_ACCESS_KEY_ID=secret",
		"AWS_SESSION_TOKEN=secret",
		"DYLD_INSERT_LIBRARIES=/tmp/hook.dylib",
		"GITHUB_TOKEN=secret",
		"GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp.json",
		"KUBECONFIG=/tmp/kubeconfig",
		"LD_PRELOAD=/tmp/hook.so",
		"NPM_TOKEN=secret",
		"SSH_AUTH_SOCK=/tmp/agent.sock",
	})

	assertEnvValue(t, got, "PATH", "/custom/bin")
	assertEnvValue(t, got, "HOME", "/work/run-1")
	assertEnvValue(t, got, "TMPDIR", "/work/run-1/.tmp")
	assertEnvValue(t, got, "CI", "true")
	assertEnvValue(t, got, "VECTIS", "true")

	for _, key := range []string{
		"VECTIS_ACTION_LAUNCHER_TOKEN",
		"VECTIS_DATABASE_DSN",
		"VECTIS_API_AUTH_BOOTSTRAP_TOKEN",
		"SPIFFE_ENDPOINT_SOCKET",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_ACCESS_KEY_ID",
		"AWS_SESSION_TOKEN",
		"DYLD_INSERT_LIBRARIES",
		"GITHUB_TOKEN",
		"GOOGLE_APPLICATION_CREDENTIALS",
		"KUBECONFIG",
		"LD_PRELOAD",
		"NPM_TOKEN",
		"SSH_AUTH_SOCK",
	} {
		if _, ok := envLookup(got, key); ok {
			t.Fatalf("SanitizedProcessEnv leaked %s in %v", key, got)
		}
	}
}

func TestCommandEnvReturnsCopy(t *testing.T) {
	state := &ExecutionState{
		ProcessEnv: []string{"PATH=/bin"},
	}

	got := state.CommandEnv()
	got[0] = "PATH=/mutated"

	if !reflect.DeepEqual(state.ProcessEnv, []string{"PATH=/bin"}) {
		t.Fatalf("CommandEnv mutated state env: %v", state.ProcessEnv)
	}
}

func TestCommandEnvDoesNotExposeWorkloadIdentity(t *testing.T) {
	state := &ExecutionState{
		Workspace: "/work/run-1",
		Workload: &workloadidentity.Identity{
			SPIFFEID: "spiffe://prod.example/cell/local/job/job-1/run/run-1/execution/execution-1",
			X509SVID: &workloadidentity.X509SVID{
				SPIFFEID: "spiffe://prod.example/cell/local/job/job-1/run/run-1/execution/execution-1",
			},
		},
	}

	got := state.CommandEnv()
	for _, key := range []string{
		"SPIFFE_ENDPOINT_SOCKET",
		"VECTIS_WORKLOAD_SPIFFE_ID",
		"VECTIS_WORKLOAD_X509_SVID",
	} {
		if _, ok := envLookup(got, key); ok {
			t.Fatalf("CommandEnv leaked %s in %v", key, got)
		}
	}
}

func TestAppendEnvOverridesExistingKey(t *testing.T) {
	got := AppendEnv([]string{"PATH=/bin", "GIT_TERMINAL_PROMPT=1"}, "GIT_TERMINAL_PROMPT", "0")
	want := []string{"PATH=/bin", "GIT_TERMINAL_PROMPT=0"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AppendEnv = %v, want %v", got, want)
	}
}

func assertEnvValue(t *testing.T, env []string, key, want string) {
	t.Helper()
	got, ok := envLookup(env, key)
	if !ok {
		t.Fatalf("missing env %s in %v", key, env)
	}

	if got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}

func envLookup(env []string, key string) (string, bool) {
	for _, entry := range env {
		k, v, ok := strings.Cut(entry, "=")
		if ok && k == key {
			return v, true
		}
	}

	return "", false
}
