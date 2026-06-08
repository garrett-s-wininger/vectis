package action

import (
	"reflect"
	"strings"
	"testing"
)

func TestSanitizedProcessEnvAllowsOnlyStableExecutionVariables(t *testing.T) {
	got := SanitizedProcessEnv("/work/run-1", []string{
		"PATH=/custom/bin",
		"VECTIS_DATABASE_DSN=postgres://secret",
		"VECTIS_API_AUTH_BOOTSTRAP_TOKEN=secret",
		"SPIFFE_ENDPOINT_SOCKET=unix:///tmp/spire.sock",
		"AWS_SECRET_ACCESS_KEY=secret",
	})

	assertEnvValue(t, got, "PATH", "/custom/bin")
	assertEnvValue(t, got, "HOME", "/work/run-1")
	assertEnvValue(t, got, "TMPDIR", "/work/run-1/.tmp")
	assertEnvValue(t, got, "CI", "true")
	assertEnvValue(t, got, "VECTIS", "true")

	for _, key := range []string{
		"VECTIS_DATABASE_DSN",
		"VECTIS_API_AUTH_BOOTSTRAP_TOKEN",
		"SPIFFE_ENDPOINT_SOCKET",
		"AWS_SECRET_ACCESS_KEY",
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
