package actionlauncher

import (
	"bytes"
	"strings"
	"testing"
)

func TestCommandBuildsLaunchSpecInvocation(t *testing.T) {
	spec := LaunchSpec{
		Path:    "/bin/echo",
		Args:    []string{"hello", "world"},
		WorkDir: "/work",
		Env:     []string{"VISIBLE=1"},
	}

	command, err := Command(spec)
	if err != nil {
		t.Fatalf("Command: %v", err)
	}

	spec.Args[0] = "mutated"
	spec.Env[0] = "MUTATED=1"
	if enabled {
		if command.Path == spec.Path {
			t.Fatalf("launcher path = target path %q, want re-exec launcher", command.Path)
		}

		if len(command.Args) != 5 || command.Args[0] != modeArg || command.Args[1] == "" || command.Args[2] != "/bin/echo" || command.Args[3] != "hello" || command.Args[4] != "world" {
			t.Fatalf("launcher args = %#v, want mode, auth token, target, and copied target args", command.Args)
		}

		if command.WorkDir != "/work" {
			t.Fatalf("work dir = %q, want /work", command.WorkDir)
		}

		if !envContains(command.Env, "VISIBLE=1") {
			t.Fatalf("env = %#v, want copied visible env", command.Env)
		}

		if !envContains(command.Env, launcherAuthEnv+"="+command.Args[1]) {
			t.Fatalf("env = %#v, want launcher auth token from argv", command.Env)
		}

		return
	}

	if command.Path != "/bin/echo" {
		t.Fatalf("path = %q, want direct target path", command.Path)
	}

	if len(command.Args) != 2 || command.Args[0] != "hello" || command.Args[1] != "world" {
		t.Fatalf("args = %#v, want copied target args", command.Args)
	}

	if command.WorkDir != "/work" {
		t.Fatalf("work dir = %q, want /work", command.WorkDir)
	}

	if len(command.Env) != 1 || command.Env[0] != "VISIBLE=1" {
		t.Fatalf("env = %#v, want copied env", command.Env)
	}
}

func TestCommandStripsCallerLauncherAuthEnv(t *testing.T) {
	command, err := Command(LaunchSpec{
		Path:    "/bin/echo",
		WorkDir: "/work",
		Env: []string{
			"VISIBLE=1",
			launcherAuthEnv + "=caller",
			launcherAuthEnv + "_SUFFIX=still-visible",
		},
	})

	if err != nil {
		t.Fatalf("Command: %v", err)
	}

	if envContains(command.Env, launcherAuthEnv+"=caller") {
		t.Fatalf("env = %#v, want caller-supplied launcher auth stripped", command.Env)
	}

	if !envContains(command.Env, "VISIBLE=1") || !envContains(command.Env, launcherAuthEnv+"_SUFFIX=still-visible") {
		t.Fatalf("env = %#v, want unrelated env preserved", command.Env)
	}

	if enabled && (len(command.Args) < 2 || !envContains(command.Env, launcherAuthEnv+"="+command.Args[1])) {
		t.Fatalf("env = %#v args = %#v, want generated launcher auth only", command.Env, command.Args)
	}
}

func TestCommandRequiresPath(t *testing.T) {
	_, err := Command(LaunchSpec{Path: " \t ", WorkDir: "/work"})
	if err == nil || !strings.Contains(err.Error(), "command path is required") {
		t.Fatalf("Command error = %v, want command path error", err)
	}
}

func TestCommandRequiresWorkDir(t *testing.T) {
	_, err := Command(LaunchSpec{Path: "/bin/echo", WorkDir: " \t "})
	if err == nil || !strings.Contains(err.Error(), "work directory is required") {
		t.Fatalf("Command error = %v, want work directory error", err)
	}
}

func TestRunReportsMissingPath(t *testing.T) {
	var stderr bytes.Buffer
	code := Run(nil, &stderr)
	if code != 127 {
		t.Fatalf("exit code = %d, want 127", code)
	}

	if got := stderr.String(); !strings.Contains(got, "vectis action launcher: command path is required") {
		t.Fatalf("stderr = %q, want stable missing path error", got)
	}
}

func TestRunReportsResolveFailure(t *testing.T) {
	var stderr bytes.Buffer
	code := Run([]string{"definitely-not-a-vectis-command"}, &stderr)
	if code != 127 {
		t.Fatalf("exit code = %d, want 127", code)
	}

	if got := stderr.String(); !strings.Contains(got, `vectis action launcher: resolve "definitely-not-a-vectis-command"`) {
		t.Fatalf("stderr = %q, want stable resolve error", got)
	}
}

func TestRunLauncherModeRequiresMatchingAuth(t *testing.T) {
	t.Setenv(launcherAuthEnv, "expected")

	var stderr bytes.Buffer
	code := runLauncherMode([]string{"actual", "/bin/echo"}, &stderr)
	if code != 126 {
		t.Fatalf("exit code = %d, want 126", code)
	}

	if got := stderr.String(); !strings.Contains(got, "unauthorized launcher mode: token mismatch") {
		t.Fatalf("stderr = %q, want stable unauthorized error", got)
	}
}

func TestRunLauncherModeAcceptsMatchingAuth(t *testing.T) {
	t.Setenv(launcherAuthEnv, "expected")

	var stderr bytes.Buffer
	code := runLauncherMode([]string{"expected"}, &stderr)
	if code != 127 {
		t.Fatalf("exit code = %d, want 127 after auth passes and target path is missing", code)
	}

	if got := stderr.String(); !strings.Contains(got, "vectis action launcher: command path is required") || strings.Contains(got, "unauthorized") {
		t.Fatalf("stderr = %q, want missing path error after successful auth", got)
	}
}

func TestStripLauncherAuthEnv(t *testing.T) {
	got := stripLauncherAuthEnv([]string{
		"A=1",
		launcherAuthEnv + "=secret",
		launcherAuthEnv + "_SUFFIX=preserved",
		"B=2",
	})

	want := []string{
		"A=1",
		launcherAuthEnv + "_SUFFIX=preserved",
		"B=2",
	}

	if !sameStrings(got, want) {
		t.Fatalf("stripLauncherAuthEnv = %#v, want %#v", got, want)
	}
}

func envContains(env []string, entry string) bool {
	for _, candidate := range env {
		if candidate == entry {
			return true
		}
	}

	return false
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
