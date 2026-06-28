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

		if len(command.Args) != 4 || command.Args[0] != modeArg || command.Args[1] != "/bin/echo" || command.Args[2] != "hello" || command.Args[3] != "world" {
			t.Fatalf("launcher args = %#v, want mode, target, and copied target args", command.Args)
		}

		if command.WorkDir != "/work" {
			t.Fatalf("work dir = %q, want /work", command.WorkDir)
		}

		if len(command.Env) != 1 || command.Env[0] != "VISIBLE=1" {
			t.Fatalf("env = %#v, want copied env", command.Env)
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
