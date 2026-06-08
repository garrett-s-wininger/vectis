package platform

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestNewLimaExecutorRequiresInstance(t *testing.T) {
	if _, err := NewLimaExecutor(LimaExecutorOptions{}); err == nil {
		t.Fatal("expected missing instance error")
	}
}

func TestLimaExecutorCommandArgs(t *testing.T) {
	executor, err := NewLimaExecutor(LimaExecutorOptions{
		Instance:    "vectis-worker",
		LimactlPath: "/opt/homebrew/bin/limactl",
		Start:       true,
		PreserveEnv: true,
	})
	if err != nil {
		t.Fatalf("new lima executor: %v", err)
	}

	got := executor.commandArgs("sh", []string{"-c", "echo hello"}, "/tmp/vectis-work")
	want := []string{
		"--tty=false",
		"shell",
		"--start",
		"--preserve-env",
		"--workdir",
		"/tmp/vectis-work",
		"vectis-worker",
		"--",
		"sh",
		"-c",
		"echo hello",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command args = %v, want %v", got, want)
	}
}

func TestLimaExecutorCommandArgsGuestWorkspaceRoot(t *testing.T) {
	executor, err := NewLimaExecutor(LimaExecutorOptions{
		Instance:           "vectis-worker",
		GuestWorkspaceRoot: "/tmp/vectis-workspaces",
	})

	if err != nil {
		t.Fatalf("new lima executor: %v", err)
	}

	got := executor.commandArgs("sh", []string{"-c", "echo hello"}, "/Users/me/vectis/vectis-run-123")
	want := []string{
		"--tty=false",
		"shell",
		"vectis-worker",
		"--",
		"sh",
		"-c",
		limaWorkspaceCommand,
		"vectis-lima-workspace",
		"/tmp/vectis-workspaces/vectis-run-123",
		"sh",
		"-c",
		"echo hello",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command args = %v, want %v", got, want)
	}
}

func TestLimaExecutorCommandArgsSanitizedEnv(t *testing.T) {
	executor, err := NewLimaExecutor(LimaExecutorOptions{
		Instance: "vectis-worker",
	})
	if err != nil {
		t.Fatalf("new lima executor: %v", err)
	}

	got := executor.commandArgsWithEnv("sh", []string{"-c", "echo hello"}, "/tmp/vectis-work", []string{
		"CI=true",
		"PATH=/usr/bin:/bin",
	})
	want := []string{
		"--tty=false",
		"shell",
		"--workdir",
		"/tmp/vectis-work",
		"vectis-worker",
		"--",
		"env",
		"-i",
		"CI=true",
		"PATH=/usr/bin:/bin",
		"sh",
		"-c",
		"echo hello",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command args = %v, want %v", got, want)
	}
}

func TestLimaExecutorCommandArgsMinimal(t *testing.T) {
	executor, err := NewLimaExecutor(LimaExecutorOptions{Instance: "vectis-worker"})
	if err != nil {
		t.Fatalf("new lima executor: %v", err)
	}

	got := executor.commandArgs("git", []string{"--version"}, "")
	want := []string{"--tty=false", "shell", "vectis-worker", "--", "git", "--version"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command args = %v, want %v", got, want)
	}
}

func TestLimaExecutorStartRequiresCommandPath(t *testing.T) {
	executor, err := NewLimaExecutor(LimaExecutorOptions{Instance: "vectis-worker"})
	if err != nil {
		t.Fatalf("new lima executor: %v", err)
	}

	_, err = executor.Start(context.Background(), "", nil, "", nil)
	if err == nil || !strings.Contains(err.Error(), "command path is required") {
		t.Fatalf("expected command path error, got %v", err)
	}
}
