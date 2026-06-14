package platform

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestNewVirtualMachineRequiresLimaInstance(t *testing.T) {
	if _, err := NewVirtualMachine(VirtualMachineConfig{Provider: VirtualMachineProviderLima}); err == nil {
		t.Fatal("expected missing instance error")
	}
}

func TestVirtualMachineCommandExecutorUnknownProvider(t *testing.T) {
	if _, err := NewVirtualMachineCommandExecutor(VirtualMachineConfig{Provider: "not-real"}); err == nil {
		t.Fatal("expected unknown provider error")
	}
}

func TestVirtualMachineManagerUnknownProvider(t *testing.T) {
	if _, err := NewVirtualMachineManager(VirtualMachineManagerConfig{Provider: "not-real"}); err == nil {
		t.Fatal("expected unknown provider error")
	}
}

func TestVirtualMachineProviderAutoResolvesToDefault(t *testing.T) {
	if got := ResolveVirtualMachineProvider(VirtualMachineProviderAuto); got != VirtualMachineProviderLima {
		t.Fatalf("auto provider = %q, want %q", got, VirtualMachineProviderLima)
	}

	manager, err := NewVirtualMachineManager(VirtualMachineManagerConfig{Provider: VirtualMachineProviderAuto})
	if err != nil {
		t.Fatalf("new VM manager with auto provider: %v", err)
	}

	if got := manager.Provider(); got != VirtualMachineProviderLima {
		t.Fatalf("manager provider = %q, want %q", got, VirtualMachineProviderLima)
	}
}

func TestLimaVirtualMachineManagerProvider(t *testing.T) {
	manager, err := NewVirtualMachineManager(VirtualMachineManagerConfig{Provider: VirtualMachineProviderLima})
	if err != nil {
		t.Fatalf("new VM manager: %v", err)
	}

	if got := manager.Provider(); got != VirtualMachineProviderLima {
		t.Fatalf("provider = %q, want %q", got, VirtualMachineProviderLima)
	}
}

func TestParseLimaStatus(t *testing.T) {
	got, err := parseLimaStatus("vectis-deploy-smoke\tStopped\n", "vectis-deploy-smoke")
	if err != nil {
		t.Fatal(err)
	}

	if got != "Stopped" {
		t.Fatalf("status = %q, want Stopped", got)
	}
}

func TestParseLimaStatusRejectsMissingInstance(t *testing.T) {
	_, err := parseLimaStatus("other\tRunning\n", "vectis-deploy-smoke")
	if err == nil || !strings.Contains(err.Error(), "not present") {
		t.Fatalf("expected missing instance error, got %v", err)
	}
}

func TestLimaVirtualMachineCommandArgs(t *testing.T) {
	vm, err := newLimaVirtualMachine(limaVirtualMachineConfig{
		instance:    "vectis-worker",
		limactlPath: "/opt/homebrew/bin/limactl",
		start:       true,
		preserveEnv: true,
	})
	if err != nil {
		t.Fatalf("new lima VM: %v", err)
	}

	got := vm.commandArgs("sh", []string{"-c", "echo hello"}, "/tmp/vectis-work")
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

func TestLimaVirtualMachineCommandArgsGuestWorkspaceRoot(t *testing.T) {
	vm, err := newLimaVirtualMachine(limaVirtualMachineConfig{
		instance:           "vectis-worker",
		guestWorkspaceRoot: "/tmp/vectis-workspaces",
	})

	if err != nil {
		t.Fatalf("new lima VM: %v", err)
	}

	got := vm.commandArgs("sh", []string{"-c", "echo hello"}, "/Users/me/vectis/vectis-run-123")
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

func TestLimaVirtualMachineCommandArgsSanitizedEnv(t *testing.T) {
	vm, err := newLimaVirtualMachine(limaVirtualMachineConfig{
		instance: "vectis-worker",
	})
	if err != nil {
		t.Fatalf("new lima VM: %v", err)
	}

	got := vm.commandArgsWithEnv("sh", []string{"-c", "echo hello"}, "/tmp/vectis-work", []string{
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

func TestLimaVirtualMachineCommandArgsMinimal(t *testing.T) {
	vm, err := newLimaVirtualMachine(limaVirtualMachineConfig{instance: "vectis-worker"})
	if err != nil {
		t.Fatalf("new lima VM: %v", err)
	}

	got := vm.commandArgs("git", []string{"--version"}, "")
	want := []string{"--tty=false", "shell", "vectis-worker", "--", "git", "--version"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command args = %v, want %v", got, want)
	}
}

func TestVirtualMachineCommandExecutorStartRequiresCommandPath(t *testing.T) {
	executor, err := NewVirtualMachineCommandExecutor(VirtualMachineConfig{
		Provider: VirtualMachineProviderLima,
		Instance: "vectis-worker",
	})
	if err != nil {
		t.Fatalf("new VM executor: %v", err)
	}

	_, err = executor.Start(context.Background(), "", nil, "", nil)
	if err == nil || !strings.Contains(err.Error(), "command path is required") {
		t.Fatalf("expected command path error, got %v", err)
	}
}
