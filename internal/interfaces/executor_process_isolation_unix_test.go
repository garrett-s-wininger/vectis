//go:build unix

package interfaces

import (
	"context"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

const directExecutorSignalGroupHelperEnv = "VECTIS_DIRECT_EXECUTOR_SIGNAL_GROUP_HELPER"

func TestDirectExecutorStartsChildInSeparateProcessGroup(t *testing.T) {
	process, err := NewDirectExecutor().Start(
		context.Background(),
		"sleep",
		[]string{"30"},
		t.TempDir(),
		nil,
	)

	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	osProcess, ok := process.(*osProcess)
	if !ok {
		t.Fatalf("process type = %T, want *osProcess", process)
	}

	t.Cleanup(func() {
		if osProcess.cmd.Process != nil {
			_ = osProcess.cmd.Process.Kill()
		}
		_ = process.Wait()
	})

	parentPGID, err := syscall.Getpgid(os.Getpid())
	if err != nil {
		t.Fatalf("parent pgid: %v", err)
	}

	childPGID, err := syscall.Getpgid(osProcess.cmd.Process.Pid)
	if err != nil {
		t.Fatalf("child pgid: %v", err)
	}

	if childPGID == parentPGID {
		t.Fatalf("child process group = %d, want different from worker process group %d", childPGID, parentPGID)
	}

	if childPGID != osProcess.cmd.Process.Pid {
		t.Fatalf("child process group = %d, want child pid %d", childPGID, osProcess.cmd.Process.Pid)
	}
}

func TestDirectExecutorIsolatesChildSignalGroup(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestDirectExecutorSignalGroupHelper$")
	cmd.Env = append(os.Environ(), directExecutorSignalGroupHelperEnv+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("signal group helper failed: %v\n%s", err, output)
	}
}

func TestDirectExecutorSignalGroupHelper(t *testing.T) {
	if os.Getenv(directExecutorSignalGroupHelperEnv) != "1" {
		return
	}

	process, err := NewDirectExecutor().Start(
		context.Background(),
		"sh",
		[]string{"-c", "kill -TERM 0"},
		t.TempDir(),
		[]string{"PATH=" + os.Getenv("PATH")},
	)

	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := process.Wait(); err == nil {
		t.Fatal("command exited successfully; want termination by its own process-group signal")
	}
}
