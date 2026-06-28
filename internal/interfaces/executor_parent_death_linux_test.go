//go:build linux

package interfaces

import (
	"context"
	"syscall"
	"testing"
)

func TestDirectExecutorConfiguresParentDeathSignal(t *testing.T) {
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

	if got := osProcess.cmd.SysProcAttr.Pdeathsig; got != syscall.SIGKILL {
		t.Fatalf("Pdeathsig = %v, want SIGKILL", got)
	}
}
