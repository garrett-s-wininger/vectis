//go:build unix

package interfaces

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
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

func TestDirectExecutorCancellationSignalsProcessGroup(t *testing.T) {
	previousGrace := commandProcessGroupCancelGrace
	commandProcessGroupCancelGrace = 50 * time.Millisecond
	t.Cleanup(func() { commandProcessGroupCancelGrace = previousGrace })

	ctx, cancel := context.WithCancel(context.Background())
	process, err := NewDirectExecutor().Start(
		ctx,
		"sh",
		[]string{"-c", "sleep 30 & echo $!; wait"},
		t.TempDir(),
		[]string{"PATH=" + os.Getenv("PATH")},
	)
	if err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}

	sleepLine, err := bufio.NewReader(process.Stdout()).ReadString('\n')
	if err != nil {
		cancel()
		_ = process.Wait()
		t.Fatalf("read background pid: %v", err)
	}

	sleepPID, err := strconv.Atoi(strings.TrimSpace(sleepLine))
	if err != nil {
		cancel()
		_ = process.Wait()
		t.Fatalf("parse background pid %q: %v", sleepLine, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(sleepPID, syscall.SIGKILL) })

	cancel()
	if err := process.Wait(); err == nil {
		t.Fatal("Wait error = nil, want cancellation error")
	}

	if err := waitForNoProcess(sleepPID, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestDirectExecutorCancellationSendsTermBeforeKill(t *testing.T) {
	previousGrace := commandProcessGroupCancelGrace
	commandProcessGroupCancelGrace = 500 * time.Millisecond
	t.Cleanup(func() { commandProcessGroupCancelGrace = previousGrace })

	workspace := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	process, err := NewDirectExecutor().Start(
		ctx,
		"sh",
		[]string{"-c", `trap 'echo term > term.marker; exit 0' TERM; echo ready; while :; do :; done`},
		workspace,
		[]string{"PATH=" + os.Getenv("PATH")},
	)

	if err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}

	if ready, err := bufio.NewReader(process.Stdout()).ReadString('\n'); err != nil || strings.TrimSpace(ready) != "ready" {
		cancel()
		_ = process.Wait()
		t.Fatalf("read ready line = %q, err=%v", ready, err)
	}

	cancel()
	_ = process.Wait()

	if err := waitForFile(filepath.Join(workspace, "term.marker"), time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestDirectExecutorCancellationKillsTermIgnoringProcessGroup(t *testing.T) {
	previousGrace := commandProcessGroupCancelGrace
	commandProcessGroupCancelGrace = 50 * time.Millisecond
	t.Cleanup(func() { commandProcessGroupCancelGrace = previousGrace })

	ctx, cancel := context.WithCancel(context.Background())
	process, err := NewDirectExecutor().Start(
		ctx,
		"sh",
		[]string{"-c", "trap '' TERM; sleep 30 & echo $!; wait"},
		t.TempDir(),
		[]string{"PATH=" + os.Getenv("PATH")},
	)

	if err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}

	sleepLine, err := bufio.NewReader(process.Stdout()).ReadString('\n')
	if err != nil {
		cancel()
		_ = process.Wait()
		t.Fatalf("read background pid: %v", err)
	}

	sleepPID, err := strconv.Atoi(strings.TrimSpace(sleepLine))
	if err != nil {
		cancel()
		_ = process.Wait()
		t.Fatalf("parse background pid %q: %v", sleepLine, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(sleepPID, syscall.SIGKILL) })

	cancel()
	if err := process.Wait(); err == nil {
		t.Fatal("Wait error = nil, want cancellation error")
	}

	if err := waitForNoProcess(sleepPID, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestTerminateActiveProcessesSignalsProcessGroup(t *testing.T) {
	previousGrace := commandProcessGroupCancelGrace
	commandProcessGroupCancelGrace = 50 * time.Millisecond
	t.Cleanup(func() { commandProcessGroupCancelGrace = previousGrace })

	process, err := NewDirectExecutor().Start(
		context.Background(),
		"sh",
		[]string{"-c", "trap '' TERM; sleep 30"},
		t.TempDir(),
		[]string{"PATH=" + os.Getenv("PATH")},
	)

	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(func() {
		if osProcess, ok := process.(*osProcess); ok && osProcess.cmd.Process != nil {
			_ = osProcess.cmd.Process.Kill()
		}
		_ = process.Wait()
	})

	TerminateActiveProcesses()
	if err := process.Wait(); err == nil {
		t.Fatal("Wait error = nil, want active process termination error")
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

func waitForNoProcess(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if err == syscall.ESRCH {
			return nil
		}

		time.Sleep(10 * time.Millisecond)
	}

	return &processStillRunningError{pid: pid}
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}

		time.Sleep(10 * time.Millisecond)
	}

	return os.ErrNotExist
}

type processStillRunningError struct {
	pid int
}

func (e *processStillRunningError) Error() string {
	return "background process " + strconv.Itoa(e.pid) + " still exists after cancellation"
}
