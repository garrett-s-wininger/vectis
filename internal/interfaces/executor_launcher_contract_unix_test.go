//go:build unix

package interfaces

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	directExecutorIgnoredSignalDispositionHelperEnv = "VECTIS_DIRECT_EXECUTOR_IGNORED_SIGNAL_DISPOSITION_HELPER"
	directExecutorSignalDispositionProbeEnv         = "VECTIS_DIRECT_EXECUTOR_SIGNAL_DISPOSITION_PROBE"
)

func TestDirectExecutorLauncherBoundaryContract(t *testing.T) {
	t.Run("argv env and cwd", func(t *testing.T) {
		t.Setenv("VECTIS_LAUNCHER_PARENT_ONLY", "secret")

		workspace := t.TempDir()
		resolvedWorkspace, err := filepath.EvalSymlinks(workspace)
		if err != nil {
			t.Fatalf("resolve workspace: %v", err)
		}

		script := `printf 'argv0=%s\narg1=%s\narg2=%s\nvisible=%s\nparent=%s\nauth=%s\npwd=%s\n' "$0" "$1" "$2" "$VECTIS_LAUNCHER_VISIBLE" "$VECTIS_LAUNCHER_PARENT_ONLY" "$VECTIS_ACTION_LAUNCHER_TOKEN" "$(pwd -P)"`
		stdout, stderr, err := runContractCommand(t, "sh", []string{"-c", script, "custom-argv0", "arg with spaces", "arg:two"}, workspace, []string{
			"PATH=" + os.Getenv("PATH"),
			"VECTIS_LAUNCHER_VISIBLE=ok",
		})

		if err != nil {
			t.Fatalf("Wait: %v\nstderr:\n%s", err, stderr)
		}

		wantLines := []string{
			"argv0=custom-argv0",
			"arg1=arg with spaces",
			"arg2=arg:two",
			"visible=ok",
			"parent=",
			"auth=",
			"pwd=" + resolvedWorkspace,
		}

		if got := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n"); !sameStringSlice(got, wantLines) {
			t.Fatalf("stdout lines = %#v, want %#v; stderr=%q", got, wantLines, stderr)
		}
	})

	t.Run("stdin is null and fds are closed", func(t *testing.T) {
		workspace := t.TempDir()
		leakPath := filepath.Join(workspace, "leak")
		file, err := os.OpenFile(leakPath, os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatalf("open leak file: %v", err)
		}
		defer file.Close()

		fd := int(file.Fd())
		script := fmt.Sprintf(`if IFS= read -r line; then echo stdin-open; exit 11; fi
if sh -c 'printf leak >&3' 2>/dev/null; then echo fd3-open; exit 12; fi
if sh -c 'printf leak >&%d' 2>/dev/null; then echo parent-fd-open; exit 13; fi`, fd)

		stdout, stderr, err := runContractCommand(t, "sh", []string{"-c", script}, workspace, []string{"PATH=" + os.Getenv("PATH")})
		if err != nil {
			t.Fatalf("Wait: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}

		if strings.TrimSpace(stdout) != "" {
			t.Fatalf("stdout = %q, want no fd/stdin failure markers; stderr=%q", stdout, stderr)
		}

		info, err := file.Stat()
		if err != nil {
			t.Fatalf("stat leak file: %v", err)
		}

		if info.Size() != 0 {
			t.Fatalf("leak file size = %d, want 0", info.Size())
		}
	})

	t.Run("process group is isolated", func(t *testing.T) {
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
	})

	t.Run("private umask", func(t *testing.T) {
		workspace := t.TempDir()
		_, stderr, err := runContractCommand(t, "sh", []string{"-c", ": > created"}, workspace, []string{"PATH=" + os.Getenv("PATH")})
		if err != nil {
			t.Fatalf("Wait: %v\nstderr:\n%s", err, stderr)
		}

		info, err := os.Stat(filepath.Join(workspace, "created"))
		if err != nil {
			t.Fatalf("stat created file: %v", err)
		}

		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("created file mode = %v, want 0600", got)
		}
	})

	t.Run("inherited signal mask is cleared", func(t *testing.T) {
		restore := blockSignalForTest(t, syscall.SIGUSR1)
		defer restore()

		_, stderr, err := runContractCommand(t, "sh", []string{"-c", "trap 'exit 0' USR1; kill -USR1 $$; sleep 1; exit 7"}, t.TempDir(), []string{"PATH=" + os.Getenv("PATH")})
		if err != nil {
			t.Fatalf("Wait: %v\nstderr:\n%s", err, stderr)
		}
	})

	t.Run("ignored signal disposition is reset", func(t *testing.T) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestDirectExecutorIgnoredSignalDispositionHelper$")
		cmd.Env = append(os.Environ(), directExecutorIgnoredSignalDispositionHelperEnv+"=1")

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("ignored signal helper failed: %v\n%s", err, output)
		}
	})

	t.Run("cancellation reaches final exec process group", func(t *testing.T) {
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
	})
}

func TestDirectExecutorIgnoredSignalDispositionHelper(t *testing.T) {
	if os.Getenv(directExecutorIgnoredSignalDispositionHelperEnv) != "1" {
		return
	}

	signal.Ignore(syscall.SIGTERM)
	stdout, stderr, err := runContractCommand(
		t,
		os.Args[0],
		[]string{"-test.run=^TestDirectExecutorFinalSignalDispositionProbe$"},
		t.TempDir(),
		append(os.Environ(), directExecutorSignalDispositionProbeEnv+"=1"),
	)

	if err != nil {
		t.Fatalf("probe failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
}

func TestDirectExecutorFinalSignalDispositionProbe(t *testing.T) {
	if os.Getenv(directExecutorSignalDispositionProbeEnv) != "1" {
		return
	}

	if signal.Ignored(syscall.SIGTERM) {
		t.Fatal("SIGTERM is ignored in final exec process")
	}
}

func runContractCommand(t *testing.T, path string, args []string, workDir string, env []string) (string, string, error) {
	t.Helper()

	process, err := NewDirectExecutor().Start(context.Background(), path, args, workDir, env)
	if err != nil {
		return "", "", err
	}

	stdoutDone := make(chan string, 1)
	stderrDone := make(chan string, 1)
	go func() {
		out, _ := io.ReadAll(process.Stdout())
		stdoutDone <- string(out)
	}()

	go func() {
		out, _ := io.ReadAll(process.Stderr())
		stderrDone <- string(out)
	}()

	waitErr := process.Wait()
	stdout := <-stdoutDone
	stderr := <-stderrDone
	return stdout, stderr, waitErr
}

func sameStringSlice(a, b []string) bool {
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
