package interfaces

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDirectExecutorNilEnvDoesNotInherit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use sh")
	}

	t.Setenv("VECTIS_DATABASE_DSN", "postgres://secret")

	process, err := NewDirectExecutor().Start(
		context.Background(),
		"sh",
		[]string{"-c", `if [ -n "$VECTIS_DATABASE_DSN" ]; then exit 7; fi`},
		t.TempDir(),
		nil,
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := process.Wait(); err != nil {
		t.Fatalf("process inherited parent env: %v", err)
	}
}

func TestDirectExecutorRequiresWorkDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use sh")
	}

	_, err := NewDirectExecutor().Start(context.Background(), "sh", []string{"-c", "true"}, "", nil)
	if err == nil || !strings.Contains(err.Error(), "work directory is required") {
		t.Fatalf("Start error = %v, want work directory error", err)
	}
}

func TestOSExecutorRequiresWorkDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use sh")
	}

	_, err := NewOSExecutor().Start(context.Background(), "true", "", nil)
	if err == nil || !strings.Contains(err.Error(), "work directory is required") {
		t.Fatalf("Start error = %v, want work directory error", err)
	}
}

func TestDirectExecutorResolvesCommandFromChildPathEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use Unix executables")
	}

	binDir := t.TempDir()
	commandPath := filepath.Join(binDir, "vectis-child-path-command")
	if err := os.WriteFile(commandPath, []byte("#!/bin/sh\nprintf child-path-ok\n"), 0o700); err != nil {
		t.Fatalf("write command: %v", err)
	}

	process, err := NewDirectExecutor().Start(
		context.Background(),
		"vectis-child-path-command",
		nil,
		t.TempDir(),
		[]string{"PATH=" + binDir},
	)

	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	stdout, err := io.ReadAll(process.Stdout())
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}

	if _, err := io.ReadAll(process.Stderr()); err != nil {
		t.Fatalf("read stderr: %v", err)
	}

	if err := process.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if string(stdout) != "child-path-ok" {
		t.Fatalf("stdout = %q, want child-path-ok", stdout)
	}
}

func TestDirectExecutorDoesNotResolveCommandFromRelativePathEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use Unix executables")
	}

	workspace := t.TempDir()
	commandPath := filepath.Join(workspace, "vectis-relative-path-command")
	if err := os.WriteFile(commandPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write command: %v", err)
	}

	_, err := NewDirectExecutor().Start(
		context.Background(),
		"vectis-relative-path-command",
		nil,
		workspace,
		[]string{"PATH=."},
	)

	if err == nil || !strings.Contains(err.Error(), "executable file not found") {
		t.Fatalf("Start error = %v, want command resolution failure", err)
	}
}

func TestDirectExecutorRequiresDirectoryWorkDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use sh")
	}

	workDir := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(workDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write workdir file: %v", err)
	}

	_, err := NewDirectExecutor().Start(context.Background(), "sh", []string{"-c", "true"}, workDir, nil)
	if err == nil || !strings.Contains(err.Error(), "work directory is not a directory") {
		t.Fatalf("Start error = %v, want non-directory work directory error", err)
	}
}

func TestDirectExecutorRejectsSymlinkWorkDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use sh")
	}

	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}

	workDir := filepath.Join(root, "workdir-link")
	if err := os.Symlink(target, workDir); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	_, err := NewDirectExecutor().Start(context.Background(), "sh", []string{"-c", "true"}, workDir, nil)
	if err == nil || !strings.Contains(err.Error(), "work directory must not be a symlink") {
		t.Fatalf("Start error = %v, want symlink work directory error", err)
	}
}

func TestDirectExecutorRunsInWorkDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use sh")
	}

	workspace := t.TempDir()
	markerName := "vectis-cwd-marker"
	process, err := NewDirectExecutor().Start(
		context.Background(),
		"sh",
		[]string{"-c", "printf ok > " + markerName},
		workspace,
		nil,
	)

	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if _, err := io.ReadAll(process.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}

	if _, err := io.ReadAll(process.Stderr()); err != nil {
		t.Fatalf("read stderr: %v", err)
	}

	if err := process.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(workspace, markerName))
	if err != nil {
		t.Fatalf("read workspace marker: %v", err)
	}

	if string(got) != "ok" {
		t.Fatalf("workspace marker = %q, want ok", got)
	}

	if cwdMarker := filepath.Join(".", markerName); fileExists(cwdMarker) {
		t.Cleanup(func() { _ = os.Remove(cwdMarker) })
		t.Fatalf("marker was created in worker cwd")
	}
}

func TestDirectExecutorDefaultStdinIsNullDevice(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use sh")
	}

	process, err := NewDirectExecutor().Start(
		context.Background(),
		"sh",
		[]string{"-c", `if IFS= read -r line; then exit 7; fi`},
		t.TempDir(),
		nil,
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := process.Wait(); err != nil {
		t.Fatalf("process stdin was not null device EOF: %v", err)
	}
}

func TestDirectExecutorDoesNotInheritParentFileDescriptor(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use sh")
	}

	dir := t.TempDir()
	leakPath := filepath.Join(dir, "leak")
	file, err := os.OpenFile(leakPath, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open leak file: %v", err)
	}
	defer file.Close()

	fd := int(file.Fd())
	process, err := NewDirectExecutor().Start(
		context.Background(),
		"sh",
		[]string{"-c", fmt.Sprintf(`if sh -c 'printf leak >&%d' 2>/dev/null; then exit 7; fi`, fd)},
		dir,
		nil,
	)

	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := process.Wait(); err != nil {
		t.Fatalf("process inherited parent fd %d: %v", fd, err)
	}

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat leak file: %v", err)
	}

	if info.Size() != 0 {
		t.Fatalf("leak file size = %d, want 0", info.Size())
	}
}

func TestDirectExecutorDoesNotPassExtraFileDescriptorsByDefault(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process tests use sh")
	}

	process, err := NewDirectExecutor().Start(
		context.Background(),
		"sh",
		[]string{"-c", `: >&3`},
		t.TempDir(),
		nil,
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := process.Wait(); err == nil || !strings.Contains(err.Error(), "exit status") {
		t.Fatalf("process unexpectedly wrote to fd 3: %v", err)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
