package interfaces

import (
	"context"
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
