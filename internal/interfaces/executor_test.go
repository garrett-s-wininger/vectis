package interfaces

import (
	"context"
	"runtime"
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
