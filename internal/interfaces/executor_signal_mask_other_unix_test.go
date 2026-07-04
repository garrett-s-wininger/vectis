//go:build unix && !linux && !darwin

package interfaces

import (
	"syscall"
	"testing"
)

func blockSignalForTest(t *testing.T, sig syscall.Signal) func() {
	t.Helper()
	t.Skip("signal mask test helper is not implemented on this Unix")
	return func() {}
}
