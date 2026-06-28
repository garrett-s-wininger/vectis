//go:build linux

package interfaces

import (
	"runtime"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func blockSignalForTest(t *testing.T, sig syscall.Signal) func() {
	t.Helper()

	runtime.LockOSThread()
	var set unix.Sigset_t
	sigIndex := int(sig) - 1
	set.Val[sigIndex/64] |= 1 << uint(sigIndex%64)
	if err := unix.PthreadSigmask(unix.SIG_BLOCK, &set, nil); err != nil {
		runtime.UnlockOSThread()
		t.Fatalf("block signal %v: %v", sig, err)
	}

	return func() {
		_ = unix.PthreadSigmask(unix.SIG_UNBLOCK, &set, nil)
		runtime.UnlockOSThread()
	}
}
