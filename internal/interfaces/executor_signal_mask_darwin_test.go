//go:build darwin

package interfaces

import (
	"runtime"
	"syscall"
	"testing"
	"unsafe"
)

const (
	darwinSIGBlock   = 1
	darwinSIGUnblock = 2
)

func blockSignalForTest(t *testing.T, sig syscall.Signal) func() {
	t.Helper()

	runtime.LockOSThread()
	mask := uint32(1 << uint(int(sig)-1))
	if err := darwinSigprocmask(darwinSIGBlock, &mask); err != nil {
		runtime.UnlockOSThread()
		t.Fatalf("block signal %v: %v", sig, err)
	}

	return func() {
		_ = darwinSigprocmask(darwinSIGUnblock, &mask)
		runtime.UnlockOSThread()
	}
}

func darwinSigprocmask(how int, mask *uint32) error {
	_, _, errno := syscall.RawSyscall(syscall.SYS_SIGPROCMASK, uintptr(how), uintptr(unsafe.Pointer(mask)), 0)
	if errno != 0 {
		return errno
	}

	return nil
}
