//go:build darwin

package actionlauncher

import (
	"syscall"
	"unsafe"
)

const darwinSIGSetmask = 3

func resetSignalMask() error {
	var empty uint32
	_, _, errno := syscall.RawSyscall(syscall.SYS_SIGPROCMASK, uintptr(darwinSIGSetmask), uintptr(unsafe.Pointer(&empty)), 0) // #nosec G103 -- resetSignalMask must call sigprocmask with a small stack-allocated sigset.
	if errno != 0 {
		return errno
	}

	return nil
}
