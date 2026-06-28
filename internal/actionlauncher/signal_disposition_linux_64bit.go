//go:build linux && (amd64 || arm64)

package actionlauncher

import (
	"syscall"
	"unsafe"
)

type linuxSigaction struct {
	handler  uintptr
	flags    uint64
	restorer uintptr
	mask     uint64
}

func resetSignalDisposition(sig syscall.Signal) error {
	var action linuxSigaction
	_, _, errno := syscall.RawSyscall6(
		syscall.SYS_RT_SIGACTION,
		uintptr(sig),
		uintptr(unsafe.Pointer(&action)),
		0,
		unsafe.Sizeof(action.mask),
		0,
		0,
	)

	if errno != 0 {
		return errno
	}

	return nil
}
