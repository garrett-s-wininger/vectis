//go:build !windows

package platform

import (
	"errors"
	"os"
	"syscall"
)

// TryLockFileExclusive takes an exclusive, non-blocking advisory lock on f.
func TryLockFileExclusive(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// UnlockFile releases a lock previously acquired with TryLockFileExclusive.
func UnlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// IsFileLockUnavailable reports whether err means another process owns the lock.
func IsFileLockUnavailable(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
}
