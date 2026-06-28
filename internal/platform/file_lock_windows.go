//go:build windows

package platform

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

const allFileLockBytes = ^uint32(0)

// TryLockFileExclusive takes an exclusive, non-blocking advisory lock on f.
func TryLockFileExclusive(f *os.File) error {
	var overlapped windows.Overlapped
	return windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		allFileLockBytes,
		allFileLockBytes,
		&overlapped,
	)
}

// UnlockFile releases a lock previously acquired with TryLockFileExclusive.
func UnlockFile(f *os.File) error {
	var overlapped windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, allFileLockBytes, allFileLockBytes, &overlapped)
}

// IsFileLockUnavailable reports whether err means another process owns the lock.
func IsFileLockUnavailable(err error) bool {
	return errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_LOCK_FAILED)
}
