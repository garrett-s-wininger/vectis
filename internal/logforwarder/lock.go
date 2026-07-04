package logforwarder

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"vectis/internal/platform"
)

// AcquireLock attempts to acquire an exclusive advisory lock on a file at the
// given path.  If another process already holds the lock, it returns an error.
// On success it returns an open file descriptor that must be kept open for the
// lifetime of the process; closing the descriptor releases the lock.
func AcquireLock(lockPath string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, fmt.Errorf("create lockfile directory: %w", err)
	}

	fd, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lockfile: %w", err)
	}

	if err := platform.TryLockFileExclusive(fd); err != nil {
		_ = fd.Close()
		if platform.IsFileLockUnavailable(err) {
			return nil, fmt.Errorf("forwarder already running")
		}

		return nil, fmt.Errorf("acquire lock: %w", err)
	}

	// Truncate and write our PID so operators can see who owns the lock.
	if err := fd.Truncate(0); err != nil {
		_ = fd.Close()
		return nil, fmt.Errorf("truncate lockfile: %w", err)
	}

	if _, err := fmt.Fprintf(fd, "%d\n", os.Getpid()); err != nil {
		_ = fd.Close()
		return nil, fmt.Errorf("write pid to lockfile: %w", err)
	}

	return fd, nil
}

// ReleaseLock closes the lockfile descriptor, which releases the advisory lock.
func ReleaseLock(fd *os.File, path string) error {
	var err error
	if fd != nil {
		err = fd.Close()
	}

	if removeErr := os.Remove(path); removeErr != nil {
		return errors.Join(err, removeErr)
	}

	return err
}
