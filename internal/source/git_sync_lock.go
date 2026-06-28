package source

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const managedGitWriterLockRetry = 25 * time.Millisecond

var managedGitProcessLocks = struct {
	mu    sync.Mutex
	locks map[string]chan struct{}
}{}

type managedGitWriterLock struct {
	file        *os.File
	path        string
	processLock chan struct{}
}

func acquireManagedGitWriterLock(ctx context.Context, checkoutPath string) (*managedGitWriterLock, error) {
	checkoutPath = strings.TrimSpace(checkoutPath)
	if checkoutPath == "" {
		return nil, fmt.Errorf("%w: checkout path is required", ErrInvalidReference)
	}

	lockPath := managedGitWriterLockPath(checkoutPath)
	processLock := managedGitProcessLock(lockPath)
	select {
	case processLock <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: wait for managed checkout lock %s: %v", ErrBusy, lockPath, ctx.Err())
	}

	releaseProcessLock := true
	defer func() {
		if releaseProcessLock {
			<-processLock
		}
	}()

	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, fmt.Errorf("create managed checkout lock parent %s: %w", filepath.Dir(lockPath), err)
	}

	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open managed checkout lock %s: %w", lockPath, err)
	}

	if err := lockManagedGitWriterFile(ctx, f, lockPath); err != nil {
		_ = f.Close()
		return nil, err
	}

	releaseProcessLock = false
	return &managedGitWriterLock{
		file:        f,
		path:        lockPath,
		processLock: processLock,
	}, nil
}

func managedGitProcessLock(lockPath string) chan struct{} {
	managedGitProcessLocks.mu.Lock()
	defer managedGitProcessLocks.mu.Unlock()

	if managedGitProcessLocks.locks == nil {
		managedGitProcessLocks.locks = make(map[string]chan struct{})
	}

	lock, ok := managedGitProcessLocks.locks[lockPath]
	if !ok {
		lock = make(chan struct{}, 1)
		managedGitProcessLocks.locks[lockPath] = lock
	}

	return lock
}

func managedGitWriterLockPath(checkoutPath string) string {
	return filepath.Clean(checkoutPath) + ".vectis.lock"
}

func lockManagedGitWriterFile(ctx context.Context, f *os.File, lockPath string) error {
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}

		if !managedGitWriterLockBlocked(err) {
			return fmt.Errorf("lock managed checkout %s: %w", lockPath, err)
		}

		timer := time.NewTimer(managedGitWriterLockRetry)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			return fmt.Errorf("%w: wait for managed checkout lock %s: %v", ErrBusy, lockPath, ctx.Err())
		case <-timer.C:
		}
	}
}

func managedGitWriterLockBlocked(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
}

func (l *managedGitWriterLock) Close() error {
	if l == nil {
		return nil
	}

	var result error
	if l.file != nil {
		if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
			result = fmt.Errorf("unlock managed checkout %s: %w", l.path, err)
		}

		if err := l.file.Close(); err != nil && result == nil {
			result = fmt.Errorf("close managed checkout lock %s: %w", l.path, err)
		}

		l.file = nil
	}

	if l.processLock != nil {
		<-l.processLock
		l.processLock = nil
	}

	return result
}
