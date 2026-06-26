package job

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"vectis/internal/interfaces"
)

const asyncWorkspaceCleanupQueueSize = 4096

type workspaceCleanupRequest struct {
	path   string
	logger interfaces.Logger
}

var (
	asyncWorkspaceCleanupOnce  sync.Once
	asyncWorkspaceCleanupQueue chan workspaceCleanupRequest
	asyncWorkspaceCleanupWG    sync.WaitGroup
)

func enqueueAsyncWorkspaceCleanup(path string, logger interfaces.Logger) bool {
	queue := ensureAsyncWorkspaceCleanupQueue()

	asyncWorkspaceCleanupWG.Add(1)
	select {
	case queue <- workspaceCleanupRequest{path: path, logger: logger}:
		return true
	default:
		asyncWorkspaceCleanupWG.Done()
		return false
	}
}

func ensureAsyncWorkspaceCleanupQueue() chan workspaceCleanupRequest {
	asyncWorkspaceCleanupOnce.Do(func() {
		queue := make(chan workspaceCleanupRequest, asyncWorkspaceCleanupQueueSize)
		asyncWorkspaceCleanupQueue = queue

		workerCount := runtime.GOMAXPROCS(0) / 2
		if workerCount < 1 {
			workerCount = 1
		}

		if workerCount > 4 {
			workerCount = 4
		}

		for range workerCount {
			go func() {
				for req := range queue {
					cleanupWorkspacePath(req.path, req.logger)
					asyncWorkspaceCleanupWG.Done()
				}
			}()
		}
	})

	return asyncWorkspaceCleanupQueue
}

func cleanupWorkspacePath(path string, logger interfaces.Logger) {
	if err := os.RemoveAll(path); err != nil && logger != nil {
		logger.Error("Failed to remove workspace %s: %v", path, err)
	}
}

func WaitForAsyncWorkspaceCleanupForTest(timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		asyncWorkspaceCleanupWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timed out waiting for async workspace cleanup after %s", timeout)
	}
}
