package workercore

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/action"
	"vectis/internal/job"
	"vectis/internal/source"
)

type ExecutorCore struct {
	executor          *job.Executor
	checkoutCacheRoot string
}

type ExecutorCoreOption func(*ExecutorCore)

func WithExecutorCheckoutCacheRoot(root string) ExecutorCoreOption {
	return func(c *ExecutorCore) {
		c.checkoutCacheRoot = strings.TrimSpace(root)
	}
}

func NewExecutorCore(executor *job.Executor, options ...ExecutorCoreOption) *ExecutorCore {
	if executor == nil {
		executor = job.NewExecutor()
	}

	core := &ExecutorCore{executor: executor}
	for _, option := range options {
		if option != nil {
			option(core)
		}
	}

	return core
}

func (c *ExecutorCore) ExecuteTask(ctx context.Context, req ExecuteTaskRequest) error {
	if c == nil || c.executor == nil {
		return fmt.Errorf("worker execution core is not configured")
	}

	if req.Job == nil {
		return fmt.Errorf("worker execution core requires a job")
	}

	if strings.TrimSpace(req.TaskKey) == "" {
		return fmt.Errorf("worker execution core requires a task key")
	}

	if req.Session == nil {
		return fmt.Errorf("worker execution core requires a task session")
	}

	logClient := req.Session.LogClient()
	if logClient == nil {
		return fmt.Errorf("worker execution core requires a log client")
	}

	logger := req.Session.Logger()
	if logger == nil {
		return fmt.Errorf("worker execution core requires a logger")
	}

	checkoutCache, err := c.checkoutCacheForSession(req.Session)
	if err != nil {
		return err
	}

	return c.executor.ExecuteTaskWithOptions(ctx, req.Job, req.TaskKey, logClient, logger, job.ExecuteOptions{
		WorkloadIdentity:  req.Session.WorkloadIdentity(),
		ArtifactPublisher: req.Session.ArtifactPublisher(),
		ActionLocks:       req.Session.ActionLocks(),
		ActionResolver:    req.Session.ActionResolver(),
		SecretFiles:       req.Session.SecretFiles(),
		CheckoutCache:     checkoutCache,
	})
}

func (c *ExecutorCore) checkoutCacheForSession(session TaskSession) (action.CheckoutCache, error) {
	if c == nil || session == nil {
		return nil, nil
	}

	checkoutCache, err := c.checkoutCacheForRemoteURLs(session.CheckoutCacheRemoteURLs())
	if checkoutCache == nil {
		return nil, err
	}

	return checkoutCache, err
}

func (c *ExecutorCore) WarmCheckoutCache(ctx context.Context, req WarmCheckoutCacheRequest) (WarmCheckoutCacheResult, error) {
	result := WarmCheckoutCacheResult{}
	if c == nil || strings.TrimSpace(c.checkoutCacheRoot) == "" {
		return result, nil
	}

	for _, remoteURL := range uniqueCheckoutCacheRemoteURLs(req.RemoteURLs) {
		cache, err := c.checkoutCacheForRemoteURLs([]string{remoteURL})
		if err != nil {
			result.Failures = append(result.Failures, CheckoutCacheWarmFailure{
				RemoteURL: remoteURL,
				Message:   err.Error(),
			})

			continue
		}

		handled, normalizedRemoteURL, err := cache.WarmRemote(ctx, remoteURL, nil)
		if err != nil {
			if ctx.Err() != nil {
				return result, ctx.Err()
			}

			if normalizedRemoteURL == "" {
				normalizedRemoteURL = remoteURL
			}

			result.Failures = append(result.Failures, CheckoutCacheWarmFailure{
				RemoteURL: normalizedRemoteURL,
				Message:   err.Error(),
			})

			continue
		}

		if handled {
			result.Warmed++
		}
	}

	return result, nil
}

func (c *ExecutorCore) checkoutCacheForRemoteURLs(remoteURLs []string) (*source.WorkerCheckoutCache, error) {
	if c == nil || strings.TrimSpace(c.checkoutCacheRoot) == "" {
		return nil, nil
	}

	remoteURLs = uniqueCheckoutCacheRemoteURLs(remoteURLs)
	if len(remoteURLs) == 0 {
		return nil, nil
	}

	checkoutCache, err := source.NewWorkerCheckoutCache(c.checkoutCacheRoot, remoteURLs)
	if err != nil {
		return nil, fmt.Errorf("initialize task checkout cache: %w", err)
	}

	return checkoutCache, nil
}

func uniqueCheckoutCacheRemoteURLs(remoteURLs []string) []string {
	if len(remoteURLs) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(remoteURLs))
	out := make([]string, 0, len(remoteURLs))
	for _, remoteURL := range remoteURLs {
		remoteURL = strings.TrimSpace(remoteURL)
		if remoteURL == "" {
			continue
		}

		if _, ok := seen[remoteURL]; ok {
			continue
		}

		seen[remoteURL] = struct{}{}
		out = append(out, remoteURL)
	}

	return out
}

func (c *ExecutorCore) CancelTask(context.Context, CancelTaskRequest) error {
	return nil
}

var _ Core = (*ExecutorCore)(nil)
var _ CancellableCore = (*ExecutorCore)(nil)
var _ CheckoutCacheWarmer = (*ExecutorCore)(nil)
