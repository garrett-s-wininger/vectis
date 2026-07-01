package workercore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"vectis/internal/action"
	"vectis/internal/job"
	"vectis/internal/source"
)

type ExecutorCore struct {
	executor                       *job.Executor
	checkoutCacheRoot              string
	checkoutCacheGenerationsToKeep int
	checkoutCacheLeaseTTL          time.Duration
}

type ExecutorCoreOption func(*ExecutorCore)

func WithExecutorCheckoutCacheRoot(root string) ExecutorCoreOption {
	return func(c *ExecutorCore) {
		c.checkoutCacheRoot = strings.TrimSpace(root)
	}
}

func WithExecutorCheckoutCacheGenerationsToKeep(generationsToKeep int) ExecutorCoreOption {
	return func(c *ExecutorCore) {
		c.checkoutCacheGenerationsToKeep = generationsToKeep
	}
}

func WithExecutorCheckoutCacheLeaseTTL(ttl time.Duration) ExecutorCoreOption {
	return func(c *ExecutorCore) {
		c.checkoutCacheLeaseTTL = ttl
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

	checkoutCache, err := c.checkoutCacheForRemotes(session.CheckoutCacheRemotes())
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
	defer c.recordCheckoutCacheRootStats(ctx)

	remotes := req.Remotes
	if len(remotes) == 0 {
		remotes = checkoutCacheRemotesFromURLs(req.RemoteURLs)
	}

	for _, remote := range uniqueCheckoutCacheRemotes(remotes) {
		cache, err := c.checkoutCacheForRemotes([]CheckoutCacheRemote{remote})
		if err != nil {
			result.Failures = append(result.Failures, CheckoutCacheWarmFailure{
				RemoteURL: remote.RemoteURL,
				Message:   err.Error(),
			})

			continue
		}

		handled, normalizedRemoteURL, err := cache.WarmRemote(ctx, remote.RemoteURL, nil)
		if err != nil {
			if ctx.Err() != nil {
				return result, ctx.Err()
			}

			if normalizedRemoteURL == "" {
				normalizedRemoteURL = remote.RemoteURL
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

func (c *ExecutorCore) recordCheckoutCacheRootStats(ctx context.Context) {
	if c == nil || strings.TrimSpace(c.checkoutCacheRoot) == "" {
		return
	}

	cache, err := source.NewWorkerCheckoutCache(c.checkoutCacheRoot, nil, workerCheckoutCacheOptions(c.checkoutCacheGenerationsToKeep, c.checkoutCacheLeaseTTL)...)
	if err != nil {
		return
	}

	stats, err := cache.Stats(ctx)
	if err != nil {
		return
	}

	recordCheckoutCacheStats(ctx, stats)
}

func (c *ExecutorCore) checkoutCacheForRemoteURLs(remoteURLs []string) (*source.WorkerCheckoutCache, error) {
	return c.checkoutCacheForRemotes(checkoutCacheRemotesFromURLs(remoteURLs))
}

func (c *ExecutorCore) checkoutCacheForRemotes(remotes []CheckoutCacheRemote) (*source.WorkerCheckoutCache, error) {
	if c == nil || strings.TrimSpace(c.checkoutCacheRoot) == "" {
		return nil, nil
	}

	remotes = uniqueCheckoutCacheRemotes(remotes)
	if len(remotes) == 0 {
		return nil, nil
	}

	checkoutCache, err := source.NewWorkerCheckoutCacheWithRemotes(c.checkoutCacheRoot, sourceWorkerCheckoutCacheRemotes(remotes), workerCheckoutCacheOptions(c.checkoutCacheGenerationsToKeep, c.checkoutCacheLeaseTTL)...)
	if err != nil {
		return nil, fmt.Errorf("initialize task checkout cache: %w", err)
	}

	return checkoutCache, nil
}

func sourceWorkerCheckoutCacheRemotes(remotes []CheckoutCacheRemote) []source.WorkerCheckoutCacheRemote {
	if len(remotes) == 0 {
		return nil
	}

	out := make([]source.WorkerCheckoutCacheRemote, 0, len(remotes))
	for _, remote := range remotes {
		out = append(out, source.WorkerCheckoutCacheRemote{
			RemoteURL:          remote.RemoteURL,
			FallbackRemoteURLs: cloneStringSlice(remote.FallbackRemoteURLs),
			Credentials:        remote.Credentials,
		})
	}

	return out
}

func workerCheckoutCacheOptions(generationsToKeep int, leaseTTL time.Duration) []source.WorkerCheckoutCacheOption {
	options := []source.WorkerCheckoutCacheOption{
		source.WithWorkerCheckoutCacheCloneRecorder(recordCheckoutCacheClone),
	}
	if generationsToKeep > 0 {
		options = append(options, source.WithWorkerCheckoutCacheGenerationsToKeep(generationsToKeep))
	}
	if leaseTTL > 0 {
		options = append(options, source.WithWorkerCheckoutCacheLeaseTTL(leaseTTL))
	}

	return options
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

func uniqueCheckoutCacheRemotes(remotes []CheckoutCacheRemote) []CheckoutCacheRemote {
	if len(remotes) == 0 {
		return nil
	}

	seen := make(map[string]int, len(remotes))
	out := make([]CheckoutCacheRemote, 0, len(remotes))
	for _, remote := range remotes {
		remoteURL := strings.TrimSpace(remote.RemoteURL)
		if remoteURL == "" {
			continue
		}

		if existing, ok := seen[remoteURL]; ok {
			out[existing].FallbackRemoteURLs = uniqueCheckoutCacheRemoteURLs(append(out[existing].FallbackRemoteURLs, remote.FallbackRemoteURLs...))
			if out[existing].Credentials.IsZero() && !remote.Credentials.IsZero() {
				out[existing].Credentials = remote.Credentials
			}

			continue
		}

		seen[remoteURL] = len(out)
		out = append(out, CheckoutCacheRemote{
			RemoteURL:          remoteURL,
			FallbackRemoteURLs: uniqueCheckoutCacheRemoteURLs(remote.FallbackRemoteURLs),
		})
	}

	return out
}

func (c *ExecutorCore) CancelTask(context.Context, CancelTaskRequest) error {
	return nil
}

var _ Core = (*ExecutorCore)(nil)
var _ CancellableCore = (*ExecutorCore)(nil)
var _ CheckoutCacheWarmer = (*ExecutorCore)(nil)
