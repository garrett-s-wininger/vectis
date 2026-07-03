package source

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"vectis/internal/action"
	"vectis/internal/gitcmd"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/source/refspec"
)

const (
	workerCheckoutGenerationPrefix         = "generation-"
	workerCheckoutReceivingSuffix          = ".receiving"
	workerCheckoutCacheRemoteName          = "vectis-cache"
	defaultWorkerCheckoutGenerationsToKeep = 2
	defaultWorkerCheckoutLeaseTTL          = time.Hour
)

type WorkerCheckoutCacheOption func(*WorkerCheckoutCache)
type WorkerCheckoutCacheCloneRecorder func(context.Context, string, string)
type WorkerCheckoutCacheDemandHydrationRecorder func(context.Context, string)
type WorkerCheckoutCacheGenerationEvictionRecorder func(context.Context, string)
type WorkerCheckoutCacheSelfHealRecorder func(context.Context, string, string)

type WorkerCheckoutCacheRemote struct {
	RemoteURL          string
	FallbackRemoteURLs []string
	WarmRefspecs       []string
	Credentials        GitCredentials
}

type WorkerCheckoutCache struct {
	root                       string
	persistentRemoteURL        map[string]WorkerCheckoutCacheRemote
	generationsToKeep          int
	leaseTTL                   time.Duration
	maxBytes                   int64
	cloneRecorder              WorkerCheckoutCacheCloneRecorder
	demandHydrationRecorder    WorkerCheckoutCacheDemandHydrationRecorder
	generationEvictionRecorder WorkerCheckoutCacheGenerationEvictionRecorder
	selfHealRecorder           WorkerCheckoutCacheSelfHealRecorder
	hardlinkProbe              workerCheckoutCacheHardlinkProbe
}

type WorkerCheckoutCacheStats struct {
	Repositories int64
	Generations  int64
	PackFiles    int64
	PackBytes    int64
	ActiveLeases int64
}

type workerCheckoutGenerationLease struct {
	path          string
	stopHeartbeat func()
	heartbeatDone <-chan struct{}
}

type WorkerCheckoutCacheScope struct {
	cache  *WorkerCheckoutCache
	mu     sync.Mutex
	leases []*workerCheckoutGenerationLease
	closed bool
}

func WithWorkerCheckoutCacheGenerationsToKeep(generationsToKeep int) WorkerCheckoutCacheOption {
	return func(c *WorkerCheckoutCache) {
		c.generationsToKeep = generationsToKeep
	}
}

func WithWorkerCheckoutCacheLeaseTTL(ttl time.Duration) WorkerCheckoutCacheOption {
	return func(c *WorkerCheckoutCache) {
		c.leaseTTL = ttl
	}
}

func WithWorkerCheckoutCacheMaxBytes(maxBytes int64) WorkerCheckoutCacheOption {
	return func(c *WorkerCheckoutCache) {
		c.maxBytes = maxBytes
	}
}

func WithWorkerCheckoutCacheCloneRecorder(recorder WorkerCheckoutCacheCloneRecorder) WorkerCheckoutCacheOption {
	return func(c *WorkerCheckoutCache) {
		c.cloneRecorder = recorder
	}
}

func WithWorkerCheckoutCacheDemandHydrationRecorder(recorder WorkerCheckoutCacheDemandHydrationRecorder) WorkerCheckoutCacheOption {
	return func(c *WorkerCheckoutCache) {
		c.demandHydrationRecorder = recorder
	}
}

func WithWorkerCheckoutCacheGenerationEvictionRecorder(recorder WorkerCheckoutCacheGenerationEvictionRecorder) WorkerCheckoutCacheOption {
	return func(c *WorkerCheckoutCache) {
		c.generationEvictionRecorder = recorder
	}
}

func WithWorkerCheckoutCacheSelfHealRecorder(recorder WorkerCheckoutCacheSelfHealRecorder) WorkerCheckoutCacheOption {
	return func(c *WorkerCheckoutCache) {
		c.selfHealRecorder = recorder
	}
}

func NewWorkerCheckoutCache(root string, persistentRemoteURLs []string, options ...WorkerCheckoutCacheOption) (*WorkerCheckoutCache, error) {
	return NewWorkerCheckoutCacheWithRemotes(root, workerCheckoutCacheRemotesFromURLs(persistentRemoteURLs), options...)
}

func NewWorkerCheckoutCacheWithRemotes(root string, persistentRemotes []WorkerCheckoutCacheRemote, options ...WorkerCheckoutCacheOption) (*WorkerCheckoutCache, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("%w: worker checkout cache root is required", ErrInvalidReference)
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve worker checkout cache root: %w", err)
	}

	cache := &WorkerCheckoutCache{
		root:                absRoot,
		persistentRemoteURL: make(map[string]WorkerCheckoutCacheRemote, len(persistentRemotes)),
		generationsToKeep:   defaultWorkerCheckoutGenerationsToKeep,
		leaseTTL:            defaultWorkerCheckoutLeaseTTL,
		hardlinkProbe:       workerCheckoutCacheHardlinksAvailable,
	}

	for _, option := range options {
		if option != nil {
			option(cache)
		}
	}

	if cache.generationsToKeep <= 0 {
		return nil, fmt.Errorf("%w: worker checkout cache generations to keep must be > 0", ErrInvalidReference)
	}
	if cache.leaseTTL <= 0 {
		return nil, fmt.Errorf("%w: worker checkout cache lease ttl must be > 0", ErrInvalidReference)
	}
	if cache.maxBytes < 0 {
		return nil, fmt.Errorf("%w: worker checkout cache max bytes must be >= 0", ErrInvalidReference)
	}

	for _, raw := range persistentRemotes {
		remoteURL, err := NormalizeGitRemoteURL(raw.RemoteURL)
		if err != nil {
			return nil, fmt.Errorf("worker checkout cache remote: %w", err)
		}

		fallbackRemoteURLs, err := normalizeWorkerCheckoutCacheFallbackRemoteURLs(remoteURL, raw.FallbackRemoteURLs)
		if err != nil {
			return nil, err
		}
		warmRefspecs, err := refspec.NormalizeFetchRefspecs(raw.WarmRefspecs)
		if err != nil {
			return nil, fmt.Errorf("worker checkout cache warm refspecs: %w", err)
		}

		credentials := raw.Credentials
		if existing, ok := cache.persistentRemoteURL[remoteURL]; ok {
			fallbackRemoteURLs = append(existing.FallbackRemoteURLs, fallbackRemoteURLs...)
			fallbackRemoteURLs, err = normalizeWorkerCheckoutCacheFallbackRemoteURLs(remoteURL, fallbackRemoteURLs)
			if err != nil {
				return nil, err
			}

			if credentials.IsZero() && !existing.Credentials.IsZero() {
				credentials = existing.Credentials
			}
			warmRefspecs = mergeWorkerCheckoutCacheWarmRefspecs(existing.WarmRefspecs, warmRefspecs)
		}

		cache.persistentRemoteURL[remoteURL] = WorkerCheckoutCacheRemote{
			RemoteURL:          remoteURL,
			FallbackRemoteURLs: fallbackRemoteURLs,
			WarmRefspecs:       warmRefspecs,
			Credentials:        credentials,
		}
	}

	return cache, nil
}

func (c *WorkerCheckoutCache) NewCheckoutCacheScope() (action.CheckoutCache, error) {
	if c == nil {
		return nil, nil
	}

	return &WorkerCheckoutCacheScope{cache: c}, nil
}

func workerCheckoutCacheRemotesFromURLs(remoteURLs []string) []WorkerCheckoutCacheRemote {
	if len(remoteURLs) == 0 {
		return nil
	}

	out := make([]WorkerCheckoutCacheRemote, 0, len(remoteURLs))
	for _, remoteURL := range remoteURLs {
		remoteURL = strings.TrimSpace(remoteURL)
		if remoteURL == "" {
			continue
		}

		out = append(out, WorkerCheckoutCacheRemote{RemoteURL: remoteURL})
	}

	return out
}

func normalizeWorkerCheckoutCacheFallbackRemoteURLs(primaryRemoteURL string, fallbackRemoteURLs []string) ([]string, error) {
	if len(fallbackRemoteURLs) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, len(fallbackRemoteURLs))
	out := make([]string, 0, len(fallbackRemoteURLs))
	for _, raw := range fallbackRemoteURLs {
		remoteURL, err := NormalizeGitRemoteURL(raw)
		if err != nil {
			return nil, fmt.Errorf("worker checkout cache fallback remote: %w", err)
		}

		if remoteURL == primaryRemoteURL {
			continue
		}

		if _, ok := seen[remoteURL]; ok {
			continue
		}

		seen[remoteURL] = struct{}{}
		out = append(out, remoteURL)
	}

	return out, nil
}

func mergeWorkerCheckoutCacheWarmRefspecs(existing, incoming []string) []string {
	if len(existing) == 0 || len(incoming) == 0 {
		return nil
	}

	merged, err := refspec.NormalizeFetchRefspecs(append(append([]string(nil), existing...), incoming...))
	if err != nil {
		return nil
	}

	return merged
}

func mergeWorkerCheckoutCacheBaseAndExtraRefspecs(base, extra []string) []string {
	if len(base) == 0 {
		return nil
	}
	if len(extra) == 0 {
		normalized, err := refspec.NormalizeFetchRefspecs(base)
		if err != nil {
			return nil
		}

		return normalized
	}

	merged, err := refspec.NormalizeFetchRefspecs(append(append([]string(nil), base...), extra...))
	if err != nil {
		return nil
	}

	return merged
}

func (c *WorkerCheckoutCache) Checkout(ctx context.Context, remoteURL, workspace string, logger interfaces.Logger) (bool, error) {
	return c.checkout(ctx, remoteURL, workspace, logger, false, nil)
}

func (c *WorkerCheckoutCache) checkout(ctx context.Context, remoteURL, workspace string, logger interfaces.Logger, allowBorrowedObjects bool, retainLease func(*workerCheckoutGenerationLease) error) (bool, error) {
	if c == nil {
		return false, nil
	}

	handled, persistentRemote, err := c.lookupPersistentRemote(remoteURL)
	if err != nil || !handled {
		return handled, err
	}
	normalizedRemoteURL := persistentRemote.RemoteURL

	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return true, fmt.Errorf("%w: workspace is required", ErrInvalidReference)
	}

	for attempt := 0; attempt < 2; attempt++ {
		mirrorPath, lease, err := c.acquireCurrentMirrorLease(ctx, normalizedRemoteURL)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return true, err
			}

			handled, normalizedRemoteURL, err = c.WarmRemote(ctx, normalizedRemoteURL, logger)
			if err != nil || !handled {
				return handled, err
			}

			mirrorPath, lease, err = c.acquireCurrentMirrorLease(ctx, normalizedRemoteURL)
			if err != nil {
				return true, err
			}
		}
		releaseLease := true
		defer func() {
			if releaseLease && lease != nil {
				_ = lease.Close()
			}
		}()

		if logger != nil {
			logger.Info("Cloning repository from worker checkout cache: %s", normalizedRemoteURL)
		}

		mode, reason, borrowedObjects, err := cloneWorkerCheckoutCacheWorkspaceForLease(ctx, workspace, mirrorPath, c.hardlinkProbe, allowBorrowedObjects)
		if err != nil {
			if attempt == 0 && workerCheckoutCacheErrorLooksCorrupt(err) {
				if lease != nil {
					_ = lease.Close()
					releaseLease = false
				}
				_ = cleanupWorkerCheckoutCachePartialClone(workspace)
				if healErr := c.retireCurrentMirrorGeneration(ctx, normalizedRemoteURL, mirrorPath); healErr == nil {
					if _, _, warmErr := c.WarmRemote(ctx, normalizedRemoteURL, logger); warmErr == nil {
						c.recordSelfHeal(ctx, observability.CheckoutCacheSelfHealOperationCheckout, observability.CheckoutCacheSelfHealOutcomeSuccess)
						continue
					}
				}
				c.recordSelfHeal(ctx, observability.CheckoutCacheSelfHealOperationCheckout, observability.CheckoutCacheSelfHealOutcomeFailed)
			}

			return true, fmt.Errorf("clone from worker checkout cache: %w", err)
		}
		c.recordClone(ctx, mode, reason)

		cacheRemoteURL := filepath.Join(c.repositoryPath(normalizedRemoteURL), "current")
		if err := configureWorkerCheckoutCacheWorkspace(ctx, workspace, normalizedRemoteURL, cacheRemoteURL); err != nil {
			return true, err
		}

		if borrowedObjects && retainLease != nil && lease != nil {
			if err := retainLease(lease); err != nil {
				return true, err
			}

			releaseLease = false
		}

		return true, nil
	}

	return true, fmt.Errorf("clone from worker checkout cache: retry exhausted")
}

func (s *WorkerCheckoutCacheScope) Checkout(ctx context.Context, remoteURL, workspace string, logger interfaces.Logger) (bool, error) {
	if s == nil || s.cache == nil {
		return false, nil
	}

	return s.cache.checkout(ctx, remoteURL, workspace, logger, true, s.retainLease)
}

func (s *WorkerCheckoutCacheScope) FetchRefspecs(ctx context.Context, remoteURL, workspace string, refspecs []string, logger interfaces.Logger) (bool, error) {
	if s == nil || s.cache == nil {
		return false, nil
	}

	return s.cache.FetchRefspecs(ctx, remoteURL, workspace, refspecs, logger)
}

func (s *WorkerCheckoutCacheScope) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	leases := append([]*workerCheckoutGenerationLease(nil), s.leases...)
	s.leases = nil
	s.closed = true
	s.mu.Unlock()

	var errs []error
	for i := len(leases) - 1; i >= 0; i-- {
		if leases[i] == nil {
			continue
		}
		if err := leases[i].Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (s *WorkerCheckoutCacheScope) retainLease(lease *workerCheckoutGenerationLease) error {
	if s == nil || lease == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("worker checkout cache scope is closed")
	}

	s.leases = append(s.leases, lease)
	return nil
}

func (c *WorkerCheckoutCache) FetchRefspecs(ctx context.Context, remoteURL, workspace string, refspecs []string, logger interfaces.Logger) (bool, error) {
	if c == nil {
		return false, nil
	}

	handled, persistentRemote, err := c.lookupPersistentRemote(remoteURL)
	if err != nil || !handled {
		return handled, err
	}

	refspecs, err = normalizeWorkerCheckoutCacheFetchRefspecs(refspecs)
	if err != nil {
		return true, err
	}
	if len(refspecs) == 0 {
		return true, nil
	}

	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return true, fmt.Errorf("%w: workspace is required", ErrInvalidReference)
	}

	if logger != nil {
		logger.Info("Refreshing worker checkout cache mirror before fetching auxiliary refs: %s", persistentRemote.RemoteURL)
	}
	mirrorRefspecs, err := workerCheckoutCacheMirrorHydrationRefspecs(refspecs)
	if err != nil {
		return true, err
	}

	_, _, _, warmErr := c.warmRemoteStatus(ctx, persistentRemote.RemoteURL, logger, mirrorRefspecs)
	if warmErr != nil {
		c.recordDemandHydration(ctx, observability.CheckoutCacheDemandHydrationOutcomeFailed)
	} else {
		c.recordDemandHydration(ctx, observability.CheckoutCacheDemandHydrationOutcomeSuccess)
	}

	if warmErr != nil && logger != nil {
		logger.Warn("Worker checkout cache mirror refresh failed before auxiliary ref fetch for %s: %v", persistentRemote.RemoteURL, warmErr)
	}

	if err := fetchWorkerCheckoutCacheWorkspaceRefspecs(ctx, workspace, workerCheckoutCacheRemoteName, refspecs); err != nil {
		if workerCheckoutCacheErrorLooksCorrupt(err) {
			if mirrorPath, currentErr := c.currentMirrorPath(persistentRemote.RemoteURL); currentErr == nil {
				if retireErr := c.retireCurrentMirrorGeneration(ctx, persistentRemote.RemoteURL, mirrorPath); retireErr == nil {
					if _, _, _, rewarmErr := c.warmRemoteStatus(ctx, persistentRemote.RemoteURL, logger, mirrorRefspecs); rewarmErr == nil {
						if retryErr := fetchWorkerCheckoutCacheWorkspaceRefspecs(ctx, workspace, workerCheckoutCacheRemoteName, refspecs); retryErr == nil {
							c.recordSelfHeal(ctx, observability.CheckoutCacheSelfHealOperationFetchRefspec, observability.CheckoutCacheSelfHealOutcomeSuccess)
							return true, nil
						}
					}
				}
			}
			c.recordSelfHeal(ctx, observability.CheckoutCacheSelfHealOperationFetchRefspec, observability.CheckoutCacheSelfHealOutcomeFailed)
		}

		if warmErr != nil {
			return true, fmt.Errorf("refresh worker checkout cache mirror: %v; fetch refspecs: %w", warmErr, err)
		}
		return true, err
	}

	return true, nil
}

type workerCheckoutCacheGitRunner func(context.Context, string, ...string) error
type workerCheckoutCacheHardlinkProbe func(mirrorPath, workspace string) bool

func (c *WorkerCheckoutCache) recordClone(ctx context.Context, mode, reason string) {
	if c == nil || c.cloneRecorder == nil {
		return
	}

	c.cloneRecorder(ctx, mode, reason)
}

func (c *WorkerCheckoutCache) recordDemandHydration(ctx context.Context, outcome string) {
	if c == nil || c.demandHydrationRecorder == nil {
		return
	}

	c.demandHydrationRecorder(ctx, outcome)
}

func (c *WorkerCheckoutCache) recordGenerationEviction(ctx context.Context, reason string) {
	if c == nil || c.generationEvictionRecorder == nil {
		return
	}

	c.generationEvictionRecorder(ctx, reason)
}

func (c *WorkerCheckoutCache) recordSelfHeal(ctx context.Context, operation, outcome string) {
	if c == nil || c.selfHealRecorder == nil {
		return
	}

	c.selfHealRecorder(ctx, operation, outcome)
}

func cloneWorkerCheckoutCacheWorkspace(ctx context.Context, workspace, mirrorPath string) (string, string, error) {
	return cloneWorkerCheckoutCacheWorkspaceWithRunner(ctx, workspace, mirrorPath, runWorkerCacheGit)
}

func cloneWorkerCheckoutCacheWorkspaceWithRunner(ctx context.Context, workspace, mirrorPath string, run workerCheckoutCacheGitRunner) (string, string, error) {
	return cloneWorkerCheckoutCacheWorkspaceWithRunnerAndProbe(ctx, workspace, mirrorPath, run, workerCheckoutCacheHardlinksAvailable)
}

func cloneWorkerCheckoutCacheWorkspaceWithRunnerAndProbe(ctx context.Context, workspace, mirrorPath string, run workerCheckoutCacheGitRunner, canHardlink workerCheckoutCacheHardlinkProbe) (string, string, error) {
	mode, reason, _, err := cloneWorkerCheckoutCacheWorkspaceWithRunnerProbeAndBorrow(ctx, workspace, mirrorPath, run, canHardlink, false)
	return mode, reason, err
}

func cloneWorkerCheckoutCacheWorkspaceForLease(ctx context.Context, workspace, mirrorPath string, canHardlink workerCheckoutCacheHardlinkProbe, allowBorrowedObjects bool) (string, string, bool, error) {
	return cloneWorkerCheckoutCacheWorkspaceWithRunnerProbeAndBorrow(ctx, workspace, mirrorPath, runWorkerCacheGit, canHardlink, allowBorrowedObjects)
}

func cloneWorkerCheckoutCacheWorkspaceWithRunnerProbeAndBorrow(ctx context.Context, workspace, mirrorPath string, run workerCheckoutCacheGitRunner, canHardlink workerCheckoutCacheHardlinkProbe, allowBorrowedObjects bool) (string, string, bool, error) {
	if run == nil {
		return "", "", false, fmt.Errorf("%w: worker checkout cache git runner is required", ErrInvalidReference)
	}

	cloneArgs := []string{"clone", "--local"}
	mode := observability.CheckoutCacheCloneModeHardlink
	reason := observability.CheckoutCacheCloneReasonOK
	borrowedObjects := false
	if canHardlink != nil && !canHardlink(mirrorPath, workspace) {
		cloneArgs, mode, borrowedObjects = workerCheckoutCacheNoHardlinkCloneArgs(allowBorrowedObjects)
		reason = observability.CheckoutCacheCloneReasonProbe
	}

	cloneArgs = append(cloneArgs, "--", mirrorPath, ".")
	err := run(ctx, workspace, cloneArgs...)
	if err == nil {
		return mode, reason, borrowedObjects, nil
	}

	if mode != observability.CheckoutCacheCloneModeHardlink || !workerCheckoutCacheCloneNeedsNoHardlinksRetry(err) {
		return "", "", false, err
	}

	if cleanupErr := cleanupWorkerCheckoutCachePartialClone(workspace); cleanupErr != nil {
		return "", "", false, fmt.Errorf("%w; cleanup partial worker checkout cache clone: %v", err, cleanupErr)
	}

	retryArgs, mode, borrowedObjects := workerCheckoutCacheNoHardlinkCloneArgs(allowBorrowedObjects)
	retryArgs = append(retryArgs, "--", mirrorPath, ".")
	if retryErr := run(ctx, workspace, retryArgs...); retryErr != nil {
		return "", "", false, fmt.Errorf("%w; retry without hardlinks: %v", err, retryErr)
	}

	return mode, observability.CheckoutCacheCloneReasonRetry, borrowedObjects, nil
}

func workerCheckoutCacheNoHardlinkCloneArgs(allowBorrowedObjects bool) ([]string, string, bool) {
	if allowBorrowedObjects {
		return []string{"clone", "--shared", "--no-hardlinks"}, observability.CheckoutCacheCloneModeBorrowed, true
	}

	return []string{"clone", "--local", "--no-hardlinks"}, observability.CheckoutCacheCloneModeCopy, false
}

func workerCheckoutCacheHardlinksAvailable(mirrorPath, workspace string) bool {
	sourcePath := filepath.Join(mirrorPath, "HEAD")
	for i := range 100 {
		targetPath := filepath.Join(workspace, fmt.Sprintf(".vectis-checkout-cache-hardlink-probe-%d-%d", os.Getpid(), i))
		err := os.Link(sourcePath, targetPath)
		if err == nil {
			_ = os.Remove(targetPath)
			return true
		}

		if errors.Is(err, os.ErrExist) {
			continue
		}

		if workerCheckoutCacheCloneNeedsNoHardlinksRetry(fmt.Errorf("link probe: %w", err)) {
			return false
		}

		return true
	}

	return true
}

func workerCheckoutCacheCloneNeedsNoHardlinksRetry(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "link") && !strings.Contains(msg, "hardlink") && !strings.Contains(msg, "hard link") {
		return false
	}

	reasons := []string{
		"cross-device",
		"operation not permitted",
		"permission denied",
		"not supported",
		"not implemented",
		"too many links",
	}

	for _, reason := range reasons {
		if strings.Contains(msg, reason) {
			return true
		}
	}

	return false
}

func workerCheckoutCacheErrorLooksCorrupt(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	reasons := []string{
		"bad object",
		"corrupt",
		"did not send all necessary objects",
		"error in object",
		"missing blob",
		"missing commit",
		"missing object",
		"missing tree",
		"not our ref",
		"nonexistent object",
		"object file",
		"unable to read",
	}
	for _, reason := range reasons {
		if strings.Contains(msg, reason) {
			return true
		}
	}

	return false
}

func cleanupWorkerCheckoutCachePartialClone(workspace string) error {
	if strings.TrimSpace(workspace) == "" {
		return nil
	}

	if err := os.RemoveAll(filepath.Join(workspace, ".git")); err != nil {
		return fmt.Errorf("remove partial git directory: %w", err)
	}

	return nil
}

func (c *WorkerCheckoutCache) WarmRemote(ctx context.Context, remoteURL string, logger interfaces.Logger) (bool, string, error) {
	handled, normalizedRemoteURL, _, err := c.WarmRemoteStatus(ctx, remoteURL, logger)
	return handled, normalizedRemoteURL, err
}

func (c *WorkerCheckoutCache) WarmRemoteStatus(ctx context.Context, remoteURL string, logger interfaces.Logger) (bool, string, bool, error) {
	return c.warmRemoteStatus(ctx, remoteURL, logger, nil)
}

func (c *WorkerCheckoutCache) warmRemoteStatus(ctx context.Context, remoteURL string, logger interfaces.Logger, extraWarmRefspecs []string) (bool, string, bool, error) {
	if c == nil {
		return false, "", false, nil
	}

	handled, persistentRemote, err := c.lookupPersistentRemote(remoteURL)
	if err != nil || !handled {
		return handled, "", false, err
	}
	remoteURL = persistentRemote.RemoteURL
	persistentRemote.WarmRefspecs = mergeWorkerCheckoutCacheBaseAndExtraRefspecs(persistentRemote.WarmRefspecs, extraWarmRefspecs)

	repoPath := c.repositoryPath(remoteURL)
	lock, err := acquireManagedGitWriterLock(ctx, repoPath)
	if err != nil {
		return true, remoteURL, false, err
	}
	defer lock.Close()

	changed, err := c.ensureMirror(ctx, persistentRemote, repoPath, logger)
	if err != nil {
		return true, remoteURL, false, err
	}

	return true, remoteURL, changed, nil
}

func (c *WorkerCheckoutCache) lookupPersistentRemote(remoteURL string) (bool, WorkerCheckoutCacheRemote, error) {
	remoteURL, err := NormalizeGitRemoteURL(remoteURL)
	if err != nil {
		return false, WorkerCheckoutCacheRemote{}, nil
	}

	persistentRemote, ok := c.persistentRemoteURL[remoteURL]
	if !ok {
		return false, WorkerCheckoutCacheRemote{}, nil
	}

	return true, persistentRemote, nil
}

func (c *WorkerCheckoutCache) ensureMirror(ctx context.Context, remote WorkerCheckoutCacheRemote, repoPath string, logger interfaces.Logger) (bool, error) {
	currentPath, err := c.currentMirrorPath(remote.RemoteURL)
	switch {
	case err == nil:
		if logger != nil {
			logger.Info("Refreshing worker checkout cache mirror: %s", remote.RemoteURL)
		}
	case errors.Is(err, os.ErrNotExist):
		if logger != nil {
			logger.Info("Creating worker checkout cache mirror: %s", remote.RemoteURL)
		}
	default:
		return false, err
	}

	generationPath, changed, err := c.buildMirrorGeneration(ctx, remote, repoPath, currentPath)
	if err != nil {
		return false, err
	}

	if changed {
		if err := c.flipCurrentGeneration(repoPath, generationPath); err != nil {
			return false, err
		}
	}

	return changed, c.cleanupOldGenerations(ctx, repoPath)
}

func (c *WorkerCheckoutCache) buildMirrorGeneration(ctx context.Context, remote WorkerCheckoutCacheRemote, repoPath, currentPath string) (string, bool, error) {
	credentialEnv, credentialCleanup, err := managedGitCredentialEnvironment(remote.Credentials)
	if err != nil {
		return "", false, fmt.Errorf("worker checkout cache credentials: %w", err)
	}
	defer credentialCleanup()

	generationsPath := filepath.Join(repoPath, "generations")
	if err := os.MkdirAll(generationsPath, 0o755); err != nil {
		return "", false, fmt.Errorf("create worker checkout cache generations parent: %w", err)
	}

	if err := cleanupStaleWorkerCheckoutReceivingGenerations(generationsPath, c.leaseTTL, time.Now()); err != nil {
		return "", false, err
	}

	if currentPath != "" {
		if resolved, err := filepath.EvalSymlinks(currentPath); err == nil {
			currentPath = resolved
		}

		sameAdvertisedRefs, err := workerCheckoutCacheRemoteRefsMatchCurrent(ctx, currentPath, credentialEnv, remote)
		if err != nil {
			return "", false, err
		}
		if sameAdvertisedRefs {
			if err := configureWorkerCheckoutCacheMirror(ctx, currentPath); err != nil {
				return "", false, err
			}

			return currentPath, false, nil
		}
	}

	generationPath := c.newGenerationPath(generationsPath)
	tmp := generationPath + workerCheckoutReceivingSuffix
	_ = os.RemoveAll(tmp)

	if currentPath == "" {
		clonedFrom, err := initializeWorkerCheckoutCacheMirror(ctx, remote, credentialEnv, tmp)
		if err != nil {
			_ = os.RemoveAll(tmp)
			return "", false, err
		}

		if clonedFrom != remote.RemoteURL {
			if err := setWorkerCheckoutCacheMirrorOrigin(ctx, tmp, remote.RemoteURL); err != nil {
				_ = os.RemoveAll(tmp)
				return "", false, err
			}
		}

		_ = fetchWorkerCheckoutCacheFallbackMirrors(ctx, tmp, credentialEnv, remote.FallbackRemoteURLs, remote.WarmRefspecs)
	} else {
		if resolved, err := filepath.EvalSymlinks(currentPath); err == nil {
			currentPath = resolved
		}

		if err := runWorkerCacheGitNoDir(ctx, managedGitCommandArgs("clone", "--mirror", "--local", "--", currentPath, tmp)...); err != nil {
			_ = os.RemoveAll(tmp)
			return "", false, fmt.Errorf("clone worker checkout cache generation: %w", err)
		}

		if err := setWorkerCheckoutCacheMirrorOrigin(ctx, tmp, remote.RemoteURL); err != nil {
			_ = os.RemoveAll(tmp)
			return "", false, err
		}

		if err := refreshWorkerCheckoutCacheMirror(ctx, tmp, credentialEnv, remote); err != nil {
			_ = os.RemoveAll(tmp)
			return "", false, err
		}

		sameRefs, err := workerCheckoutCacheMirrorsHaveSameRefs(ctx, currentPath, tmp, remote.WarmRefspecs)
		if err != nil {
			_ = os.RemoveAll(tmp)
			return "", false, err
		}
		if sameRefs {
			_ = os.RemoveAll(tmp)
			if err := configureWorkerCheckoutCacheMirror(ctx, currentPath); err != nil {
				return "", false, err
			}

			return currentPath, false, nil
		}
	}

	if err := configureWorkerCheckoutCacheMirror(ctx, tmp); err != nil {
		_ = os.RemoveAll(tmp)
		return "", false, err
	}

	if err := os.Rename(tmp, generationPath); err != nil {
		_ = os.RemoveAll(tmp)
		return "", false, fmt.Errorf("install worker checkout cache generation: %w", err)
	}

	return generationPath, true, nil
}

func workerCheckoutCacheMirrorsHaveSameRefs(ctx context.Context, left, right string, warmRefspecs []string) (bool, error) {
	leftRefs, err := workerCheckoutCacheMirrorRefSnapshot(ctx, left, warmRefspecs)
	if err != nil {
		return false, fmt.Errorf("snapshot current worker checkout cache mirror refs: %w", err)
	}

	rightRefs, err := workerCheckoutCacheMirrorRefSnapshot(ctx, right, warmRefspecs)
	if err != nil {
		return false, fmt.Errorf("snapshot refreshed worker checkout cache mirror refs: %w", err)
	}

	return bytes.Equal(leftRefs, rightRefs), nil
}

func workerCheckoutCacheRemoteRefsMatchCurrent(ctx context.Context, currentPath string, env []string, remote WorkerCheckoutCacheRemote) (bool, error) {
	currentRefs, err := workerCheckoutCacheMirrorRefsSnapshot(ctx, currentPath, workerCheckoutCacheDestinationRefPatterns(remote.WarmRefspecs))
	if err != nil {
		return false, fmt.Errorf("snapshot current worker checkout cache advertised refs: %w", err)
	}

	remoteRefs, ok, err := workerCheckoutCacheAdvertisedRefSnapshot(ctx, env, remote)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	return bytes.Equal(currentRefs, remoteRefs), nil
}

func workerCheckoutCacheMirrorRefSnapshot(ctx context.Context, mirrorPath string, warmRefspecs []string) ([]byte, error) {
	var snapshot bytes.Buffer
	head, err := os.ReadFile(filepath.Join(mirrorPath, "HEAD"))
	if err != nil {
		return nil, fmt.Errorf("read HEAD: %w", err)
	}

	snapshot.WriteString("HEAD ")
	snapshot.Write(bytes.TrimSpace(head))
	snapshot.WriteByte('\n')

	refs, err := workerCheckoutCacheMirrorRefsSnapshot(ctx, mirrorPath, workerCheckoutCacheDestinationRefPatterns(warmRefspecs))
	if err != nil {
		return nil, err
	}

	snapshot.Write(refs)
	return snapshot.Bytes(), nil
}

func workerCheckoutCacheMirrorRefsSnapshot(ctx context.Context, mirrorPath string, patterns []string) ([]byte, error) {
	args := workerCacheMirrorGitArgs(mirrorPath, "for-each-ref", "--sort=refname", "--format=%(refname)%00%(objectname)")
	args = append(args, patterns...)
	refs, err := runWorkerCacheGitNoDirOutput(ctx, args...)
	if err != nil {
		return nil, err
	}

	return refs, nil
}

func workerCheckoutCacheAdvertisedRefSnapshot(ctx context.Context, env []string, remote WorkerCheckoutCacheRemote) ([]byte, bool, error) {
	warmRefspecs := workerCheckoutCacheWarmRefspecs(remote.WarmRefspecs)
	refs, err := workerCheckoutCacheLsRemoteRefs(ctx, env, remote.RemoteURL, warmRefspecs)
	if err != nil {
		return nil, false, nil
	}

	for _, fallbackRemoteURL := range remote.FallbackRemoteURLs {
		fallbackRefs, err := workerCheckoutCacheLsRemoteRefs(ctx, env, fallbackRemoteURL, warmRefspecs)
		if err != nil {
			continue
		}

		for refName, objectName := range fallbackRefs {
			refs[refName] = objectName
		}
	}

	return workerCheckoutCacheFormatRefSnapshot(refs), true, nil
}

func workerCheckoutCacheLsRemoteRefs(ctx context.Context, env []string, remoteURL string, warmRefspecs []string) (map[string]string, error) {
	args := managedGitCommandArgs("ls-remote", "--refs", "--", remoteURL)
	args = append(args, workerCheckoutCacheSourceRefPatterns(warmRefspecs)...)
	out, err := runWorkerCacheGitCommandOutputWithEnv(ctx, "", env, args...)
	if err != nil {
		return nil, err
	}

	return parseWorkerCheckoutCacheAdvertisedRefs(out, warmRefspecs)
}

func parseWorkerCheckoutCacheAdvertisedRefs(out []byte, warmRefspecs []string) (map[string]string, error) {
	refs := make(map[string]string)
	for _, line := range bytes.Split(out, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		fields := bytes.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("parse advertised worker checkout cache ref %q", line)
		}

		sourceRefName := string(fields[1])
		if !strings.HasPrefix(sourceRefName, "refs/") || strings.HasSuffix(sourceRefName, "^{}") {
			continue
		}

		for _, destinationRefName := range workerCheckoutCacheDestinationRefsForSource(sourceRefName, warmRefspecs) {
			refs[destinationRefName] = string(fields[0])
		}
	}

	return refs, nil
}

func workerCheckoutCacheFormatRefSnapshot(refs map[string]string) []byte {
	refNames := make([]string, 0, len(refs))
	for refName := range refs {
		refNames = append(refNames, refName)
	}
	sort.Strings(refNames)

	var snapshot bytes.Buffer
	for _, refName := range refNames {
		snapshot.WriteString(refName)
		snapshot.WriteByte(0)
		snapshot.WriteString(refs[refName])
		snapshot.WriteByte('\n')
	}

	return snapshot.Bytes()
}

func workerCheckoutCacheWarmRefspecs(warmRefspecs []string) []string {
	normalized, err := refspec.NormalizeFetchRefspecs(warmRefspecs)
	if err != nil || len(normalized) == 0 {
		return []string{"+refs/*:refs/*"}
	}

	return normalized
}

func workerCheckoutCacheSourceRefPatterns(warmRefspecs []string) []string {
	warmRefspecs = workerCheckoutCacheWarmRefspecs(warmRefspecs)
	out := make([]string, 0, len(warmRefspecs))
	for _, raw := range warmRefspecs {
		source, _, ok := splitWorkerCheckoutCacheRefspec(raw)
		if !ok {
			continue
		}

		out = append(out, source)
	}

	if len(out) == 0 {
		return []string{"refs/*"}
	}

	return out
}

func workerCheckoutCacheDestinationRefPatterns(warmRefspecs []string) []string {
	if len(warmRefspecs) == 0 {
		return nil
	}

	warmRefspecs = workerCheckoutCacheWarmRefspecs(warmRefspecs)
	out := make([]string, 0, len(warmRefspecs))
	for _, raw := range warmRefspecs {
		_, destination, ok := splitWorkerCheckoutCacheRefspec(raw)
		if !ok {
			continue
		}
		if destination == "refs/*" {
			return nil
		}

		out = append(out, destination)
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func workerCheckoutCacheDestinationRefsForSource(sourceRef string, warmRefspecs []string) []string {
	warmRefspecs = workerCheckoutCacheWarmRefspecs(warmRefspecs)
	out := make([]string, 0, 1)
	for _, raw := range warmRefspecs {
		source, destination, ok := splitWorkerCheckoutCacheRefspec(raw)
		if !ok {
			continue
		}

		if mapped, ok := mapWorkerCheckoutCacheRefspecSource(sourceRef, source, destination); ok {
			out = append(out, mapped)
		}
	}

	return out
}

func splitWorkerCheckoutCacheRefspec(raw string) (string, string, bool) {
	refspec := strings.TrimPrefix(strings.TrimSpace(raw), "+")
	parts := strings.Split(refspec, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}

	return parts[0], parts[1], true
}

func mapWorkerCheckoutCacheRefspecSource(sourceRef, sourcePattern, destinationPattern string) (string, bool) {
	sourceWildcard := strings.Index(sourcePattern, "*")
	destinationWildcard := strings.Index(destinationPattern, "*")
	if sourceWildcard < 0 || destinationWildcard < 0 {
		if sourceRef == sourcePattern {
			return destinationPattern, true
		}

		return "", false
	}

	sourcePrefix := sourcePattern[:sourceWildcard]
	sourceSuffix := sourcePattern[sourceWildcard+1:]
	if !strings.HasPrefix(sourceRef, sourcePrefix) || !strings.HasSuffix(sourceRef, sourceSuffix) {
		return "", false
	}

	matched := strings.TrimSuffix(strings.TrimPrefix(sourceRef, sourcePrefix), sourceSuffix)
	return destinationPattern[:destinationWildcard] + matched + destinationPattern[destinationWildcard+1:], true
}

func initializeWorkerCheckoutCacheMirror(ctx context.Context, remote WorkerCheckoutCacheRemote, env []string, tmp string) (string, error) {
	if len(remote.WarmRefspecs) == 0 {
		return cloneWorkerCheckoutCacheMirror(ctx, remote, env, tmp)
	}

	if err := runWorkerCacheGitNoDirWithEnv(ctx, env, managedGitCommandArgs("init", "--bare", tmp)...); err != nil {
		_ = os.RemoveAll(tmp)
		return "", fmt.Errorf("initialize worker checkout cache mirror: %w", err)
	}

	if err := addWorkerCheckoutCacheMirrorOrigin(ctx, tmp, remote.RemoteURL); err != nil {
		_ = os.RemoveAll(tmp)
		return "", err
	}

	primaryErr := fetchWorkerCheckoutCacheMirror(ctx, tmp, env, "origin", true, remote.WarmRefspecs)
	fetchedFrom := ""
	if primaryErr == nil {
		fetchedFrom = remote.RemoteURL
	}

	fallbackErr := fetchWorkerCheckoutCacheFallbackMirrors(ctx, tmp, env, remote.FallbackRemoteURLs, remote.WarmRefspecs)
	if primaryErr != nil {
		if fallbackErr == nil && len(remote.FallbackRemoteURLs) > 0 {
			fetchedFrom = remote.FallbackRemoteURLs[0]
		} else if fallbackErr != nil {
			return "", fmt.Errorf("initialize worker checkout cache mirror: origin: %v; fallbacks: %w", primaryErr, fallbackErr)
		} else {
			return "", fmt.Errorf("initialize worker checkout cache mirror: %w", primaryErr)
		}
	}

	if fetchedFrom != "" {
		if err := configureWorkerCheckoutCacheMirrorHead(ctx, tmp, env, fetchedFrom, remote.WarmRefspecs); err != nil {
			return "", err
		}
	}

	return fetchedFrom, nil
}

func cloneWorkerCheckoutCacheMirror(ctx context.Context, remote WorkerCheckoutCacheRemote, env []string, tmp string) (string, error) {
	remoteURLs := append([]string{remote.RemoteURL}, remote.FallbackRemoteURLs...)
	var errs []string
	for _, remoteURL := range remoteURLs {
		if err := runWorkerCacheGitNoDirWithEnv(ctx, env, managedGitCommandArgs("clone", "--mirror", "--", remoteURL, tmp)...); err != nil {
			_ = os.RemoveAll(tmp)
			errs = append(errs, fmt.Sprintf("%s: %v", remoteURL, err))
			continue
		}

		return remoteURL, nil
	}

	if len(errs) == 0 {
		return "", fmt.Errorf("%w: worker checkout cache remote is required", ErrInvalidReference)
	}

	return "", fmt.Errorf("clone worker checkout cache mirror: %s", strings.Join(errs, "; "))
}

func addWorkerCheckoutCacheMirrorOrigin(ctx context.Context, mirrorPath, remoteURL string) error {
	if err := runWorkerCacheGitNoDir(ctx, workerCacheMirrorGitArgs(mirrorPath, "remote", "add", "origin", remoteURL)...); err != nil {
		return fmt.Errorf("add worker checkout cache generation origin: %w", err)
	}

	return nil
}

func setWorkerCheckoutCacheMirrorOrigin(ctx context.Context, mirrorPath, remoteURL string) error {
	if err := runWorkerCacheGitNoDir(ctx, workerCacheMirrorGitArgs(mirrorPath, "remote", "set-url", "origin", remoteURL)...); err != nil {
		return fmt.Errorf("set worker checkout cache generation origin: %w", err)
	}

	return nil
}

func configureWorkerCheckoutCacheMirrorHead(ctx context.Context, mirrorPath string, env []string, remoteURL string, warmRefspecs []string) error {
	out, err := runWorkerCacheGitCommandOutputWithEnv(ctx, "", env, managedGitCommandArgs("ls-remote", "--symref", "--", remoteURL, "HEAD")...)
	if err != nil {
		return nil
	}

	headRef := parseWorkerCheckoutCacheRemoteHead(out)
	if headRef == "" {
		return nil
	}

	destinations := workerCheckoutCacheDestinationRefsForSource(headRef, warmRefspecs)
	if len(destinations) == 0 {
		return nil
	}

	if err := runWorkerCacheGitNoDir(ctx, workerCacheMirrorGitArgs(mirrorPath, "symbolic-ref", "HEAD", destinations[0])...); err != nil {
		return fmt.Errorf("set worker checkout cache generation HEAD: %w", err)
	}

	return nil
}

func parseWorkerCheckoutCacheRemoteHead(out []byte) string {
	for _, line := range bytes.Split(out, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("ref: ")) {
			continue
		}

		fields := bytes.Fields(line)
		if len(fields) == 3 && string(fields[2]) == "HEAD" {
			return string(fields[1])
		}
	}

	return ""
}

func refreshWorkerCheckoutCacheMirror(ctx context.Context, mirrorPath string, env []string, remote WorkerCheckoutCacheRemote) error {
	primaryErr := fetchWorkerCheckoutCacheMirror(ctx, mirrorPath, env, "origin", true, remote.WarmRefspecs)
	fallbackErr := fetchWorkerCheckoutCacheFallbackMirrors(ctx, mirrorPath, env, remote.FallbackRemoteURLs, remote.WarmRefspecs)
	if primaryErr == nil {
		return nil
	}
	if fallbackErr == nil && len(remote.FallbackRemoteURLs) > 0 {
		return nil
	}

	if fallbackErr != nil {
		return fmt.Errorf("refresh worker checkout cache mirror: origin: %v; fallbacks: %w", primaryErr, fallbackErr)
	}

	return fmt.Errorf("refresh worker checkout cache mirror: %w", primaryErr)
}

func fetchWorkerCheckoutCacheFallbackMirrors(ctx context.Context, mirrorPath string, env []string, fallbackRemoteURLs []string, warmRefspecs []string) error {
	var errs []string
	for _, remoteURL := range fallbackRemoteURLs {
		if err := fetchWorkerCheckoutCacheMirror(ctx, mirrorPath, env, remoteURL, false, warmRefspecs); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", remoteURL, err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func fetchWorkerCheckoutCacheMirror(ctx context.Context, mirrorPath string, env []string, remote string, prune bool, warmRefspecs []string) error {
	args := workerCacheMirrorGitArgs(mirrorPath, "fetch")
	if prune {
		args = append(args, "--prune")
	}
	args = append(args, "--no-auto-gc", remote)
	args = append(args, workerCheckoutCacheWarmRefspecs(warmRefspecs)...)
	return runWorkerCacheGitNoDirWithEnv(ctx, env, args...)
}

func configureWorkerCheckoutCacheMirror(ctx context.Context, mirrorPath string) error {
	for _, setting := range gitcmd.NoAutoMaintenanceSettings() {
		if err := runWorkerCacheGitNoDir(ctx, workerCacheMirrorGitArgs(mirrorPath, "config", "--local", setting[0], setting[1])...); err != nil {
			return fmt.Errorf("set worker checkout cache git config %s: %w", setting[0], err)
		}
	}

	return nil
}

func configureWorkerCheckoutCacheWorkspace(ctx context.Context, workspace, originURL, cacheRemoteURL string) error {
	if err := runWorkerCacheGit(ctx, workspace, "remote", "set-url", "origin", originURL); err != nil {
		return fmt.Errorf("restore checkout origin URL: %w", err)
	}

	_, _ = execGitRunner{}.RunGit(ctx, workspace, "remote", "remove", workerCheckoutCacheRemoteName)
	if err := runWorkerCacheGit(ctx, workspace, "remote", "add", workerCheckoutCacheRemoteName, cacheRemoteURL); err != nil {
		return fmt.Errorf("add worker checkout cache remote: %w", err)
	}

	settings := gitcmd.NoAutoMaintenanceSettings()
	settings = append(settings,
		[2]string{"remote." + workerCheckoutCacheRemoteName + ".tagOpt", "--no-tags"},
		[2]string{"remote." + workerCheckoutCacheRemoteName + ".skipDefaultUpdate", "true"},
		[2]string{"remote." + workerCheckoutCacheRemoteName + ".skipFetchAll", "true"},
	)

	for _, setting := range settings {
		if err := runWorkerCacheGit(ctx, workspace, "config", "--local", setting[0], setting[1]); err != nil {
			return fmt.Errorf("set worker checkout cache workspace config %s: %w", setting[0], err)
		}
	}

	return nil
}

func normalizeWorkerCheckoutCacheFetchRefspecs(refspecs []string) ([]string, error) {
	normalized, err := refspec.NormalizeFetchRefspecs(refspecs)
	if err != nil {
		return nil, fmt.Errorf("%w: worker checkout cache fetch refspec is not safe", ErrInvalidReference)
	}

	return normalized, nil
}

func fetchWorkerCheckoutCacheWorkspaceRefspecs(ctx context.Context, workspace, remote string, refspecs []string) error {
	args := []string{"fetch", "--no-auto-gc", "--no-tags", "--", remote}
	args = append(args, refspecs...)
	if err := runWorkerCacheGit(ctx, workspace, args...); err != nil {
		return fmt.Errorf("fetch worker checkout cache refspecs from %s: %w", remote, err)
	}

	return nil
}

func workerCheckoutCacheMirrorHydrationRefspecs(refspecs []string) ([]string, error) {
	refspecs, err := refspec.NormalizeFetchRefspecs(refspecs)
	if err != nil {
		return nil, fmt.Errorf("%w: worker checkout cache fetch refspec is not safe", ErrInvalidReference)
	}

	mirrorRefspecs := make([]string, 0, len(refspecs))
	for _, raw := range refspecs {
		source, _, ok := splitWorkerCheckoutCacheRefspec(raw)
		if !ok || !strings.HasPrefix(source, "refs/") {
			continue
		}

		mirrorRefspecs = append(mirrorRefspecs, "+"+source+":"+source)
	}

	mirrorRefspecs, err = refspec.NormalizeFetchRefspecs(mirrorRefspecs)
	if err != nil {
		return nil, fmt.Errorf("%w: worker checkout cache fetch refspec is not safe", ErrInvalidReference)
	}

	return mirrorRefspecs, nil
}

func (c *WorkerCheckoutCache) mirrorPath(remoteURL string) string {
	if mirrorPath, err := c.currentMirrorPath(remoteURL); err == nil {
		return mirrorPath
	}

	return c.legacyMirrorPath(remoteURL)
}

func (c *WorkerCheckoutCache) repositoryPath(remoteURL string) string {
	sum := sha256.Sum256([]byte(remoteURL))
	return filepath.Join(c.root, "mirrors", hex.EncodeToString(sum[:]))
}

func (c *WorkerCheckoutCache) legacyMirrorPath(remoteURL string) string {
	sum := sha256.Sum256([]byte(remoteURL))
	return filepath.Join(c.root, "mirrors", hex.EncodeToString(sum[:])+".git")
}

func (c *WorkerCheckoutCache) currentMirrorPath(remoteURL string) (string, error) {
	return c.currentMirrorPathForRepo(c.repositoryPath(remoteURL), c.legacyMirrorPath(remoteURL))
}

func (c *WorkerCheckoutCache) currentMirrorPathForRepo(repoPath, legacyPath string) (string, error) {
	currentPath := filepath.Join(repoPath, "current")
	target, err := os.Readlink(currentPath)
	if err == nil {
		var current string
		if filepath.IsAbs(target) {
			current = filepath.Clean(target)
		} else {
			current = filepath.Clean(filepath.Join(repoPath, target))
		}

		if _, statErr := os.Stat(current); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return "", os.ErrNotExist
			}

			return "", fmt.Errorf("stat worker checkout cache current generation: %w", statErr)
		}

		return current, nil
	}

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("read worker checkout cache current pointer: %w", err)
	}

	if _, statErr := os.Stat(legacyPath); statErr == nil {
		return legacyPath, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", fmt.Errorf("stat legacy worker checkout cache mirror: %w", statErr)
	}

	return "", os.ErrNotExist
}

func (c *WorkerCheckoutCache) acquireCurrentMirrorLease(ctx context.Context, remoteURL string) (string, *workerCheckoutGenerationLease, error) {
	repoPath := c.repositoryPath(remoteURL)
	lock, err := c.acquireLeaseLock(ctx, repoPath)
	if err != nil {
		return "", nil, err
	}
	defer lock.Close()

	mirrorPath, err := c.currentMirrorPath(remoteURL)
	if err != nil {
		return "", nil, err
	}

	if resolved, err := filepath.EvalSymlinks(mirrorPath); err == nil {
		mirrorPath = resolved
	}

	resolvedRepoPath := repoPath
	if resolved, err := filepath.EvalSymlinks(repoPath); err == nil {
		resolvedRepoPath = resolved
	}

	if !isWorkerCheckoutGenerationPath(resolvedRepoPath, mirrorPath) {
		return mirrorPath, nil, nil
	}

	leaseRoot := filepath.Join(resolvedRepoPath, "leases", filepath.Base(mirrorPath))
	if err := os.MkdirAll(leaseRoot, 0o755); err != nil {
		return "", nil, fmt.Errorf("create worker checkout cache generation lease parent: %w", err)
	}

	var leasePath string
	var f *os.File
	for i := range 1000 {
		name := workerCheckoutLeaseName()
		if i > 0 {
			name = fmt.Sprintf("%s-%d", name, i)
		}

		leasePath = filepath.Join(leaseRoot, name)
		f, err = os.OpenFile(leasePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			break
		}

		if !errors.Is(err, os.ErrExist) {
			return "", nil, fmt.Errorf("create worker checkout cache generation lease: %w", err)
		}
	}

	if f == nil {
		return "", nil, fmt.Errorf("create worker checkout cache generation lease: %w", os.ErrExist)
	}

	if _, err := fmt.Fprintf(f, "%s\n", mirrorPath); err != nil {
		_ = f.Close()
		_ = os.Remove(leasePath)
		return "", nil, fmt.Errorf("write worker checkout cache generation lease: %w", err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(leasePath)
		return "", nil, fmt.Errorf("close worker checkout cache generation lease: %w", err)
	}

	return mirrorPath, newWorkerCheckoutGenerationLease(leasePath, c.leaseTTL), nil
}

func (c *WorkerCheckoutCache) newGenerationPath(generationsPath string) string {
	base := fmt.Sprintf("%s%020d-%d", workerCheckoutGenerationPrefix, time.Now().UnixNano(), os.Getpid())
	for i := range 1000 {
		name := base
		if i > 0 {
			name = fmt.Sprintf("%s-%d", base, i)
		}

		path := filepath.Join(generationsPath, name+".git")
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return path
		}
	}

	return filepath.Join(generationsPath, fmt.Sprintf("%s%020d-%d-fallback.git", workerCheckoutGenerationPrefix, time.Now().UnixNano(), os.Getpid()))
}

func (c *WorkerCheckoutCache) flipCurrentGeneration(repoPath, generationPath string) error {
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		return fmt.Errorf("create worker checkout cache repository parent: %w", err)
	}

	target, err := filepath.Rel(repoPath, generationPath)
	if err != nil {
		return fmt.Errorf("resolve worker checkout cache generation link: %w", err)
	}

	tmpLink := filepath.Join(repoPath, ".current."+filepath.Base(generationPath)+".tmp")
	_ = os.Remove(tmpLink)
	if err := os.Symlink(target, tmpLink); err != nil {
		return fmt.Errorf("create worker checkout cache generation link: %w", err)
	}

	currentLink := filepath.Join(repoPath, "current")
	if err := os.Rename(tmpLink, currentLink); err != nil {
		if runtime.GOOS == "windows" {
			if removeErr := os.Remove(currentLink); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				_ = os.Remove(tmpLink)
				return fmt.Errorf("replace worker checkout cache current generation: %w", removeErr)
			}
			
			if renameErr := os.Rename(tmpLink, currentLink); renameErr == nil {
				return nil
			} else {
				err = renameErr
			}
		}
		_ = os.Remove(tmpLink)
		return fmt.Errorf("flip worker checkout cache current generation: %w", err)
	}

	return nil
}

func (c *WorkerCheckoutCache) retireCurrentMirrorGeneration(ctx context.Context, remoteURL, mirrorPath string) error {
	repoPath := c.repositoryPath(remoteURL)
	lock, err := acquireManagedGitWriterLock(ctx, repoPath)
	if err != nil {
		return err
	}
	defer lock.Close()

	currentPath, err := c.currentMirrorPath(remoteURL)
	if err != nil {
		return err
	}
	if resolved, err := filepath.EvalSymlinks(currentPath); err == nil {
		currentPath = resolved
	}
	if resolved, err := filepath.EvalSymlinks(mirrorPath); err == nil {
		mirrorPath = resolved
	}

	if !sameCleanPath(currentPath, mirrorPath) {
		return nil
	}

	if err := os.Remove(filepath.Join(repoPath, "current")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove corrupt worker checkout cache current pointer: %w", err)
	}

	resolvedRepoPath := repoPath
	if resolved, err := filepath.EvalSymlinks(repoPath); err == nil {
		resolvedRepoPath = resolved
	}
	if isWorkerCheckoutGenerationPath(resolvedRepoPath, mirrorPath) {
		if err := os.RemoveAll(mirrorPath); err != nil {
			return fmt.Errorf("remove corrupt worker checkout cache generation: %w", err)
		}
		c.recordGenerationEviction(ctx, observability.CheckoutCacheEvictionReasonCorrupt)
	}

	return nil
}

func (c *WorkerCheckoutCache) cleanupOldGenerations(ctx context.Context, repoPath string) error {
	lock, err := c.acquireLeaseLock(ctx, repoPath)
	if err != nil {
		return err
	}
	defer lock.Close()

	now := time.Now()
	if err := cleanupStaleWorkerCheckoutLeases(repoPath, c.leaseTTL, now); err != nil {
		return err
	}

	generationPaths, err := workerCheckoutGenerationPaths(repoPath)
	if err != nil {
		return err
	}

	if len(generationPaths) == 0 {
		return cleanupEmptyWorkerCheckoutLeaseDirs(repoPath)
	}

	currentPath, err := currentGenerationPath(repoPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	sort.Sort(sort.Reverse(sort.StringSlice(generationPaths)))
	kept := 0
	remaining := make([]string, 0, len(generationPaths))
	for _, generationPath := range generationPaths {
		if currentPath != "" && sameCleanPath(generationPath, currentPath) {
			kept++
			remaining = append(remaining, generationPath)
			continue
		}

		if kept < c.generationsToKeep {
			kept++
			remaining = append(remaining, generationPath)
			continue
		}

		leased, err := workerCheckoutGenerationHasLeases(repoPath, generationPath, c.leaseTTL, now)
		if err != nil {
			return err
		}

		if leased {
			remaining = append(remaining, generationPath)
			continue
		}

		if err := os.RemoveAll(generationPath); err != nil {
			return fmt.Errorf("remove worker checkout cache old generation: %w", err)
		}
		c.recordGenerationEviction(ctx, observability.CheckoutCacheEvictionReasonRetention)
	}

	if c.maxBytes > 0 {
		if err := c.cleanupGenerationsOverBudget(ctx, repoPath, remaining, currentPath, now); err != nil {
			return err
		}
	}

	return cleanupEmptyWorkerCheckoutLeaseDirs(repoPath)
}

func (c *WorkerCheckoutCache) cleanupGenerationsOverBudget(ctx context.Context, repoPath string, generationPaths []string, currentPath string, now time.Time) error {
	type generationBudgetEntry struct {
		path  string
		bytes int64
	}

	entries := make([]generationBudgetEntry, 0, len(generationPaths))
	var total int64
	for _, generationPath := range generationPaths {
		if err := ctx.Err(); err != nil {
			return err
		}

		bytes, err := workerCheckoutGenerationPackBytes(ctx, generationPath)
		if err != nil {
			return err
		}

		total = addWorkerCheckoutCacheBytes(total, bytes)
		entries = append(entries, generationBudgetEntry{path: generationPath, bytes: bytes})
	}

	if total <= c.maxBytes {
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].path < entries[j].path
	})
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if total <= c.maxBytes {
			return nil
		}
		if currentPath != "" && sameCleanPath(entry.path, currentPath) {
			continue
		}

		leased, err := workerCheckoutGenerationHasLeases(repoPath, entry.path, c.leaseTTL, now)
		if err != nil {
			return err
		}
		if leased {
			continue
		}

		if err := os.RemoveAll(entry.path); err != nil {
			return fmt.Errorf("remove worker checkout cache generation over budget: %w", err)
		}
		c.recordGenerationEviction(ctx, observability.CheckoutCacheEvictionReasonBudget)
		total -= entry.bytes
	}

	return nil
}

func (c *WorkerCheckoutCache) Stats(ctx context.Context) (WorkerCheckoutCacheStats, error) {
	if c == nil {
		return WorkerCheckoutCacheStats{}, nil
	}

	return workerCheckoutCacheStats(ctx, c.root, c.leaseTTL)
}

func (c *WorkerCheckoutCache) acquireLeaseLock(ctx context.Context, repoPath string) (*managedGitWriterLock, error) {
	return acquireManagedGitWriterLock(ctx, filepath.Join(repoPath, "leases"))
}

func workerCheckoutCacheStats(ctx context.Context, root string, leaseTTL time.Duration) (WorkerCheckoutCacheStats, error) {
	var stats WorkerCheckoutCacheStats
	mirrorsPath := filepath.Join(root, "mirrors")
	entries, err := os.ReadDir(mirrorsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return stats, nil
		}

		return stats, fmt.Errorf("read worker checkout cache mirrors: %w", err)
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !entry.IsDir() {
			continue
		}

		path := filepath.Join(mirrorsPath, entry.Name())
		if strings.HasSuffix(entry.Name(), ".git") {
			stats.Repositories++
			if err := addWorkerCheckoutCacheGenerationStats(ctx, path, &stats); err != nil {
				return stats, err
			}
			continue
		}

		repoStats, err := workerCheckoutCacheRepositoryStats(ctx, path, leaseTTL)
		if err != nil {
			return stats, err
		}
		if repoStats.Generations > 0 {
			stats.Repositories++
		}
		stats.Generations += repoStats.Generations
		stats.PackFiles += repoStats.PackFiles
		stats.PackBytes = addWorkerCheckoutCacheBytes(stats.PackBytes, repoStats.PackBytes)
		stats.ActiveLeases += repoStats.ActiveLeases
	}

	return stats, nil
}

func workerCheckoutCacheRepositoryStats(ctx context.Context, repoPath string, leaseTTL time.Duration) (WorkerCheckoutCacheStats, error) {
	var stats WorkerCheckoutCacheStats
	generationPaths, err := workerCheckoutGenerationPaths(repoPath)
	if err != nil {
		return stats, err
	}

	for _, generationPath := range generationPaths {
		if err := addWorkerCheckoutCacheGenerationStats(ctx, generationPath, &stats); err != nil {
			return stats, err
		}
	}

	activeLeases, err := workerCheckoutActiveLeaseCount(ctx, repoPath, leaseTTL)
	if err != nil {
		return stats, err
	}
	stats.ActiveLeases = activeLeases
	return stats, nil
}

func addWorkerCheckoutCacheGenerationStats(ctx context.Context, generationPath string, stats *WorkerCheckoutCacheStats) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	stats.Generations++
	packEntries, err := os.ReadDir(filepath.Join(generationPath, "objects", "pack"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("read worker checkout cache generation pack directory: %w", err)
	}

	for _, entry := range packEntries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return fmt.Errorf("stat worker checkout cache pack file: %w", err)
		}

		stats.PackFiles++
		stats.PackBytes = addWorkerCheckoutCacheBytes(stats.PackBytes, info.Size())
	}

	return nil
}

func workerCheckoutGenerationPackBytes(ctx context.Context, generationPath string) (int64, error) {
	var bytes int64
	packEntries, err := os.ReadDir(filepath.Join(generationPath, "objects", "pack"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, fmt.Errorf("read worker checkout cache generation pack directory: %w", err)
	}

	for _, entry := range packEntries {
		if err := ctx.Err(); err != nil {
			return bytes, err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return bytes, fmt.Errorf("stat worker checkout cache pack file: %w", err)
		}

		bytes = addWorkerCheckoutCacheBytes(bytes, info.Size())
	}

	return bytes, nil
}

func workerCheckoutActiveLeaseCount(ctx context.Context, repoPath string, leaseTTL time.Duration) (int64, error) {
	leaseRoot := filepath.Join(repoPath, "leases")
	leaseDirs, err := os.ReadDir(leaseRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, fmt.Errorf("read worker checkout cache leases: %w", err)
	}

	var leases int64
	now := time.Now()
	for _, leaseDir := range leaseDirs {
		if err := ctx.Err(); err != nil {
			return leases, err
		}
		if !leaseDir.IsDir() {
			continue
		}

		count, err := workerCheckoutFreshLeaseCount(filepath.Join(leaseRoot, leaseDir.Name()), leaseTTL, now, false)
		if err != nil {
			return leases, err
		}
		leases += count
	}

	return leases, nil
}

func addWorkerCheckoutCacheBytes(a, b int64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if b <= 0 {
		return a
	}
	if maxInt64-a < b {
		return maxInt64
	}

	return a + b
}

func workerCheckoutGenerationPaths(repoPath string) ([]string, error) {
	generationsPath := filepath.Join(repoPath, "generations")
	entries, err := os.ReadDir(generationsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("read worker checkout cache generations: %w", err)
	}

	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasPrefix(name, workerCheckoutGenerationPrefix) || !strings.HasSuffix(name, ".git") {
			continue
		}

		out = append(out, filepath.Join(generationsPath, name))
	}

	return out, nil
}

func cleanupStaleWorkerCheckoutReceivingGenerations(generationsPath string, ttl time.Duration, now time.Time) error {
	entries, err := os.ReadDir(generationsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("read worker checkout cache receiving generations: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() || !isWorkerCheckoutReceivingGenerationName(entry.Name()) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return fmt.Errorf("stat worker checkout cache receiving generation: %w", err)
		}

		if !workerCheckoutLeaseIsStale(info.ModTime(), ttl, now) {
			continue
		}

		path := filepath.Join(generationsPath, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("remove stale worker checkout cache receiving generation: %w", err)
		}
	}

	return nil
}

func isWorkerCheckoutReceivingGenerationName(name string) bool {
	return strings.HasPrefix(name, workerCheckoutGenerationPrefix) && strings.HasSuffix(name, ".git"+workerCheckoutReceivingSuffix)
}

func currentGenerationPath(repoPath string) (string, error) {
	currentPath := filepath.Join(repoPath, "current")
	target, err := os.Readlink(currentPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", os.ErrNotExist
		}

		return "", fmt.Errorf("read worker checkout cache current generation: %w", err)
	}

	if filepath.IsAbs(target) {
		return filepath.Clean(target), nil
	}

	return filepath.Clean(filepath.Join(repoPath, target)), nil
}

func isWorkerCheckoutGenerationPath(repoPath, generationPath string) bool {
	rel, err := filepath.Rel(filepath.Join(repoPath, "generations"), generationPath)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || rel == ".." {
		return false
	}

	return strings.HasPrefix(filepath.Base(generationPath), workerCheckoutGenerationPrefix)
}

func workerCheckoutGenerationHasLeases(repoPath, generationPath string, leaseTTL time.Duration, now time.Time) (bool, error) {
	leases, err := workerCheckoutFreshLeaseCount(filepath.Join(repoPath, "leases", filepath.Base(generationPath)), leaseTTL, now, true)
	return leases > 0, err
}

func cleanupStaleWorkerCheckoutLeases(repoPath string, leaseTTL time.Duration, now time.Time) error {
	leaseRoot := filepath.Join(repoPath, "leases")
	entries, err := os.ReadDir(leaseRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("read worker checkout cache leases: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		leaseDir := filepath.Join(leaseRoot, entry.Name())
		if _, err := workerCheckoutFreshLeaseCount(leaseDir, leaseTTL, now, true); err != nil {
			return err
		}
		_ = os.Remove(leaseDir)
	}

	return nil
}

func workerCheckoutFreshLeaseCount(leaseDir string, leaseTTL time.Duration, now time.Time, removeStale bool) (int64, error) {
	entries, err := os.ReadDir(leaseDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, fmt.Errorf("read worker checkout cache generation leases: %w", err)
	}

	var leases int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(leaseDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return leases, fmt.Errorf("stat worker checkout cache generation lease: %w", err)
		}

		if workerCheckoutLeaseIsStale(info.ModTime(), leaseTTL, now) {
			if !removeStale {
				continue
			}

			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return leases, fmt.Errorf("remove stale worker checkout cache generation lease: %w", err)
			}

			continue
		}

		leases++
	}

	return leases, nil
}

func workerCheckoutLeaseIsStale(modTime time.Time, leaseTTL time.Duration, now time.Time) bool {
	if leaseTTL <= 0 || modTime.IsZero() || now.Before(modTime) {
		return false
	}

	return now.Sub(modTime) >= leaseTTL
}

func cleanupEmptyWorkerCheckoutLeaseDirs(repoPath string) error {
	leaseRoot := filepath.Join(repoPath, "leases")
	entries, err := os.ReadDir(leaseRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("read worker checkout cache leases: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		leaseDir := filepath.Join(leaseRoot, entry.Name())
		_ = os.Remove(leaseDir)
	}

	return nil
}

func workerCheckoutLeaseName() string {
	return fmt.Sprintf("%020d-%d", time.Now().UnixNano(), os.Getpid())
}

func newWorkerCheckoutGenerationLease(path string, leaseTTL time.Duration) *workerCheckoutGenerationLease {
	stop, done := startWorkerCheckoutLeaseHeartbeat(path, leaseTTL)
	return &workerCheckoutGenerationLease{
		path:          path,
		stopHeartbeat: stop,
		heartbeatDone: done,
	}
}

func startWorkerCheckoutLeaseHeartbeat(path string, leaseTTL time.Duration) (func(), <-chan struct{}) {
	interval := workerCheckoutLeaseHeartbeatInterval(leaseTTL)
	if strings.TrimSpace(path) == "" || interval <= 0 {
		return nil, nil
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case now := <-ticker.C:
				if err := os.Chtimes(path, now, now); errors.Is(err, os.ErrNotExist) {
					return
				}
			}
		}
	}()

	return func() { close(stop) }, done
}

func workerCheckoutLeaseHeartbeatInterval(leaseTTL time.Duration) time.Duration {
	if leaseTTL <= 0 {
		return 0
	}

	interval := leaseTTL / 4
	if interval < 10*time.Millisecond {
		return 10 * time.Millisecond
	}

	if interval > time.Minute {
		return time.Minute
	}

	return interval
}

func sameCleanPath(a, b string) bool {
	return filepath.Clean(a) == filepath.Clean(b)
}

func (l *workerCheckoutGenerationLease) Close() error {
	if l == nil || strings.TrimSpace(l.path) == "" {
		return nil
	}

	path := l.path
	l.path = ""
	if l.stopHeartbeat != nil {
		l.stopHeartbeat()
		l.stopHeartbeat = nil
	}
	if l.heartbeatDone != nil {
		<-l.heartbeatDone
		l.heartbeatDone = nil
	}

	err := os.Remove(path)
	_ = os.Remove(filepath.Dir(path))
	return err
}

func workerCacheMirrorGitArgs(mirrorPath string, args ...string) []string {
	out := gitcmd.NoAutoMaintenanceArgs("--git-dir", mirrorPath)
	out = append(out, args...)
	return out
}

func runWorkerCacheGitNoDir(ctx context.Context, args ...string) error {
	return runWorkerCacheGitCommand(ctx, "", args...)
}

func runWorkerCacheGitNoDirWithEnv(ctx context.Context, env []string, args ...string) error {
	return runWorkerCacheGitCommandWithEnv(ctx, "", env, args...)
}

func runWorkerCacheGitNoDirOutput(ctx context.Context, args ...string) ([]byte, error) {
	return runWorkerCacheGitCommandOutputWithEnv(ctx, "", nil, args...)
}

func runWorkerCacheGit(ctx context.Context, dir string, args ...string) error {
	return runWorkerCacheGitCommand(ctx, dir, managedGitCommandArgs(args...)...)
}

func runWorkerCacheGitCommand(ctx context.Context, dir string, args ...string) error {
	return runWorkerCacheGitCommandWithEnv(ctx, dir, nil, args...)
}

func runWorkerCacheGitCommandWithEnv(ctx context.Context, dir string, env []string, args ...string) error {
	_, err := runWorkerCacheGitCommandOutputWithEnv(ctx, dir, env, args...)
	return err
}

func runWorkerCacheGitCommandOutputWithEnv(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	if strings.TrimSpace(dir) != "" {
		cmd.Dir = dir
	}

	cmd.Env = gitCommandEnv(append([]string{"GIT_TERMINAL_PROMPT=0"}, env...))
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg != "" {
			return nil, fmt.Errorf("%w: %s", err, msg)
		}

		return nil, err
	}

	return out, nil
}
