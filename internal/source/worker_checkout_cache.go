package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"vectis/internal/interfaces"
	"vectis/internal/observability"
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

type WorkerCheckoutCacheRemote struct {
	RemoteURL          string
	FallbackRemoteURLs []string
}

type WorkerCheckoutCache struct {
	root                string
	persistentRemoteURL map[string]WorkerCheckoutCacheRemote
	generationsToKeep   int
	leaseTTL            time.Duration
	cloneRecorder       WorkerCheckoutCacheCloneRecorder
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

func WithWorkerCheckoutCacheCloneRecorder(recorder WorkerCheckoutCacheCloneRecorder) WorkerCheckoutCacheOption {
	return func(c *WorkerCheckoutCache) {
		c.cloneRecorder = recorder
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

	for _, raw := range persistentRemotes {
		remoteURL, err := NormalizeGitRemoteURL(raw.RemoteURL)
		if err != nil {
			return nil, fmt.Errorf("worker checkout cache remote: %w", err)
		}

		fallbackRemoteURLs, err := normalizeWorkerCheckoutCacheFallbackRemoteURLs(remoteURL, raw.FallbackRemoteURLs)
		if err != nil {
			return nil, err
		}

		if existing, ok := cache.persistentRemoteURL[remoteURL]; ok {
			fallbackRemoteURLs = append(existing.FallbackRemoteURLs, fallbackRemoteURLs...)
			fallbackRemoteURLs, err = normalizeWorkerCheckoutCacheFallbackRemoteURLs(remoteURL, fallbackRemoteURLs)
			if err != nil {
				return nil, err
			}
		}

		cache.persistentRemoteURL[remoteURL] = WorkerCheckoutCacheRemote{
			RemoteURL:          remoteURL,
			FallbackRemoteURLs: fallbackRemoteURLs,
		}
	}

	return cache, nil
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

func (c *WorkerCheckoutCache) Checkout(ctx context.Context, remoteURL, workspace string, logger interfaces.Logger) (bool, error) {
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
	defer lease.Close()

	if logger != nil {
		logger.Info("Cloning repository from worker checkout cache: %s", normalizedRemoteURL)
	}

	mode, reason, err := cloneWorkerCheckoutCacheWorkspace(ctx, workspace, mirrorPath)
	if err != nil {
		return true, fmt.Errorf("clone from worker checkout cache: %w", err)
	}
	c.recordClone(ctx, mode, reason)

	cacheRemoteURL := filepath.Join(c.repositoryPath(normalizedRemoteURL), "current")
	if err := configureWorkerCheckoutCacheWorkspace(ctx, workspace, normalizedRemoteURL, cacheRemoteURL); err != nil {
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

func cloneWorkerCheckoutCacheWorkspace(ctx context.Context, workspace, mirrorPath string) (string, string, error) {
	return cloneWorkerCheckoutCacheWorkspaceWithRunner(ctx, workspace, mirrorPath, runWorkerCacheGit)
}

func cloneWorkerCheckoutCacheWorkspaceWithRunner(ctx context.Context, workspace, mirrorPath string, run workerCheckoutCacheGitRunner) (string, string, error) {
	return cloneWorkerCheckoutCacheWorkspaceWithRunnerAndProbe(ctx, workspace, mirrorPath, run, workerCheckoutCacheHardlinksAvailable)
}

func cloneWorkerCheckoutCacheWorkspaceWithRunnerAndProbe(ctx context.Context, workspace, mirrorPath string, run workerCheckoutCacheGitRunner, canHardlink workerCheckoutCacheHardlinkProbe) (string, string, error) {
	if run == nil {
		return "", "", fmt.Errorf("%w: worker checkout cache git runner is required", ErrInvalidReference)
	}

	cloneArgs := []string{"clone", "--local"}
	mode := observability.CheckoutCacheCloneModeHardlink
	reason := observability.CheckoutCacheCloneReasonOK
	if canHardlink != nil && !canHardlink(mirrorPath, workspace) {
		cloneArgs = append(cloneArgs, "--no-hardlinks")
		mode = observability.CheckoutCacheCloneModeCopy
		reason = observability.CheckoutCacheCloneReasonProbe
	}

	cloneArgs = append(cloneArgs, "--", mirrorPath, ".")
	err := run(ctx, workspace, cloneArgs...)
	if err == nil {
		return mode, reason, nil
	}

	if mode != observability.CheckoutCacheCloneModeHardlink || !workerCheckoutCacheCloneNeedsNoHardlinksRetry(err) {
		return "", "", err
	}

	if cleanupErr := cleanupWorkerCheckoutCachePartialClone(workspace); cleanupErr != nil {
		return "", "", fmt.Errorf("%w; cleanup partial worker checkout cache clone: %v", err, cleanupErr)
	}

	if retryErr := run(ctx, workspace, "clone", "--local", "--no-hardlinks", "--", mirrorPath, "."); retryErr != nil {
		return "", "", fmt.Errorf("%w; retry without hardlinks: %v", err, retryErr)
	}

	return observability.CheckoutCacheCloneModeCopy, observability.CheckoutCacheCloneReasonRetry, nil
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
	if c == nil {
		return false, "", nil
	}

	handled, persistentRemote, err := c.lookupPersistentRemote(remoteURL)
	if err != nil || !handled {
		return handled, "", err
	}
	remoteURL = persistentRemote.RemoteURL

	repoPath := c.repositoryPath(remoteURL)
	lock, err := acquireManagedGitWriterLock(ctx, repoPath)
	if err != nil {
		return true, remoteURL, err
	}
	defer lock.Close()

	if err := c.ensureMirror(ctx, persistentRemote, repoPath, logger); err != nil {
		return true, remoteURL, err
	}

	return true, remoteURL, nil
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

func (c *WorkerCheckoutCache) ensureMirror(ctx context.Context, remote WorkerCheckoutCacheRemote, repoPath string, logger interfaces.Logger) error {
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
		return err
	}

	generationPath, err := c.buildMirrorGeneration(ctx, remote, repoPath, currentPath)
	if err != nil {
		return err
	}

	if err := c.flipCurrentGeneration(repoPath, generationPath); err != nil {
		return err
	}

	return c.cleanupOldGenerations(ctx, repoPath)
}

func (c *WorkerCheckoutCache) buildMirrorGeneration(ctx context.Context, remote WorkerCheckoutCacheRemote, repoPath, currentPath string) (string, error) {
	generationsPath := filepath.Join(repoPath, "generations")
	if err := os.MkdirAll(generationsPath, 0o755); err != nil {
		return "", fmt.Errorf("create worker checkout cache generations parent: %w", err)
	}

	if err := cleanupStaleWorkerCheckoutReceivingGenerations(generationsPath, c.leaseTTL, time.Now()); err != nil {
		return "", err
	}

	generationPath := c.newGenerationPath(generationsPath)
	tmp := generationPath + workerCheckoutReceivingSuffix
	_ = os.RemoveAll(tmp)

	if currentPath == "" {
		clonedFrom, err := cloneWorkerCheckoutCacheMirror(ctx, remote, tmp)
		if err != nil {
			_ = os.RemoveAll(tmp)
			return "", err
		}

		if clonedFrom != remote.RemoteURL {
			if err := setWorkerCheckoutCacheMirrorOrigin(ctx, tmp, remote.RemoteURL); err != nil {
				_ = os.RemoveAll(tmp)
				return "", err
			}
		}

		_ = fetchWorkerCheckoutCacheFallbackMirrors(ctx, tmp, remote.FallbackRemoteURLs)
	} else {
		if resolved, err := filepath.EvalSymlinks(currentPath); err == nil {
			currentPath = resolved
		}

		if err := runWorkerCacheGitNoDir(ctx, managedGitCommandArgs("clone", "--mirror", "--local", "--", currentPath, tmp)...); err != nil {
			_ = os.RemoveAll(tmp)
			return "", fmt.Errorf("clone worker checkout cache generation: %w", err)
		}

		if err := setWorkerCheckoutCacheMirrorOrigin(ctx, tmp, remote.RemoteURL); err != nil {
			_ = os.RemoveAll(tmp)
			return "", err
		}

		if err := refreshWorkerCheckoutCacheMirror(ctx, tmp, remote); err != nil {
			_ = os.RemoveAll(tmp)
			return "", err
		}
	}

	if err := configureWorkerCheckoutCacheMirror(ctx, tmp); err != nil {
		_ = os.RemoveAll(tmp)
		return "", err
	}

	if err := os.Rename(tmp, generationPath); err != nil {
		_ = os.RemoveAll(tmp)
		return "", fmt.Errorf("install worker checkout cache generation: %w", err)
	}

	return generationPath, nil
}

func cloneWorkerCheckoutCacheMirror(ctx context.Context, remote WorkerCheckoutCacheRemote, tmp string) (string, error) {
	remoteURLs := append([]string{remote.RemoteURL}, remote.FallbackRemoteURLs...)
	var errs []string
	for _, remoteURL := range remoteURLs {
		if err := runWorkerCacheGitNoDir(ctx, managedGitCommandArgs("clone", "--mirror", "--", remoteURL, tmp)...); err != nil {
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

func setWorkerCheckoutCacheMirrorOrigin(ctx context.Context, mirrorPath, remoteURL string) error {
	if err := runWorkerCacheGitNoDir(ctx, workerCacheMirrorGitArgs(mirrorPath, "remote", "set-url", "origin", remoteURL)...); err != nil {
		return fmt.Errorf("set worker checkout cache generation origin: %w", err)
	}

	return nil
}

func refreshWorkerCheckoutCacheMirror(ctx context.Context, mirrorPath string, remote WorkerCheckoutCacheRemote) error {
	primaryErr := fetchWorkerCheckoutCacheMirror(ctx, mirrorPath, "origin", true)
	fallbackErr := fetchWorkerCheckoutCacheFallbackMirrors(ctx, mirrorPath, remote.FallbackRemoteURLs)
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

func fetchWorkerCheckoutCacheFallbackMirrors(ctx context.Context, mirrorPath string, fallbackRemoteURLs []string) error {
	var errs []string
	for _, remoteURL := range fallbackRemoteURLs {
		if err := fetchWorkerCheckoutCacheMirror(ctx, mirrorPath, remoteURL, false); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", remoteURL, err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func fetchWorkerCheckoutCacheMirror(ctx context.Context, mirrorPath, remote string, prune bool) error {
	args := workerCacheMirrorGitArgs(mirrorPath, "fetch")
	if prune {
		args = append(args, "--prune")
	}
	args = append(args, "--no-auto-gc", remote, "+refs/*:refs/*")
	return runWorkerCacheGitNoDir(ctx, args...)
}

func configureWorkerCheckoutCacheMirror(ctx context.Context, mirrorPath string) error {
	for _, setting := range managedGitMaintenanceSettings {
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

	settings := [][2]string{
		{"remote." + workerCheckoutCacheRemoteName + ".tagOpt", "--no-tags"},
		{"remote." + workerCheckoutCacheRemoteName + ".skipDefaultUpdate", "true"},
		{"remote." + workerCheckoutCacheRemoteName + ".skipFetchAll", "true"},
	}

	for _, setting := range settings {
		if err := runWorkerCacheGit(ctx, workspace, "config", "--local", setting[0], setting[1]); err != nil {
			return fmt.Errorf("set worker checkout cache workspace config %s: %w", setting[0], err)
		}
	}

	return nil
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

	if err := os.Rename(tmpLink, filepath.Join(repoPath, "current")); err != nil {
		_ = os.Remove(tmpLink)
		return fmt.Errorf("flip worker checkout cache current generation: %w", err)
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

	if len(generationPaths) <= c.generationsToKeep {
		return cleanupEmptyWorkerCheckoutLeaseDirs(repoPath)
	}

	currentPath, err := currentGenerationPath(repoPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	sort.Sort(sort.Reverse(sort.StringSlice(generationPaths)))
	kept := 0
	for _, generationPath := range generationPaths {
		if currentPath != "" && sameCleanPath(generationPath, currentPath) {
			kept++
			continue
		}

		if kept < c.generationsToKeep {
			kept++
			continue
		}

		leased, err := workerCheckoutGenerationHasLeases(repoPath, generationPath, c.leaseTTL, now)
		if err != nil {
			return err
		}

		if leased {
			continue
		}

		if err := os.RemoveAll(generationPath); err != nil {
			return fmt.Errorf("remove worker checkout cache old generation: %w", err)
		}
	}

	return cleanupEmptyWorkerCheckoutLeaseDirs(repoPath)
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
	out := []string{"--git-dir", mirrorPath}
	out = append(out, args...)
	return out
}

func runWorkerCacheGitNoDir(ctx context.Context, args ...string) error {
	return runWorkerCacheGitCommand(ctx, "", args...)
}

func runWorkerCacheGit(ctx context.Context, dir string, args ...string) error {
	return runWorkerCacheGitCommand(ctx, dir, managedGitCommandArgs(args...)...)
}

func runWorkerCacheGitCommand(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	if strings.TrimSpace(dir) != "" {
		cmd.Dir = dir
	}

	cmd.Env = gitCommandEnv([]string{"GIT_TERMINAL_PROMPT=0"})
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg != "" {
			return fmt.Errorf("%w: %s", err, msg)
		}

		return err
	}

	return nil
}
