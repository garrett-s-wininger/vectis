package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"vectis/internal/interfaces"
)

type WorkerCheckoutCache struct {
	root                string
	persistentRemoteURL map[string]struct{}
}

func NewWorkerCheckoutCache(root string, persistentRemoteURLs []string) (*WorkerCheckoutCache, error) {
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
		persistentRemoteURL: make(map[string]struct{}, len(persistentRemoteURLs)),
	}

	for _, raw := range persistentRemoteURLs {
		remoteURL, err := NormalizeGitRemoteURL(raw)
		if err != nil {
			return nil, fmt.Errorf("worker checkout cache remote: %w", err)
		}

		cache.persistentRemoteURL[remoteURL] = struct{}{}
	}

	return cache, nil
}

func (c *WorkerCheckoutCache) Checkout(ctx context.Context, remoteURL, workspace string, logger interfaces.Logger) (bool, error) {
	if c == nil {
		return false, nil
	}

	remoteURL, err := NormalizeGitRemoteURL(remoteURL)
	if err != nil {
		return false, nil
	}

	if _, ok := c.persistentRemoteURL[remoteURL]; !ok {
		return false, nil
	}

	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return true, fmt.Errorf("%w: workspace is required", ErrInvalidReference)
	}

	mirrorPath := c.mirrorPath(remoteURL)
	lock, err := acquireManagedGitWriterLock(ctx, mirrorPath)
	if err != nil {
		return true, err
	}
	defer lock.Close()

	if err := c.ensureMirror(ctx, remoteURL, mirrorPath, logger); err != nil {
		return true, err
	}

	if logger != nil {
		logger.Info("Cloning repository from worker checkout cache: %s", remoteURL)
	}

	if err := runWorkerCacheGit(ctx, workspace, "clone", "--shared", "--local", "--", mirrorPath, "."); err != nil {
		return true, fmt.Errorf("clone from worker checkout cache: %w", err)
	}

	if err := runWorkerCacheGit(ctx, workspace, "remote", "set-url", "origin", remoteURL); err != nil {
		return true, fmt.Errorf("restore checkout origin URL: %w", err)
	}

	return true, nil
}

func (c *WorkerCheckoutCache) ensureMirror(ctx context.Context, remoteURL, mirrorPath string, logger interfaces.Logger) error {
	if _, err := os.Stat(mirrorPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat worker checkout cache mirror: %w", err)
		}

		if logger != nil {
			logger.Info("Creating worker checkout cache mirror: %s", remoteURL)
		}

		return c.cloneMirror(ctx, remoteURL, mirrorPath)
	}

	if logger != nil {
		logger.Info("Refreshing worker checkout cache mirror: %s", remoteURL)
	}

	if err := runWorkerCacheGitNoDir(ctx, append(workerCacheMirrorGitArgs(mirrorPath, "fetch", "--prune", "--no-auto-gc", "origin", "+refs/*:refs/*"))...); err != nil {
		return fmt.Errorf("refresh worker checkout cache mirror: %w", err)
	}

	return configureWorkerCheckoutCacheMirror(ctx, mirrorPath)
}

func (c *WorkerCheckoutCache) cloneMirror(ctx context.Context, remoteURL, mirrorPath string) error {
	if err := os.MkdirAll(filepath.Dir(mirrorPath), 0o755); err != nil {
		return fmt.Errorf("create worker checkout cache parent: %w", err)
	}

	tmp := mirrorPath + ".tmp"
	_ = os.RemoveAll(tmp)
	if err := runWorkerCacheGitNoDir(ctx, managedGitCommandArgs("clone", "--mirror", "--", remoteURL, tmp)...); err != nil {
		_ = os.RemoveAll(tmp)
		return fmt.Errorf("clone worker checkout cache mirror: %w", err)
	}

	if err := configureWorkerCheckoutCacheMirror(ctx, tmp); err != nil {
		_ = os.RemoveAll(tmp)
		return err
	}

	_ = os.RemoveAll(mirrorPath)
	if err := os.Rename(tmp, mirrorPath); err != nil {
		_ = os.RemoveAll(tmp)
		return fmt.Errorf("install worker checkout cache mirror: %w", err)
	}

	return nil
}

func configureWorkerCheckoutCacheMirror(ctx context.Context, mirrorPath string) error {
	for _, setting := range managedGitMaintenanceSettings {
		if err := runWorkerCacheGitNoDir(ctx, workerCacheMirrorGitArgs(mirrorPath, "config", "--local", setting[0], setting[1])...); err != nil {
			return fmt.Errorf("set worker checkout cache git config %s: %w", setting[0], err)
		}
	}

	return nil
}

func (c *WorkerCheckoutCache) mirrorPath(remoteURL string) string {
	sum := sha256.Sum256([]byte(remoteURL))
	return filepath.Join(c.root, "mirrors", hex.EncodeToString(sum[:])+".git")
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
