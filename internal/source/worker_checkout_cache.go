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
	"strings"
	"time"

	"vectis/internal/interfaces"
)

const workerCheckoutGenerationPrefix = "generation-"

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

	handled, normalizedRemoteURL, err := c.normalizePersistentRemote(remoteURL)
	if err != nil || !handled {
		return handled, err
	}

	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return true, fmt.Errorf("%w: workspace is required", ErrInvalidReference)
	}

	mirrorPath, err := c.currentMirrorPath(normalizedRemoteURL)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return true, err
		}

		handled, normalizedRemoteURL, err = c.WarmRemote(ctx, normalizedRemoteURL, logger)
		if err != nil || !handled {
			return handled, err
		}

		mirrorPath, err = c.currentMirrorPath(normalizedRemoteURL)
		if err != nil {
			return true, err
		}
	}

	if resolved, err := filepath.EvalSymlinks(mirrorPath); err == nil {
		mirrorPath = resolved
	}

	if logger != nil {
		logger.Info("Cloning repository from worker checkout cache: %s", normalizedRemoteURL)
	}

	if err := runWorkerCacheGit(ctx, workspace, "clone", "--shared", "--local", "--", mirrorPath, "."); err != nil {
		return true, fmt.Errorf("clone from worker checkout cache: %w", err)
	}

	if err := runWorkerCacheGit(ctx, workspace, "remote", "set-url", "origin", normalizedRemoteURL); err != nil {
		return true, fmt.Errorf("restore checkout origin URL: %w", err)
	}

	return true, nil
}

func (c *WorkerCheckoutCache) WarmRemote(ctx context.Context, remoteURL string, logger interfaces.Logger) (bool, string, error) {
	if c == nil {
		return false, "", nil
	}

	handled, remoteURL, err := c.normalizePersistentRemote(remoteURL)
	if err != nil || !handled {
		return handled, remoteURL, err
	}

	repoPath := c.repositoryPath(remoteURL)
	lock, err := acquireManagedGitWriterLock(ctx, repoPath)
	if err != nil {
		return true, remoteURL, err
	}
	defer lock.Close()

	if err := c.ensureMirror(ctx, remoteURL, repoPath, logger); err != nil {
		return true, remoteURL, err
	}

	return true, remoteURL, nil
}

func (c *WorkerCheckoutCache) normalizePersistentRemote(remoteURL string) (bool, string, error) {
	remoteURL, err := NormalizeGitRemoteURL(remoteURL)
	if err != nil {
		return false, "", nil
	}

	if _, ok := c.persistentRemoteURL[remoteURL]; !ok {
		return false, "", nil
	}

	return true, remoteURL, nil
}

func (c *WorkerCheckoutCache) ensureMirror(ctx context.Context, remoteURL, repoPath string, logger interfaces.Logger) error {
	currentPath, err := c.currentMirrorPath(remoteURL)
	switch {
	case err == nil:
		if logger != nil {
			logger.Info("Refreshing worker checkout cache mirror: %s", remoteURL)
		}
	case errors.Is(err, os.ErrNotExist):
		if logger != nil {
			logger.Info("Creating worker checkout cache mirror: %s", remoteURL)
		}
	default:
		return err
	}

	generationPath, err := c.buildMirrorGeneration(ctx, remoteURL, repoPath, currentPath)
	if err != nil {
		return err
	}

	if err := c.flipCurrentGeneration(repoPath, generationPath); err != nil {
		return err
	}

	return nil
}

func (c *WorkerCheckoutCache) buildMirrorGeneration(ctx context.Context, remoteURL, repoPath, currentPath string) (string, error) {
	generationsPath := filepath.Join(repoPath, "generations")
	if err := os.MkdirAll(generationsPath, 0o755); err != nil {
		return "", fmt.Errorf("create worker checkout cache generations parent: %w", err)
	}

	generationPath := c.newGenerationPath(generationsPath)
	tmp := generationPath + ".receiving"
	_ = os.RemoveAll(tmp)

	if currentPath == "" {
		if err := runWorkerCacheGitNoDir(ctx, managedGitCommandArgs("clone", "--mirror", "--", remoteURL, tmp)...); err != nil {
			_ = os.RemoveAll(tmp)
			return "", fmt.Errorf("clone worker checkout cache mirror: %w", err)
		}
	} else {
		if resolved, err := filepath.EvalSymlinks(currentPath); err == nil {
			currentPath = resolved
		}

		if err := runWorkerCacheGitNoDir(ctx, managedGitCommandArgs("clone", "--mirror", "--local", "--", currentPath, tmp)...); err != nil {
			_ = os.RemoveAll(tmp)
			return "", fmt.Errorf("clone worker checkout cache generation: %w", err)
		}

		if err := runWorkerCacheGitNoDir(ctx, workerCacheMirrorGitArgs(tmp, "remote", "set-url", "origin", remoteURL)...); err != nil {
			_ = os.RemoveAll(tmp)
			return "", fmt.Errorf("set worker checkout cache generation origin: %w", err)
		}

		if err := runWorkerCacheGitNoDir(ctx, append(workerCacheMirrorGitArgs(tmp, "fetch", "--prune", "--no-auto-gc", "origin", "+refs/*:refs/*"))...); err != nil {
			_ = os.RemoveAll(tmp)
			return "", fmt.Errorf("refresh worker checkout cache mirror: %w", err)
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

func configureWorkerCheckoutCacheMirror(ctx context.Context, mirrorPath string) error {
	for _, setting := range managedGitMaintenanceSettings {
		if err := runWorkerCacheGitNoDir(ctx, workerCacheMirrorGitArgs(mirrorPath, "config", "--local", setting[0], setting[1])...); err != nil {
			return fmt.Errorf("set worker checkout cache git config %s: %w", setting[0], err)
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
	repoPath := c.repositoryPath(remoteURL)
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

	legacyPath := c.legacyMirrorPath(remoteURL)
	if _, statErr := os.Stat(legacyPath); statErr == nil {
		return legacyPath, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", fmt.Errorf("stat legacy worker checkout cache mirror: %w", statErr)
	}

	return "", os.ErrNotExist
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
