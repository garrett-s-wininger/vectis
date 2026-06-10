package source

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type ManagedGitCheckoutRequest struct {
	CheckoutPath string
	RemoteURL    string
	DefaultRef   string
}

// SyncManagedGitCheckout materializes or refreshes a Vectis-owned Git checkout.
func SyncManagedGitCheckout(ctx context.Context, req ManagedGitCheckoutRequest) GitCheckoutStatus {
	checkoutPath := strings.TrimSpace(req.CheckoutPath)
	defaultRef := strings.TrimSpace(req.DefaultRef)
	status := GitCheckoutStatus{
		CheckoutPath: checkoutPath,
		DefaultRef:   defaultRef,
	}

	if checkoutPath == "" {
		status.setError("missing_checkout_path", "checkout path is required")
		return status
	}

	info, err := os.Stat(checkoutPath)
	switch {
	case err == nil:
		status.PathExists = true
		status.PathIsDirectory = info.IsDir()
		if !info.IsDir() {
			status.setError("checkout_path_not_directory", "checkout path is not a directory")
			return status
		}

		checkoutStatus := NewGitCheckout(checkoutPath).Status(ctx, "")
		if checkoutStatus.ErrorCode != "" {
			checkoutStatus.DefaultRef = defaultRef
			if checkoutStatus.ErrorCode != "not_git_checkout" || !directoryIsEmpty(checkoutPath) {
				return checkoutStatus
			}

			if err := cloneManagedGitCheckout(ctx, checkoutPath, req.RemoteURL); err != nil {
				checkoutStatus.ErrorCode = ""
				checkoutStatus.ErrorMessage = ""
				checkoutStatus.setError("git_clone_failed", err.Error())
				return checkoutStatus
			}
		} else if err := fetchManagedGitCheckout(ctx, checkoutPath); err != nil {
			checkoutStatus.DefaultRef = defaultRef
			checkoutStatus.setError("git_fetch_failed", err.Error())
			return checkoutStatus
		}
	case os.IsNotExist(err):
		if err := cloneManagedGitCheckout(ctx, checkoutPath, req.RemoteURL); err != nil {
			status.setError("git_clone_failed", err.Error())
			return status
		}
	default:
		status.setError("checkout_path_unavailable", fmt.Sprintf("checkout path is unavailable: %v", err))
		return status
	}

	if err := alignManagedGitCheckoutRef(ctx, checkoutPath, defaultRef); err != nil {
		status := NewGitCheckout(checkoutPath).Status(ctx, "")
		status.DefaultRef = defaultRef
		status.setError("git_ref_update_failed", err.Error())
		return status
	}

	return NewManagedGitCheckout(checkoutPath).Status(ctx, defaultRef)
}

func NewManagedGitCheckout(checkoutPath string, opts ...GitCheckoutOption) *GitCheckout {
	return NewGitCheckout(checkoutPath, append([]GitCheckoutOption{WithRemoteFallback("origin")}, opts...)...)
}

func cloneManagedGitCheckout(ctx context.Context, checkoutPath, remoteURL string) error {
	remoteURL, err := normalizeGitRemoteURL(remoteURL)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(checkoutPath), 0o755); err != nil {
		return fmt.Errorf("create checkout parent: %w", err)
	}

	if _, err := runGitCommand(ctx, "clone", "--filter=blob:none", "--no-checkout", "--", remoteURL, checkoutPath); err != nil {
		return err
	}

	return nil
}

func fetchManagedGitCheckout(ctx context.Context, checkoutPath string) error {
	_, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "fetch", "--filter=blob:none", "--prune", "--tags", "origin")
	return err
}

func alignManagedGitCheckoutRef(ctx context.Context, checkoutPath, defaultRef string) error {
	ref := strings.TrimSpace(defaultRef)
	if ref == "" {
		ref = "HEAD"
	}

	ref, err := normalizeRef(ref)
	if err != nil {
		return nil
	}

	branch := ""
	if ref == "HEAD" {
		out, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD")
		if err != nil {
			return nil
		}

		branch = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(out)), "origin/"))
	} else if b, ok := managedLocalBranchName(ref); ok {
		branch = b
	}

	if branch == "" || !validManagedBranchName(ctx, checkoutPath, branch) {
		return nil
	}

	remoteRef := "refs/remotes/origin/" + branch
	out, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "rev-parse", "--verify", remoteRef+"^{commit}")
	if err != nil {
		return nil
	}

	commit := strings.TrimSpace(string(out))
	if commit == "" {
		return fmt.Errorf("remote ref %s resolved to an empty commit", remoteRef)
	}

	if _, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "update-ref", "refs/heads/"+branch, commit); err != nil {
		return fmt.Errorf("update local branch %s: %w", branch, err)
	}

	if ref == "HEAD" {
		if _, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "symbolic-ref", "HEAD", "refs/heads/"+branch); err != nil {
			return fmt.Errorf("set HEAD to %s: %w", branch, err)
		}
	}

	return nil
}

func managedLocalBranchName(ref string) (string, bool) {
	if strings.HasPrefix(ref, "refs/heads/") {
		branch := strings.TrimPrefix(ref, "refs/heads/")
		return branch, branch != ""
	}

	if strings.HasPrefix(ref, "refs/") || strings.HasPrefix(ref, "origin/") || looksLikeFullObjectID(ref) {
		return "", false
	}

	return ref, true
}

func remoteFallbackBranchName(ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" || ref == "HEAD" || looksLikeFullObjectID(ref) {
		return "", false
	}

	if strings.HasPrefix(ref, "refs/heads/") {
		branch := strings.TrimPrefix(ref, "refs/heads/")
		return branch, branch != ""
	}

	if strings.HasPrefix(ref, "refs/") || strings.HasPrefix(ref, "origin/") {
		return "", false
	}

	return ref, true
}

func validManagedBranchName(ctx context.Context, checkoutPath, branch string) bool {
	if branch == "" || strings.HasPrefix(branch, "-") {
		return false
	}

	_, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "check-ref-format", "--branch", branch)
	return err == nil
}

func normalizeGitRemoteURL(remoteURL string) (string, error) {
	if strings.ContainsAny(remoteURL, "\x00\n\r") {
		return "", fmt.Errorf("%w: canonical_url is not a safe Git remote", ErrInvalidReference)
	}

	remoteURL = strings.TrimSpace(remoteURL)
	if remoteURL == "" {
		return "", fmt.Errorf("%w: canonical_url is required to clone a managed checkout", ErrInvalidReference)
	}

	if strings.HasPrefix(remoteURL, "-") {
		return "", fmt.Errorf("%w: canonical_url is not a safe Git remote", ErrInvalidReference)
	}

	return remoteURL, nil
}

func directoryIsEmpty(dir string) bool {
	entries, err := os.ReadDir(dir)
	return err == nil && len(entries) == 0
}

func looksLikeFullObjectID(ref string) bool {
	if len(ref) != 40 && len(ref) != 64 {
		return false
	}

	for _, r := range ref {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}

	return true
}

func runGitCommand(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("%w: %s", err, msg)
		}

		return nil, err
	}

	return out, nil
}
