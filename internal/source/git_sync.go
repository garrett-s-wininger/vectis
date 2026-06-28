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
	Credentials  GitCredentials
}

type ManagedGitRefHydrationRequest struct {
	CheckoutPath string
	Ref          string
	Credentials  GitCredentials
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

	credentialEnv, credentialCleanup, err := managedGitCredentialEnvironment(req.Credentials)
	if err != nil {
		status.setError("git_credentials_invalid", err.Error())
		return status
	}
	defer credentialCleanup()

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

			if err := cloneManagedGitCheckout(ctx, checkoutPath, req.RemoteURL, credentialEnv); err != nil {
				checkoutStatus.ErrorCode = ""
				checkoutStatus.ErrorMessage = ""
				checkoutStatus.setError("git_clone_failed", err.Error())
				return checkoutStatus
			}
		} else {
			if err := configureManagedGitCheckout(ctx, checkoutPath); err != nil {
				checkoutStatus.DefaultRef = defaultRef
				checkoutStatus.setError("git_config_failed", err.Error())
				return checkoutStatus
			}

			if err := fetchManagedGitCheckout(ctx, checkoutPath, credentialEnv); err != nil {
				checkoutStatus.DefaultRef = defaultRef
				checkoutStatus.setError("git_fetch_failed", err.Error())
				return checkoutStatus
			}
		}
	case os.IsNotExist(err):
		if err := cloneManagedGitCheckout(ctx, checkoutPath, req.RemoteURL, credentialEnv); err != nil {
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

	finalStatus := NewManagedGitCheckout(checkoutPath).Status(ctx, defaultRef)
	if finalStatus.ErrorCode == "default_ref_not_found" && fetchManagedGitMissingDefaultRef(ctx, checkoutPath, defaultRef, credentialEnv) == nil {
		finalStatus = NewManagedGitCheckout(checkoutPath).Status(ctx, defaultRef)
	}

	return finalStatus
}

// HydrateManagedGitRef fetches one safe branch or tag ref into an existing managed checkout.
func HydrateManagedGitRef(ctx context.Context, req ManagedGitRefHydrationRequest) GitCheckoutStatus {
	checkoutPath := strings.TrimSpace(req.CheckoutPath)
	ref := strings.TrimSpace(req.Ref)
	status := GitCheckoutStatus{
		CheckoutPath: checkoutPath,
		DefaultRef:   ref,
	}

	if checkoutPath == "" {
		status.setError("missing_checkout_path", "checkout path is required")
		return status
	}

	if ref == "" {
		status.setError("missing_ref", "ref is required")
		return status
	}

	credentialEnv, credentialCleanup, err := managedGitCredentialEnvironment(req.Credentials)
	if err != nil {
		status.setError("git_credentials_invalid", err.Error())
		return status
	}
	defer credentialCleanup()

	checkoutStatus := NewManagedGitCheckout(checkoutPath).Status(ctx, "")
	if checkoutStatus.ErrorCode != "" {
		checkoutStatus.DefaultRef = ref
		return checkoutStatus
	}

	if err := configureManagedGitCheckout(ctx, checkoutPath); err != nil {
		checkoutStatus.DefaultRef = ref
		checkoutStatus.setError("git_config_failed", err.Error())
		return checkoutStatus
	}

	if err := fetchManagedGitRef(ctx, checkoutPath, credentialEnv, ref); err != nil {
		checkoutStatus.DefaultRef = ref
		checkoutStatus.setError("git_fetch_failed", err.Error())
		return checkoutStatus
	}

	return NewManagedGitCheckout(checkoutPath).Status(ctx, ref)
}

func NewManagedGitCheckout(checkoutPath string, opts ...GitCheckoutOption) *GitCheckout {
	return NewGitCheckout(checkoutPath, append([]GitCheckoutOption{WithRemoteFallback("origin")}, opts...)...)
}

func managedGitCredentialEnvironment(creds GitCredentials) ([]string, func(), error) {
	if creds.IsZero() {
		return nil, func() {}, nil
	}

	return gitCredentialEnvironment(creds)
}

func cloneManagedGitCheckout(ctx context.Context, checkoutPath, remoteURL string, env []string) error {
	remoteURL, err := normalizeGitRemoteURL(remoteURL)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(checkoutPath), 0o755); err != nil {
		return fmt.Errorf("create checkout parent: %w", err)
	}

	if _, err := runGitCommandWithEnv(ctx, env, managedGitCommandArgs("clone", "--filter=blob:none", "--no-checkout", "--no-tags", "--", remoteURL, checkoutPath)...); err != nil {
		return err
	}

	if err := configureManagedGitCheckout(ctx, checkoutPath); err != nil {
		return err
	}

	return nil
}

func fetchManagedGitCheckout(ctx context.Context, checkoutPath string, env []string) error {
	_, err := (execGitRunner{}).RunGitWithInputEnv(ctx, checkoutPath, nil, env, managedGitCommandArgs("fetch", "--filter=blob:none", "--prune", "--no-tags", "--no-auto-gc", "origin")...)
	return err
}

func fetchManagedGitMissingDefaultRef(ctx context.Context, checkoutPath, defaultRef string, env []string) error {
	ref := strings.TrimSpace(defaultRef)
	if ref == "" || ref == "HEAD" || strings.HasPrefix(ref, "origin/") || looksLikeFullObjectID(ref) {
		return fmt.Errorf("%w: default ref is not a targeted fetch candidate", ErrInvalidReference)
	}

	normalized, err := normalizeRef(ref)
	if err != nil {
		return err
	}

	if strings.HasPrefix(normalized, "refs/tags/") {
		return fetchManagedGitRef(ctx, checkoutPath, env, normalized)
	}

	if strings.HasPrefix(normalized, "refs/") {
		return fmt.Errorf("%w: default ref %q is not supported for targeted fallback", ErrInvalidReference, normalized)
	}

	tagRef, err := normalizeRef("refs/tags/" + normalized)
	if err != nil {
		return err
	}

	return fetchManagedGitRef(ctx, checkoutPath, env, tagRef)
}

func fetchManagedGitRef(ctx context.Context, checkoutPath string, env []string, ref string) error {
	ref, err := normalizeRef(ref)
	if err != nil {
		return err
	}

	sourceRef, destRef, ok := managedGitFetchRefspec(ref)
	if !ok {
		return fmt.Errorf("%w: managed fetch ref %q is not supported", ErrInvalidReference, ref)
	}

	refspec := "+" + sourceRef + ":" + destRef
	_, err = (execGitRunner{}).RunGitWithInputEnv(ctx, checkoutPath, nil, env, managedGitCommandArgs("fetch", "--filter=blob:none", "--no-tags", "--no-auto-gc", "origin", refspec)...)
	return err
}

func managedGitFetchRefspec(ref string) (string, string, bool) {
	ref = strings.TrimSpace(ref)
	switch {
	case ref == "" || ref == "HEAD" || looksLikeFullObjectID(ref):
		return "", "", false
	case strings.HasPrefix(ref, "refs/heads/"):
		branch := strings.TrimPrefix(ref, "refs/heads/")
		if branch == "" {
			return "", "", false
		}

		return ref, "refs/remotes/origin/" + branch, true
	case strings.HasPrefix(ref, "refs/tags/"):
		tag := strings.TrimPrefix(ref, "refs/tags/")
		if tag == "" {
			return "", "", false
		}

		return ref, ref, true
	default:
		branch, ok := managedLocalBranchName(ref)
		if !ok || branch == "" {
			return "", "", false
		}

		return "refs/heads/" + branch, "refs/remotes/origin/" + branch, true
	}
}

func configureManagedGitCheckout(ctx context.Context, checkoutPath string) error {
	settings := [][2]string{
		{"gc.auto", "0"},
		{"maintenance.auto", "false"},
		{"fetch.writeCommitGraph", "false"},
		{"remote.origin.tagOpt", "--no-tags"},
	}

	for _, setting := range settings {
		if _, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "config", "--local", setting[0], setting[1]); err != nil {
			return fmt.Errorf("set git config %s: %w", setting[0], err)
		}
	}

	return nil
}

func managedGitCommandArgs(args ...string) []string {
	out := []string{
		"-c", "gc.auto=0",
		"-c", "maintenance.auto=false",
		"-c", "fetch.writeCommitGraph=false",
	}

	out = append(out, args...)
	return out
}

func alignManagedGitCheckoutRef(ctx context.Context, checkoutPath, defaultRef string) error {
	ref := strings.TrimSpace(defaultRef)
	if ref == "" {
		ref = "HEAD"
	}

	normalizedRef, ok := normalizeManagedGitSyncRef(ref)
	if !ok {
		return nil
	}
	ref = normalizedRef

	branch := ""
	if ref == "HEAD" {
		out, ok := managedGitCommandOutput(ctx, checkoutPath, "symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD")
		if !ok {
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
	out, ok := managedGitCommandOutput(ctx, checkoutPath, "rev-parse", "--verify", remoteRef+"^{commit}")
	if !ok {
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

func normalizeManagedGitSyncRef(ref string) (string, bool) {
	normalized, err := normalizeRef(ref)
	return normalized, err == nil
}

func managedGitCommandOutput(ctx context.Context, checkoutPath string, args ...string) ([]byte, bool) {
	out, err := (execGitRunner{}).RunGit(ctx, checkoutPath, args...)
	return out, err == nil
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
	return runGitCommandWithEnv(ctx, nil, args...)
}

func runGitCommandWithEnv(ctx context.Context, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}

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
