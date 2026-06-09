package source

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

type GitCheckoutOption func(*GitCheckout)

type GitCheckout struct {
	checkoutPath string
	maxFileBytes int64
	runner       gitRunner
}

var _ Repository = (*GitCheckout)(nil)

type GitCheckoutStatus struct {
	CheckoutPath       string
	PathExists         bool
	PathIsDirectory    bool
	GitRepository      bool
	WorkTreePath       string
	HeadRef            string
	DefaultRef         string
	DefaultRefResolved bool
	ResolvedCommit     string
	ErrorCode          string
	ErrorMessage       string
}

func NewGitCheckout(checkoutPath string, opts ...GitCheckoutOption) *GitCheckout {
	g := &GitCheckout{
		checkoutPath: strings.TrimSpace(checkoutPath),
		maxFileBytes: DefaultMaxFileBytes,
		runner:       execGitRunner{},
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func WithMaxFileBytes(maxBytes int64) GitCheckoutOption {
	return func(g *GitCheckout) {
		if maxBytes > 0 {
			g.maxFileBytes = maxBytes
		}
	}
}

func (g *GitCheckout) Path() string {
	return g.checkoutPath
}

func (g *GitCheckout) Status(ctx context.Context, defaultRef string) GitCheckoutStatus {
	status := GitCheckoutStatus{
		CheckoutPath: strings.TrimSpace(g.checkoutPath),
		DefaultRef:   strings.TrimSpace(defaultRef),
	}

	if status.CheckoutPath == "" {
		status.setError("missing_checkout_path", "checkout path is required")
		return status
	}

	info, err := os.Stat(status.CheckoutPath)
	if err != nil {
		if os.IsNotExist(err) {
			status.setError("checkout_path_missing", "checkout path does not exist")
			return status
		}

		status.setError("checkout_path_unavailable", fmt.Sprintf("checkout path is unavailable: %v", err))
		return status
	}

	status.PathExists = true
	status.PathIsDirectory = info.IsDir()
	if !status.PathIsDirectory {
		status.setError("checkout_path_not_directory", "checkout path is not a directory")
		return status
	}

	out, err := g.run(ctx, "rev-parse", "--is-inside-work-tree")
	if err != nil || strings.TrimSpace(string(out)) != "true" {
		status.setError("not_git_checkout", "checkout path is not inside a git work tree")
		return status
	}

	status.GitRepository = true

	if out, err := g.run(ctx, "rev-parse", "--show-toplevel"); err == nil {
		status.WorkTreePath = strings.TrimSpace(string(out))
	}

	if out, err := g.run(ctx, "symbolic-ref", "--quiet", "--short", "HEAD"); err == nil {
		status.HeadRef = strings.TrimSpace(string(out))
	}

	if status.DefaultRef == "" {
		return status
	}

	ref, err := normalizeRef(status.DefaultRef)
	if err != nil {
		status.setError("default_ref_invalid", "default ref is not a safe git revision")
		return status
	}

	out, err = g.run(ctx, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		status.setError("default_ref_not_found", "default ref does not resolve to a commit")
		return status
	}

	commit := strings.TrimSpace(string(out))
	if commit == "" {
		status.setError("default_ref_invalid", "default ref resolved to an empty commit")
		return status
	}

	status.DefaultRefResolved = true
	status.ResolvedCommit = commit
	return status
}

func (g *GitCheckout) ResolveRevision(ctx context.Context, ref string) (Revision, error) {
	if err := g.validateCheckout(); err != nil {
		return Revision{}, err
	}

	ref, err := normalizeRef(ref)
	if err != nil {
		return Revision{}, err
	}

	out, err := g.run(ctx, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return Revision{}, fmt.Errorf("%w: revision %q", ErrNotFound, ref)
	}

	commit := strings.TrimSpace(string(out))
	if commit == "" {
		return Revision{}, fmt.Errorf("%w: revision %q resolved to an empty commit", ErrInvalidReference, ref)
	}

	return Revision{Commit: commit}, nil
}

func (g *GitCheckout) ReadFile(ctx context.Context, revision Revision, filePath string) (File, error) {
	if err := g.validateCheckout(); err != nil {
		return File{}, err
	}

	commit, err := normalizeCommit(revision.Commit)
	if err != nil {
		return File{}, err
	}

	cleanPath, err := normalizeTreePath(filePath)
	if err != nil {
		return File{}, err
	}

	blobSHA, err := g.resolveBlob(ctx, commit, cleanPath)
	if err != nil {
		return File{}, err
	}

	size, err := g.blobSize(ctx, blobSHA)
	if err != nil {
		return File{}, err
	}

	if g.maxFileBytes > 0 && size > g.maxFileBytes {
		return File{}, fmt.Errorf("%w: %s has %d bytes (limit %d)", ErrTooLarge, cleanPath, size, g.maxFileBytes)
	}

	content, err := g.run(ctx, "cat-file", "-p", blobSHA)
	if err != nil {
		return File{}, fmt.Errorf("%w: read %s at %s", ErrNotFound, cleanPath, commit)
	}

	return File{
		Path:     cleanPath,
		Revision: Revision{Commit: commit},
		BlobSHA:  blobSHA,
		Content:  content,
	}, nil
}

func (g *GitCheckout) validateCheckout() error {
	checkoutPath := strings.TrimSpace(g.checkoutPath)
	if checkoutPath == "" {
		return fmt.Errorf("%w: checkout path is required", ErrInvalidReference)
	}

	info, err := os.Stat(checkoutPath)
	if err != nil {
		return fmt.Errorf("%w: checkout path %s: %v", ErrNotFound, checkoutPath, err)
	}

	if !info.IsDir() {
		return fmt.Errorf("%w: checkout path is not a directory: %s", ErrInvalidReference, checkoutPath)
	}

	return nil
}

func (g *GitCheckout) resolveBlob(ctx context.Context, commit, filePath string) (string, error) {
	out, err := g.run(ctx, "rev-parse", "--verify", commit+":"+filePath)
	if err != nil {
		return "", fmt.Errorf("%w: %s at %s", ErrNotFound, filePath, commit)
	}

	blobSHA := strings.TrimSpace(string(out))
	if blobSHA == "" {
		return "", fmt.Errorf("%w: %s at %s resolved to an empty object", ErrInvalidReference, filePath, commit)
	}

	typ, err := g.run(ctx, "cat-file", "-t", blobSHA)
	if err != nil {
		return "", fmt.Errorf("%w: object %s", ErrNotFound, blobSHA)
	}

	if strings.TrimSpace(string(typ)) != "blob" {
		return "", fmt.Errorf("%w: %s at %s is not a file", ErrNotFound, filePath, commit)
	}

	return blobSHA, nil
}

func (g *GitCheckout) blobSize(ctx context.Context, blobSHA string) (int64, error) {
	out, err := g.run(ctx, "cat-file", "-s", blobSHA)
	if err != nil {
		return 0, fmt.Errorf("%w: object %s", ErrNotFound, blobSHA)
	}

	size, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: object %s size %q", ErrInvalidReference, blobSHA, strings.TrimSpace(string(out)))
	}

	return size, nil
}

func (g *GitCheckout) run(ctx context.Context, args ...string) ([]byte, error) {
	runner := g.runner
	if runner == nil {
		runner = execGitRunner{}
	}

	return runner.RunGit(ctx, g.checkoutPath, args...)
}

func (s *GitCheckoutStatus) setError(code, message string) {
	if s.ErrorCode == "" {
		s.ErrorCode = code
		s.ErrorMessage = message
	}
}

func normalizeRef(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", fmt.Errorf("%w: revision is required", ErrInvalidReference)
	}

	if strings.HasPrefix(ref, "-") || strings.ContainsAny(ref, ":\x00\n\r") {
		return "", fmt.Errorf("%w: unsafe revision %q", ErrInvalidReference, ref)
	}

	return ref, nil
}

func normalizeCommit(commit string) (string, error) {
	commit = strings.TrimSpace(commit)
	if commit == "" {
		return "", fmt.Errorf("%w: commit is required", ErrInvalidReference)
	}

	if len(commit) != 40 && len(commit) != 64 {
		return "", fmt.Errorf("%w: commit %q is not a full object id", ErrInvalidReference, commit)
	}

	for _, r := range commit {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return "", fmt.Errorf("%w: commit %q is not hexadecimal", ErrInvalidReference, commit)
		}
	}

	return commit, nil
}

func normalizeTreePath(filePath string) (string, error) {
	filePath = strings.TrimSpace(filepath.ToSlash(filePath))
	if filePath == "" {
		return "", fmt.Errorf("%w: file path is required", ErrInvalidReference)
	}

	if strings.ContainsAny(filePath, "\x00\n\r") || path.IsAbs(filePath) {
		return "", fmt.Errorf("%w: unsafe file path %q", ErrInvalidReference, filePath)
	}

	cleanPath := path.Clean(filePath)
	if cleanPath == "." || cleanPath == ".." || strings.HasPrefix(cleanPath, "../") {
		return "", fmt.Errorf("%w: unsafe file path %q", ErrInvalidReference, filePath)
	}

	return cleanPath, nil
}

type gitRunner interface {
	RunGit(ctx context.Context, checkoutPath string, args ...string) ([]byte, error)
}

type execGitRunner struct{}

func (execGitRunner) RunGit(ctx context.Context, checkoutPath string, args ...string) ([]byte, error) {
	gitArgs := append([]string{"-C", checkoutPath}, args...)
	cmd := exec.CommandContext(ctx, "git", gitArgs...)

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
