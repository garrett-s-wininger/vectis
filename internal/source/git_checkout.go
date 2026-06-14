package source

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"vectis/internal/source/refspec"
)

type GitCheckoutOption func(*GitCheckout)

type GitCheckout struct {
	checkoutPath   string
	maxFileBytes   int64
	remoteFallback string
	runner         gitRunner
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

func WithRemoteFallback(remote string) GitCheckoutOption {
	return func(g *GitCheckout) {
		remote = strings.TrimSpace(remote)
		if remote != "" && !strings.HasPrefix(remote, "-") && !strings.ContainsAny(remote, "\x00\n\r/") {
			g.remoteFallback = remote
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

	commit, err := g.resolveCommit(ctx, ref)
	if err != nil {
		status.setError("default_ref_not_found", "default ref does not resolve to a commit")
		return status
	}

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

	commit, err := g.resolveCommit(ctx, ref)
	if err != nil {
		return Revision{}, fmt.Errorf("%w: revision %q", ErrNotFound, ref)
	}

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

func (g *GitCheckout) CommitFile(ctx context.Context, opts CommitFileOptions) (FileCommit, error) {
	if err := g.validateCheckout(); err != nil {
		return FileCommit{}, err
	}

	requestedRef, branch, err := g.normalizeWriteRef(ctx, opts.Ref)
	if err != nil {
		return FileCommit{}, err
	}

	cleanPath, err := normalizeTreePath(opts.Path)
	if err != nil {
		return FileCommit{}, err
	}

	if g.maxFileBytes > 0 && int64(len(opts.Content)) > g.maxFileBytes {
		return FileCommit{}, fmt.Errorf("%w: %s has %d bytes (limit %d)", ErrTooLarge, cleanPath, len(opts.Content), g.maxFileBytes)
	}

	parent, err := g.resolveCommit(ctx, "refs/heads/"+branch)
	if err != nil {
		return FileCommit{}, fmt.Errorf("%w: branch %q", ErrNotFound, branch)
	}

	if expected := strings.TrimSpace(opts.ExpectedHead); expected != "" {
		expectedCommit, err := normalizeCommit(expected)
		if err != nil {
			return FileCommit{}, err
		}
		if !strings.EqualFold(expectedCommit, parent) {
			return FileCommit{}, fmt.Errorf("%w: branch %s moved from %s to %s", ErrConflict, branch, expectedCommit, parent)
		}
	}

	message := strings.TrimSpace(opts.Message)
	if message == "" {
		message = "Update " + cleanPath
	}

	blobOut, err := g.runWithInputEnv(ctx, opts.Content, nil, "hash-object", "-w", "--stdin")
	if err != nil {
		return FileCommit{}, fmt.Errorf("%w: write blob for %s: %v", ErrInvalidReference, cleanPath, err)
	}
	blobSHA := strings.TrimSpace(string(blobOut))
	if blobSHA == "" {
		return FileCommit{}, fmt.Errorf("%w: write blob for %s returned an empty object id", ErrInvalidReference, cleanPath)
	}

	indexFile, err := tempGitIndexFile()
	if err != nil {
		return FileCommit{}, err
	}
	defer os.Remove(indexFile)

	indexEnv := []string{"GIT_INDEX_FILE=" + indexFile}
	if _, err := g.runWithEnv(ctx, indexEnv, "read-tree", parent); err != nil {
		return FileCommit{}, fmt.Errorf("%w: prepare index for %s: %v", ErrInvalidReference, parent, err)
	}

	if _, err := g.runWithEnv(ctx, indexEnv, "update-index", "--add", "--cacheinfo", "100644", blobSHA, cleanPath); err != nil {
		return FileCommit{}, fmt.Errorf("%w: update index for %s: %v", ErrInvalidReference, cleanPath, err)
	}

	treeOut, err := g.runWithEnv(ctx, indexEnv, "write-tree")
	if err != nil {
		return FileCommit{}, fmt.Errorf("%w: write tree for %s: %v", ErrInvalidReference, cleanPath, err)
	}
	treeSHA := strings.TrimSpace(string(treeOut))
	if treeSHA == "" {
		return FileCommit{}, fmt.Errorf("%w: write tree returned an empty object id", ErrInvalidReference)
	}

	parentTreeOut, err := g.run(ctx, "rev-parse", parent+"^{tree}")
	if err != nil {
		return FileCommit{}, fmt.Errorf("%w: resolve parent tree for %s: %v", ErrInvalidReference, parent, err)
	}
	if strings.TrimSpace(string(parentTreeOut)) == treeSHA {
		return FileCommit{
			RequestedRef: requestedRef,
			Commit:       parent,
			ParentCommit: parent,
			Path:         cleanPath,
			BlobSHA:      blobSHA,
		}, nil
	}

	commitEnv := append([]string{}, indexEnv...)
	commitEnv = append(commitEnv,
		"GIT_AUTHOR_NAME=Vectis",
		"GIT_AUTHOR_EMAIL=vectis@example.invalid",
		"GIT_COMMITTER_NAME=Vectis",
		"GIT_COMMITTER_EMAIL=vectis@example.invalid",
	)
	commitOut, err := g.runWithEnv(ctx, commitEnv, "commit-tree", treeSHA, "-p", parent, "-m", message)
	if err != nil {
		return FileCommit{}, fmt.Errorf("%w: commit %s: %v", ErrInvalidReference, cleanPath, err)
	}
	commitSHA := strings.TrimSpace(string(commitOut))
	if commitSHA == "" {
		return FileCommit{}, fmt.Errorf("%w: commit-tree returned an empty commit id", ErrInvalidReference)
	}

	if _, err := g.run(ctx, "update-ref", "refs/heads/"+branch, commitSHA, parent); err != nil {
		return FileCommit{}, fmt.Errorf("%w: branch %s moved while committing %s: %v", ErrConflict, branch, cleanPath, err)
	}

	return FileCommit{
		RequestedRef: requestedRef,
		Commit:       commitSHA,
		ParentCommit: parent,
		Path:         cleanPath,
		BlobSHA:      blobSHA,
	}, nil
}

func (g *GitCheckout) ListBranches(ctx context.Context, opts ListBranchesOptions) (BranchListing, error) {
	if err := g.validateCheckout(); err != nil {
		return BranchListing{}, err
	}

	prefix, err := normalizeBranchPrefix(opts.Prefix, g.remoteFallback)
	if err != nil {
		return BranchListing{}, err
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultBranchListLimit
	}
	collectLimit := limit + 1

	scopes := []branchRefScope{{baseRef: "refs/heads"}}
	if g.remoteFallback != "" {
		scopes = []branchRefScope{
			{baseRef: "refs/remotes/" + g.remoteFallback, remote: g.remoteFallback},
			{baseRef: "refs/heads"},
		}
	}

	branches := make([]BranchRef, 0, min(limit, DefaultBranchListLimit))
	seen := make(map[string]struct{})
	for _, scope := range scopes {
		if len(branches) >= collectLimit {
			break
		}

		found, err := g.listBranchScope(ctx, scope, prefix, collectLimit)
		if err != nil {
			return BranchListing{}, err
		}

		for _, branch := range found {
			if _, ok := seen[branch.Name]; ok {
				continue
			}

			seen[branch.Name] = struct{}{}
			branches = append(branches, branch)
			if len(branches) >= collectLimit {
				break
			}
		}
	}

	truncated := len(branches) > limit
	if truncated {
		branches = branches[:limit]
	}

	return BranchListing{
		Truncated: truncated,
		Branches:  branches,
	}, nil
}

func (g *GitCheckout) ListTree(ctx context.Context, opts ListTreeOptions) (TreeListing, error) {
	if err := g.validateCheckout(); err != nil {
		return TreeListing{}, err
	}

	ref := strings.TrimSpace(opts.Ref)
	if ref == "" {
		ref = "HEAD"
	}

	revision, err := g.ResolveRevision(ctx, ref)
	if err != nil {
		return TreeListing{}, err
	}

	cleanPath, err := normalizeTreeListPath(opts.Path)
	if err != nil {
		return TreeListing{}, err
	}

	cursor, err := normalizeTreeListCursor(opts.Cursor)
	if err != nil {
		return TreeListing{}, err
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultTreeListLimit
	}

	entries, truncated, nextCursor, err := g.listTreeEntries(ctx, revision.Commit, cleanPath, opts.Recursive, limit, cursor)
	if err != nil {
		return TreeListing{}, err
	}

	return TreeListing{
		RequestedRef: ref,
		Revision:     revision,
		Path:         cleanPath,
		Recursive:    opts.Recursive,
		Truncated:    truncated,
		NextCursor:   nextCursor,
		Entries:      entries,
	}, nil
}

func (g *GitCheckout) ListDefinitionFiles(ctx context.Context, opts ListDefinitionFilesOptions) (DefinitionFileListing, error) {
	if err := g.validateCheckout(); err != nil {
		return DefinitionFileListing{}, err
	}

	ref := strings.TrimSpace(opts.Ref)
	if ref == "" {
		ref = "HEAD"
	}

	revision, err := g.ResolveRevision(ctx, ref)
	if err != nil {
		return DefinitionFileListing{}, err
	}

	treePath := strings.TrimSpace(opts.Path)
	if treePath == "" {
		treePath = DefaultDefinitionPath
	}

	cleanPath, err := normalizeTreeListPath(treePath)
	if err != nil {
		return DefinitionFileListing{}, err
	}

	cursor, err := normalizeTreeListCursor(opts.Cursor)
	if err != nil {
		return DefinitionFileListing{}, err
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultTreeListLimit
	}

	files, truncated, nextCursor, err := g.listDefinitionFileEntries(ctx, revision.Commit, cleanPath, limit, cursor)
	if err != nil {
		return DefinitionFileListing{}, err
	}

	return DefinitionFileListing{
		RequestedRef: ref,
		Revision:     revision,
		Path:         cleanPath,
		Truncated:    truncated,
		NextCursor:   nextCursor,
		Files:        files,
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

type branchRefScope struct {
	baseRef string
	remote  string
}

func (g *GitCheckout) listBranchScope(ctx context.Context, scope branchRefScope, prefix string, limit int) ([]BranchRef, error) {
	if limit <= 0 {
		return nil, nil
	}

	pattern := scope.baseRef
	if prefix != "" {
		pattern += "/" + prefix + "*"
	}

	out, err := g.run(ctx, "for-each-ref", "--format=%(refname)%00%(objectname)", "--count="+strconv.Itoa(limit), pattern)
	if err != nil {
		return nil, fmt.Errorf("%w: list branches: %v", ErrInvalidReference, err)
	}

	lines := bytes.Split(out, []byte{'\n'})
	branches := make([]BranchRef, 0, len(lines))
	for _, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		parts := bytes.SplitN(line, []byte{0}, 2)
		if len(parts) != 2 {
			continue
		}

		fullRef := strings.TrimSpace(string(parts[0]))
		commit := strings.TrimSpace(string(parts[1]))
		name := strings.TrimPrefix(fullRef, scope.baseRef+"/")
		if name == "" || name == "HEAD" || !strings.HasPrefix(name, prefix) || commit == "" {
			continue
		}

		branches = append(branches, BranchRef{
			Name:   name,
			Ref:    fullRef,
			Commit: commit,
			Remote: scope.remote,
		})
	}

	return branches, nil
}

func (g *GitCheckout) listTreeEntries(ctx context.Context, commit, treePath string, recursive bool, limit int, cursor string) ([]TreeEntry, bool, string, error) {
	if limit <= 0 {
		return nil, false, "", nil
	}

	treeish := commit
	if treePath != "" {
		treeish += ":" + treePath
	}

	args := []string{"ls-tree", "-z", "--long"}
	if recursive {
		args = append(args, "-r")
	}

	args = append(args, treeish)
	entries := make([]TreeEntry, 0, min(limit, DefaultTreeListLimit))
	truncated := false
	nextCursor := ""
	err := g.streamGitRecords(ctx, args, func(record []byte) error {
		entry, ok, err := parseTreeEntryRecord(record, treePath)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}

		if cursor != "" && entry.Path <= cursor {
			return nil
		}

		if len(entries) >= limit {
			truncated = true
			nextCursor = entries[len(entries)-1].Path
			return errStopGitStream
		}

		entries = append(entries, entry)
		return nil
	})

	if err != nil {
		if treePath == "" {
			return nil, false, "", fmt.Errorf("%w: list tree at %s: %v", ErrNotFound, commit, err)
		}

		return nil, false, "", fmt.Errorf("%w: list tree %s at %s: %v", ErrNotFound, treePath, commit, err)
	}

	return entries, truncated, nextCursor, nil
}

func (g *GitCheckout) listDefinitionFileEntries(ctx context.Context, commit, treePath string, limit int, cursor string) ([]DefinitionFile, bool, string, error) {
	if limit <= 0 {
		return nil, false, "", nil
	}

	treeish := commit
	if treePath != "" {
		treeish += ":" + treePath
	}

	files := make([]DefinitionFile, 0, min(limit, DefaultTreeListLimit))
	truncated := false
	nextCursor := ""
	err := g.streamGitRecords(ctx, []string{"ls-tree", "-z", "--long", "-r", treeish}, func(record []byte) error {
		entry, ok, err := parseTreeEntryRecord(record, treePath)
		if err != nil {
			return err
		}
		if !ok || entry.Type != "blob" || !strings.HasSuffix(entry.Name, ".json") {
			return nil
		}

		if cursor != "" && entry.Path <= cursor {
			return nil
		}

		if len(files) >= limit {
			truncated = true
			nextCursor = files[len(files)-1].Path
			return errStopGitStream
		}

		files = append(files, DefinitionFile{
			Path:      entry.Path,
			Name:      entry.Name,
			BlobSHA:   entry.ObjectSHA,
			SizeBytes: entry.SizeBytes,
		})
		return nil
	})
	if err != nil {
		if treePath == "" {
			return nil, false, "", fmt.Errorf("%w: list definition files at %s: %v", ErrNotFound, commit, err)
		}

		return nil, false, "", fmt.Errorf("%w: list definition files %s at %s: %v", ErrNotFound, treePath, commit, err)
	}

	return files, truncated, nextCursor, nil
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

func (g *GitCheckout) resolveCommit(ctx context.Context, ref string) (string, error) {
	for _, candidate := range g.refCandidates(ref) {
		out, err := g.run(ctx, "rev-parse", "--verify", candidate+"^{commit}")
		if err != nil {
			continue
		}

		commit := strings.TrimSpace(string(out))
		if commit != "" {
			return commit, nil
		}
	}

	return "", fmt.Errorf("%w: revision %q", ErrNotFound, ref)
}

func (g *GitCheckout) refCandidates(ref string) []string {
	candidates := []string{ref}
	branch, ok := remoteFallbackBranchName(ref)
	if !ok || g.remoteFallback == "" {
		return candidates
	}

	candidates = append(candidates, "refs/remotes/"+g.remoteFallback+"/"+branch)
	return candidates
}

func (g *GitCheckout) normalizeWriteRef(ctx context.Context, ref string) (string, string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		ref = "HEAD"
	}

	requestedRef, err := normalizeRef(ref)
	if err != nil {
		return "", "", err
	}

	var branch string
	if requestedRef == "HEAD" {
		out, err := g.run(ctx, "symbolic-ref", "--quiet", "--short", "HEAD")
		if err != nil {
			return "", "", fmt.Errorf("%w: write ref HEAD does not point to a branch", ErrInvalidReference)
		}
		branch = strings.TrimSpace(string(out))
	} else if b, ok := managedLocalBranchName(requestedRef); ok {
		branch = b
	} else {
		return "", "", fmt.Errorf("%w: write ref must be a local branch or HEAD", ErrInvalidReference)
	}

	if branch == "" || strings.HasPrefix(branch, "-") {
		return "", "", fmt.Errorf("%w: write branch is required", ErrInvalidReference)
	}

	if _, err := g.run(ctx, "check-ref-format", "--branch", branch); err != nil {
		return "", "", fmt.Errorf("%w: unsafe write branch %q", ErrInvalidReference, branch)
	}

	return requestedRef, branch, nil
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

func (g *GitCheckout) runWithEnv(ctx context.Context, env []string, args ...string) ([]byte, error) {
	return g.runWithInputEnv(ctx, nil, env, args...)
}

func (g *GitCheckout) runWithInputEnv(ctx context.Context, input []byte, env []string, args ...string) ([]byte, error) {
	runner := g.runner
	if runner == nil {
		runner = execGitRunner{}
	}

	return runner.RunGitWithInputEnv(ctx, g.checkoutPath, input, env, args...)
}

func (g *GitCheckout) streamGitRecords(ctx context.Context, args []string, handle func([]byte) error) error {
	runner := g.runner
	if runner == nil {
		runner = execGitRunner{}
	}

	return runner.StreamGitRecords(ctx, g.checkoutPath, args, handle)
}

func (s *GitCheckoutStatus) setError(code, message string) {
	if s.ErrorCode == "" {
		s.ErrorCode = code
		s.ErrorMessage = message
	}
}

func NormalizeRef(ref string) (string, error) {
	return normalizeRef(ref)
}

func NormalizeTreePath(filePath string) (string, error) {
	return normalizeTreePath(filePath)
}

func normalizeRef(ref string) (string, error) {
	ref, err := refspec.NormalizeRef(ref)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidReference, err)
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

func normalizeBranchPrefix(prefix, remote string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "", nil
	}

	prefix = strings.TrimPrefix(prefix, "refs/heads/")
	if remote = strings.TrimSpace(remote); remote != "" {
		prefix = strings.TrimPrefix(prefix, "refs/remotes/"+remote+"/")
		prefix = strings.TrimPrefix(prefix, remote+"/")
	}

	if prefix == "" {
		return "", nil
	}

	if strings.HasPrefix(prefix, "-") ||
		strings.HasPrefix(prefix, "/") ||
		strings.Contains(prefix, "//") ||
		strings.Contains(prefix, "..") ||
		strings.ContainsAny(prefix, "\x00\n\r~^:?*[\\") {
		return "", fmt.Errorf("%w: unsafe branch prefix %q", ErrInvalidReference, prefix)
	}

	return prefix, nil
}

func normalizeTreeListPath(filePath string) (string, error) {
	filePath, err := refspec.NormalizeTreeListPath(filePath)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidReference, err)
	}

	return filePath, nil
}

func normalizeTreeListCursor(cursor string) (string, error) {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return "", nil
	}

	return normalizeTreeListPath(cursor)
}

func normalizeTreePath(filePath string) (string, error) {
	filePath, err := refspec.NormalizeTreePath(filePath)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidReference, err)
	}

	return filePath, nil
}

func parseTreeEntryRecord(record []byte, treePath string) (TreeEntry, bool, error) {
	record = bytes.Trim(record, "\x00\n\r ")
	if len(record) == 0 {
		return TreeEntry{}, false, nil
	}

	meta, nameBytes, ok := bytes.Cut(record, []byte{'\t'})
	if !ok {
		return TreeEntry{}, false, fmt.Errorf("%w: malformed tree entry", ErrInvalidReference)
	}

	fields := strings.Fields(string(meta))
	if len(fields) < 4 {
		return TreeEntry{}, false, fmt.Errorf("%w: malformed tree entry", ErrInvalidReference)
	}

	entryPath := strings.TrimSpace(string(nameBytes))
	if entryPath == "" {
		return TreeEntry{}, false, nil
	}

	if treePath != "" {
		entryPath = path.Join(treePath, entryPath)
	}

	sizeBytes := int64(0)
	if fields[3] != "-" {
		size, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil {
			return TreeEntry{}, false, fmt.Errorf("%w: malformed tree entry size %q", ErrInvalidReference, fields[3])
		}

		sizeBytes = size
	}

	return TreeEntry{
		Path:      entryPath,
		Name:      path.Base(entryPath),
		Type:      fields[1],
		Mode:      fields[0],
		ObjectSHA: fields[2],
		SizeBytes: sizeBytes,
	}, true, nil
}

type gitRunner interface {
	RunGit(ctx context.Context, checkoutPath string, args ...string) ([]byte, error)
	RunGitWithInputEnv(ctx context.Context, checkoutPath string, input []byte, env []string, args ...string) ([]byte, error)
	StreamGitRecords(ctx context.Context, checkoutPath string, args []string, handle func([]byte) error) error
}

type execGitRunner struct{}

func (execGitRunner) RunGit(ctx context.Context, checkoutPath string, args ...string) ([]byte, error) {
	return (execGitRunner{}).RunGitWithInputEnv(ctx, checkoutPath, nil, nil, args...)
}

func (execGitRunner) RunGitWithInputEnv(ctx context.Context, checkoutPath string, input []byte, env []string, args ...string) ([]byte, error) {
	gitArgs := append([]string{"-C", checkoutPath}, args...)
	cmd := exec.CommandContext(ctx, "git", gitArgs...) // #nosec G204 -- Git arguments are assembled by source-control helpers after ref/path normalization.

	if len(input) > 0 {
		cmd.Stdin = bytes.NewReader(input)
	}
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

func tempGitIndexFile() (string, error) {
	f, err := os.CreateTemp("", "vectis-source-index-*")
	if err != nil {
		return "", fmt.Errorf("%w: create temporary git index: %v", ErrInvalidReference, err)
	}

	name := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return "", fmt.Errorf("%w: close temporary git index: %v", ErrInvalidReference, err)
	}
	if err := os.Remove(name); err != nil {
		return "", fmt.Errorf("%w: prepare temporary git index: %v", ErrInvalidReference, err)
	}

	return name, nil
}

var errStopGitStream = errors.New("stop git stream")

func (execGitRunner) StreamGitRecords(ctx context.Context, checkoutPath string, args []string, handle func([]byte) error) error {
	gitArgs := append([]string{"-C", checkoutPath}, args...)
	cmd := exec.CommandContext(ctx, "git", gitArgs...) // #nosec G204 -- Git arguments are assembled by source-control helpers after ref/path normalization.

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return err
	}

	stopped := false
	reader := bufio.NewReader(stdout)
	for {
		record, readErr := reader.ReadBytes(0)
		if len(record) > 0 {
			record = bytes.TrimSuffix(record, []byte{0})
			if err := handle(record); err != nil {
				if errors.Is(err, errStopGitStream) {
					stopped = true
					if cmd.Process != nil {
						_ = cmd.Process.Kill()
					}

					break
				}

				if cmd.Process != nil {
					_ = cmd.Process.Kill()
				}

				_ = cmd.Wait()
				return err
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}

			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}

			_ = cmd.Wait()
			return readErr
		}
	}

	err = cmd.Wait()
	if stopped {
		return nil
	}

	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("%w: %s", err, msg)
		}

		return err
	}

	return nil
}
