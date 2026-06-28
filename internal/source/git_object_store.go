package source

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const gitObjectLooseScanLimit = 10000

type GitCheckoutObjectStoreStatus struct {
	PackFiles                 int
	PackBytes                 int64
	PackKeepFiles             int
	LooseObjects              int
	LooseObjectsTruncated     bool
	LooseObjectScanLimit      int
	CommitGraph               bool
	MultiPackIndex            bool
	MaintenanceIndicatorFiles []string
}

func (g *GitCheckout) objectStoreStatus(ctx context.Context) GitCheckoutObjectStoreStatus {
	status := GitCheckoutObjectStoreStatus{
		LooseObjectScanLimit: gitObjectLooseScanLimit,
	}

	gitDir, err := g.absoluteGitDir(ctx)
	if err != nil {
		return status
	}

	commonDir := g.gitCommonDir(ctx, gitDir)
	objectsDir := filepath.Join(commonDir, "objects")
	status.scanPackDirectory(filepath.Join(objectsDir, "pack"))
	status.scanLooseObjects(objectsDir)
	status.CommitGraph = fileExists(filepath.Join(objectsDir, "info", "commit-graph")) ||
		fileExists(filepath.Join(objectsDir, "info", "commit-graphs", "commit-graph-chain"))
	status.MaintenanceIndicatorFiles = gitMaintenanceIndicatorFiles(gitDir, commonDir)

	return status
}

func (g *GitCheckout) absoluteGitDir(ctx context.Context) (string, error) {
	out, err := g.run(ctx, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(out)), nil
}

func (g *GitCheckout) gitCommonDir(ctx context.Context, gitDir string) string {
	out, err := g.run(ctx, "rev-parse", "--git-common-dir")
	if err != nil {
		return gitDir
	}

	commonDir := strings.TrimSpace(string(out))
	if commonDir == "" {
		return gitDir
	}
	if filepath.IsAbs(commonDir) {
		return filepath.Clean(commonDir)
	}

	return filepath.Clean(filepath.Join(g.checkoutPath, commonDir))
}

func (s *GitCheckoutObjectStoreStatus) scanPackDirectory(packDir string) {
	entries, err := os.ReadDir(packDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		switch {
		case name == "multi-pack-index":
			s.MultiPackIndex = true
		case strings.HasSuffix(name, ".pack"):
			s.PackFiles++
			if info, err := entry.Info(); err == nil {
				s.PackBytes += info.Size()
			}
		case strings.HasSuffix(name, ".keep"):
			s.PackKeepFiles++
		}
	}
}

func (s *GitCheckoutObjectStoreStatus) scanLooseObjects(objectsDir string) {
	entries, err := os.ReadDir(objectsDir)
	if err != nil {
		return
	}

	for _, shard := range entries {
		if !shard.IsDir() || !isLooseObjectShard(shard.Name()) {
			continue
		}

		objects, err := os.ReadDir(filepath.Join(objectsDir, shard.Name()))
		if err != nil {
			continue
		}

		for _, object := range objects {
			if object.IsDir() || !isLooseObjectFile(object.Name()) {
				continue
			}

			s.LooseObjects++
			if s.LooseObjects >= s.LooseObjectScanLimit {
				s.LooseObjectsTruncated = true
				return
			}
		}
	}
}

func gitMaintenanceIndicatorFiles(gitDir, commonDir string) []string {
	seen := make(map[string]struct{})
	var files []string

	for _, dir := range uniqueNonEmptyStrings(gitDir, commonDir) {
		for _, name := range []string{
			"gc.pid",
			"maintenance.lock",
			"index.lock",
			"packed-refs.lock",
			"shallow.lock",
		} {
			if fileExists(filepath.Join(dir, name)) {
				if _, ok := seen[name]; !ok {
					files = append(files, name)
					seen[name] = struct{}{}
				}
			}
		}

		packEntries, err := os.ReadDir(filepath.Join(dir, "objects", "pack"))
		if err != nil {
			continue
		}
		for _, entry := range packEntries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".lock") {
				continue
			}

			name := filepath.ToSlash(filepath.Join("objects", "pack", entry.Name()))
			if _, ok := seen[name]; !ok {
				files = append(files, name)
				seen[name] = struct{}{}
			}
		}
	}

	sort.Strings(files)
	return files
}

func uniqueNonEmptyStrings(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}

		seen[value] = struct{}{}
		out = append(out, value)
	}

	return out
}

func isLooseObjectShard(name string) bool {
	return len(name) == 2 && isHexString(name)
}

func isLooseObjectFile(name string) bool {
	return len(name) == 38 && isHexString(name)
}

func isHexString(value string) bool {
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}

	return true
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
