package source

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"vectis/internal/gitcmd"
)

const (
	gitObjectLooseScanLimit               = 10000
	gitHydratedRefScanLimit               = 5000
	gitObjectStorePressureOK              = "ok"
	gitObjectStorePressureWarning         = "warning"
	gitObjectStorePressureCritical        = "critical"
	gitObjectStorePackFilesWarning        = 50
	gitObjectStorePackFilesCritical       = 200
	gitObjectStoreLooseObjectsWarning     = 5000
	gitObjectStoreLooseObjectsCritical    = 9000
	gitObjectStoreHydratedRefsWarning     = 1000
	gitObjectStoreHydratedRefsCritical    = gitHydratedRefScanLimit
	gitObjectStoreWarningManyPacks        = "many_pack_files"
	gitObjectStoreWarningManyLoose        = "many_loose_objects"
	gitObjectStoreWarningManyHydratedRefs = "many_hydrated_refs"
	gitObjectStoreWarningLooseTruncated   = "loose_object_scan_truncated"
	gitObjectStoreWarningHydratedRefsScan = "hydrated_ref_scan_truncated"
	gitObjectStoreWarningKeepFiles        = "pack_keep_files_present"
	gitObjectStoreWarningMaintenance      = "maintenance_indicator_files"
	gitObjectStoreWarningMissingCommit    = "commit_graph_missing"
	gitObjectStoreWarningMissingMultiPack = "multi_pack_index_missing"
)

type GitCheckoutObjectStoreStatus struct {
	PackFiles                 int
	PackBytes                 int64
	PackKeepFiles             int
	LooseObjects              int
	LooseObjectsTruncated     bool
	LooseObjectScanLimit      int
	HydratedRefs              int
	HydratedRefsTruncated     bool
	HydratedRefScanLimit      int
	CommitGraph               bool
	MultiPackIndex            bool
	MaintenanceIndicatorFiles []string
	Pressure                  string
	Warnings                  []GitCheckoutObjectStoreWarning
}

type GitCheckoutObjectStoreWarning struct {
	Code     string
	Severity string
	Message  string
}

func (g *GitCheckout) objectStoreStatus(ctx context.Context) GitCheckoutObjectStoreStatus {
	status := GitCheckoutObjectStoreStatus{
		LooseObjectScanLimit: gitObjectLooseScanLimit,
		HydratedRefScanLimit: gitHydratedRefScanLimit,
		Pressure:             gitObjectStorePressureOK,
	}

	gitDir, err := g.absoluteGitDir(ctx)
	if err != nil {
		return status
	}

	commonDir := g.gitCommonDir(ctx, gitDir)
	objectsDir := filepath.Join(commonDir, "objects")
	status.scanPackDirectory(filepath.Join(objectsDir, "pack"))
	status.scanLooseObjects(objectsDir)
	status.countHydratedRefs(commonDir)
	status.CommitGraph = fileExists(filepath.Join(objectsDir, "info", "commit-graph")) ||
		fileExists(filepath.Join(objectsDir, "info", "commit-graphs", "commit-graph-chain"))

	status.MaintenanceIndicatorFiles = gitMaintenanceIndicatorFiles(gitDir, commonDir)
	status.classifyPressure()

	return status
}

func (s *GitCheckoutObjectStoreStatus) classifyPressure() {
	s.Pressure = gitObjectStorePressureOK
	s.Warnings = nil

	switch {
	case s.PackFiles >= gitObjectStorePackFilesCritical:
		s.addWarning(gitObjectStoreWarningManyPacks, gitObjectStorePressureCritical, "repository has a high number of pack files")
	case s.PackFiles >= gitObjectStorePackFilesWarning:
		s.addWarning(gitObjectStoreWarningManyPacks, gitObjectStorePressureWarning, "repository has many pack files")
	}

	switch {
	case s.LooseObjectsTruncated:
		s.addWarning(gitObjectStoreWarningLooseTruncated, gitObjectStorePressureCritical, "loose object scan hit the safety limit")
	case s.LooseObjects >= gitObjectStoreLooseObjectsCritical:
		s.addWarning(gitObjectStoreWarningManyLoose, gitObjectStorePressureCritical, "repository has a high number of loose objects")
	case s.LooseObjects >= gitObjectStoreLooseObjectsWarning:
		s.addWarning(gitObjectStoreWarningManyLoose, gitObjectStorePressureWarning, "repository has many loose objects")
	}

	switch {
	case s.HydratedRefsTruncated:
		s.addWarning(gitObjectStoreWarningHydratedRefsScan, gitObjectStorePressureCritical, "hydrated ref scan hit the safety limit")
	case s.HydratedRefs >= gitObjectStoreHydratedRefsCritical:
		s.addWarning(gitObjectStoreWarningManyHydratedRefs, gitObjectStorePressureCritical, "repository has a high number of hydrated refs")
	case s.HydratedRefs >= gitObjectStoreHydratedRefsWarning:
		s.addWarning(gitObjectStoreWarningManyHydratedRefs, gitObjectStorePressureWarning, "repository has many hydrated refs")
	}

	if s.PackKeepFiles > 0 {
		s.addWarning(gitObjectStoreWarningKeepFiles, gitObjectStorePressureWarning, "pack .keep files may prevent consolidation")
	}

	if len(s.MaintenanceIndicatorFiles) > 0 {
		s.addWarning(gitObjectStoreWarningMaintenance, gitObjectStorePressureWarning, "maintenance indicator files are present")
	}

	if s.PackFiles >= gitObjectStorePackFilesWarning && !s.MultiPackIndex {
		s.addWarning(gitObjectStoreWarningMissingMultiPack, gitObjectStorePressureWarning, "repository has many packs without a multi-pack-index")
	}

	if (s.PackFiles >= gitObjectStorePackFilesWarning || s.LooseObjects >= gitObjectStoreLooseObjectsWarning || s.LooseObjectsTruncated) && !s.CommitGraph {
		s.addWarning(gitObjectStoreWarningMissingCommit, gitObjectStorePressureWarning, "repository pressure is elevated without a commit-graph")
	}
}

func (s *GitCheckoutObjectStoreStatus) addWarning(code, severity, message string) {
	s.Warnings = append(s.Warnings, GitCheckoutObjectStoreWarning{
		Code:     code,
		Severity: severity,
		Message:  message,
	})

	if severityRank(severity) > severityRank(s.Pressure) {
		s.Pressure = severity
	}
}

func severityRank(severity string) int {
	switch severity {
	case gitObjectStorePressureCritical:
		return 2
	case gitObjectStorePressureWarning:
		return 1
	default:
		return 0
	}
}

func (g *GitCheckout) absoluteGitDir(ctx context.Context) (string, error) {
	if gitDir, err := gitcmd.WorkTreeGitDir(g.checkoutPath); err == nil {
		return gitDir, nil
	}

	out, err := g.run(ctx, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(out)), nil
}

func (g *GitCheckout) gitCommonDir(ctx context.Context, gitDir string) string {
	return gitcmd.GitCommonDir(gitDir)
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

var errHydratedRefScanLimit = errors.New("hydrated ref scan limit reached")

func (s *GitCheckoutObjectStoreStatus) countHydratedRefs(commonDir string) {
	if s.HydratedRefScanLimit <= 0 {
		s.HydratedRefScanLimit = gitHydratedRefScanLimit
	}

	seen := make(map[string]struct{})
	if err := s.countLooseHydratedRefs(commonDir, seen); errors.Is(err, errHydratedRefScanLimit) {
		return
	}

	s.countPackedHydratedRefs(commonDir, seen)
}

func (s *GitCheckoutObjectStoreStatus) countLooseHydratedRefs(commonDir string, seen map[string]struct{}) error {
	root := filepath.Join(commonDir, "refs", "vectis", "hydrated")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if entry.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil || rel == "." {
			return nil
		}

		ref := "refs/vectis/hydrated/" + filepath.ToSlash(rel)
		if !s.recordHydratedRef(ref, seen) {
			return errHydratedRefScanLimit
		}

		return nil
	})

	if os.IsNotExist(err) {
		return nil
	}

	return err
}

func (s *GitCheckoutObjectStoreStatus) countPackedHydratedRefs(commonDir string, seen map[string]struct{}) {
	file, err := os.Open(filepath.Join(commonDir, "packed-refs"))
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "^") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 || !strings.HasPrefix(fields[1], "refs/vectis/hydrated/") {
			continue
		}

		if !s.recordHydratedRef(fields[1], seen) {
			return
		}
	}
}

func (s *GitCheckoutObjectStoreStatus) recordHydratedRef(ref string, seen map[string]struct{}) bool {
	if _, ok := seen[ref]; ok {
		return true
	}
	
	if s.HydratedRefs >= s.HydratedRefScanLimit {
		s.HydratedRefsTruncated = true
		return false
	}

	seen[ref] = struct{}{}
	s.HydratedRefs++
	return true
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
