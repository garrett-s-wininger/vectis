package gitcmd

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// ResolveWorkTreeRef resolves a simple ref name from a worktree's loose or packed refs.
func ResolveWorkTreeRef(workTree, ref string) (string, bool, error) {
	gitDir, err := WorkTreeGitDir(workTree)
	if err != nil {
		return "", false, err
	}

	return ResolveGitDirRef(gitDir, ref)
}

// ResolveGitDirRef resolves a simple ref name from a Git directory's loose or packed refs.
func ResolveGitDirRef(gitDir, ref string) (string, bool, error) {
	commonDir := GitCommonDir(gitDir)
	return resolveGitDirRef(gitDir, commonDir, strings.TrimSpace(ref), make(map[string]struct{}))
}

// SymbolicWorkTreeRef returns the target of a symbolic loose ref in a worktree.
func SymbolicWorkTreeRef(workTree, ref string) (string, bool, error) {
	gitDir, err := WorkTreeGitDir(workTree)
	if err != nil {
		return "", false, err
	}

	return SymbolicGitDirRef(gitDir, ref)
}

// SymbolicGitDirRef returns the target of a symbolic loose ref in a Git directory.
func SymbolicGitDirRef(gitDir, ref string) (string, bool, error) {
	ref = strings.TrimSpace(ref)
	if !safeRefName(ref) {
		return "", false, fmt.Errorf("unsafe git ref %q", ref)
	}

	data, ok, err := readLooseRef(gitDir, GitCommonDir(gitDir), ref)
	if err != nil || !ok {
		return "", false, err
	}

	target, ok := parseSymbolicRef(data)
	return target, ok, nil
}

// WriteWorkTreeRef writes a simple loose ref in a worktree's common Git directory.
func WriteWorkTreeRef(workTree, ref, objectID string) error {
	gitDir, err := WorkTreeGitDir(workTree)
	if err != nil {
		return err
	}

	return WriteGitDirRef(gitDir, ref, objectID)
}

// WriteGitDirRef writes a simple loose ref in a Git directory's common Git directory.
func WriteGitDirRef(gitDir, ref, objectID string) error {
	ref = strings.TrimSpace(ref)
	objectID = strings.TrimSpace(objectID)
	if ref == "HEAD" || !safeRefName(ref) {
		return fmt.Errorf("unsafe git ref %q", ref)
	}

	if !looksLikeObjectID(objectID) {
		return fmt.Errorf("git ref %q object id %q is not a full object id", ref, objectID)
	}

	refPath := filepath.Join(append([]string{GitCommonDir(gitDir)}, strings.Split(ref, "/")...)...)
	if err := os.MkdirAll(filepath.Dir(refPath), 0o755); err != nil {
		return err
	}

	if err := os.WriteFile(refPath, []byte(objectID+"\n"), 0o644); err != nil {
		return err
	}

	return nil
}

func resolveGitDirRef(gitDir, commonDir, ref string, seen map[string]struct{}) (string, bool, error) {
	if !safeRefName(ref) {
		return "", false, fmt.Errorf("unsafe git ref %q", ref)
	}
	
	if _, ok := seen[ref]; ok {
		return "", false, fmt.Errorf("cyclic git symbolic ref %q", ref)
	}

	seen[ref] = struct{}{}
	data, ok, err := readLooseRef(gitDir, commonDir, ref)
	if err != nil {
		return "", false, err
	}

	if ok {
		if target, symbolic := parseSymbolicRef(data); symbolic {
			return resolveGitDirRef(gitDir, commonDir, target, seen)
		}

		objectID := strings.Fields(strings.TrimSpace(string(data)))
		if len(objectID) > 0 && looksLikeObjectID(objectID[0]) {
			return objectID[0], true, nil
		}

		return "", false, nil
	}

	return resolvePackedRef(commonDir, ref)
}

func readLooseRef(gitDir, commonDir, ref string) ([]byte, bool, error) {
	root := commonDir
	if ref == "HEAD" {
		root = gitDir
	}

	refPath := filepath.Join(append([]string{root}, strings.Split(ref, "/")...)...)
	data, err := os.ReadFile(refPath)
	if err == nil {
		return data, true, nil
	}

	if os.IsNotExist(err) {
		return nil, false, nil
	}

	return nil, false, err
}

func resolvePackedRef(commonDir, ref string) (string, bool, error) {
	file, err := os.Open(filepath.Join(commonDir, "packed-refs"))
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}

		return "", false, err
	}
	defer file.Close()

	var pendingTagObject string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "^") {
			peeled := strings.TrimPrefix(line, "^")
			if pendingTagObject != "" && looksLikeObjectID(peeled) {
				return peeled, true, nil
			}

			pendingTagObject = ""
			continue
		}

		if pendingTagObject != "" {
			return pendingTagObject, true, nil
		}

		fields := strings.Fields(line)
		if len(fields) < 2 || fields[1] != ref || !looksLikeObjectID(fields[0]) {
			continue
		}

		if strings.HasPrefix(ref, "refs/tags/") {
			pendingTagObject = fields[0]
			continue
		}

		return fields[0], true, nil
	}

	if err := scanner.Err(); err != nil {
		return "", false, err
	}

	if pendingTagObject != "" {
		return pendingTagObject, true, nil
	}

	return "", false, nil
}

func parseSymbolicRef(data []byte) (string, bool) {
	target, ok := strings.CutPrefix(strings.TrimSpace(string(data)), "ref:")
	if !ok {
		return "", false
	}

	target = strings.TrimSpace(target)
	return target, target != ""
}

func safeRefName(ref string) bool {
	if ref == "HEAD" {
		return true
	}

	if !strings.HasPrefix(ref, "refs/") {
		return false
	}

	if strings.ContainsAny(ref, "\x00\r\n\\") {
		return false
	}
	
	clean := path.Clean(ref)
	return clean == ref && !path.IsAbs(ref) && !strings.HasPrefix(clean, "../") && clean != ".."
}

func looksLikeObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}

	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}

	return true
}
