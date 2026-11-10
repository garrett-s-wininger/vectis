package source

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"vectis/internal/source/refspec"
)

const PrimaryDefinitionPath = refspec.PrimaryDefinitionPath
const NamedDefinitionDirectory = refspec.NamedDefinitionDirectory

func JobDefinitionPaths(jobID string) ([]string, error) {
	modern, err := DefinitionPathForJobID(jobID)
	if err != nil {
		return nil, err
	}

	jobID = strings.TrimSpace(jobID)
	candidates := []string{
		modern,
		NamedDefinitionDirectory + "/" + jobID + ".json",
		DefaultDefinitionPath + "/" + jobID + ".json",
		DefaultDefinitionPath + "/" + strings.ReplaceAll(jobID, ".", "/") + ".json",
	}

	seen := make(map[string]bool)
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if !seen[candidate] {
			out = append(out, candidate)
			seen[candidate] = true
		}
	}

	return out, nil
}

func ConventionalJobID(filePath string) (string, error) {
	if filePath == PrimaryDefinitionPath {
		return "ci", nil
	}

	var name string
	switch {
	case strings.HasPrefix(filePath, DefaultDefinitionPath+"/"):
		name = strings.ReplaceAll(strings.TrimPrefix(filePath, DefaultDefinitionPath+"/"), "/", ".")
	case strings.HasPrefix(filePath, NamedDefinitionDirectory+"/"):
		name = strings.TrimPrefix(filePath, NamedDefinitionDirectory+"/")
		if strings.Contains(name, "/") {
			return "", fmt.Errorf("definition is outside the named job layout")
		}
	default:
		return "", fmt.Errorf("definition is outside the job layout")
	}

	if !strings.HasSuffix(name, ".json") {
		return "", fmt.Errorf("definition path must end in .json")
	}

	jobID := strings.TrimSuffix(name, ".json")
	if _, err := DefinitionPathForJobID(jobID); err != nil {
		return "", fmt.Errorf("definition path contains an unsupported job_id segment")
	}

	return jobID, nil
}

type JobDefinitionLocation struct {
	Path string
	Err  error
}

func (g *GitCheckout) ResolveJobDefinitionPaths(
	ctx context.Context,
	revision Revision,
	jobIDs []string,
) (map[string]JobDefinitionLocation, error) {
	if err := g.validateCheckout(); err != nil {
		return nil, err
	}

	commit, err := normalizeCommit(revision.Commit)
	if err != nil {
		return nil, err
	}

	results := make(map[string]JobDefinitionLocation, len(jobIDs))
	candidates := make(map[string][]string, len(jobIDs))
	pathSet := make(map[string]bool)
	for _, jobID := range jobIDs {
		paths, err := JobDefinitionPaths(jobID)
		if err != nil {
			results[jobID] = JobDefinitionLocation{Err: fmt.Errorf("%w: %w", ErrInvalidReference, err)}
			continue
		}

		candidates[jobID] = paths
		for _, filePath := range paths {
			pathSet[filePath] = true
		}
	}

	paths := make([]string, 0, len(pathSet))
	for filePath := range pathSet {
		paths = append(paths, filePath)
	}

	sort.Strings(paths)
	found := make(map[string]bool)
	for first := 0; first < len(paths); {
		end, bytes := first, 0
		for end < len(paths) && end-first < 64 && (end == first || bytes+len(paths[end]) < 32768) {
			bytes += len(paths[end])
			end++
		}

		args := append([]string{"--literal-pathspecs", "ls-tree", "-z", "--long", commit, "--"}, paths[first:end]...)
		if err := g.streamGitRecords(ctx, args, func(record []byte) error {
			entry, ok, err := parseTreeEntryRecord(record, "")
			if err != nil {
				return err
			}

			if ok && entry.Type == "blob" && pathSet[entry.Path] {
				found[entry.Path] = true
			}

			return nil
		}); err != nil {
			return nil, err
		}

		first = end
	}

	for jobID, paths := range candidates {
		var matches []string
		for _, filePath := range paths {
			if found[filePath] {
				matches = append(matches, filePath)
			}
		}

		switch len(matches) {
		case 0:
			results[jobID] = JobDefinitionLocation{Err: fmt.Errorf("%w: no definition for job %s", ErrNotFound, jobID)}
		case 1:
			results[jobID] = JobDefinitionLocation{Path: matches[0]}
		default:
			results[jobID] = JobDefinitionLocation{
				Err: fmt.Errorf(
					"%w: job %s has multiple definitions: %s",
					ErrInvalidDefinition,
					jobID,
					strings.Join(matches, ", "),
				),
			}
		}
	}

	return results, nil
}

func (g *GitCheckout) ResolveJobDefinitionPath(ctx context.Context, ref, jobID string) (string, Revision, error) {
	if _, err := JobDefinitionPaths(jobID); err != nil {
		return "", Revision{}, fmt.Errorf("%w: %w", ErrInvalidReference, err)
	}

	revision, err := g.ResolveRevision(ctx, ref)
	if err != nil {
		return "", Revision{}, err
	}

	results, err := g.ResolveJobDefinitionPaths(ctx, revision, []string{jobID})
	if err != nil {
		return "", Revision{}, err
	}

	return results[jobID].Path, revision, results[jobID].Err
}

func (g *GitCheckout) listConventionalJobDefinitions(
	ctx context.Context,
	ref string,
	revision Revision,
	opts ListDefinitionFilesOptions,
) (DefinitionFileListing, error) {
	cursor, err := normalizeTreeListCursor(opts.Cursor)
	if err != nil {
		return DefinitionFileListing{}, err
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultTreeListLimit
	}

	listing := DefinitionFileListing{
		RequestedRef: ref,
		Revision:     revision,
		Path:         NamedDefinitionDirectory,
		Conventional: true,
		Files:        make([]DefinitionFile, 0, min(limit, DefaultTreeListLimit)),
	}

	err = g.streamGitRecords(
		ctx,
		[]string{
			"--literal-pathspecs",
			"ls-tree",
			"-z",
			"--long",
			"-r",
			revision.Commit,
			"--",
			PrimaryDefinitionPath,
			NamedDefinitionDirectory,
		},
		func(record []byte) error {
			entry, ok, err := parseTreeEntryRecord(record, "")
			if err != nil {
				return err
			}

			if !ok || entry.Type != "blob" || !strings.HasSuffix(entry.Name, ".json") {
				return nil
			}

			relative := strings.TrimPrefix(entry.Path, NamedDefinitionDirectory+"/")
			if entry.Path != PrimaryDefinitionPath && strings.Contains(relative, "/") &&
				!strings.HasPrefix(entry.Path, DefaultDefinitionPath+"/") {
				return nil
			}

			if cursor != "" && entry.Path <= cursor {
				return nil
			}

			if len(listing.Files) >= limit {
				listing.Truncated = true
				listing.NextCursor = listing.Files[len(listing.Files)-1].Path
				return errStopGitStream
			}

			listing.Files = append(
				listing.Files,
				DefinitionFile{
					Path:      entry.Path,
					Name:      entry.Name,
					BlobSHA:   entry.ObjectSHA,
					SizeBytes: entry.SizeBytes,
				},
			)

			return nil
		},
	)

	if err != nil && !errors.Is(err, errStopGitStream) {
		return DefinitionFileListing{}, err
	}

	return listing, nil
}

func loadJobDefinition(
	ctx context.Context,
	repo Repository,
	revision Revision,
	ref string,
	req DefinitionRequest,
) (Definition, error) {
	candidates, err := JobDefinitionPaths(req.JobID)
	if err != nil {
		return Definition{}, fmt.Errorf("%w: %w", ErrInvalidReference, err)
	}

	var found *File
	for _, candidate := range candidates {
		file, readErr := repo.ReadFile(ctx, revision, candidate)
		if errors.Is(readErr, ErrNotFound) {
			continue
		}

		if readErr != nil {
			return Definition{}, readErr
		}

		if found != nil {
			return Definition{}, fmt.Errorf("%w: job %s has multiple definitions", ErrInvalidDefinition, req.JobID)
		}

		found = &file
	}

	if found == nil {
		return Definition{}, fmt.Errorf("%w: no definition for job %s", ErrNotFound, req.JobID)
	}

	return ParseDefinitionFile(*found, ref, req.Validation)
}
