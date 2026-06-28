package source

import (
	"fmt"
	"strings"

	"vectis/internal/source/refspec"
)

type DefinitionTargetRequest struct {
	JobID      string
	Ref        string
	DefaultRef string
	Path       string
}

type DefinitionTarget struct {
	Ref  string
	Path string
}

func DefinitionPathForJobID(jobID string) (string, error) {
	return refspec.DefinitionPathForJobID(jobID)
}

func ResolveDefinitionTarget(req DefinitionTargetRequest) (DefinitionTarget, error) {
	ref := strings.TrimSpace(req.Ref)
	if ref == "" {
		ref = strings.TrimSpace(req.DefaultRef)
	}
	if ref == "" {
		ref = "HEAD"
	}

	ref, err := normalizeRef(ref)
	if err != nil {
		return DefinitionTarget{}, err
	}

	definitionPath := strings.TrimSpace(req.Path)
	if definitionPath == "" {
		definitionPath, err = DefinitionPathForJobID(req.JobID)
		if err != nil {
			return DefinitionTarget{}, fmt.Errorf("%w: %w", ErrInvalidReference, err)
		}
	} else {
		definitionPath, err = normalizeTreePath(definitionPath)
		if err != nil {
			return DefinitionTarget{}, err
		}
	}

	return DefinitionTarget{Ref: ref, Path: definitionPath}, nil
}
