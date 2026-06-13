package source

import "vectis/internal/source/refspec"

func DefinitionPathForJobID(jobID string) (string, error) {
	return refspec.DefinitionPathForJobID(jobID)
}
