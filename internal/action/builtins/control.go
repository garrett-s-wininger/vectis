package builtins

import (
	"fmt"
	"strings"

	"vectis/internal/action"
	"vectis/internal/taskgraph"
)

func validateExecutionMode(with map[string]string) []action.FieldError {
	raw, ok := with[taskgraph.ExecutionField]
	if !ok || taskgraph.ValidExecutionMode(raw) {
		return nil
	}

	return []action.FieldError{{
		Field:   taskgraph.ExecutionField,
		Message: fmt.Sprintf("must be %q or %q, got %q", taskgraph.ExecutionLocal, taskgraph.ExecutionDistributed, strings.TrimSpace(raw)),
	}}
}
