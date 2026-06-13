package dal

import "fmt"

func ValidateMirroredExecutionFinalization(primary, mirror ExecutionFinalizationResult) error {
	if primary.ExecutionID != mirror.ExecutionID {
		return fmt.Errorf("execution finalization mismatch: primary execution %q mirror execution %q", primary.ExecutionID, mirror.ExecutionID)
	}

	if primary.RunID != mirror.RunID {
		return fmt.Errorf("execution finalization mismatch: primary run %q mirror run %q", primary.RunID, mirror.RunID)
	}

	if primary.Outcome != mirror.Outcome {
		return fmt.Errorf("execution finalization mismatch: primary outcome %q mirror outcome %q", primary.Outcome, mirror.Outcome)
	}

	if primary.Summary != mirror.Summary {
		return fmt.Errorf("execution finalization mismatch: primary summary %+v mirror summary %+v", primary.Summary, mirror.Summary)
	}

	if primary.Activated != mirror.Activated {
		return fmt.Errorf("execution finalization mismatch: primary activated %d mirror activated %d", primary.Activated, mirror.Activated)
	}

	if len(primary.Children) != len(mirror.Children) {
		return fmt.Errorf("execution finalization mismatch: primary children %d mirror children %d", len(primary.Children), len(mirror.Children))
	}

	for i := range primary.Children {
		if primary.Children[i] != mirror.Children[i] {
			return fmt.Errorf("execution finalization mismatch: child %d primary %+v mirror %+v", i, primary.Children[i], mirror.Children[i])
		}
	}

	return nil
}
