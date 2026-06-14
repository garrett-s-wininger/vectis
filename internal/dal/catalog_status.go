package dal

type statusTransitionDecision int

const (
	statusTransitionApply statusTransitionDecision = iota
	statusTransitionNoop
	statusTransitionConflict
)

func catalogRunStatusDecision(current, target string) statusTransitionDecision {
	if current == target {
		return statusTransitionNoop
	}

	if isTerminalRunStatus(current) {
		if isTerminalRunStatus(target) {
			return statusTransitionConflict
		}

		return statusTransitionNoop
	}

	if isTerminalRunStatus(target) {
		return statusTransitionApply
	}

	switch target {
	case RunStatusRunning, RunStatusOrphaned:
		return statusTransitionApply
	default:
		return statusTransitionConflict
	}
}

func catalogExecutionStatusDecision(current, target string) statusTransitionDecision {
	if current == target {
		return statusTransitionNoop
	}

	if isTerminalExecutionStatus(current) {
		if isTerminalExecutionStatus(target) {
			return statusTransitionConflict
		}

		return statusTransitionNoop
	}

	currentRank, currentKnown := nonTerminalExecutionStatusRank(current)
	targetRank, targetKnown := nonTerminalExecutionStatusRank(target)
	if currentKnown && targetKnown {
		if targetRank > currentRank {
			return statusTransitionApply
		}

		return statusTransitionNoop
	}

	if currentKnown && isTerminalExecutionStatus(target) {
		return statusTransitionApply
	}

	return statusTransitionConflict
}

func nonTerminalExecutionStatusRank(status string) (int, bool) {
	switch status {
	case ExecutionStatusPlanned:
		return 0, true
	case ExecutionStatusPending:
		return 1, true
	case ExecutionStatusAccepted:
		return 2, true
	case ExecutionStatusRunning:
		return 3, true
	default:
		return 0, false
	}
}

func isTerminalRunStatus(status string) bool {
	switch status {
	case RunStatusSucceeded, RunStatusFailed, RunStatusCancelled, RunStatusAbandoned, RunStatusAborted:
		return true
	default:
		return false
	}
}
