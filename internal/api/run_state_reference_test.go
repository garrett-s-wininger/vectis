package api

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/taskfinalize"
	"vectis/internal/taskreduce"
)

func TestRunStateReferenceMentionsLifecycleConstants(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "docs", "operating", "reference", "run-state-reference.md"))
	if err != nil {
		t.Fatalf("read run state reference: %v", err)
	}

	docText := string(raw)
	want := []string{
		dal.RunStatusQueued,
		dal.RunStatusRunning,
		dal.RunStatusSucceeded,
		dal.RunStatusFailed,
		dal.RunStatusOrphaned,
		dal.RunStatusCancelled,
		dal.RunStatusAbandoned,
		dal.RunStatusAborted,

		dal.TaskStatusPlanned,
		dal.TaskStatusPending,
		dal.TaskStatusAccepted,
		dal.TaskStatusRunning,
		dal.TaskStatusSucceeded,
		dal.TaskStatusFailed,
		dal.TaskStatusCancelled,
		dal.TaskStatusAborted,

		dal.OrphanReasonLeaseExpired,
		dal.OrphanReasonAckUncertain,
		dal.OrphanReasonWorkerCoreUnknown,
		dal.CancelReasonAPI,
		dal.RepairReasonManual,
		dal.FailureCodeExecution,
		dal.FailureCodeForceFailed,
		dal.FailureCodeDispatchExpired,
		dal.FailureCodeInvalidEnvelope,

		dal.ExecutionSecurityEventSVIDCheck,
		dal.ExecutionSecurityEventSecretResolution,
		dal.DispatchSourceAPI,
		dal.DispatchSourceCron,
		dal.DispatchSourceReconciler,
		dal.DispatchEventAccepted,
		dal.DispatchEventAttempt,
		dal.DispatchEventSuccess,
		dal.DispatchEventFailure,

		runNextActionTaskCompletionPending,
		runNextActionTaskContinuationPending,
		runNextActionTaskFinalizationRepairPending,
		runNextActionSecurityGateFailed,

		string(taskreduce.OutcomeWaiting),
		string(taskreduce.OutcomeSucceeded),
		string(taskreduce.OutcomeFailed),
		string(taskfinalize.OutcomeContinue),
		string(taskfinalize.OutcomeReduceSucceeded),
		string(taskfinalize.OutcomeReduceFailed),
		string(taskfinalize.OutcomeIncomplete),
		string(taskfinalize.OutcomeExecutionFailed),
		string(taskfinalize.OutcomeExecutionAborted),
	}

	seen := map[string]bool{}
	var missing []string
	for _, value := range want {
		if seen[value] {
			continue
		}
		seen[value] = true

		if !strings.Contains(docText, "`"+value+"`") {
			missing = append(missing, value)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("run state reference is missing lifecycle constants: %s", strings.Join(missing, ", "))
	}
}
