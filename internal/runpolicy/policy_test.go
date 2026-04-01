package runpolicy_test

import (
	"testing"

	"vectis/internal/dal"
	"vectis/internal/runpolicy"
)

func TestDecide_AckResult(t *testing.T) {
	t.Run("transient retry", func(t *testing.T) {
		d := runpolicy.Decide(runpolicy.Input{
			Trigger:     runpolicy.TriggerAckResult,
			Attempt:     1,
			MaxAttempts: 4,
			Transient:   true,
		})

		if d.Outcome != runpolicy.OutcomeRetry {
			t.Fatalf("expected retry, got %q", d.Outcome)
		}

		if d.ReasonCode != "ack_transient_retry" {
			t.Fatalf("unexpected reason code: %q", d.ReasonCode)
		}
	})

	t.Run("transient exhausted", func(t *testing.T) {
		d := runpolicy.Decide(runpolicy.Input{
			Trigger:     runpolicy.TriggerAckResult,
			Attempt:     4,
			MaxAttempts: 4,
			Transient:   true,
		})

		if d.Outcome != runpolicy.OutcomeOrphaned {
			t.Fatalf("expected orphaned, got %q", d.Outcome)
		}

		if d.OrphanReason != dal.OrphanReasonAckUncertain {
			t.Fatalf("expected orphan reason %q, got %q", dal.OrphanReasonAckUncertain, d.OrphanReason)
		}

		if d.ReasonCode != "ack_retry_exhausted_orphan" {
			t.Fatalf("unexpected reason code: %q", d.ReasonCode)
		}
	})

	t.Run("non transient", func(t *testing.T) {
		d := runpolicy.Decide(runpolicy.Input{
			Trigger:     runpolicy.TriggerAckResult,
			Attempt:     1,
			MaxAttempts: 4,
			Transient:   false,
		})

		if d.Outcome != runpolicy.OutcomeOrphaned {
			t.Fatalf("expected orphaned, got %q", d.Outcome)
		}

		if d.OrphanReason != dal.OrphanReasonAckUncertain {
			t.Fatalf("expected orphan reason %q, got %q", dal.OrphanReasonAckUncertain, d.OrphanReason)
		}

		if d.ReasonCode != "ack_non_transient_orphan" {
			t.Fatalf("unexpected reason code: %q", d.ReasonCode)
		}
	})
}

func TestDecide_OtherTriggers(t *testing.T) {
	exec := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerExecutionResult})
	if exec.Outcome != runpolicy.OutcomeFailed {
		t.Fatalf("execution: expected failed, got %q", exec.Outcome)
	}

	if exec.FailureCode != dal.FailureCodeExecution {
		t.Fatalf("execution: expected failure code %q, got %q", dal.FailureCodeExecution, exec.FailureCode)
	}

	requeue := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerDispatchRecover})
	if requeue.Outcome != runpolicy.OutcomeRequeue {
		t.Fatalf("dispatch: expected requeue, got %q", requeue.Outcome)
	}

	if requeue.ReasonCode != "dispatch_gap_requeue" {
		t.Fatalf("dispatch: unexpected reason code %q", requeue.ReasonCode)
	}

	orphan := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerLeaseExpired})
	if orphan.Outcome != runpolicy.OutcomeOrphaned {
		t.Fatalf("lease: expected orphaned, got %q", orphan.Outcome)
	}

	if orphan.OrphanReason != dal.OrphanReasonLeaseExpired {
		t.Fatalf("lease: expected orphan reason %q, got %q", dal.OrphanReasonLeaseExpired, orphan.OrphanReason)
	}
}
