package runpolicy

import "vectis/internal/dal"

type Outcome string

const (
	OutcomeRetry    Outcome = "retry"
	OutcomeRequeue  Outcome = "requeue"
	OutcomeFailed   Outcome = "failed"
	OutcomeOrphaned Outcome = "orphaned"
)

type Trigger string

const (
	TriggerAckResult       Trigger = "ack_result"
	TriggerExecutionResult Trigger = "execution_result"
	TriggerDispatchRecover Trigger = "dispatch_recover"
	TriggerLeaseExpired    Trigger = "lease_expired"
)

type Input struct {
	Trigger     Trigger
	Attempt     int
	MaxAttempts int
	Transient   bool
}

type Decision struct {
	Outcome      Outcome
	ReasonCode   string
	FailureCode  string
	OrphanReason string
}

func Decide(in Input) Decision {
	switch in.Trigger {
	case TriggerAckResult:
		max := in.MaxAttempts
		if max <= 0 {
			max = 1
		}

		if in.Transient && in.Attempt < max {
			return Decision{
				Outcome:    OutcomeRetry,
				ReasonCode: "ack_transient_retry",
			}
		}

		if in.Transient {
			return Decision{
				Outcome:      OutcomeOrphaned,
				ReasonCode:   "ack_retry_exhausted_orphan",
				OrphanReason: dal.OrphanReasonAckUncertain,
			}
		}

		return Decision{
			Outcome:      OutcomeOrphaned,
			ReasonCode:   "ack_non_transient_orphan",
			OrphanReason: dal.OrphanReasonAckUncertain,
		}

	case TriggerExecutionResult:
		return Decision{
			Outcome:     OutcomeFailed,
			ReasonCode:  "execution_error",
			FailureCode: dal.FailureCodeExecution,
		}

	case TriggerDispatchRecover:
		return Decision{
			Outcome:    OutcomeRequeue,
			ReasonCode: "dispatch_gap_requeue",
		}

	case TriggerLeaseExpired:
		return Decision{
			Outcome:      OutcomeOrphaned,
			ReasonCode:   "lease_expired_orphan",
			OrphanReason: dal.OrphanReasonLeaseExpired,
		}
	}

	return Decision{
		Outcome:    OutcomeFailed,
		ReasonCode: "unknown_policy_trigger_failed",
	}
}
