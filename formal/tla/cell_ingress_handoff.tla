---- MODULE cell_ingress_handoff ----
EXTENDS Naturals

CONSTANTS
    Envelopes,
    MaxHandoffs

Nil == "none"
ExecutionStatuses == {"none", "accepted", "running", "terminal"}

VARIABLES
    AcceptedEnvelope,
    ConflictRejected,
    DriftDetected,
    EnqueuedReceipt,
    ExecutionStatus,
    HandoffCount,
    HandoffEnvelope,
    LastError,
    RetryReady

Vars == <<
    AcceptedEnvelope,
    ConflictRejected,
    DriftDetected,
    EnqueuedReceipt,
    ExecutionStatus,
    HandoffCount,
    HandoffEnvelope,
    LastError,
    RetryReady
>>

Init ==
    /\ AcceptedEnvelope = Nil
    /\ ConflictRejected = FALSE
    /\ DriftDetected = FALSE
    /\ EnqueuedReceipt = FALSE
    /\ ExecutionStatus = "none"
    /\ HandoffCount = 0
    /\ HandoffEnvelope = Nil
    /\ LastError = FALSE
    /\ RetryReady = FALSE

RepairEligible ==
    /\ AcceptedEnvelope # Nil
    /\ ~EnqueuedReceipt
    /\ ExecutionStatus = "accepted"
    /\ RetryReady

AcceptNew(env) ==
    /\ env \in Envelopes
    /\ AcceptedEnvelope = Nil
    /\ AcceptedEnvelope' = env
    /\ ExecutionStatus' = "accepted"
    /\ LastError' = FALSE
    /\ RetryReady' = TRUE
    /\ UNCHANGED <<
        ConflictRejected,
        DriftDetected,
        EnqueuedReceipt,
        HandoffCount,
        HandoffEnvelope
        >>

AcceptDuplicateSame(env) ==
    /\ env \in Envelopes
    /\ AcceptedEnvelope = env
    /\ UNCHANGED Vars

RejectConflictingAccept(env) ==
    /\ env \in Envelopes
    /\ AcceptedEnvelope # Nil
    /\ env # AcceptedEnvelope
    /\ ConflictRejected' = TRUE
    /\ UNCHANGED <<
        AcceptedEnvelope,
        DriftDetected,
        EnqueuedReceipt,
        ExecutionStatus,
        HandoffCount,
        HandoffEnvelope,
        LastError,
        RetryReady
        >>

DirectQueueHandoff(env) ==
    /\ env \in Envelopes
    /\ AcceptedEnvelope = env
    /\ HandoffCount < MaxHandoffs
    /\ HandoffCount' = HandoffCount + 1
    /\ HandoffEnvelope' = env
    /\ UNCHANGED <<
        AcceptedEnvelope,
        ConflictRejected,
        DriftDetected,
        EnqueuedReceipt,
        ExecutionStatus,
        LastError,
        RetryReady
        >>

RepairQueueHandoff ==
    /\ RepairEligible
    /\ HandoffCount < MaxHandoffs
    /\ HandoffCount' = HandoffCount + 1
    /\ HandoffEnvelope' = AcceptedEnvelope
    /\ UNCHANGED <<
        AcceptedEnvelope,
        ConflictRejected,
        DriftDetected,
        EnqueuedReceipt,
        ExecutionStatus,
        LastError,
        RetryReady
        >>

QueueUnavailableMarkedFailed ==
    /\ AcceptedEnvelope # Nil
    /\ LastError' = TRUE
    /\ RetryReady' = FALSE
    /\ UNCHANGED <<
        AcceptedEnvelope,
        ConflictRejected,
        DriftDetected,
        EnqueuedReceipt,
        ExecutionStatus,
        HandoffCount,
        HandoffEnvelope
        >>

RetryGapElapsed ==
    /\ LastError
    /\ ~RetryReady
    /\ ~EnqueuedReceipt
    /\ RetryReady' = TRUE
    /\ UNCHANGED <<
        AcceptedEnvelope,
        ConflictRejected,
        DriftDetected,
        EnqueuedReceipt,
        ExecutionStatus,
        HandoffCount,
        HandoffEnvelope,
        LastError
        >>

MarkEnqueued ==
    /\ AcceptedEnvelope # Nil
    /\ HandoffCount > 0
    /\ EnqueuedReceipt' = TRUE
    /\ LastError' = FALSE
    /\ RetryReady' = FALSE
    /\ UNCHANGED <<
        AcceptedEnvelope,
        ConflictRejected,
        DriftDetected,
        ExecutionStatus,
        HandoffCount,
        HandoffEnvelope
        >>

MarkEnqueuedFails ==
    /\ AcceptedEnvelope # Nil
    /\ HandoffCount > 0
    /\ ~EnqueuedReceipt
    /\ UNCHANGED Vars

RejectDriftedRepairHandoff ==
    /\ RepairEligible
    /\ DriftDetected' = TRUE
    /\ LastError' = TRUE
    /\ RetryReady' = FALSE
    /\ UNCHANGED <<
        AcceptedEnvelope,
        ConflictRejected,
        EnqueuedReceipt,
        ExecutionStatus,
        HandoffCount,
        HandoffEnvelope
        >>

WorkerStartsFromQueue ==
    /\ ExecutionStatus = "accepted"
    /\ HandoffCount > 0
    /\ ExecutionStatus' = "running"
    /\ UNCHANGED <<
        AcceptedEnvelope,
        ConflictRejected,
        DriftDetected,
        EnqueuedReceipt,
        HandoffCount,
        HandoffEnvelope,
        LastError,
        RetryReady
        >>

WorkerTerminal ==
    /\ ExecutionStatus = "running"
    /\ ExecutionStatus' = "terminal"
    /\ UNCHANGED <<
        AcceptedEnvelope,
        ConflictRejected,
        DriftDetected,
        EnqueuedReceipt,
        HandoffCount,
        HandoffEnvelope,
        LastError,
        RetryReady
        >>

Idle ==
    /\ ExecutionStatus = "terminal" \/ EnqueuedReceipt
    /\ UNCHANGED Vars

Next ==
    \/ \E env \in Envelopes:
        \/ AcceptNew(env)
        \/ AcceptDuplicateSame(env)
        \/ RejectConflictingAccept(env)
        \/ DirectQueueHandoff(env)
    \/ RepairQueueHandoff
    \/ QueueUnavailableMarkedFailed
    \/ RetryGapElapsed
    \/ MarkEnqueued
    \/ MarkEnqueuedFails
    \/ RejectDriftedRepairHandoff
    \/ WorkerStartsFromQueue
    \/ WorkerTerminal
    \/ Idle

InvTypeOK ==
    /\ AcceptedEnvelope \in Envelopes \union {Nil}
    /\ ConflictRejected \in BOOLEAN
    /\ DriftDetected \in BOOLEAN
    /\ EnqueuedReceipt \in BOOLEAN
    /\ ExecutionStatus \in ExecutionStatuses
    /\ HandoffCount \in 0..MaxHandoffs
    /\ HandoffEnvelope \in Envelopes \union {Nil}
    /\ LastError \in BOOLEAN
    /\ RetryReady \in BOOLEAN

InvDurableAcceptedHasRecoveryPath ==
    AcceptedEnvelope # Nil =>
        \/ EnqueuedReceipt
        \/ HandoffCount > 0
        \/ RepairEligible
        \/ (ExecutionStatus = "accepted" /\ LastError /\ ~RetryReady)

InvQueueHandoffUsesAcceptedEnvelope ==
    HandoffCount > 0 =>
        /\ AcceptedEnvelope # Nil
        /\ HandoffEnvelope = AcceptedEnvelope

InvEnqueuedReceiptRequiresQueueHandoff ==
    EnqueuedReceipt => HandoffCount > 0

InvRepairSkippedAfterEnqueuedReceipt ==
    EnqueuedReceipt => ~RepairEligible

InvConflictDoesNotQueueDifferentEnvelope ==
    ConflictRejected =>
        \/ HandoffEnvelope = Nil
        \/ HandoffEnvelope = AcceptedEnvelope

InvExecutionRunsOnlyAfterQueueHandoff ==
    ExecutionStatus \in {"running", "terminal"} => HandoffCount > 0

InvRetryReadyFailedReceiptIsRepairable ==
    (AcceptedEnvelope # Nil /\ ~EnqueuedReceipt /\ ExecutionStatus = "accepted" /\ LastError /\ RetryReady)
        => RepairEligible

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
