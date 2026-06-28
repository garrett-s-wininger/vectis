---- MODULE worker_choreography ----
EXTENDS Naturals

VARIABLES
    CancelRequested,
    ClaimActive,
    CoreResult,
    CoreRunning,
    DurableStatus,
    LeaseActive,
    QueueDelivery,
    WorkerOutcome

Vars == <<
    CancelRequested,
    ClaimActive,
    CoreResult,
    CoreRunning,
    DurableStatus,
    LeaseActive,
    QueueDelivery,
    WorkerOutcome
>>

Init ==
    /\ CancelRequested = FALSE
    /\ ClaimActive = TRUE
    /\ CoreResult = "none"
    /\ CoreRunning = FALSE
    /\ DurableStatus = "running"
    /\ LeaseActive = TRUE
    /\ QueueDelivery = "acked"
    /\ WorkerOutcome = "none"

StartCoreExecution ==
    /\ ClaimActive
    /\ LeaseActive
    /\ DurableStatus = "running"
    /\ ~CoreRunning
    /\ CoreResult = "none"
    /\ CoreRunning' = TRUE
    /\ UNCHANGED <<
        CancelRequested,
        ClaimActive,
        CoreResult,
        DurableStatus,
        LeaseActive,
        QueueDelivery,
        WorkerOutcome
        >>

RequestCancel ==
    /\ DurableStatus = "running"
    /\ CoreRunning
    /\ CancelRequested' = TRUE
    /\ UNCHANGED <<
        ClaimActive,
        CoreResult,
        CoreRunning,
        DurableStatus,
        LeaseActive,
        QueueDelivery,
        WorkerOutcome
        >>

CoreReturnsSuccess ==
    /\ CoreRunning
    /\ CoreResult = "none"
    /\ LeaseActive
    /\ CoreRunning' = FALSE
    /\ CoreResult' = "success"
    /\ WorkerOutcome' = "success"
    /\ DurableStatus' = "succeeded"
    /\ ClaimActive' = FALSE
    /\ UNCHANGED <<
        CancelRequested,
        LeaseActive,
        QueueDelivery
        >>

CoreReturnsCancelled ==
    /\ CoreRunning
    /\ CoreResult = "none"
    /\ CoreRunning' = FALSE
    /\ CoreResult' = "cancelled"
    /\ WorkerOutcome' = "aborted"
    /\ DurableStatus' = "cancelled"
    /\ ClaimActive' = FALSE
    /\ UNCHANGED <<
        CancelRequested,
        LeaseActive,
        QueueDelivery
        >>

CoreReturnsFailure ==
    /\ CoreRunning
    /\ CoreResult = "none"
    /\ CoreRunning' = FALSE
    /\ CoreResult' = "failure"
    /\ WorkerOutcome' = IF CancelRequested THEN "aborted" ELSE "failed"
    /\ DurableStatus' = IF CancelRequested THEN "cancelled" ELSE "failed"
    /\ ClaimActive' = FALSE
    /\ UNCHANGED <<
        CancelRequested,
        LeaseActive,
        QueueDelivery
        >>

LeaseExpires ==
    /\ DurableStatus = "running"
    /\ ClaimActive
    /\ LeaseActive
    /\ LeaseActive' = FALSE
    /\ UNCHANGED <<
        CancelRequested,
        ClaimActive,
        CoreResult,
        CoreRunning,
        DurableStatus,
        QueueDelivery,
        WorkerOutcome
        >>

CompleteAfterExpiredLeaseRejected ==
    /\ CoreRunning
    /\ CoreResult = "none"
    /\ ~LeaseActive
    /\ CoreRunning' = FALSE
    /\ CoreResult' = "success"
    /\ WorkerOutcome' = "failed"
    /\ UNCHANGED <<
        CancelRequested,
        ClaimActive,
        DurableStatus,
        LeaseActive,
        QueueDelivery
        >>

ReconcilerOrphansExpiredLease ==
    /\ DurableStatus = "running"
    /\ ClaimActive
    /\ ~LeaseActive
    /\ DurableStatus' = "orphaned"
    /\ ClaimActive' = FALSE
    /\ UNCHANGED <<
        CancelRequested,
        CoreResult,
        CoreRunning,
        LeaseActive,
        QueueDelivery,
        WorkerOutcome
        >>

Idle ==
    /\ DurableStatus \in {"succeeded", "failed", "cancelled", "orphaned"}
    /\ UNCHANGED Vars

Next ==
    \/ StartCoreExecution
    \/ RequestCancel
    \/ CoreReturnsSuccess
    \/ CoreReturnsCancelled
    \/ CoreReturnsFailure
    \/ LeaseExpires
    \/ CompleteAfterExpiredLeaseRejected
    \/ ReconcilerOrphansExpiredLease
    \/ Idle

InvTypeOK ==
    /\ CancelRequested \in BOOLEAN
    /\ ClaimActive \in BOOLEAN
    /\ CoreResult \in {"none", "success", "failure", "cancelled"}
    /\ CoreRunning \in BOOLEAN
    /\ DurableStatus \in {"running", "succeeded", "failed", "cancelled", "orphaned"}
    /\ LeaseActive \in BOOLEAN
    /\ QueueDelivery \in {"acked"}
    /\ WorkerOutcome \in {"none", "success", "failed", "aborted"}

InvQueueAckedHasDurableRecovery ==
    QueueDelivery = "acked" =>
        \/ DurableStatus = "running"
        \/ DurableStatus = "orphaned"
        \/ DurableStatus \in {"succeeded", "failed", "cancelled"}

InvExpiredLeaseDoesNotComplete ==
    (CoreResult = "success" /\ ~LeaseActive) => DurableStatus # "succeeded"

InvSuccessfulCoreResultWinsCancelRace ==
    CoreResult = "success" => WorkerOutcome # "aborted"

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
