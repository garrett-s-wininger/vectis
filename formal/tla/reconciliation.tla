---- MODULE reconciliation ----
EXTENDS Naturals

CONSTANTS
    Runs,
    Workers,
    Reconcilers,
    Nil,
    MaxAttempts,
    MaxDeliveries,
    MaxStarts

ASSUME MaxAttempts \in Nat /\ MaxAttempts > 0
ASSUME MaxDeliveries \in Nat /\ MaxDeliveries > 0
ASSUME MaxStarts \in Nat /\ MaxStarts > 0

Attempts == 1..MaxAttempts

RunStatuses == {
    "queued",
    "running",
    "orphaned",
    "failed",
    "succeeded",
    "cancelled",
    "abandoned"
}

ExecutionStatuses == {
    "unused",
    "pending",
    "accepted",
    "running",
    "failed",
    "succeeded",
    "cancelled"
}

TerminalRunStatuses == {
    "failed",
    "succeeded",
    "cancelled",
    "abandoned"
}

VARIABLES
    RunStatus,
    ExecutionStatus,
    CurrentAttempt,
    QueueDeliveries,
    ClaimOwner,
    LeaseActive,
    HotOwnerActive,
    DispatchFresh,
    ExecutionStarts,
    ExplicitRequeues,
    OrphanedSeen,
    AutoTerminalAfterOrphan,
    ReconcilerLeaseOwner

Vars == <<
    RunStatus,
    ExecutionStatus,
    CurrentAttempt,
    QueueDeliveries,
    ClaimOwner,
    LeaseActive,
    HotOwnerActive,
    DispatchFresh,
    ExecutionStarts,
    ExplicitRequeues,
    OrphanedSeen,
    AutoTerminalAfterOrphan,
    ReconcilerLeaseOwner
>>

Init ==
    /\ RunStatus = [r \in Runs |-> "queued"]
    /\ ExecutionStatus =
        [r \in Runs |->
            [a \in Attempts |->
                IF a = 1 THEN "pending" ELSE "unused"]]
    /\ CurrentAttempt = [r \in Runs |-> 1]
    /\ QueueDeliveries = [r \in Runs |-> [a \in Attempts |-> 0]]
    /\ ClaimOwner = [r \in Runs |-> [a \in Attempts |-> Nil]]
    /\ LeaseActive = [r \in Runs |-> [a \in Attempts |-> FALSE]]
    /\ HotOwnerActive = [r \in Runs |-> FALSE]
    /\ DispatchFresh = [r \in Runs |-> FALSE]
    /\ ExecutionStarts = [r \in Runs |-> [a \in Attempts |-> 0]]
    /\ ExplicitRequeues = [r \in Runs |-> 0]
    /\ OrphanedSeen = [r \in Runs |-> FALSE]
    /\ AutoTerminalAfterOrphan = [r \in Runs |-> FALSE]
    /\ ReconcilerLeaseOwner = Nil

ActiveExecutionStatus(status) ==
    status = "accepted" \/ status = "running"

ActiveExecutionLease(r) ==
    \E a \in Attempts:
        /\ ActiveExecutionStatus(ExecutionStatus[r][a])
        /\ LeaseActive[r][a]

PendingDispatchable(r) ==
    LET a == CurrentAttempt[r] IN
        /\ RunStatus[r] = "queued"
        /\ ExecutionStatus[r][a] = "pending"
        /\ ~HotOwnerActive[r]
        /\ ~DispatchFresh[r]

Claimable(r, a) ==
    /\ a = CurrentAttempt[r]
    /\ ExecutionStatus[r][a] = "pending"
    /\ RunStatus[r] \in {"queued", "running"}
    /\ ClaimOwner[r][a] = Nil

CanOperatorRequeue(r) ==
    /\ RunStatus[r] \in {"queued", "failed", "orphaned", "cancelled", "abandoned"}
    /\ CurrentAttempt[r] < MaxAttempts

AcquireReconcilerLease(rec) ==
    /\ rec \in Reconcilers
    /\ ReconcilerLeaseOwner = Nil
    /\ ReconcilerLeaseOwner' = rec
    /\ UNCHANGED <<
        RunStatus,
        ExecutionStatus,
        CurrentAttempt,
        QueueDeliveries,
        ClaimOwner,
        LeaseActive,
        HotOwnerActive,
        DispatchFresh,
        ExecutionStarts,
        ExplicitRequeues,
        OrphanedSeen,
        AutoTerminalAfterOrphan
            >>

ReleaseReconcilerLease(rec) ==
    /\ rec \in Reconcilers
    /\ ReconcilerLeaseOwner = rec
    /\ ReconcilerLeaseOwner' = Nil
    /\ UNCHANGED <<
        RunStatus,
        ExecutionStatus,
        CurrentAttempt,
        QueueDeliveries,
        ClaimOwner,
        LeaseActive,
        HotOwnerActive,
        DispatchFresh,
        ExecutionStarts,
        ExplicitRequeues,
        OrphanedSeen,
        AutoTerminalAfterOrphan
            >>

DispatchGapPasses(r) ==
    /\ r \in Runs
    /\ DispatchFresh[r]
    /\ DispatchFresh' = [DispatchFresh EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<
        RunStatus,
        ExecutionStatus,
        CurrentAttempt,
        QueueDeliveries,
        ClaimOwner,
        LeaseActive,
        HotOwnerActive,
        ExecutionStarts,
        ExplicitRequeues,
        OrphanedSeen,
        AutoTerminalAfterOrphan,
        ReconcilerLeaseOwner
            >>

ReconcilerRedispatch(rec, r) ==
    LET a == CurrentAttempt[r] IN
        /\ rec \in Reconcilers
        /\ ReconcilerLeaseOwner = rec
        /\ PendingDispatchable(r)
        /\ QueueDeliveries[r][a] < MaxDeliveries
        /\ QueueDeliveries' = [QueueDeliveries EXCEPT ![r][a] = @ + 1]
        /\ DispatchFresh' = [DispatchFresh EXCEPT ![r] = TRUE]
        /\ UNCHANGED <<
            RunStatus,
            ExecutionStatus,
            CurrentAttempt,
            ClaimOwner,
            LeaseActive,
            HotOwnerActive,
            ExecutionStarts,
            ExplicitRequeues,
            OrphanedSeen,
            AutoTerminalAfterOrphan,
            ReconcilerLeaseOwner
            >>

QueueDeliveryLost(r, a) ==
    /\ r \in Runs
    /\ a \in Attempts
    /\ QueueDeliveries[r][a] > 0
    /\ QueueDeliveries' = [QueueDeliveries EXCEPT ![r][a] = @ - 1]
    /\ UNCHANGED <<
        RunStatus,
        ExecutionStatus,
        CurrentAttempt,
        ClaimOwner,
        LeaseActive,
        HotOwnerActive,
        DispatchFresh,
        ExecutionStarts,
        ExplicitRequeues,
        OrphanedSeen,
        AutoTerminalAfterOrphan,
        ReconcilerLeaseOwner
            >>

WorkerDropsUnclaimableDelivery(w, r, a) ==
    /\ w \in Workers
    /\ r \in Runs
    /\ a \in Attempts
    /\ QueueDeliveries[r][a] > 0
    /\ ~Claimable(r, a)
    /\ QueueDeliveries' = [QueueDeliveries EXCEPT ![r][a] = @ - 1]
    /\ UNCHANGED <<
        RunStatus,
        ExecutionStatus,
        CurrentAttempt,
        ClaimOwner,
        LeaseActive,
        HotOwnerActive,
        DispatchFresh,
        ExecutionStarts,
        ExplicitRequeues,
        OrphanedSeen,
        AutoTerminalAfterOrphan,
        ReconcilerLeaseOwner
            >>

WorkerClaim(w, r, a) ==
    /\ w \in Workers
    /\ r \in Runs
    /\ a \in Attempts
    /\ QueueDeliveries[r][a] > 0
    /\ Claimable(r, a)
    /\ QueueDeliveries' = [QueueDeliveries EXCEPT ![r][a] = @ - 1]
    /\ ExecutionStatus' = [ExecutionStatus EXCEPT ![r][a] = "accepted"]
    /\ RunStatus' = [RunStatus EXCEPT ![r] = "running"]
    /\ ClaimOwner' = [ClaimOwner EXCEPT ![r][a] = w]
    /\ LeaseActive' = [LeaseActive EXCEPT ![r][a] = TRUE]
    /\ HotOwnerActive' = [HotOwnerActive EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<
        CurrentAttempt,
        DispatchFresh,
        ExecutionStarts,
        ExplicitRequeues,
        OrphanedSeen,
        AutoTerminalAfterOrphan,
        ReconcilerLeaseOwner
            >>

WorkerStart(w, r, a) ==
    /\ w \in Workers
    /\ r \in Runs
    /\ a \in Attempts
    /\ ExecutionStatus[r][a] = "accepted"
    /\ ClaimOwner[r][a] = w
    /\ LeaseActive[r][a]
    /\ ExecutionStarts[r][a] < MaxStarts
    /\ ExecutionStatus' = [ExecutionStatus EXCEPT ![r][a] = "running"]
    /\ ExecutionStarts' = [ExecutionStarts EXCEPT ![r][a] = @ + 1]
    /\ UNCHANGED <<
        RunStatus,
        CurrentAttempt,
        QueueDeliveries,
        ClaimOwner,
        LeaseActive,
        HotOwnerActive,
        DispatchFresh,
        ExplicitRequeues,
        OrphanedSeen,
        AutoTerminalAfterOrphan,
        ReconcilerLeaseOwner
            >>

WorkerComplete(w, r, a) ==
    /\ w \in Workers
    /\ r \in Runs
    /\ a \in Attempts
    /\ ExecutionStatus[r][a] = "running"
    /\ ClaimOwner[r][a] = w
    /\ LeaseActive[r][a]
    /\ ExecutionStatus' = [ExecutionStatus EXCEPT ![r][a] = "succeeded"]
    /\ RunStatus' = [RunStatus EXCEPT ![r] = "succeeded"]
    /\ ClaimOwner' = [ClaimOwner EXCEPT ![r][a] = Nil]
    /\ LeaseActive' = [LeaseActive EXCEPT ![r][a] = FALSE]
    /\ HotOwnerActive' = [HotOwnerActive EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<
        CurrentAttempt,
        QueueDeliveries,
        DispatchFresh,
        ExecutionStarts,
        ExplicitRequeues,
        OrphanedSeen,
        AutoTerminalAfterOrphan,
        ReconcilerLeaseOwner
            >>

WorkerFail(w, r, a) ==
    /\ w \in Workers
    /\ r \in Runs
    /\ a \in Attempts
    /\ ExecutionStatus[r][a] = "running"
    /\ ClaimOwner[r][a] = w
    /\ LeaseActive[r][a]
    /\ ExecutionStatus' = [ExecutionStatus EXCEPT ![r][a] = "failed"]
    /\ RunStatus' = [RunStatus EXCEPT ![r] = "failed"]
    /\ ClaimOwner' = [ClaimOwner EXCEPT ![r][a] = Nil]
    /\ LeaseActive' = [LeaseActive EXCEPT ![r][a] = FALSE]
    /\ HotOwnerActive' = [HotOwnerActive EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<
        CurrentAttempt,
        QueueDeliveries,
        DispatchFresh,
        ExecutionStarts,
        ExplicitRequeues,
        OrphanedSeen,
        AutoTerminalAfterOrphan,
        ReconcilerLeaseOwner
            >>

LeaseExpires(r, a) ==
    /\ r \in Runs
    /\ a \in Attempts
    /\ ActiveExecutionStatus(ExecutionStatus[r][a])
    /\ LeaseActive[r][a]
    /\ LeaseActive' = [LeaseActive EXCEPT ![r][a] = FALSE]
    /\ HotOwnerActive' = [HotOwnerActive EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<
        RunStatus,
        ExecutionStatus,
        CurrentAttempt,
        QueueDeliveries,
        ClaimOwner,
        DispatchFresh,
        ExecutionStarts,
        ExplicitRequeues,
        OrphanedSeen,
        AutoTerminalAfterOrphan,
        ReconcilerLeaseOwner
            >>

ReconcilerOrphansExpiredRun(rec, r) ==
    /\ rec \in Reconcilers
    /\ ReconcilerLeaseOwner = rec
    /\ r \in Runs
    /\ RunStatus[r] = "running"
    /\ ~HotOwnerActive[r]
    /\ ~ActiveExecutionLease(r)
    /\ RunStatus' = [RunStatus EXCEPT ![r] = "orphaned"]
    /\ OrphanedSeen' = [OrphanedSeen EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<
        ExecutionStatus,
        CurrentAttempt,
        QueueDeliveries,
        ClaimOwner,
        LeaseActive,
        HotOwnerActive,
        DispatchFresh,
        ExecutionStarts,
        ExplicitRequeues,
        AutoTerminalAfterOrphan,
        ReconcilerLeaseOwner
            >>

ReconcilerExpiresDispatchDeadline(rec, r, a) ==
    /\ rec \in Reconcilers
    /\ ReconcilerLeaseOwner = rec
    /\ r \in Runs
    /\ a \in Attempts
    /\ ExecutionStatus[r][a] \in {"pending", "accepted"}
    /\ RunStatus[r] \in {"queued", "running"}
    /\ ~LeaseActive[r][a]
    /\ ExecutionStatus' = [ExecutionStatus EXCEPT ![r][a] = "failed"]
    /\ RunStatus' = [RunStatus EXCEPT ![r] = "failed"]
    /\ ClaimOwner' = [ClaimOwner EXCEPT ![r][a] = Nil]
    /\ AutoTerminalAfterOrphan' =
        [AutoTerminalAfterOrphan EXCEPT ![r] = @ \/ RunStatus[r] = "orphaned"]
    /\ UNCHANGED <<
        CurrentAttempt,
        QueueDeliveries,
        LeaseActive,
        HotOwnerActive,
        DispatchFresh,
        ExecutionStarts,
        ExplicitRequeues,
        OrphanedSeen,
        ReconcilerLeaseOwner
            >>

OperatorForceRequeue(r) ==
    LET next == CurrentAttempt[r] + 1 IN
        /\ r \in Runs
        /\ CanOperatorRequeue(r)
        /\ RunStatus' = [RunStatus EXCEPT ![r] = "queued"]
        /\ CurrentAttempt' = [CurrentAttempt EXCEPT ![r] = next]
        /\ ExecutionStatus' = [ExecutionStatus EXCEPT ![r][next] = "pending"]
        /\ QueueDeliveries' = [QueueDeliveries EXCEPT ![r][next] = 0]
        /\ ClaimOwner' = [ClaimOwner EXCEPT ![r][next] = Nil]
        /\ LeaseActive' = [LeaseActive EXCEPT ![r][next] = FALSE]
        /\ HotOwnerActive' = [HotOwnerActive EXCEPT ![r] = FALSE]
        /\ DispatchFresh' = [DispatchFresh EXCEPT ![r] = FALSE]
        /\ ExplicitRequeues' = [ExplicitRequeues EXCEPT ![r] = @ + 1]
        /\ UNCHANGED <<
            ExecutionStarts,
            OrphanedSeen,
            AutoTerminalAfterOrphan,
            ReconcilerLeaseOwner
            >>

Idle ==
    UNCHANGED Vars

Next ==
    \/ \E rec \in Reconcilers:
        \/ AcquireReconcilerLease(rec)
        \/ ReleaseReconcilerLease(rec)
        \/ \E r \in Runs:
            \/ ReconcilerRedispatch(rec, r)
            \/ ReconcilerOrphansExpiredRun(rec, r)
            \/ \E a \in Attempts:
                ReconcilerExpiresDispatchDeadline(rec, r, a)
    \/ \E w \in Workers:
        \E r \in Runs:
            \E a \in Attempts:
                \/ WorkerClaim(w, r, a)
                \/ WorkerStart(w, r, a)
                \/ WorkerComplete(w, r, a)
                \/ WorkerFail(w, r, a)
                \/ WorkerDropsUnclaimableDelivery(w, r, a)
    \/ \E r \in Runs:
        \/ DispatchGapPasses(r)
        \/ OperatorForceRequeue(r)
        \/ \E a \in Attempts:
            \/ QueueDeliveryLost(r, a)
            \/ LeaseExpires(r, a)
    \/ Idle

InvTypeOK ==
    /\ RunStatus \in [Runs -> RunStatuses]
    /\ ExecutionStatus \in [Runs -> [Attempts -> ExecutionStatuses]]
    /\ CurrentAttempt \in [Runs -> Attempts]
    /\ QueueDeliveries \in [Runs -> [Attempts -> 0..MaxDeliveries]]
    /\ ClaimOwner \in [Runs -> [Attempts -> Workers \union {Nil}]]
    /\ LeaseActive \in [Runs -> [Attempts -> BOOLEAN]]
    /\ HotOwnerActive \in [Runs -> BOOLEAN]
    /\ DispatchFresh \in [Runs -> BOOLEAN]
    /\ ExecutionStarts \in [Runs -> [Attempts -> 0..MaxStarts]]
    /\ ExplicitRequeues \in [Runs -> 0..MaxAttempts]
    /\ OrphanedSeen \in [Runs -> BOOLEAN]
    /\ AutoTerminalAfterOrphan \in [Runs -> BOOLEAN]
    /\ ReconcilerLeaseOwner \in Reconcilers \union {Nil}

InvAtMostOncePerExecutionAttempt ==
    \A r \in Runs:
        \A a \in Attempts:
            ExecutionStarts[r][a] <= 1

InvNoAutomaticTakeoverOfClaimedAttempt ==
    \A r \in Runs:
        \A a \in Attempts:
            /\ ActiveExecutionStatus(ExecutionStatus[r][a])
            /\ ClaimOwner[r][a] # Nil
            => ~(RunStatus[r] = "queued" /\ a = CurrentAttempt[r])

InvOrphanedRunsAreNotRedispatchable ==
    \A r \in Runs:
        RunStatus[r] = "orphaned" => ~PendingDispatchable(r)

InvNoAutomaticTerminalAfterOrphan ==
    \A r \in Runs:
        ~AutoTerminalAfterOrphan[r]

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
