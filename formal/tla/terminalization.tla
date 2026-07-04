---- MODULE terminalization ----
EXTENDS Naturals

CONSTANTS
    Nil,
    MaxSnapshots

ASSUME MaxSnapshots \in Nat /\ MaxSnapshots > 0

RunStatuses == {
    "queued",
    "running",
    "orphaned",
    "failed",
    "succeeded",
    "cancelled",
    "abandoned",
    "aborted"
}

ExecutionStatuses == {
    "planned",
    "pending",
    "accepted",
    "running",
    "failed",
    "succeeded",
    "cancelled",
    "aborted"
}

TerminalRunStatuses == {
    "failed",
    "succeeded",
    "cancelled",
    "abandoned",
    "aborted"
}

TerminalExecutionStatuses == {
    "failed",
    "succeeded",
    "cancelled",
    "aborted"
}

NonTerminalExecutionStatuses == {
    "planned",
    "pending",
    "accepted",
    "running"
}

CatalogRunTargets == {
    "running",
    "orphaned",
    "failed",
    "succeeded",
    "cancelled",
    "aborted"
}

CatalogExecutionTargets == {
    "accepted",
    "running",
    "failed",
    "succeeded",
    "cancelled",
    "aborted"
}

VARIABLES
    RunStatus,
    ExecutionStatus,
    ClaimActive,
    FirstTerminalRunStatus,
    TerminalConflict,
    SnapshotWrites

Vars == <<
    RunStatus,
    ExecutionStatus,
    ClaimActive,
    FirstTerminalRunStatus,
    TerminalConflict,
    SnapshotWrites
>>

Init ==
    /\ RunStatus = "queued"
    /\ ExecutionStatus = "pending"
    /\ ClaimActive = FALSE
    /\ FirstTerminalRunStatus = Nil
    /\ TerminalConflict = FALSE
    /\ SnapshotWrites = 0

IsTerminalRunStatus(status) ==
    status \in TerminalRunStatuses

IsTerminalExecutionStatus(status) ==
    status \in TerminalExecutionStatuses

IsNonTerminalExecutionStatus(status) ==
    status \in NonTerminalExecutionStatuses

NonTerminalExecutionRank(status) ==
    CASE status = "planned" -> 0
      [] status = "pending" -> 1
      [] status = "accepted" -> 2
      [] status = "running" -> 3
      [] OTHER -> 0

CatalogRunDecision(current, target) ==
    IF current = target THEN "noop"
    ELSE IF IsTerminalRunStatus(current) THEN
        IF IsTerminalRunStatus(target) THEN "conflict" ELSE "noop"
    ELSE IF IsTerminalRunStatus(target) THEN "apply"
    ELSE IF target \in {"running", "orphaned"} THEN "apply"
    ELSE "conflict"

CatalogExecutionDecision(current, target) ==
    IF current = target THEN "noop"
    ELSE IF IsTerminalExecutionStatus(current) THEN
        IF IsTerminalExecutionStatus(target) THEN "conflict" ELSE "noop"
    ELSE
        LET currentRank == NonTerminalExecutionRank(current) IN
        LET targetRank == NonTerminalExecutionRank(target) IN
            IF IsNonTerminalExecutionStatus(current) /\ IsNonTerminalExecutionStatus(target) THEN
                IF targetRank > currentRank THEN "apply" ELSE "noop"
            ELSE IF IsNonTerminalExecutionStatus(current) /\ IsTerminalExecutionStatus(target) THEN "apply"
            ELSE "conflict"

NextFirstTerminalRunStatus(target) ==
    IF IsTerminalRunStatus(target) /\ FirstTerminalRunStatus = Nil
    THEN target
    ELSE FirstTerminalRunStatus

NextTerminalConflict(target) ==
    TerminalConflict \/
        (IsTerminalRunStatus(target) /\
            FirstTerminalRunStatus # Nil /\
            FirstTerminalRunStatus # target)

RecordTerminal(target) ==
    /\ FirstTerminalRunStatus' = NextFirstTerminalRunStatus(target)
    /\ TerminalConflict' = NextTerminalConflict(target)

CatalogRunStatusUpdate(target) ==
    /\ target \in CatalogRunTargets
    /\ CatalogRunDecision(RunStatus, target) = "apply"
    /\ RunStatus' = IF target = "aborted" THEN "cancelled" ELSE target
    /\ RecordTerminal(RunStatus')
    /\ ClaimActive' =
        IF IsTerminalRunStatus(RunStatus') \/ RunStatus' = "orphaned"
        THEN FALSE
        ELSE ClaimActive
    /\ UNCHANGED << ExecutionStatus, SnapshotWrites >>

CatalogRunStatusNoop(target) ==
    /\ target \in CatalogRunTargets
    /\ CatalogRunDecision(RunStatus, target) = "noop"
    /\ UNCHANGED Vars

CatalogExecutionStatusUpdate(target) ==
    /\ target \in CatalogExecutionTargets
    /\ CatalogExecutionDecision(ExecutionStatus, target) = "apply"
    /\ ExecutionStatus' = target
    /\ ClaimActive' = IF IsTerminalExecutionStatus(target) THEN FALSE ELSE ClaimActive
    /\ UNCHANGED << RunStatus, FirstTerminalRunStatus, TerminalConflict, SnapshotWrites >>

CatalogExecutionStatusNoop(target) ==
    /\ target \in CatalogExecutionTargets
    /\ CatalogExecutionDecision(ExecutionStatus, target) = "noop"
    /\ UNCHANGED Vars

ClaimExecution ==
    /\ RunStatus = "queued"
    /\ ExecutionStatus = "pending"
    /\ ~ClaimActive
    /\ RunStatus' = "running"
    /\ ExecutionStatus' = "accepted"
    /\ ClaimActive' = TRUE
    /\ UNCHANGED << FirstTerminalRunStatus, TerminalConflict, SnapshotWrites >>

StartExecution ==
    /\ RunStatus = "running"
    /\ ExecutionStatus = "accepted"
    /\ ClaimActive
    /\ ExecutionStatus' = "running"
    /\ UNCHANGED << RunStatus, ClaimActive, FirstTerminalRunStatus, TerminalConflict, SnapshotWrites >>

WorkerFinalize(status) ==
    /\ status \in TerminalExecutionStatuses
    /\ RunStatus \in {"running", "orphaned"}
    /\ ExecutionStatus \in {"pending", "accepted", "running"}
    /\ ClaimActive
    /\ ExecutionStatus' = status
    /\ RunStatus' =
        IF status = "succeeded" THEN "succeeded"
        ELSE IF status \in {"cancelled", "aborted"} THEN "cancelled"
        ELSE "failed"
    /\ ClaimActive' = FALSE
    /\ RecordTerminal(RunStatus')
    /\ UNCHANGED SnapshotWrites

OrphanRun ==
    /\ RunStatus = "running"
    /\ ~ClaimActive
    /\ RunStatus' = "orphaned"
    /\ UNCHANGED << ExecutionStatus, ClaimActive, FirstTerminalRunStatus, TerminalConflict, SnapshotWrites >>

LeaseExpires ==
    /\ ClaimActive
    /\ ClaimActive' = FALSE
    /\ UNCHANGED << RunStatus, ExecutionStatus, FirstTerminalRunStatus, TerminalConflict, SnapshotWrites >>

TerminalSnapshot(outcome, executionStatus) ==
    /\ outcome \in {"succeeded", "failed", "cancelled"}
    /\ executionStatus \in {"accepted", "running", "succeeded", "failed", "cancelled", "aborted"}
    /\ SnapshotWrites < MaxSnapshots
    /\ CatalogRunDecision(RunStatus, outcome) # "conflict"
    /\ IF CatalogExecutionDecision(ExecutionStatus, executionStatus) = "apply"
        THEN
            /\ ExecutionStatus' = executionStatus
        ELSE
            /\ ExecutionStatus' = ExecutionStatus
    /\ ClaimActive' = FALSE
    /\ RunStatus' =
        IF CatalogRunDecision(RunStatus, outcome) = "apply" THEN outcome ELSE RunStatus
    /\ RecordTerminal(RunStatus')
    /\ SnapshotWrites' = SnapshotWrites + 1

Idle ==
    UNCHANGED Vars

Next ==
    \/ \E target \in CatalogRunTargets:
        \/ CatalogRunStatusUpdate(target)
        \/ CatalogRunStatusNoop(target)
    \/ \E target \in CatalogExecutionTargets:
        \/ CatalogExecutionStatusUpdate(target)
        \/ CatalogExecutionStatusNoop(target)
    \/ ClaimExecution
    \/ StartExecution
    \/ \E status \in TerminalExecutionStatuses:
        WorkerFinalize(status)
    \/ OrphanRun
    \/ LeaseExpires
    \/ \E outcome \in {"succeeded", "failed", "cancelled"}:
        \E executionStatus \in {"accepted", "running", "succeeded", "failed", "cancelled", "aborted"}:
            TerminalSnapshot(outcome, executionStatus)
    \/ Idle

InvTypeOK ==
    /\ RunStatus \in RunStatuses
    /\ ExecutionStatus \in ExecutionStatuses
    /\ ClaimActive \in BOOLEAN
    /\ FirstTerminalRunStatus \in TerminalRunStatuses \union {Nil}
    /\ TerminalConflict \in BOOLEAN
    /\ SnapshotWrites \in 0..MaxSnapshots

InvTerminalRunStatusIsStableWithoutExplicitRepair ==
    ~TerminalConflict

InvTerminalRunClearsActiveExecutionClaim ==
    IsTerminalRunStatus(RunStatus) => ~ClaimActive

InvTerminalExecutionDoesNotBelongToNonTerminalClaim ==
    IsTerminalExecutionStatus(ExecutionStatus) => ~ClaimActive

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
