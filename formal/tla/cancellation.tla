---- MODULE cancellation ----
EXTENDS Naturals

CONSTANTS Nil

RunStatuses == {
    "queued",
    "running",
    "orphaned",
    "succeeded",
    "failed",
    "cancelled"
}

ExecutionStatuses == {
    "none",
    "planned",
    "pending",
    "accepted",
    "running",
    "succeeded",
    "failed",
    "aborted"
}

TerminalRunStatuses == {"succeeded", "failed", "cancelled"}
TerminalExecutionStatuses == {"succeeded", "failed", "aborted"}

VARIABLES
    RunStatus,
    RootStatus,
    ChildStatus,
    RootClaimActive,
    ChildClaimActive,
    OtherClaimActive,
    CancelRequested,
    CancelDelivered,
    FirstTerminalRunStatus,
    TerminalConflict,
    NewAuthorityAfterCancel

Vars == <<
    RunStatus,
    RootStatus,
    ChildStatus,
    RootClaimActive,
    ChildClaimActive,
    OtherClaimActive,
    CancelRequested,
    CancelDelivered,
    FirstTerminalRunStatus,
    TerminalConflict,
    NewAuthorityAfterCancel
>>

Init ==
    /\ RunStatus = "queued"
    /\ RootStatus = "pending"
    /\ ChildStatus \in {"none", "planned"}
    /\ RootClaimActive = FALSE
    /\ ChildClaimActive = FALSE
    /\ OtherClaimActive = FALSE
    /\ CancelRequested = FALSE
    /\ CancelDelivered = FALSE
    /\ FirstTerminalRunStatus = Nil
    /\ TerminalConflict = FALSE
    /\ NewAuthorityAfterCancel = FALSE

IsTerminalRunStatus(status) ==
    status \in TerminalRunStatuses

IsTerminalExecutionStatus(status) ==
    status \in TerminalExecutionStatuses

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

ClaimRoot ==
    /\ RunStatus = "queued"
    /\ RootStatus = "pending"
    /\ ~RootClaimActive
    /\ RunStatus' = "running"
    /\ RootStatus' = "accepted"
    /\ RootClaimActive' = TRUE
    /\ UNCHANGED <<
        ChildStatus,
        ChildClaimActive,
        OtherClaimActive,
        CancelRequested,
        CancelDelivered,
        FirstTerminalRunStatus,
        TerminalConflict,
        NewAuthorityAfterCancel
        >>

StartRoot ==
    /\ RunStatus = "running"
    /\ RootStatus = "accepted"
    /\ RootClaimActive
    /\ RootStatus' = "running"
    /\ UNCHANGED <<
        RunStatus,
        ChildStatus,
        RootClaimActive,
        ChildClaimActive,
        OtherClaimActive,
        CancelRequested,
        CancelDelivered,
        FirstTerminalRunStatus,
        TerminalConflict,
        NewAuthorityAfterCancel
        >>

RequestCancel ==
    /\ RunStatus = "running"
    /\ CancelRequested' = TRUE
    /\ UNCHANGED <<
        RunStatus,
        RootStatus,
        ChildStatus,
        RootClaimActive,
        ChildClaimActive,
        OtherClaimActive,
        CancelDelivered,
        FirstTerminalRunStatus,
        TerminalConflict,
        NewAuthorityAfterCancel
        >>

DeliverCancel ==
    /\ CancelRequested
    /\ RunStatus \in {"running", "orphaned"}
    /\ (RootClaimActive \/ ChildClaimActive \/ OtherClaimActive)
    /\ CancelDelivered' = TRUE
    /\ UNCHANGED <<
        RunStatus,
        RootStatus,
        ChildStatus,
        RootClaimActive,
        ChildClaimActive,
        OtherClaimActive,
        CancelRequested,
        FirstTerminalRunStatus,
        TerminalConflict,
        NewAuthorityAfterCancel
        >>

RootSuccessTerminal ==
    /\ RunStatus = "running"
    /\ RootStatus \in {"accepted", "running"}
    /\ ChildStatus = "none"
    /\ RootClaimActive
    /\ RunStatus' = "succeeded"
    /\ RootStatus' = "succeeded"
    /\ RootClaimActive' = FALSE
    /\ ChildClaimActive' = FALSE
    /\ OtherClaimActive' = FALSE
    /\ CancelRequested' = FALSE
    /\ RecordTerminal("succeeded")
    /\ UNCHANGED <<
        ChildStatus,
        CancelDelivered,
        NewAuthorityAfterCancel
        >>

RootSuccessContinuation ==
    /\ RunStatus = "running"
    /\ RootStatus \in {"accepted", "running"}
    /\ ChildStatus = "planned"
    /\ RootClaimActive
    /\ ~CancelRequested
    /\ RunStatus' = "queued"
    /\ RootStatus' = "succeeded"
    /\ ChildStatus' = "pending"
    /\ RootClaimActive' = FALSE
    /\ CancelRequested' = FALSE
    /\ NewAuthorityAfterCancel' = (NewAuthorityAfterCancel \/ CancelRequested)
    /\ UNCHANGED <<
        ChildClaimActive,
        OtherClaimActive,
        CancelDelivered,
        FirstTerminalRunStatus,
        TerminalConflict
        >>

RootSuccessCancelStopsContinuation ==
    /\ RunStatus = "running"
    /\ RootStatus \in {"accepted", "running"}
    /\ ChildStatus = "planned"
    /\ RootClaimActive
    /\ CancelRequested
    /\ RunStatus' = "cancelled"
    /\ RootStatus' = "succeeded"
    /\ RootClaimActive' = FALSE
    /\ ChildClaimActive' = FALSE
    /\ OtherClaimActive' = FALSE
    /\ CancelRequested' = FALSE
    /\ RecordTerminal("cancelled")
    /\ UNCHANGED <<
        ChildStatus,
        CancelDelivered,
        NewAuthorityAfterCancel
        >>

ClaimChild ==
    /\ RunStatus = "queued"
    /\ ChildStatus = "pending"
    /\ ~ChildClaimActive
    /\ RunStatus' = "running"
    /\ ChildStatus' = "accepted"
    /\ ChildClaimActive' = TRUE
    /\ UNCHANGED <<
        RootStatus,
        RootClaimActive,
        OtherClaimActive,
        CancelRequested,
        CancelDelivered,
        FirstTerminalRunStatus,
        TerminalConflict,
        NewAuthorityAfterCancel
        >>

ClaimOther ==
    /\ RunStatus = "running"
    /\ ChildClaimActive
    /\ ~OtherClaimActive
    /\ OtherClaimActive' = TRUE
    /\ UNCHANGED <<
        RunStatus,
        RootStatus,
        ChildStatus,
        RootClaimActive,
        ChildClaimActive,
        CancelRequested,
        CancelDelivered,
        FirstTerminalRunStatus,
        TerminalConflict,
        NewAuthorityAfterCancel
        >>

StartChild ==
    /\ RunStatus = "running"
    /\ ChildStatus = "accepted"
    /\ ChildClaimActive
    /\ ChildStatus' = "running"
    /\ UNCHANGED <<
        RunStatus,
        RootStatus,
        RootClaimActive,
        ChildClaimActive,
        OtherClaimActive,
        CancelRequested,
        CancelDelivered,
        FirstTerminalRunStatus,
        TerminalConflict,
        NewAuthorityAfterCancel
        >>

RootAbort ==
    /\ RunStatus \in {"running", "orphaned"}
    /\ RootStatus \in {"accepted", "running"}
    /\ RootClaimActive
    /\ RunStatus' = "cancelled"
    /\ RootStatus' = "aborted"
    /\ RootClaimActive' = FALSE
    /\ ChildClaimActive' = FALSE
    /\ OtherClaimActive' = FALSE
    /\ CancelRequested' = FALSE
    /\ RecordTerminal("cancelled")
    /\ UNCHANGED <<
        ChildStatus,
        CancelDelivered,
        NewAuthorityAfterCancel
        >>

ChildAbort ==
    /\ RunStatus \in {"running", "orphaned"}
    /\ ChildStatus \in {"accepted", "running"}
    /\ ChildClaimActive
    /\ RunStatus' = "cancelled"
    /\ ChildStatus' = "aborted"
    /\ RootClaimActive' = FALSE
    /\ ChildClaimActive' = FALSE
    /\ OtherClaimActive' = FALSE
    /\ CancelRequested' = FALSE
    /\ RecordTerminal("cancelled")
    /\ UNCHANGED <<
        RootStatus,
        CancelDelivered,
        NewAuthorityAfterCancel
        >>

ChildSuccessTerminal ==
    /\ RunStatus = "running"
    /\ RootStatus = "succeeded"
    /\ ChildStatus \in {"accepted", "running"}
    /\ ChildClaimActive
    /\ RunStatus' = "succeeded"
    /\ ChildStatus' = "succeeded"
    /\ RootClaimActive' = FALSE
    /\ ChildClaimActive' = FALSE
    /\ OtherClaimActive' = FALSE
    /\ CancelRequested' = FALSE
    /\ RecordTerminal("succeeded")
    /\ UNCHANGED <<
        RootStatus,
        CancelDelivered,
        NewAuthorityAfterCancel
        >>

LeaseExpires ==
    /\ RunStatus = "running"
    /\ RootClaimActive \/ ChildClaimActive
    /\ RootClaimActive' = FALSE
    /\ ChildClaimActive' = FALSE
    /\ OtherClaimActive' = FALSE
    /\ RunStatus' = "orphaned"
    /\ UNCHANGED <<
        RootStatus,
        ChildStatus,
        CancelRequested,
        CancelDelivered,
        FirstTerminalRunStatus,
        TerminalConflict,
        NewAuthorityAfterCancel
        >>

OperatorRetry ==
    /\ IsTerminalRunStatus(RunStatus) \/ RunStatus = "orphaned"
    /\ RunStatus' = "queued"
    /\ RootStatus' = "pending"
    /\ ChildStatus' = "planned"
    /\ RootClaimActive' = FALSE
    /\ ChildClaimActive' = FALSE
    /\ OtherClaimActive' = FALSE
    /\ CancelRequested' = FALSE
    /\ CancelDelivered' = FALSE
    /\ NewAuthorityAfterCancel' = FALSE
    /\ FirstTerminalRunStatus' = Nil
    /\ TerminalConflict' = FALSE

Idle ==
    UNCHANGED Vars

Next ==
    \/ ClaimRoot
    \/ StartRoot
    \/ RequestCancel
    \/ DeliverCancel
    \/ RootSuccessTerminal
    \/ RootSuccessContinuation
    \/ RootSuccessCancelStopsContinuation
    \/ ClaimChild
    \/ ClaimOther
    \/ StartChild
    \/ RootAbort
    \/ ChildAbort
    \/ ChildSuccessTerminal
    \/ LeaseExpires
    \/ OperatorRetry
    \/ Idle

InvTypeOK ==
    /\ RunStatus \in RunStatuses
    /\ RootStatus \in ExecutionStatuses
    /\ ChildStatus \in ExecutionStatuses
    /\ RootClaimActive \in BOOLEAN
    /\ ChildClaimActive \in BOOLEAN
    /\ OtherClaimActive \in BOOLEAN
    /\ CancelRequested \in BOOLEAN
    /\ CancelDelivered \in BOOLEAN
    /\ FirstTerminalRunStatus \in TerminalRunStatuses \union {Nil}
    /\ TerminalConflict \in BOOLEAN
    /\ NewAuthorityAfterCancel \in BOOLEAN

InvBestEffortCancelDoesNotCreateFollowOnAuthority ==
    ~NewAuthorityAfterCancel

InvTerminalRunClearsExecutionAuthority ==
    IsTerminalRunStatus(RunStatus) =>
        /\ ~RootClaimActive
        /\ ~ChildClaimActive
        /\ ~OtherClaimActive

InvTerminalRunStatusStableWithoutRetry ==
    ~TerminalConflict

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
