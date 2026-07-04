---- MODULE continuation ----
EXTENDS FiniteSets, Naturals

CONSTANTS
    Implementations

ASSUME Implementations \subseteq {"durable", "hot"} /\ Implementations # {}

Tasks == {"root", "build", "compile", "test", "deploy"}

RunStatuses == {
    "queued",
    "running",
    "succeeded",
    "failed",
    "cancelled"
}

ExecutionStatuses == {
    "planned",
    "pending",
    "running",
    "succeeded",
    "failed",
    "cancelled"
}

TerminalRunStatuses == {"succeeded", "failed", "cancelled"}
AuthorityStatuses == ExecutionStatuses \ {"planned"}

VARIABLES
    Implementation,
    RunStatus,
    Status,
    ClaimActive,
    ExecutionStarts

Vars == <<
    Implementation,
    RunStatus,
    Status,
    ClaimActive,
    ExecutionStarts
>>

Init ==
    /\ Implementation \in Implementations
    /\ RunStatus = "queued"
    /\ Status = [t \in Tasks |-> IF t = "root" THEN "pending" ELSE "planned"]
    /\ ClaimActive = [t \in Tasks |-> FALSE]
    /\ ExecutionStarts = [t \in Tasks |-> 0]

BuildSubtreeSucceededIn(status) ==
    /\ status["build"] = "succeeded"
    /\ status["compile"] = "succeeded"
    /\ status["test"] = "succeeded"

ReadyIn(status, task) ==
    CASE task = "root" -> TRUE
      [] task = "build" ->
            status["root"] = "succeeded"
      [] task = "compile" \/ task = "test" ->
            status["build"] = "succeeded"
      [] task = "deploy" ->
            /\ status["root"] = "succeeded"
            /\ BuildSubtreeSucceededIn(status)
      [] OTHER -> FALSE

AllSucceeded(status) ==
    \A t \in Tasks:
        status[t] = "succeeded"

NoClaims ==
    [t \in Tasks |-> FALSE]

DurableActivate(status) ==
    [t \in Tasks |->
        IF status[t] = "planned" /\ ReadyIn(status, t)
        THEN "pending"
        ELSE status[t]]

HotActivate(status) ==
    DurableActivate(status)

RunAfterSuccess(status, dispatchable) ==
    IF AllSucceeded(status) THEN "succeeded"
    ELSE IF dispatchable # {} THEN "queued"
    ELSE "running"

Claim(task) ==
    /\ task \in Tasks
    /\ RunStatus \in {"queued", "running"}
    /\ Status[task] = "pending"
    /\ ~ClaimActive[task]
    /\ RunStatus' = "running"
    /\ Status' = [Status EXCEPT ![task] = "running"]
    /\ ClaimActive' = [ClaimActive EXCEPT ![task] = TRUE]
    /\ ExecutionStarts' = [ExecutionStarts EXCEPT ![task] = @ + 1]
    /\ UNCHANGED Implementation

CompleteSucceeded(task) ==
    /\ task \in Tasks
    /\ RunStatus = "running"
    /\ Status[task] = "running"
    /\ ClaimActive[task]
    /\ LET completed == [Status EXCEPT ![task] = "succeeded"] IN
       LET nextStatus ==
            IF Implementation = "durable"
            THEN DurableActivate(completed)
            ELSE HotActivate(completed) IN
       LET dispatchable ==
            {t \in Tasks:
                /\ nextStatus[t] = "pending"
                /\ ReadyIn(nextStatus, t)} IN
            /\ Status' = nextStatus
            /\ ClaimActive' = [ClaimActive EXCEPT ![task] = FALSE]
            /\ RunStatus' = RunAfterSuccess(nextStatus, dispatchable)
    /\ UNCHANGED << Implementation, ExecutionStarts >>

CompleteFailed(task) ==
    /\ task \in Tasks
    /\ RunStatus = "running"
    /\ Status[task] = "running"
    /\ ClaimActive[task]
    /\ RunStatus' = "failed"
    /\ Status' = [Status EXCEPT ![task] = "failed"]
    /\ ClaimActive' = NoClaims
    /\ UNCHANGED << Implementation, ExecutionStarts >>

CompleteCancelled(task) ==
    /\ task \in Tasks
    /\ RunStatus = "running"
    /\ Status[task] = "running"
    /\ ClaimActive[task]
    /\ RunStatus' = "cancelled"
    /\ Status' = [Status EXCEPT ![task] = "cancelled"]
    /\ ClaimActive' = NoClaims
    /\ UNCHANGED << Implementation, ExecutionStarts >>

Idle ==
    UNCHANGED Vars

Next ==
    \/ \E task \in Tasks:
        \/ Claim(task)
        \/ CompleteSucceeded(task)
        \/ CompleteFailed(task)
        \/ CompleteCancelled(task)
    \/ Idle

InvTypeOK ==
    /\ Implementation \in Implementations
    /\ RunStatus \in RunStatuses
    /\ Status \in [Tasks -> ExecutionStatuses]
    /\ ClaimActive \in [Tasks -> BOOLEAN]
    /\ ExecutionStarts \in [Tasks -> Nat]

InvNoSequenceTailAuthorityBeforePreviousSubtreeSucceeded ==
    Status["deploy"] \in AuthorityStatuses =>
        /\ Status["root"] = "succeeded"
        /\ BuildSubtreeSucceededIn(Status)

InvNoAuthorityBeforeTaskReady ==
    \A task \in Tasks:
        Status[task] \in AuthorityStatuses => ReadyIn(Status, task)

InvAtMostOnceTaskExecutionStart ==
    \A task \in Tasks:
        ExecutionStarts[task] <= 1

InvTerminalRunClearsExecutionAuthority ==
    RunStatus \in TerminalRunStatuses =>
        \A task \in Tasks:
            ~ClaimActive[task]

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
