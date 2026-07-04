---- MODULE action_timeout ----
EXTENDS FiniteSets, Naturals

CONSTANTS BodyNodes

VARIABLES
    Completed,
    DeadlineExpired,
    NextBodyIndex,
    ParentStatus,
    RunningChild,
    Started,
    StartedAfterDeadline

Vars == <<
    Completed,
    DeadlineExpired,
    NextBodyIndex,
    ParentStatus,
    RunningChild,
    Started,
    StartedAfterDeadline
>>

Init ==
    /\ Completed = {}
    /\ DeadlineExpired = FALSE
    /\ NextBodyIndex = 1
    /\ ParentStatus = "running"
    /\ RunningChild = 0
    /\ Started = {}
    /\ StartedAfterDeadline = FALSE

BodyCount == Cardinality(BodyNodes)

StartNextBodyNode ==
    /\ ParentStatus = "running"
    /\ RunningChild = 0
    /\ NextBodyIndex <= BodyCount
    /\ ~DeadlineExpired
    /\ RunningChild' = NextBodyIndex
    /\ Started' = Started \union {NextBodyIndex}
    /\ StartedAfterDeadline' = (StartedAfterDeadline \/ DeadlineExpired)
    /\ UNCHANGED <<
        Completed,
        DeadlineExpired,
        NextBodyIndex,
        ParentStatus
        >>

CompleteRunningBodyNode ==
    /\ ParentStatus = "running"
    /\ RunningChild # 0
    /\ Completed' = Completed \union {RunningChild}
    /\ NextBodyIndex' = NextBodyIndex + 1
    /\ RunningChild' = 0
    /\ ParentStatus' =
        IF NextBodyIndex = BodyCount
        THEN IF DeadlineExpired THEN "timeout" ELSE "succeeded"
        ELSE ParentStatus
    /\ UNCHANGED <<
        DeadlineExpired,
        Started,
        StartedAfterDeadline
        >>

FailRunningBodyNode ==
    /\ ParentStatus = "running"
    /\ RunningChild # 0
    /\ RunningChild' = 0
    /\ ParentStatus' =
        IF DeadlineExpired THEN "timeout" ELSE "failed"
    /\ UNCHANGED <<
        Completed,
        DeadlineExpired,
        NextBodyIndex,
        Started,
        StartedAfterDeadline
        >>

ObserveExpiredDeadlineBetweenBodyNodes ==
    /\ ParentStatus = "running"
    /\ RunningChild = 0
    /\ DeadlineExpired
    /\ ParentStatus' = "timeout"
    /\ UNCHANGED <<
        Completed,
        DeadlineExpired,
        NextBodyIndex,
        RunningChild,
        Started,
        StartedAfterDeadline
        >>

DeadlineFires ==
    /\ ParentStatus = "running"
    /\ DeadlineExpired' = TRUE
    /\ UNCHANGED <<
        Completed,
        NextBodyIndex,
        ParentStatus,
        RunningChild,
        Started,
        StartedAfterDeadline
        >>

Idle ==
    /\ ParentStatus \in {"succeeded", "failed", "timeout"}
    /\ UNCHANGED Vars

Next ==
    \/ StartNextBodyNode
    \/ CompleteRunningBodyNode
    \/ FailRunningBodyNode
    \/ ObserveExpiredDeadlineBetweenBodyNodes
    \/ DeadlineFires
    \/ Idle

InvTypeOK ==
    /\ Completed \subseteq BodyNodes
    /\ DeadlineExpired \in BOOLEAN
    /\ NextBodyIndex \in 1..(BodyCount + 1)
    /\ ParentStatus \in {"running", "succeeded", "failed", "timeout"}
    /\ RunningChild \in {0} \union BodyNodes
    /\ Started \subseteq BodyNodes
    /\ StartedAfterDeadline \in BOOLEAN

InvOrderedStarts ==
    2 \in Started => 1 \in Completed

InvNoBodyStartAfterDeadline ==
    ~StartedAfterDeadline

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
