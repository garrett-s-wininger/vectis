---- MODULE reconciliation ----

CONSTANTS Tasks

VARIABLES StoredTasks, PendingTasks, EnqueuedTasks
Vars == << StoredTasks, PendingTasks, EnqueuedTasks >>

Init ==
    /\ StoredTasks = Tasks
    /\ PendingTasks = {}
    /\ EnqueuedTasks = {}

Idle ==
    /\ PendingTasks = {}
    /\ StoredTasks = {}
    /\ UNCHANGED Vars

Reconcile(t) ==
    /\ t \in PendingTasks
    /\ EnqueuedTasks' = EnqueuedTasks \union {t}
    /\ PendingTasks' = PendingTasks \ {t}
    /\ UNCHANGED StoredTasks

FailToEnqueue(t) ==
    /\ t \in StoredTasks
    /\ StoredTasks' = StoredTasks \ {t}
    /\ PendingTasks' = PendingTasks \union {t}
    /\ UNCHANGED EnqueuedTasks

SuccessfullyEnqueue(t) ==
    /\ t \in StoredTasks
    /\ StoredTasks' = StoredTasks \ {t}
    /\ EnqueuedTasks' = EnqueuedTasks \union {t}
    /\ UNCHANGED PendingTasks

Next ==
    \/ \E t \in Tasks:
        \/ Reconcile(t)
        \/ FailToEnqueue(t)
        \/ SuccessfullyEnqueue(t)
    \/ Idle

InvTasksPartition ==
    /\ StoredTasks \union PendingTasks \union EnqueuedTasks = Tasks
    /\ StoredTasks \intersect PendingTasks = {}
    /\ PendingTasks \intersect EnqueuedTasks = {}
    /\ StoredTasks \intersect EnqueuedTasks = {}

LeadsToEnqueue ==
    \A t \in Tasks: (t \in StoredTasks) ~> (t \in EnqueuedTasks)

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ \A t \in Tasks: WF_Vars(Reconcile(t))
    /\ \A t \in Tasks: WF_Vars(SuccessfullyEnqueue(t))

====
