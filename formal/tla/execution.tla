---- MODULE execution ----

CONSTANTS
    Tasks,
    Workers,
    Nil

VARIABLES
    EnqueuedTasks,
    CancelledTasks,
    FailedTasks,
    OrphanedTasks,
    RunningTasks,
    SuccessfulTasks,
    WorkerAssignments

Vars == <<
    CancelledTasks,
    EnqueuedTasks,
    FailedTasks,
    OrphanedTasks,
    RunningTasks,
    SuccessfulTasks,
    WorkerAssignments
>>

Init ==
    /\ CancelledTasks = {}
    /\ EnqueuedTasks = Tasks
    /\ FailedTasks = {}
    /\ OrphanedTasks = {}
    /\ RunningTasks = {}
    /\ SuccessfulTasks = {}
    /\ WorkerAssignments = [w \in Workers |-> Nil]

Idle ==
    /\ EnqueuedTasks = {}
    /\ OrphanedTasks = {}
    /\ RunningTasks = {}
    /\ UNCHANGED Vars

Cancel(w, t) ==
    /\ w \in Workers
    /\ t \in RunningTasks
    /\ WorkerAssignments[w] = t
    /\ RunningTasks' = RunningTasks \ {t}
    /\ CancelledTasks' = CancelledTasks \union {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = Nil]
    /\ UNCHANGED << EnqueuedTasks, FailedTasks, OrphanedTasks, SuccessfulTasks >>

Dequeue(w, t) ==
    /\ w \in Workers
    /\ t \in EnqueuedTasks
    /\ WorkerAssignments[w] = Nil
    /\ EnqueuedTasks' = EnqueuedTasks \ {t}
    /\ RunningTasks' = RunningTasks \union {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = t]
    /\ UNCHANGED << CancelledTasks, FailedTasks, OrphanedTasks, SuccessfulTasks >>

Complete(w, t) ==
    /\ w \in Workers
    /\ t \in RunningTasks
    /\ WorkerAssignments[w] = t
    /\ RunningTasks' = RunningTasks \ {t}
    /\ SuccessfulTasks' = SuccessfulTasks \union {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = Nil]
    /\ UNCHANGED << CancelledTasks, EnqueuedTasks, FailedTasks, OrphanedTasks >>

Fail(w, t) ==
    /\ w \in Workers
    /\ t \in RunningTasks
    /\ WorkerAssignments[w] = t
    /\ RunningTasks' = RunningTasks \ {t}
    /\ FailedTasks' = FailedTasks \union {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = Nil]
    /\ UNCHANGED << CancelledTasks, EnqueuedTasks, OrphanedTasks, SuccessfulTasks >>

LateCompletion(w, t) ==
    /\ w \in Workers
    /\ t \in OrphanedTasks
    /\ WorkerAssignments[w] = t
    /\ OrphanedTasks' = OrphanedTasks \ {t}
    /\ SuccessfulTasks' = SuccessfulTasks \union {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = Nil]
    /\ UNCHANGED << CancelledTasks, EnqueuedTasks, FailedTasks, RunningTasks >>

LeaseExpiry(w, t) ==
    /\ w \in Workers
    /\ t \in RunningTasks
    /\ WorkerAssignments[w] = t
    /\ OrphanedTasks' = OrphanedTasks \union {t}
    /\ RunningTasks' = RunningTasks \ {t}
    /\ UNCHANGED <<
        CancelledTasks,
        EnqueuedTasks,
        FailedTasks,
        SuccessfulTasks,
        WorkerAssignments >>

OperatorKillOrLateFailure(w, t) ==
    /\ w \in Workers
    /\ t \in OrphanedTasks
    /\ WorkerAssignments[w] = t
    /\ FailedTasks' = FailedTasks \union {t}
    /\ OrphanedTasks' = OrphanedTasks \ {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = Nil]
    /\ UNCHANGED << CancelledTasks, EnqueuedTasks, RunningTasks, SuccessfulTasks >>

Next ==
    \/ \E w \in Workers:
        \E t \in EnqueuedTasks:
            \/ Dequeue(w, t)
    \/ \E w \in Workers:
        \E t \in RunningTasks:
            \/ Cancel(w, t)
            \/ Complete(w, t)
            \/ Fail(w, t)
            \/ LeaseExpiry(w, t)
    \/ \E w \in Workers:
        \E t \in OrphanedTasks:
            \/ LateCompletion(w, t)
            \/ OperatorKillOrLateFailure(w, t)
    \/ Idle

InvAtMostOneWorkerPerTask ==
    \A w1, w2 \in Workers:
        (w1 # w2 /\ WorkerAssignments[w1] = WorkerAssignments[w2])
            => WorkerAssignments[w1] = Nil

InvTasksPartition ==
    /\ CancelledTasks \intersect EnqueuedTasks = {}
    /\ CancelledTasks \intersect FailedTasks = {}
    /\ CancelledTasks \intersect RunningTasks = {}
    /\ CancelledTasks \intersect OrphanedTasks = {}
    /\ CancelledTasks \intersect SuccessfulTasks = {}
    /\ EnqueuedTasks \intersect RunningTasks = {}
    /\ EnqueuedTasks \intersect OrphanedTasks = {}
    /\ EnqueuedTasks \intersect SuccessfulTasks = {}
    /\ EnqueuedTasks \intersect FailedTasks = {}
    /\ FailedTasks \intersect RunningTasks = {}
    /\ FailedTasks \intersect OrphanedTasks = {}
    /\ FailedTasks \intersect SuccessfulTasks = {}
    /\ OrphanedTasks \intersect RunningTasks = {}
    /\ OrphanedTasks \intersect SuccessfulTasks = {}
    /\ RunningTasks \intersect SuccessfulTasks = {}
    /\ EnqueuedTasks
        \union CancelledTasks
        \union FailedTasks
        \union OrphanedTasks
        \union RunningTasks
        \union SuccessfulTasks = Tasks

LeadsToCompletion ==
    \A t \in Tasks:
        (t \in EnqueuedTasks) ~>
            \/ (t \in CancelledTasks)
            \/ (t \in FailedTasks)
            \/ (t \in SuccessfulTasks)

Spec ==
    /\ Init
    /\ [][Next]_Vars
    /\ \A w \in Workers:
        \A t \in Tasks:
            /\ WF_Vars(Cancel(w, t))
            /\ WF_Vars(Complete(w, t))
            /\ WF_Vars(Dequeue(w, t))
            /\ WF_Vars(Fail(w, t))
            /\ WF_Vars(OperatorKillOrLateFailure(w, t))

====
