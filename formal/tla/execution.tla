---- MODULE execution ----
EXTENDS Naturals

CONSTANTS
    Tasks,
    Workers,
    Nil

VARIABLES
    AbandonedTasks,
    EnqueuedTasks,
    CancelledTasks,
    FailedTasks,
    OrphanedTasks,
    RunningTasks,
    SuccessfulTasks,
    ExecutionStarts,
    WorkerAssignments

Vars == <<
    AbandonedTasks,
    CancelledTasks,
    EnqueuedTasks,
    FailedTasks,
    OrphanedTasks,
    RunningTasks,
    SuccessfulTasks,
    ExecutionStarts,
    WorkerAssignments
>>

Init ==
    /\ AbandonedTasks = {}
    /\ CancelledTasks = {}
    /\ EnqueuedTasks = Tasks
    /\ FailedTasks = {}
    /\ OrphanedTasks = {}
    /\ RunningTasks = {}
    /\ SuccessfulTasks = {}
    /\ ExecutionStarts = [t \in Tasks |-> 0]
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
    /\ UNCHANGED << AbandonedTasks, EnqueuedTasks, FailedTasks, OrphanedTasks, SuccessfulTasks, ExecutionStarts >>

Dequeue(w, t) ==
    /\ w \in Workers
    /\ t \in EnqueuedTasks
    /\ WorkerAssignments[w] = Nil
    /\ EnqueuedTasks' = EnqueuedTasks \ {t}
    /\ RunningTasks' = RunningTasks \union {t}
    /\ ExecutionStarts' = [ExecutionStarts EXCEPT ![t] = @ + 1]
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = t]
    /\ UNCHANGED << AbandonedTasks, CancelledTasks, FailedTasks, OrphanedTasks, SuccessfulTasks >>

Complete(w, t) ==
    /\ w \in Workers
    /\ t \in RunningTasks
    /\ WorkerAssignments[w] = t
    /\ RunningTasks' = RunningTasks \ {t}
    /\ SuccessfulTasks' = SuccessfulTasks \union {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = Nil]
    /\ UNCHANGED << AbandonedTasks, CancelledTasks, EnqueuedTasks, FailedTasks, OrphanedTasks, ExecutionStarts >>

Fail(w, t) ==
    /\ w \in Workers
    /\ t \in RunningTasks
    /\ WorkerAssignments[w] = t
    /\ RunningTasks' = RunningTasks \ {t}
    /\ FailedTasks' = FailedTasks \union {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = Nil]
    /\ UNCHANGED << AbandonedTasks, CancelledTasks, EnqueuedTasks, OrphanedTasks, SuccessfulTasks, ExecutionStarts >>

LeaseExpiry(w, t) ==
    /\ w \in Workers
    /\ t \in RunningTasks
    /\ WorkerAssignments[w] = t
    /\ OrphanedTasks' = OrphanedTasks \union {t}
    /\ RunningTasks' = RunningTasks \ {t}
    /\ UNCHANGED <<
        AbandonedTasks,
        CancelledTasks,
        EnqueuedTasks,
        ExecutionStarts,
        FailedTasks,
        SuccessfulTasks,
        WorkerAssignments >>

OperatorAbandon(w, t) ==
    /\ w \in Workers
    /\ t \in OrphanedTasks
    /\ WorkerAssignments[w] = t
    /\ AbandonedTasks' = AbandonedTasks \union {t}
    /\ OrphanedTasks' = OrphanedTasks \ {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = Nil]
    /\ UNCHANGED << CancelledTasks, EnqueuedTasks, FailedTasks, RunningTasks, SuccessfulTasks, ExecutionStarts >>

OperatorMarkSucceededOrLateCompletion(w, t) ==
    /\ w \in Workers
    /\ t \in OrphanedTasks
    /\ WorkerAssignments[w] = t
    /\ OrphanedTasks' = OrphanedTasks \ {t}
    /\ SuccessfulTasks' = SuccessfulTasks \union {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = Nil]
    /\ UNCHANGED << AbandonedTasks, CancelledTasks, EnqueuedTasks, FailedTasks, RunningTasks, ExecutionStarts >>

OperatorMarkFailedOrLateFailure(w, t) ==
    /\ w \in Workers
    /\ t \in OrphanedTasks
    /\ WorkerAssignments[w] = t
    /\ FailedTasks' = FailedTasks \union {t}
    /\ OrphanedTasks' = OrphanedTasks \ {t}
    /\ WorkerAssignments' = [WorkerAssignments EXCEPT ![w] = Nil]
    /\ UNCHANGED << AbandonedTasks, CancelledTasks, EnqueuedTasks, RunningTasks, SuccessfulTasks, ExecutionStarts >>

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
            \/ OperatorMarkSucceededOrLateCompletion(w, t)
            \/ OperatorMarkFailedOrLateFailure(w, t)
            \/ OperatorAbandon(w, t)
    \/ Idle

InvAtMostOneWorkerPerTask ==
    \A w1, w2 \in Workers:
        (w1 # w2 /\ WorkerAssignments[w1] = WorkerAssignments[w2])
            => WorkerAssignments[w1] = Nil

InvAtMostOnceExecution ==
    \A t \in Tasks:
        ExecutionStarts[t] <= 1

InvOrphanedTasksRemainFenced ==
    \A t \in OrphanedTasks:
        \E w \in Workers:
            WorkerAssignments[w] = t

InvTasksPartition ==
    /\ AbandonedTasks \intersect CancelledTasks = {}
    /\ AbandonedTasks \intersect EnqueuedTasks = {}
    /\ AbandonedTasks \intersect FailedTasks = {}
    /\ AbandonedTasks \intersect OrphanedTasks = {}
    /\ AbandonedTasks \intersect RunningTasks = {}
    /\ AbandonedTasks \intersect SuccessfulTasks = {}
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
    /\ AbandonedTasks
        \union EnqueuedTasks
        \union CancelledTasks
        \union FailedTasks
        \union OrphanedTasks
        \union RunningTasks
        \union SuccessfulTasks = Tasks

LeadsToCompletion ==
    \A t \in Tasks:
        (t \in EnqueuedTasks) ~>
            \/ (t \in CancelledTasks)
            \/ (t \in AbandonedTasks)
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
            /\ WF_Vars(OperatorAbandon(w, t))
            /\ WF_Vars(OperatorMarkFailedOrLateFailure(w, t))
            /\ WF_Vars(OperatorMarkSucceededOrLateCompletion(w, t))

====
