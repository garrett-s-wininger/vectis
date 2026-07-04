---- MODULE queue_handoff ----
EXTENDS Naturals

CONSTANTS
    MaxOperatorRequeues,
    MaxRequeues

Locations == {
    "none",
    "pending",
    "inflight",
    "acked",
    "dropped",
    "dlq"
}

LiveLocations == {"pending", "inflight", "dlq"}
TerminalLocations == {"acked", "dropped", "dlq"}
NoDelivery == 0

VARIABLES
    AckedDeliveries,
    DispatchExpired,
    Enqueued,
    ExpiredDeliveries,
    LeaseExpired,
    MemAttempt,
    MemDelivery,
    MemLoc,
    NextDelivery,
    OperatorRequeues,
    PendingMutation,
    PersistedAttempt,
    PersistedDelivery,
    PersistedLoc

Vars == <<
    AckedDeliveries,
    DispatchExpired,
    Enqueued,
    ExpiredDeliveries,
    LeaseExpired,
    MemAttempt,
    MemDelivery,
    MemLoc,
    NextDelivery,
    OperatorRequeues,
    PendingMutation,
    PersistedAttempt,
    PersistedDelivery,
    PersistedLoc
>>

Init ==
    /\ AckedDeliveries = {}
    /\ DispatchExpired = FALSE
    /\ Enqueued = FALSE
    /\ ExpiredDeliveries = {}
    /\ LeaseExpired = FALSE
    /\ MemAttempt = 0
    /\ MemDelivery = NoDelivery
    /\ MemLoc = "none"
    /\ NextDelivery = 1
    /\ OperatorRequeues = 0
    /\ PendingMutation = "none"
    /\ PersistedAttempt = 0
    /\ PersistedDelivery = NoDelivery
    /\ PersistedLoc = "none"

Stable ==
    PendingMutation = "none"

IsTerminal(loc) ==
    loc \in TerminalLocations

IsLive(loc) ==
    loc \in LiveLocations

SetPersisted(nextLoc, nextAttempt, nextDelivery, mutation) ==
    /\ Stable
    /\ PersistedLoc' = nextLoc
    /\ PersistedAttempt' = nextAttempt
    /\ PersistedDelivery' = nextDelivery
    /\ PendingMutation' = mutation

Enqueue ==
    /\ Stable
    /\ ~Enqueued
    /\ SetPersisted("pending", 0, NoDelivery, "enqueue")
    /\ Enqueued' = TRUE
    /\ LeaseExpired' = FALSE
    /\ DispatchExpired' = FALSE
    /\ UNCHANGED <<
        AckedDeliveries,
        ExpiredDeliveries,
        MemAttempt,
        MemDelivery,
        MemLoc,
        NextDelivery,
        OperatorRequeues
        >>

Deliver ==
    /\ Stable
    /\ MemLoc = "pending"
    /\ ~DispatchExpired
    /\ SetPersisted("inflight", MemAttempt, NextDelivery, "deliver")
    /\ LeaseExpired' = FALSE
    /\ NextDelivery' = NextDelivery + 1
    /\ UNCHANGED <<
        AckedDeliveries,
        DispatchExpired,
        Enqueued,
        ExpiredDeliveries,
        MemAttempt,
        MemDelivery,
        MemLoc,
        OperatorRequeues
        >>

AckCurrentDelivery ==
    /\ Stable
    /\ MemLoc = "inflight"
    /\ ~LeaseExpired
    /\ SetPersisted("acked", MemAttempt, MemDelivery, "ack")
    /\ AckedDeliveries' = AckedDeliveries \union {MemDelivery}
    /\ UNCHANGED <<
        DispatchExpired,
        Enqueued,
        ExpiredDeliveries,
        LeaseExpired,
        MemAttempt,
        MemDelivery,
        MemLoc,
        NextDelivery,
        OperatorRequeues
        >>

RequeueExpiredDelivery ==
    /\ Stable
    /\ MemLoc = "inflight"
    /\ LeaseExpired
    /\ ~DispatchExpired
    /\ MemAttempt < MaxRequeues
    /\ SetPersisted("pending", MemAttempt + 1, NoDelivery, "requeue_expired")
    /\ ExpiredDeliveries' = ExpiredDeliveries \union {MemDelivery}
    /\ LeaseExpired' = FALSE
    /\ UNCHANGED <<
        AckedDeliveries,
        DispatchExpired,
        Enqueued,
        MemAttempt,
        MemDelivery,
        MemLoc,
        NextDelivery,
        OperatorRequeues
        >>

MoveExpiredDeliveryToDLQ ==
    /\ Stable
    /\ MemLoc = "inflight"
    /\ LeaseExpired
    /\ ~DispatchExpired
    /\ MemAttempt + 1 > MaxRequeues
    /\ SetPersisted("dlq", MemAttempt + 1, NoDelivery, "dlq")
    /\ ExpiredDeliveries' = ExpiredDeliveries \union {MemDelivery}
    /\ LeaseExpired' = FALSE
    /\ UNCHANGED <<
        AckedDeliveries,
        DispatchExpired,
        Enqueued,
        MemAttempt,
        MemDelivery,
        MemLoc,
        NextDelivery,
        OperatorRequeues
        >>

DropExpiredPending ==
    /\ Stable
    /\ MemLoc = "pending"
    /\ DispatchExpired
    /\ SetPersisted("dropped", MemAttempt, NoDelivery, "drop_expired")
    /\ UNCHANGED <<
        AckedDeliveries,
        DispatchExpired,
        Enqueued,
        ExpiredDeliveries,
        LeaseExpired,
        MemAttempt,
        MemDelivery,
        MemLoc,
        NextDelivery,
        OperatorRequeues
        >>

DropExpiredInflight ==
    /\ Stable
    /\ MemLoc = "inflight"
    /\ LeaseExpired
    /\ DispatchExpired
    /\ SetPersisted("dropped", MemAttempt, NoDelivery, "drop_expired")
    /\ ExpiredDeliveries' = ExpiredDeliveries \union {MemDelivery}
    /\ LeaseExpired' = FALSE
    /\ UNCHANGED <<
        AckedDeliveries,
        DispatchExpired,
        Enqueued,
        MemAttempt,
        MemDelivery,
        MemLoc,
        NextDelivery,
        OperatorRequeues
        >>

OperatorRequeueDLQ ==
    /\ Stable
    /\ MemLoc = "dlq"
    /\ OperatorRequeues < MaxOperatorRequeues
    /\ SetPersisted("pending", 0, NoDelivery, "dlq_requeue")
    /\ LeaseExpired' = FALSE
    /\ DispatchExpired' = FALSE
    /\ OperatorRequeues' = OperatorRequeues + 1
    /\ UNCHANGED <<
        AckedDeliveries,
        Enqueued,
        ExpiredDeliveries,
        MemAttempt,
        MemDelivery,
        MemLoc,
        NextDelivery
        >>

FinishMutation ==
    /\ ~Stable
    /\ MemLoc' = PersistedLoc
    /\ MemAttempt' = PersistedAttempt
    /\ MemDelivery' = PersistedDelivery
    /\ PendingMutation' = "none"
    /\ UNCHANGED <<
        AckedDeliveries,
        DispatchExpired,
        Enqueued,
        ExpiredDeliveries,
        LeaseExpired,
        NextDelivery,
        OperatorRequeues,
        PersistedAttempt,
        PersistedDelivery,
        PersistedLoc
        >>

Restart ==
    /\ MemLoc' = PersistedLoc
    /\ MemAttempt' = PersistedAttempt
    /\ MemDelivery' = PersistedDelivery
    /\ PendingMutation' = "none"
    /\ UNCHANGED <<
        AckedDeliveries,
        DispatchExpired,
        Enqueued,
        ExpiredDeliveries,
        LeaseExpired,
        NextDelivery,
        OperatorRequeues,
        PersistedAttempt,
        PersistedDelivery,
        PersistedLoc
        >>

LeaseExpires ==
    /\ Stable
    /\ MemLoc = "inflight"
    /\ LeaseExpired' = TRUE
    /\ UNCHANGED <<
        AckedDeliveries,
        DispatchExpired,
        Enqueued,
        ExpiredDeliveries,
        MemAttempt,
        MemDelivery,
        MemLoc,
        NextDelivery,
        OperatorRequeues,
        PendingMutation,
        PersistedAttempt,
        PersistedDelivery,
        PersistedLoc
        >>

DispatchDeadlineExpires ==
    /\ Stable
    /\ Enqueued
    /\ IsLive(MemLoc)
    /\ DispatchExpired' = TRUE
    /\ UNCHANGED <<
        AckedDeliveries,
        Enqueued,
        ExpiredDeliveries,
        LeaseExpired,
        MemAttempt,
        MemDelivery,
        MemLoc,
        NextDelivery,
        OperatorRequeues,
        PendingMutation,
        PersistedAttempt,
        PersistedDelivery,
        PersistedLoc
        >>

LateAckExpiredDelivery ==
    /\ Stable
    /\ \E delivery \in ExpiredDeliveries:
        /\ delivery # NoDelivery
        /\ delivery # MemDelivery
    /\ UNCHANGED Vars

Idle ==
    /\ Stable
    /\ IsTerminal(MemLoc)
    /\ UNCHANGED Vars

Next ==
    \/ Enqueue
    \/ Deliver
    \/ AckCurrentDelivery
    \/ RequeueExpiredDelivery
    \/ MoveExpiredDeliveryToDLQ
    \/ DropExpiredPending
    \/ DropExpiredInflight
    \/ OperatorRequeueDLQ
    \/ FinishMutation
    \/ Restart
    \/ LeaseExpires
    \/ DispatchDeadlineExpires
    \/ LateAckExpiredDelivery
    \/ Idle

InvTypeOK ==
    /\ AckedDeliveries \subseteq 1..NextDelivery
    /\ DispatchExpired \in BOOLEAN
    /\ Enqueued \in BOOLEAN
    /\ ExpiredDeliveries \subseteq 1..NextDelivery
    /\ LeaseExpired \in BOOLEAN
    /\ MemAttempt \in 0..(MaxRequeues + 1)
    /\ MemDelivery \in 0..NextDelivery
    /\ MemLoc \in Locations
    /\ NextDelivery \in Nat
    /\ NextDelivery >= 1
    /\ OperatorRequeues \in 0..MaxOperatorRequeues
    /\ PendingMutation \in {
        "none",
        "ack",
        "deliver",
        "dlq",
        "dlq_requeue",
        "drop_expired",
        "enqueue",
        "requeue_expired"
        }
    /\ PersistedAttempt \in 0..(MaxRequeues + 1)
    /\ PersistedDelivery \in 0..NextDelivery
    /\ PersistedLoc \in Locations

InvStableMemoryMatchesPersistence ==
    Stable => /\ MemLoc = PersistedLoc
              /\ MemAttempt = PersistedAttempt
              /\ MemDelivery = PersistedDelivery

InvPersistedWorkNotLostBeforeTerminal ==
    (Enqueued /\ ~IsTerminal(PersistedLoc)) => IsLive(PersistedLoc)

InvStableWorkNotLostBeforeTerminal ==
    (Stable /\ Enqueued /\ ~IsTerminal(MemLoc)) => IsLive(MemLoc)

InvAckedIsAbsorbing ==
    AckedDeliveries # {} => PersistedLoc = "acked"

InvLateAckCannotAckExpiredDelivery ==
    AckedDeliveries \intersect ExpiredDeliveries = {}

InvAttemptsBoundedByQueuePolicy ==
    /\ (PersistedLoc # "dlq" => PersistedAttempt <= MaxRequeues)
    /\ (MemLoc # "dlq" => MemAttempt <= MaxRequeues)
    /\ (PersistedLoc = "dlq" => PersistedAttempt = MaxRequeues + 1)
    /\ (MemLoc = "dlq" => MemAttempt = MaxRequeues + 1)

InvLiveDeliveryHasDeliveryID ==
    /\ (MemLoc = "inflight" => MemDelivery # NoDelivery)
    /\ (PersistedLoc = "inflight" => PersistedDelivery # NoDelivery)
    /\ (MemLoc # "inflight" => MemDelivery = NoDelivery \/ MemLoc = "acked")
    /\ (PersistedLoc # "inflight" => PersistedDelivery = NoDelivery \/ PersistedLoc = "acked")

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
