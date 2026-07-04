---- MODULE cron_schedule ----
EXTENDS Naturals

CONSTANTS
    Owners

Nil == "none"
NextRunStates == {"due", "advanced"}

VARIABLES
    ClaimExpired,
    ClaimOwner,
    Claimed,
    DurableRunCount,
    HandoffCount,
    NextRun,
    PollObservedDue,
    Triggered

Vars == <<
    ClaimExpired,
    ClaimOwner,
    Claimed,
    DurableRunCount,
    HandoffCount,
    NextRun,
    PollObservedDue,
    Triggered
>>

Init ==
    /\ ClaimExpired = FALSE
    /\ ClaimOwner = Nil
    /\ Claimed = [owner \in Owners |-> FALSE]
    /\ DurableRunCount = 0
    /\ HandoffCount = 0
    /\ NextRun = "due"
    /\ PollObservedDue = FALSE
    /\ Triggered = [owner \in Owners |-> FALSE]

ClaimActive ==
    ClaimOwner # Nil /\ ~ClaimExpired

ClaimDue(owner) ==
    /\ owner \in Owners
    /\ NextRun = "due"
    /\ ~ClaimActive
    /\ ~Claimed[owner]
    /\ ClaimOwner' = owner
    /\ ClaimExpired' = FALSE
    /\ Claimed' = [Claimed EXCEPT ![owner] = TRUE]
    /\ PollObservedDue' = TRUE
    /\ UNCHANGED <<
        DurableRunCount,
        HandoffCount,
        NextRun,
        Triggered
        >>

ClaimExpires ==
    /\ ClaimOwner # Nil
    /\ ~ClaimExpired
    /\ ClaimExpired' = TRUE
    /\ UNCHANGED <<
        ClaimOwner,
        Claimed,
        DurableRunCount,
        HandoffCount,
        NextRun,
        PollObservedDue,
        Triggered
        >>

TriggerSchedule(owner) ==
    /\ owner \in Owners
    /\ Claimed[owner]
    /\ ~Triggered[owner]
    /\ Triggered' = [Triggered EXCEPT ![owner] = TRUE]
    /\ DurableRunCount' = IF DurableRunCount = 0 THEN 1 ELSE DurableRunCount
    /\ HandoffCount' = HandoffCount + 1
    /\ UNCHANGED <<
        ClaimExpired,
        ClaimOwner,
        Claimed,
        NextRun,
        PollObservedDue
        >>

CompleteClaim(owner) ==
    /\ owner \in Owners
    /\ Triggered[owner]
    /\ ClaimOwner = owner
    /\ NextRun = "due"
    /\ NextRun' = "advanced"
    /\ ClaimOwner' = Nil
    /\ ClaimExpired' = FALSE
    /\ UNCHANGED <<
        Claimed,
        DurableRunCount,
        HandoffCount,
        PollObservedDue,
        Triggered
        >>

Idle ==
    /\ NextRun = "advanced"
    /\ UNCHANGED Vars

Next ==
    \/ \E owner \in Owners:
        \/ ClaimDue(owner)
        \/ TriggerSchedule(owner)
        \/ CompleteClaim(owner)
    \/ ClaimExpires
    \/ Idle

InvTypeOK ==
    /\ ClaimExpired \in BOOLEAN
    /\ ClaimOwner \in Owners \union {Nil}
    /\ Claimed \in [Owners -> BOOLEAN]
    /\ DurableRunCount \in 0..1
    /\ HandoffCount \in 0..2
    /\ NextRun \in NextRunStates
    /\ PollObservedDue \in BOOLEAN
    /\ Triggered \in [Owners -> BOOLEAN]

InvAtMostOneDurableRunPerScheduledTick ==
    DurableRunCount <= 1

InvQueueHandoffsHaveDurableRun ==
    HandoffCount > 0 => DurableRunCount = 1

InvNextRunAdvancesOnlyAfterDurableRun ==
    NextRun = "advanced" => DurableRunCount = 1

InvDueObservationMakesProgress ==
    PollObservedDue =>
        \/ ClaimOwner # Nil
        \/ DurableRunCount = 1
        \/ NextRun = "advanced"

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
