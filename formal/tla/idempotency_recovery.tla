---- MODULE idempotency_recovery ----
EXTENDS Naturals

CONSTANTS
    Hashes,
    MaxRuns

Nil == "none"
Responses == {"none", "accepted", "replayed", "recovered", "in_progress", "conflict"}

VARIABLES
    HandlerRouteRecoverable,
    InvocationRecorded,
    LastRequestHash,
    LastResponse,
    LastSuccessfulHash,
    ResourceAttached,
    ResponseCached,
    RowExists,
    RunCreated,
    RunCount,
    StoredHash

Vars == <<
    HandlerRouteRecoverable,
    InvocationRecorded,
    LastRequestHash,
    LastResponse,
    LastSuccessfulHash,
    ResourceAttached,
    ResponseCached,
    RowExists,
    RunCreated,
    RunCount,
    StoredHash
>>

Init ==
    /\ HandlerRouteRecoverable = FALSE
    /\ InvocationRecorded = FALSE
    /\ LastRequestHash = Nil
    /\ LastResponse = "none"
    /\ LastSuccessfulHash = Nil
    /\ ResourceAttached = FALSE
    /\ ResponseCached = FALSE
    /\ RowExists = FALSE
    /\ RunCreated = FALSE
    /\ RunCount = 0
    /\ StoredHash = Nil

ReserveNew(hash, recoverable) ==
    /\ hash \in Hashes
    /\ recoverable \in BOOLEAN
    /\ ~RowExists
    /\ RowExists' = TRUE
    /\ StoredHash' = hash
    /\ HandlerRouteRecoverable' = recoverable
    /\ LastRequestHash' = hash
    /\ LastResponse' = "none"
    /\ UNCHANGED <<
        InvocationRecorded,
        LastSuccessfulHash,
        ResourceAttached,
        ResponseCached,
        RunCreated,
        RunCount
        >>

RecordTriggerInvocation ==
    /\ RowExists
    /\ ~InvocationRecorded
    /\ InvocationRecorded' = TRUE
    /\ UNCHANGED <<
        HandlerRouteRecoverable,
        LastRequestHash,
        LastResponse,
        LastSuccessfulHash,
        ResourceAttached,
        ResponseCached,
        RowExists,
        RunCreated,
        RunCount,
        StoredHash
        >>

AttachIdempotencyResource ==
    /\ RowExists
    /\ HandlerRouteRecoverable
    /\ InvocationRecorded
    /\ ~ResponseCached
    /\ ResourceAttached' = TRUE
    /\ UNCHANGED <<
        HandlerRouteRecoverable,
        InvocationRecorded,
        LastRequestHash,
        LastResponse,
        LastSuccessfulHash,
        ResponseCached,
        RowExists,
        RunCreated,
        RunCount,
        StoredHash
        >>

CreateRun ==
    /\ RowExists
    /\ InvocationRecorded
    /\ ResourceAttached
    /\ ~RunCreated
    /\ RunCount < MaxRuns
    /\ RunCreated' = TRUE
    /\ RunCount' = RunCount + 1
    /\ UNCHANGED <<
        HandlerRouteRecoverable,
        InvocationRecorded,
        LastRequestHash,
        LastResponse,
        LastSuccessfulHash,
        ResourceAttached,
        ResponseCached,
        RowExists,
        StoredHash
        >>

CompleteIdempotency ==
    /\ RowExists
    /\ RunCreated
    /\ ~ResponseCached
    /\ ResponseCached' = TRUE
    /\ LastResponse' = "accepted"
    /\ LastSuccessfulHash' = StoredHash
    /\ UNCHANGED <<
        HandlerRouteRecoverable,
        InvocationRecorded,
        LastRequestHash,
        ResourceAttached,
        RowExists,
        RunCreated,
        RunCount,
        StoredHash
        >>

ReleaseBeforeRunCommit ==
    /\ RowExists
    /\ ~RunCreated
    /\ ~ResponseCached
    /\ RowExists' = FALSE
    /\ StoredHash' = Nil
    /\ HandlerRouteRecoverable' = FALSE
    /\ ResourceAttached' = FALSE
    /\ LastResponse' = "none"
    /\ UNCHANGED <<
        InvocationRecorded,
        LastRequestHash,
        LastSuccessfulHash,
        ResponseCached,
        RunCreated,
        RunCount
        >>

RetrySameHashReplays(hash) ==
    /\ hash \in Hashes
    /\ RowExists
    /\ StoredHash = hash
    /\ ResponseCached
    /\ LastRequestHash' = hash
    /\ LastResponse' = "replayed"
    /\ LastSuccessfulHash' = hash
    /\ UNCHANGED <<
        HandlerRouteRecoverable,
        InvocationRecorded,
        ResourceAttached,
        ResponseCached,
        RowExists,
        RunCreated,
        RunCount,
        StoredHash
        >>

RetrySameHashRecovers(hash) ==
    /\ hash \in Hashes
    /\ RowExists
    /\ StoredHash = hash
    /\ ~ResponseCached
    /\ ResourceAttached
    /\ RunCreated
    /\ ResponseCached' = TRUE
    /\ LastRequestHash' = hash
    /\ LastResponse' = "recovered"
    /\ LastSuccessfulHash' = hash
    /\ UNCHANGED <<
        HandlerRouteRecoverable,
        InvocationRecorded,
        ResourceAttached,
        RowExists,
        RunCreated,
        RunCount,
        StoredHash
        >>

RetrySameHashInProgress(hash) ==
    /\ hash \in Hashes
    /\ RowExists
    /\ StoredHash = hash
    /\ ~ResponseCached
    /\ ~(ResourceAttached /\ RunCreated)
    /\ LastRequestHash' = hash
    /\ LastResponse' = "in_progress"
    /\ UNCHANGED <<
        HandlerRouteRecoverable,
        InvocationRecorded,
        LastSuccessfulHash,
        ResourceAttached,
        ResponseCached,
        RowExists,
        RunCreated,
        RunCount,
        StoredHash
        >>

RetryDifferentHashConflicts(hash) ==
    /\ hash \in Hashes
    /\ RowExists
    /\ StoredHash # hash
    /\ LastRequestHash' = hash
    /\ LastResponse' = "conflict"
    /\ UNCHANGED <<
        HandlerRouteRecoverable,
        InvocationRecorded,
        LastSuccessfulHash,
        ResourceAttached,
        ResponseCached,
        RowExists,
        RunCreated,
        RunCount,
        StoredHash
        >>

Next ==
    \/ \E hash \in Hashes:
        \/ ReserveNew(hash, TRUE)
        \/ RetrySameHashReplays(hash)
        \/ RetrySameHashRecovers(hash)
        \/ RetrySameHashInProgress(hash)
        \/ RetryDifferentHashConflicts(hash)
    \/ RecordTriggerInvocation
    \/ AttachIdempotencyResource
    \/ CreateRun
    \/ CompleteIdempotency
    \/ ReleaseBeforeRunCommit

InvTypeOK ==
    /\ HandlerRouteRecoverable \in BOOLEAN
    /\ InvocationRecorded \in BOOLEAN
    /\ LastRequestHash \in Hashes \union {Nil}
    /\ LastResponse \in Responses
    /\ LastSuccessfulHash \in Hashes \union {Nil}
    /\ ResourceAttached \in BOOLEAN
    /\ ResponseCached \in BOOLEAN
    /\ RowExists \in BOOLEAN
    /\ RunCreated \in BOOLEAN
    /\ RunCount \in 0..MaxRuns
    /\ StoredHash \in Hashes \union {Nil}

InvAtMostOneRunPerIdempotencyKey ==
    RunCount <= 1

InvCompletedResponseRequiresRun ==
    ResponseCached => RunCreated

InvCommittedRunIsRecoverable ==
    (RowExists /\ RunCreated /\ ~ResponseCached) => ResourceAttached

InvDifferentHashDoesNotSucceed ==
    (LastResponse \in {"accepted", "replayed", "recovered"} /\ LastSuccessfulHash # Nil)
        => LastSuccessfulHash = StoredHash

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
