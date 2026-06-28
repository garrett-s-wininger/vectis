---- MODULE secrets_authorization ----
EXTENDS Naturals

CONSTANTS
    Identities

Nil == "none"
RunStatuses == {"none", "running", "orphaned", "terminal"}
ExecutionStatuses == {"none", "accepted", "running", "terminal"}
ResolveOutcomes == {"none", "success", "denied", "failed"}

VARIABLES
    AuthorizerConfigured,
    ClaimTokenPresent,
    ExecutionStatus,
    ExpectedSPIFFE,
    LastResolveClaimActive,
    LastResolveExecutionStatus,
    LastResolveOutcome,
    LastResolvePeerMatches,
    LastResolvePolicyAllows,
    LastResolveProviderValid,
    LastResolveRunStatus,
    LastResolveScopeBound,
    LeaseActive,
    PolicyAllows,
    ProviderBundleValid,
    ProviderConfigured,
    RequestClaimMatches,
    RequestExecutionMatches,
    RequestPeerSPIFFE,
    RequestRunMatches,
    RunStatus,
    ScopeResolverConfigured,
    ServerStarted,
    WorkerIdentityConfigured

Vars == <<
    AuthorizerConfigured,
    ClaimTokenPresent,
    ExecutionStatus,
    ExpectedSPIFFE,
    LastResolveClaimActive,
    LastResolveExecutionStatus,
    LastResolveOutcome,
    LastResolvePeerMatches,
    LastResolvePolicyAllows,
    LastResolveProviderValid,
    LastResolveRunStatus,
    LastResolveScopeBound,
    LeaseActive,
    PolicyAllows,
    ProviderBundleValid,
    ProviderConfigured,
    RequestClaimMatches,
    RequestExecutionMatches,
    RequestPeerSPIFFE,
    RequestRunMatches,
    RunStatus,
    ScopeResolverConfigured,
    ServerStarted,
    WorkerIdentityConfigured
>>

Init ==
    /\ AuthorizerConfigured = FALSE
    /\ ClaimTokenPresent = FALSE
    /\ ExecutionStatus = "none"
    /\ ExpectedSPIFFE = Nil
    /\ LastResolveClaimActive = FALSE
    /\ LastResolveExecutionStatus = "none"
    /\ LastResolveOutcome = "none"
    /\ LastResolvePeerMatches = FALSE
    /\ LastResolvePolicyAllows = FALSE
    /\ LastResolveProviderValid = FALSE
    /\ LastResolveRunStatus = "none"
    /\ LastResolveScopeBound = FALSE
    /\ LeaseActive = FALSE
    /\ PolicyAllows = FALSE
    /\ ProviderBundleValid = FALSE
    /\ ProviderConfigured = FALSE
    /\ RequestClaimMatches = FALSE
    /\ RequestExecutionMatches = FALSE
    /\ RequestPeerSPIFFE = Nil
    /\ RequestRunMatches = FALSE
    /\ RunStatus = "none"
    /\ ScopeResolverConfigured = FALSE
    /\ ServerStarted = FALSE
    /\ WorkerIdentityConfigured = FALSE

ActiveExecutionClaim ==
    /\ ClaimTokenPresent
    /\ LeaseActive
    /\ RunStatus = "running"
    /\ ExecutionStatus \in {"accepted", "running"}

PeerMatches ==
    /\ ExpectedSPIFFE # Nil
    /\ RequestPeerSPIFFE = ExpectedSPIFFE

ScopeBound ==
    /\ ScopeResolverConfigured
    /\ WorkerIdentityConfigured
    /\ PeerMatches
    /\ RequestRunMatches
    /\ RequestExecutionMatches

AuthorizedResolveRequest ==
    /\ ServerStarted
    /\ ProviderConfigured
    /\ AuthorizerConfigured
    /\ ScopeBound
    /\ ActiveExecutionClaim
    /\ RequestClaimMatches
    /\ PolicyAllows

StartSecureServer ==
    /\ ~ServerStarted
    /\ ServerStarted' = TRUE
    /\ ProviderConfigured' = TRUE
    /\ AuthorizerConfigured' = TRUE
    /\ ScopeResolverConfigured' = TRUE
    /\ WorkerIdentityConfigured' = TRUE
    /\ UNCHANGED <<
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        LeaseActive,
        PolicyAllows,
        ProviderBundleValid,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        RunStatus
        >>

StartWithoutProvider ==
    /\ ~ServerStarted
    /\ ServerStarted' = TRUE
    /\ ProviderConfigured' = FALSE
    /\ AuthorizerConfigured' = TRUE
    /\ ScopeResolverConfigured' = TRUE
    /\ WorkerIdentityConfigured' = TRUE
    /\ UNCHANGED <<
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        LeaseActive,
        PolicyAllows,
        ProviderBundleValid,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        RunStatus
        >>

StartWithoutAuthorizer ==
    /\ ~ServerStarted
    /\ ServerStarted' = TRUE
    /\ ProviderConfigured' = TRUE
    /\ AuthorizerConfigured' = FALSE
    /\ ScopeResolverConfigured' = TRUE
    /\ WorkerIdentityConfigured' = TRUE
    /\ UNCHANGED <<
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        LeaseActive,
        PolicyAllows,
        ProviderBundleValid,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        RunStatus
        >>

ClaimExecution(id) ==
    /\ id \in Identities
    /\ ServerStarted
    /\ RunStatus = "none"
    /\ RunStatus' = "running"
    /\ ExecutionStatus' = "accepted"
    /\ ClaimTokenPresent' = TRUE
    /\ LeaseActive' = TRUE
    /\ ExpectedSPIFFE' = id
    /\ UNCHANGED <<
        AuthorizerConfigured,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        PolicyAllows,
        ProviderBundleValid,
        ProviderConfigured,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

StartExecution ==
    /\ ExecutionStatus = "accepted"
    /\ ExecutionStatus' = "running"
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        LeaseActive,
        PolicyAllows,
        ProviderBundleValid,
        ProviderConfigured,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        RunStatus,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

OrphanRun ==
    /\ RunStatus = "running"
    /\ RunStatus' = "orphaned"
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        LeaseActive,
        PolicyAllows,
        ProviderBundleValid,
        ProviderConfigured,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

ExpireLease ==
    /\ LeaseActive
    /\ LeaseActive' = FALSE
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        PolicyAllows,
        ProviderBundleValid,
        ProviderConfigured,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        RunStatus,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

TerminalizeRun ==
    /\ RunStatus \in {"running", "orphaned"}
    /\ RunStatus' = "terminal"
    /\ ExecutionStatus' = "terminal"
    /\ ClaimTokenPresent' = FALSE
    /\ LeaseActive' = FALSE
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        PolicyAllows,
        ProviderBundleValid,
        ProviderConfigured,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

PrepareValidRequest ==
    /\ ExpectedSPIFFE # Nil
    /\ RequestPeerSPIFFE' = ExpectedSPIFFE
    /\ RequestRunMatches' = TRUE
    /\ RequestExecutionMatches' = TRUE
    /\ RequestClaimMatches' = TRUE
    /\ PolicyAllows' = TRUE
    /\ ProviderBundleValid' = TRUE
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        LeaseActive,
        ProviderConfigured,
        RunStatus,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

PrepareWrongPeer(id) ==
    /\ id \in Identities
    /\ id # ExpectedSPIFFE
    /\ RequestPeerSPIFFE' = id
    /\ RequestRunMatches' = TRUE
    /\ RequestExecutionMatches' = TRUE
    /\ RequestClaimMatches' = TRUE
    /\ PolicyAllows' = TRUE
    /\ ProviderBundleValid' = TRUE
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        LeaseActive,
        ProviderConfigured,
        RunStatus,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

PrepareWrongClaim ==
    /\ ExpectedSPIFFE # Nil
    /\ RequestPeerSPIFFE' = ExpectedSPIFFE
    /\ RequestRunMatches' = TRUE
    /\ RequestExecutionMatches' = TRUE
    /\ RequestClaimMatches' = FALSE
    /\ PolicyAllows' = TRUE
    /\ ProviderBundleValid' = TRUE
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        LeaseActive,
        ProviderConfigured,
        RunStatus,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

PreparePolicyDenied ==
    /\ ExpectedSPIFFE # Nil
    /\ RequestPeerSPIFFE' = ExpectedSPIFFE
    /\ RequestRunMatches' = TRUE
    /\ RequestExecutionMatches' = TRUE
    /\ RequestClaimMatches' = TRUE
    /\ PolicyAllows' = FALSE
    /\ ProviderBundleValid' = TRUE
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        LeaseActive,
        ProviderConfigured,
        RunStatus,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

PrepareInvalidBundle ==
    /\ ExpectedSPIFFE # Nil
    /\ RequestPeerSPIFFE' = ExpectedSPIFFE
    /\ RequestRunMatches' = TRUE
    /\ RequestExecutionMatches' = TRUE
    /\ RequestClaimMatches' = TRUE
    /\ PolicyAllows' = TRUE
    /\ ProviderBundleValid' = FALSE
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LastResolveClaimActive,
        LastResolveExecutionStatus,
        LastResolveOutcome,
        LastResolvePeerMatches,
        LastResolvePolicyAllows,
        LastResolveProviderValid,
        LastResolveRunStatus,
        LastResolveScopeBound,
        LeaseActive,
        ProviderConfigured,
        RunStatus,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

ResolveSecretsSuccess ==
    /\ AuthorizedResolveRequest
    /\ ProviderBundleValid
    /\ LastResolveOutcome' = "success"
    /\ LastResolveRunStatus' = RunStatus
    /\ LastResolveExecutionStatus' = ExecutionStatus
    /\ LastResolveClaimActive' = ActiveExecutionClaim
    /\ LastResolvePeerMatches' = PeerMatches
    /\ LastResolveScopeBound' = ScopeBound
    /\ LastResolvePolicyAllows' = PolicyAllows
    /\ LastResolveProviderValid' = ProviderBundleValid
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LeaseActive,
        PolicyAllows,
        ProviderBundleValid,
        ProviderConfigured,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        RunStatus,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

ResolveSecretsDenied ==
    /\ ServerStarted
    /\ ProviderConfigured
    /\ AuthorizerConfigured
    /\ ~(
        ScopeBound
        /\ ActiveExecutionClaim
        /\ RequestClaimMatches
        /\ PolicyAllows
        )
    /\ LastResolveOutcome' = "denied"
    /\ LastResolveRunStatus' = RunStatus
    /\ LastResolveExecutionStatus' = ExecutionStatus
    /\ LastResolveClaimActive' = ActiveExecutionClaim
    /\ LastResolvePeerMatches' = PeerMatches
    /\ LastResolveScopeBound' = ScopeBound
    /\ LastResolvePolicyAllows' = PolicyAllows
    /\ LastResolveProviderValid' = ProviderBundleValid
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LeaseActive,
        PolicyAllows,
        ProviderBundleValid,
        ProviderConfigured,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        RunStatus,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

ResolveSecretsFailed ==
    /\ ServerStarted
    /\ \/ ~ProviderConfigured
       \/ ~AuthorizerConfigured
       \/ (AuthorizedResolveRequest /\ ~ProviderBundleValid)
    /\ LastResolveOutcome' = "failed"
    /\ LastResolveRunStatus' = RunStatus
    /\ LastResolveExecutionStatus' = ExecutionStatus
    /\ LastResolveClaimActive' = ActiveExecutionClaim
    /\ LastResolvePeerMatches' = PeerMatches
    /\ LastResolveScopeBound' = ScopeBound
    /\ LastResolvePolicyAllows' = PolicyAllows
    /\ LastResolveProviderValid' = ProviderBundleValid
    /\ UNCHANGED <<
        AuthorizerConfigured,
        ClaimTokenPresent,
        ExecutionStatus,
        ExpectedSPIFFE,
        LeaseActive,
        PolicyAllows,
        ProviderBundleValid,
        ProviderConfigured,
        RequestClaimMatches,
        RequestExecutionMatches,
        RequestPeerSPIFFE,
        RequestRunMatches,
        RunStatus,
        ScopeResolverConfigured,
        ServerStarted,
        WorkerIdentityConfigured
        >>

Next ==
    \/ StartSecureServer
    \/ StartWithoutProvider
    \/ StartWithoutAuthorizer
    \/ \E id \in Identities:
        \/ ClaimExecution(id)
        \/ PrepareWrongPeer(id)
    \/ StartExecution
    \/ OrphanRun
    \/ ExpireLease
    \/ TerminalizeRun
    \/ PrepareValidRequest
    \/ PrepareWrongClaim
    \/ PreparePolicyDenied
    \/ PrepareInvalidBundle
    \/ ResolveSecretsSuccess
    \/ ResolveSecretsDenied
    \/ ResolveSecretsFailed

InvTypeOK ==
    /\ AuthorizerConfigured \in BOOLEAN
    /\ ClaimTokenPresent \in BOOLEAN
    /\ ExecutionStatus \in ExecutionStatuses
    /\ ExpectedSPIFFE \in Identities \union {Nil}
    /\ LastResolveClaimActive \in BOOLEAN
    /\ LastResolveExecutionStatus \in ExecutionStatuses
    /\ LastResolveOutcome \in ResolveOutcomes
    /\ LastResolvePeerMatches \in BOOLEAN
    /\ LastResolvePolicyAllows \in BOOLEAN
    /\ LastResolveProviderValid \in BOOLEAN
    /\ LastResolveRunStatus \in RunStatuses
    /\ LastResolveScopeBound \in BOOLEAN
    /\ LeaseActive \in BOOLEAN
    /\ PolicyAllows \in BOOLEAN
    /\ ProviderBundleValid \in BOOLEAN
    /\ ProviderConfigured \in BOOLEAN
    /\ RequestClaimMatches \in BOOLEAN
    /\ RequestExecutionMatches \in BOOLEAN
    /\ RequestPeerSPIFFE \in Identities \union {Nil}
    /\ RequestRunMatches \in BOOLEAN
    /\ RunStatus \in RunStatuses
    /\ ScopeResolverConfigured \in BOOLEAN
    /\ ServerStarted \in BOOLEAN
    /\ WorkerIdentityConfigured \in BOOLEAN

InvSuccessfulResolveWasAuthorized ==
    LastResolveOutcome = "success" =>
        /\ LastResolveRunStatus = "running"
        /\ LastResolveExecutionStatus \in {"accepted", "running"}
        /\ LastResolveClaimActive
        /\ LastResolvePeerMatches
        /\ LastResolveScopeBound
        /\ LastResolvePolicyAllows
        /\ LastResolveProviderValid

InvOrphanedRunDoesNotReceiveSecretMaterial ==
    LastResolveOutcome = "success" => LastResolveRunStatus # "orphaned"

InvMisconfiguredServerDoesNotReturnSecretMaterial ==
    LastResolveOutcome = "success" =>
        /\ ProviderConfigured
        /\ AuthorizerConfigured
        /\ ScopeResolverConfigured
        /\ WorkerIdentityConfigured

InvWrongPeerDoesNotReceiveSecretMaterial ==
    LastResolveOutcome = "success" => LastResolvePeerMatches

InvPolicyDeniedDoesNotReceiveSecretMaterial ==
    LastResolveOutcome = "success" => LastResolvePolicyAllows

InvInvalidProviderBundleDoesNotReturnMaterial ==
    LastResolveOutcome = "success" => LastResolveProviderValid

Spec ==
    /\ Init
    /\ [][Next]_Vars

====
