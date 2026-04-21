package authz

// SelectAuthorizer returns the authorizer for the current instance state.
// Before initial setup completes, only [SetupPending] applies. After setup, the shipped
// policy is [AuthenticatedFull] (any authenticated principal may access non-setup actions).
//
// Future RBAC, DAC, or MAC-style engines can branch here when api.authz.engine gains
// additional supported values; see internal/config validation.
func SelectAuthorizer(setupComplete bool) Authorizer {
	if !setupComplete {
		return SetupPending{}
	}

	return AuthenticatedFull{}
}
