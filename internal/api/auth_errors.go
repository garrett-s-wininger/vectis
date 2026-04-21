package api

// Stable values for the JSON "error" field on authentication and setup responses.
// Clients should branch on these strings; HTTP status supplements but is not sufficient alone.
const (
	AuthJSONSetupRequired          = "setup_required"
	AuthJSONAuthenticationRequired = "authentication_required"
	AuthJSONAuthorizationDenied    = "authorization_denied"
	AuthJSONInvalidBootstrapToken  = "invalid_bootstrap_token"
	AuthJSONBootstrapNotConfigured = "bootstrap_not_configured"
	AuthJSONSetupAlreadyComplete   = "setup_already_complete"
	AuthJSONUsernameExists         = "username_already_exists"
	AuthJSONUnavailable            = "auth_unavailable"
	AuthJSONInternal               = "internal_error"
)
